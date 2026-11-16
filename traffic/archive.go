package traffic

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// ArchiveSnapshot is the portable traffic subset embedded in one task archive.
// Large content-addressed bodies are copied alongside this manifest rather than
// base64-encoded, preserving deduplication and allowing the tar writer to stream.
type ArchiveSnapshot struct {
	Version   int               `json:"version"`
	Exchanges []ArchiveExchange `json:"exchanges"`
	Blobs     []string          `json:"blobs"`
}

type ArchiveExchange struct {
	ID          string                 `json:"id"`
	TS          int64                  `json:"ts"`
	Host        string                 `json:"host"`
	Method      string                 `json:"method"`
	URLTemplate string                 `json:"url_template"`
	URL         string                 `json:"url"`
	Status      int                    `json:"status"`
	ContentType string                 `json:"content_type"`
	ReqLen      int                    `json:"req_len"`
	RespLen     int                    `json:"resp_len"`
	ReqHead     string                 `json:"req_head"`
	ReqBody     []byte                 `json:"req_body,omitempty"`
	ReqBlob     string                 `json:"req_blob,omitempty"`
	RespHead    string                 `json:"resp_head"`
	RespBody    []byte                 `json:"resp_body,omitempty"`
	RespBlob    string                 `json:"resp_blob,omitempty"`
	Messages    []ArchiveStreamMessage `json:"messages,omitempty"`
}

// ArchiveStreamMessage preserves the wire payload and stream metadata, including
// payloads spilled to the content-addressed blob store.
type ArchiveStreamMessage struct {
	Seq         int64  `json:"seq"`
	TS          int64  `json:"ts"`
	Direction   string `json:"direction"`
	Kind        int    `json:"kind"`
	Event       string `json:"event,omitempty"`
	EventID     string `json:"event_id,omitempty"`
	Retry       int    `json:"retry,omitempty"`
	Size        int    `json:"size"`
	Payload     []byte `json:"payload,omitempty"`
	PayloadBlob string `json:"payload_blob,omitempty"`
	Truncated   bool   `json:"truncated,omitempty"`
}

// ExportHosts writes an exact-host snapshot to dir. It holds the traffic writer
// lock while reading SQLite and blobs, so each body and its index row come from
// one consistent point in time.
func (t *Traffic) ExportHosts(hosts []string, dir string) (int64, error) {
	if t == nil || len(hosts) == 0 {
		return 0, nil
	}
	if err := os.MkdirAll(filepath.Join(dir, "blobs"), 0o700); err != nil {
		return 0, err
	}
	t.wmu.Lock()
	defer t.wmu.Unlock()
	unique := uniqueArchiveHosts(hosts)
	placeholders := make([]string, len(unique))
	args := make([]any, len(unique))
	for i, host := range unique {
		placeholders[i] = "?"
		args[i] = host
	}
	rows, err := t.db.Query(`SELECT id,ts,host,method,url_template,url,status,content_type,req_len,resp_len,path
FROM exchanges WHERE host IN (`+strings.Join(placeholders, ",")+`) ORDER BY ts,id`, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	snapshot := ArchiveSnapshot{Version: 2}
	blobs := map[string]struct{}{}
	for rows.Next() {
		var item ArchiveExchange
		var legacyPath string
		if err := rows.Scan(&item.ID, &item.TS, &item.Host, &item.Method, &item.URLTemplate,
			&item.URL, &item.Status, &item.ContentType, &item.ReqLen, &item.RespLen, &legacyPath); err != nil {
			return 0, err
		}
		var reqBlob, respBlob sql.NullString
		err := t.db.QueryRow(`SELECT req_head,req_body,req_blob,resp_head,resp_body,resp_blob
FROM exchange_bodies WHERE id=?`, item.ID).Scan(&item.ReqHead, &item.ReqBody, &reqBlob, &item.RespHead, &item.RespBody, &respBlob)
		if errors.Is(err, sql.ErrNoRows) && strings.TrimSpace(legacyPath) != "" {
			req, readErr := os.ReadFile(filepath.Join(t.dir, legacyPath, "request.http"))
			if readErr != nil && !os.IsNotExist(readErr) {
				return 0, readErr
			}
			resp, readErr := os.ReadFile(filepath.Join(t.dir, legacyPath, "response.http"))
			if readErr != nil && !os.IsNotExist(readErr) {
				return 0, readErr
			}
			item.ReqHead, item.RespHead = string(req), string(resp)
		} else if err != nil {
			return 0, err
		}
		item.ReqBlob, item.RespBlob = reqBlob.String, respBlob.String
		for _, hash := range []string{item.ReqBlob, item.RespBlob} {
			if hash != "" {
				blobs[hash] = struct{}{}
			}
		}
		item.Messages, err = t.archiveStreamMessages(item.ID)
		if err != nil {
			return 0, err
		}
		for _, message := range item.Messages {
			if message.PayloadBlob != "" {
				blobs[message.PayloadBlob] = struct{}{}
			}
		}
		snapshot.Exchanges = append(snapshot.Exchanges, item)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for hash := range blobs {
		source, err := t.blobPath(hash)
		if err != nil {
			return 0, err
		}
		data, err := os.ReadFile(source)
		if err != nil {
			return 0, err
		}
		if err := os.WriteFile(filepath.Join(dir, "blobs", hash+".bin"), data, 0o600); err != nil {
			return 0, err
		}
		snapshot.Blobs = append(snapshot.Blobs, hash)
	}
	sort.Strings(snapshot.Blobs)
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return 0, err
	}
	if err := os.WriteFile(filepath.Join(dir, "traffic.json"), raw, 0o600); err != nil {
		return 0, err
	}
	return int64(len(snapshot.Exchanges)), nil
}

func (t *Traffic) archiveStreamMessages(exchangeID string) ([]ArchiveStreamMessage, error) {
	rows, err := t.db.Query(`SELECT seq,ts,direction,kind,event,event_id,retry,size,payload,payload_blob,truncated
FROM stream_messages WHERE exchange_id=? ORDER BY seq`, exchangeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var messages []ArchiveStreamMessage
	for rows.Next() {
		var message ArchiveStreamMessage
		var blob sql.NullString
		var truncated int
		if err := rows.Scan(&message.Seq, &message.TS, &message.Direction, &message.Kind,
			&message.Event, &message.EventID, &message.Retry, &message.Size, &message.Payload, &blob, &truncated); err != nil {
			return nil, err
		}
		message.PayloadBlob, message.Truncated = blob.String, truncated != 0
		messages = append(messages, message)
	}
	return messages, rows.Err()
}

// ImportArchive imports only missing exchange IDs. Current hot rows always win,
// and repeated restore attempts are safe after a partial external failure.
func (t *Traffic) ImportArchive(dir string) (int64, error) {
	if t == nil {
		return 0, nil
	}
	raw, err := os.ReadFile(filepath.Join(dir, "traffic.json"))
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var snapshot ArchiveSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return 0, err
	}
	if snapshot.Version != 1 && snapshot.Version != 2 {
		return 0, fmt.Errorf("unsupported traffic archive version %d", snapshot.Version)
	}
	t.wmu.Lock()
	defer t.wmu.Unlock()
	for _, hash := range snapshot.Blobs {
		if !blobHashRe.MatchString(hash) {
			return 0, fmt.Errorf("invalid archived traffic blob %q", hash)
		}
		data, err := os.ReadFile(filepath.Join(dir, "blobs", hash+".bin"))
		if err != nil {
			return 0, err
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != hash {
			return 0, fmt.Errorf("traffic blob checksum mismatch: %s", hash)
		}
		destination := filepath.Join(t.dir, "_blobs", "sha256", hash[:2], hash+".bin")
		if _, err := os.Stat(destination); os.IsNotExist(err) {
			if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
				return 0, err
			}
			if err := os.WriteFile(destination, data, 0o644); err != nil {
				return 0, err
			}
		}
	}
	tx, err := t.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck
	var imported int64
	for _, item := range snapshot.Exchanges {
		res, err := tx.Exec(`INSERT OR IGNORE INTO exchanges(id,ts,host,method,url_template,url,status,content_type,req_len,resp_len,path)
VALUES(?,?,?,?,?,?,?,?,?,?,'')`, item.ID, item.TS, item.Host, item.Method, item.URLTemplate,
			item.URL, item.Status, item.ContentType, item.ReqLen, item.RespLen)
		if err != nil {
			return imported, err
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			continue
		}
		if _, err := tx.Exec(`INSERT INTO exchange_bodies(id,req_head,req_body,req_blob,resp_head,resp_body,resp_blob)
VALUES(?,?,?,?,?,?,?)`, item.ID, item.ReqHead, item.ReqBody, nullIfEmpty(item.ReqBlob),
			item.RespHead, item.RespBody, nullIfEmpty(item.RespBlob)); err != nil {
			return imported, err
		}
		for _, hash := range []string{item.ReqBlob, item.RespBlob} {
			if hash != "" {
				if _, err := tx.Exec(`INSERT OR IGNORE INTO blob_refs(hash,exchange_id) VALUES(?,?)`, hash, item.ID); err != nil {
					return imported, err
				}
			}
		}
		for _, message := range item.Messages {
			if _, err := tx.Exec(`INSERT INTO stream_messages(exchange_id,seq,ts,direction,kind,event,event_id,retry,size,payload,payload_blob,truncated)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, item.ID, message.Seq, message.TS, message.Direction, message.Kind,
				message.Event, message.EventID, message.Retry, message.Size, message.Payload,
				nullIfEmpty(message.PayloadBlob), boolInt(message.Truncated)); err != nil {
				return imported, err
			}
			if message.PayloadBlob != "" {
				if _, err := tx.Exec(`INSERT OR IGNORE INTO blob_refs(hash,exchange_id) VALUES(?,?)`, message.PayloadBlob, item.ID); err != nil {
					return imported, err
				}
			}
		}
		if t.fts {
			var rowID int64
			if err := tx.QueryRow(`SELECT rowid FROM exchanges WHERE id=?`, item.ID).Scan(&rowID); err != nil {
				return imported, err
			}
			content := strings.Join([]string{item.URL, item.ReqHead, string(item.ReqBody), item.RespHead, string(item.RespBody)}, "\n")
			if _, err := tx.Exec(`INSERT INTO ex_fts(rowid,content) VALUES(?,?)`, rowID, content); err != nil {
				return imported, err
			}
		}
		imported++
	}
	if err := tx.Commit(); err != nil {
		return imported, err
	}
	return imported, nil
}

func uniqueArchiveHosts(hosts []string) []string {
	seen := make(map[string]struct{}, len(hosts))
	out := make([]string, 0, len(hosts))
	for _, host := range hosts {
		host = strings.ToLower(strings.TrimSpace(host))
		if host == "" {
			continue
		}
		if _, exists := seen[host]; exists {
			continue
		}
		seen[host] = struct{}{}
		out = append(out, host)
	}
	sort.Strings(out)
	return out
}
