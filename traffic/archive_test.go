package traffic

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestTrafficArchivePreservesStreamsAndPayloadBlobs(t *testing.T) {
	for _, contentType := range []string{"websocket", "text/event-stream"} {
		t.Run(contentType, func(t *testing.T) {
			tr, err := Open(t.TempDir(), "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer tr.Close()
			const id, host = "archive-stream", "stream.example"
			insertArchiveRecoveryExchange(t, tr, id, host)
			if _, err := tr.DB().Exec(`UPDATE exchanges SET content_type=? WHERE id=?`, contentType, id); err != nil {
				t.Fatal(err)
			}
			if _, err := tr.DB().Exec(`INSERT INTO exchange_bodies(id,req_head,resp_head) VALUES(?,'GET / HTTP/1.1','HTTP/1.1 200 OK')`, id); err != nil {
				t.Fatal(err)
			}
			payload := bytes.Repeat([]byte{0, 1, 2, 255}, maxStreamInline)
			body := tr.spillAt(payload, "application/octet-stream", maxStreamInline)
			if body.hash == "" {
				t.Fatal("test payload must spill to a blob")
			}
			messages := []ArchiveStreamMessage{
				{Seq: 1, TS: 10, Direction: "c2s", Kind: 1, Size: 7, Payload: []byte("request")},
				{Seq: 2, TS: 11, Direction: "s2c", Kind: 0, Event: "update", EventID: "evt-1", Retry: 1500, Size: 5, Payload: []byte("proof"), Truncated: true},
				{Seq: 3, TS: 12, Direction: "s2c", Kind: 2, Size: len(payload), Payload: body.inline, PayloadBlob: body.hash},
				{Seq: 4, TS: 13, Direction: "meta", Size: 15, Payload: []byte("{\"closed\":true}\n")},
			}
			for _, m := range messages {
				if _, err := tr.DB().Exec(`INSERT INTO stream_messages(exchange_id,seq,ts,direction,kind,event,event_id,retry,size,payload,payload_blob,truncated)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, id, m.Seq, m.TS, m.Direction, m.Kind, m.Event, m.EventID, m.Retry, m.Size,
					m.Payload, nullIfEmpty(m.PayloadBlob), boolInt(m.Truncated)); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := tr.DB().Exec(`INSERT INTO blob_refs(hash,exchange_id) VALUES(?,?)`, body.hash, id); err != nil {
				t.Fatal(err)
			}
			archiveDir := t.TempDir()
			if count, err := tr.ExportHosts([]string{host}, archiveDir); err != nil || count != 1 {
				t.Fatalf("export count=%d err=%v", count, err)
			}
			raw, err := os.ReadFile(filepath.Join(archiveDir, "traffic.json"))
			if err != nil {
				t.Fatal(err)
			}
			var snapshot ArchiveSnapshot
			if err := json.Unmarshal(raw, &snapshot); err != nil || snapshot.Version != 2 {
				t.Fatalf("archive version=%d err=%v", snapshot.Version, err)
			}
			blobPath, err := tr.blobPath(body.hash)
			if err != nil {
				t.Fatal(err)
			}
			stage, err := tr.StageDeleteHostsExactForArchive([]string{host}, 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			if err := stage.Commit(); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(blobPath); !os.IsNotExist(err) {
				t.Fatalf("blob should be collected before restore, got %v", err)
			}
			if count, err := tr.ImportArchive(archiveDir); err != nil || count != 1 {
				t.Fatalf("import count=%d err=%v", count, err)
			}
			got, err := tr.archiveStreamMessages(id)
			if err != nil || !reflect.DeepEqual(got, messages) {
				t.Fatalf("restored messages=%+v want=%+v err=%v", got, messages, err)
			}
			data, total, err := tr.BlobRange(body.hash, 0, int64(len(payload)))
			if err != nil || total != int64(len(payload)) || !bytes.Equal(data, payload) {
				t.Fatalf("payload did not survive restore: total=%d err=%v", total, err)
			}
			if err := tr.gcBlobs(); err != nil {
				t.Fatal(err)
			}
			if _, _, err := tr.BlobRange(body.hash, 0, 1); err != nil {
				t.Fatalf("restored blob reference was not registered: %v", err)
			}
			if _, err := tr.DB().Exec(`UPDATE stream_messages SET payload=? WHERE exchange_id=? AND seq=1`, []byte("current"), id); err != nil {
				t.Fatal(err)
			}
			if count, err := tr.ImportArchive(archiveDir); err != nil || count != 0 {
				t.Fatalf("repeat import count=%d err=%v", count, err)
			}
			got, err = tr.archiveStreamMessages(id)
			messages[0].Payload = []byte("current")
			if err != nil || !reflect.DeepEqual(got, messages) {
				t.Fatalf("repeat restore changed current stream: %+v err=%v", got, err)
			}
		})
	}
}

func TestTrafficArchiveImportsVersion1(t *testing.T) {
	tr, _ := openTraffic(t)
	archiveDir := t.TempDir()
	const legacy = `{"version":1,"exchanges":[{"id":"legacy-archive","host":"legacy.example","req_head":"GET / HTTP/1.1","resp_head":"HTTP/1.1 200 OK"}],"blobs":[]}`
	if err := os.WriteFile(filepath.Join(archiveDir, "traffic.json"), []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	if count, err := tr.ImportArchive(archiveDir); err != nil || count != 1 {
		t.Fatalf("legacy import count=%d err=%v", count, err)
	}
	if req, resp, err := tr.Get("legacy-archive"); err != nil || !strings.Contains(req, "GET /") || !strings.Contains(resp, "200 OK") {
		t.Fatalf("legacy exchange req=%q resp=%q err=%v", req, resp, err)
	}
}

func TestTrafficArchiveRoundTripAndCurrentRowWins(t *testing.T) {
	trafficDir := t.TempDir()
	tr, err := Open(trafficDir, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	const (
		id   = "archive-exchange-1"
		host = "archive.example"
	)
	requestHead := "POST /v1/test HTTP/1.1\nHost: archive.example"
	responseHead := "HTTP/1.1 200 OK\nContent-Type: application/json"
	if _, err := tr.DB().Exec(`INSERT INTO exchanges(id,ts,host,method,url_template,url,status,content_type,req_len,resp_len,path)
VALUES(?,?,?,?,?,?,?,?,?,?,?)`, id, 1, host, "POST", "/v1/test", "https://archive.example/v1/test", 200,
		"application/json", 7, 11, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.DB().Exec(`INSERT INTO exchange_bodies(id,req_head,req_body,resp_head,resp_body)
VALUES(?,?,?,?,?)`, id, requestHead, []byte("payload"), responseHead, []byte(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}

	archiveDir := filepath.Join(t.TempDir(), "traffic")
	count, err := tr.ExportHosts([]string{host, host}, archiveDir)
	if err != nil || count != 1 {
		t.Fatalf("ExportHosts count=%d err=%v", count, err)
	}
	if _, err := os.Stat(filepath.Join(archiveDir, "traffic.json")); err != nil {
		t.Fatal(err)
	}
	stage, err := tr.StageDeleteHostsExact([]string{host})
	if err != nil {
		t.Fatal(err)
	}
	if stage.Deleted() != 1 {
		t.Fatalf("staged deleted=%d, want 1", stage.Deleted())
	}
	if err := stage.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := tr.Get(id); err == nil {
		t.Fatal("deleted exchange remained readable")
	}
	imported, err := tr.ImportArchive(archiveDir)
	if err != nil || imported != 1 {
		t.Fatalf("ImportArchive imported=%d err=%v", imported, err)
	}
	req, resp, err := tr.Get(id)
	if err != nil || !strings.Contains(req, "payload") || !strings.Contains(resp, `{"ok":true}`) {
		t.Fatalf("restored exchange req=%q resp=%q err=%v", req, resp, err)
	}
	if _, err := tr.DB().Exec(`UPDATE exchange_bodies SET resp_body=? WHERE id=?`, []byte(`{"current":true}`), id); err != nil {
		t.Fatal(err)
	}
	imported, err = tr.ImportArchive(archiveDir)
	if err != nil || imported != 0 {
		t.Fatalf("idempotent import imported=%d err=%v", imported, err)
	}
	_, resp, err = tr.Get(id)
	if err != nil || !strings.Contains(resp, `{"current":true}`) || strings.Contains(resp, `{"ok":true}`) {
		t.Fatalf("archive overwrote current exchange resp=%q err=%v", resp, err)
	}
}

func TestRecoverArchiveHostDeleteStageRollsBackBeforePostgresCommit(t *testing.T) {
	trafficDir := t.TempDir()
	tr, err := Open(trafficDir, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	const host = "rollback-archive.example"
	insertArchiveRecoveryExchange(t, tr, "rollback-exchange", host)
	legacyDir := filepath.Join(trafficDir, sanitize(host))
	if err := os.MkdirAll(legacyDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacyDir, "request.http"), []byte("request"), 0o600); err != nil {
		t.Fatal(err)
	}
	stage, err := tr.StageDeleteHostsExactForArchive([]string{host}, 17, 42)
	if err != nil {
		t.Fatal(err)
	}
	simulateTrafficStageProcessExit(t, stage)
	if err := tr.RecoverHostDeleteStages(func(id, taskID int64) (bool, error) {
		if id != 17 || taskID != 42 {
			t.Fatalf("archive id=%d task=%d, want 17/42", id, taskID)
		}
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(legacyDir, "request.http")); err != nil {
		t.Fatalf("legacy traffic tree not restored: %v", err)
	}
	var count int
	if err := tr.DB().QueryRow(`SELECT count(*) FROM exchanges WHERE host=?`, host).Scan(&count); err != nil || count != 1 {
		t.Fatalf("exchange count=%d err=%v, want 1", count, err)
	}
}

func TestRecoverArchiveHostDeleteStageCompletesAfterPostgresCommit(t *testing.T) {
	trafficDir := t.TempDir()
	tr, err := Open(trafficDir, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	const host = "committed-archive.example"
	insertArchiveRecoveryExchange(t, tr, "committed-exchange", host)
	legacyDir := filepath.Join(trafficDir, sanitize(host))
	if err := os.MkdirAll(legacyDir, 0o755); err != nil {
		t.Fatal(err)
	}
	stage, err := tr.StageDeleteHostsExactForArchive([]string{host}, 18, 43)
	if err != nil {
		t.Fatal(err)
	}
	simulateTrafficStageProcessExit(t, stage)
	if err := tr.RecoverHostDeleteStages(func(id, taskID int64) (bool, error) { return id == 18 && taskID == 43, nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(legacyDir); !os.IsNotExist(err) {
		t.Fatalf("committed legacy traffic tree remains: %v", err)
	}
	var count int
	if err := tr.DB().QueryRow(`SELECT count(*) FROM exchanges WHERE host=?`, host).Scan(&count); err != nil || count != 0 {
		t.Fatalf("exchange count=%d err=%v, want 0", count, err)
	}
}

func insertArchiveRecoveryExchange(t *testing.T, tr *Traffic, id, host string) {
	t.Helper()
	if _, err := tr.DB().Exec(`INSERT INTO exchanges(id,ts,host,method,url_template,url,status,content_type,req_len,resp_len,path)
VALUES(?,?,?,?,?,?,?,?,?,?,?)`, id, 1, host, "GET", "/", "https://"+host+"/", 200, "text/plain", 0, 0, ""); err != nil {
		t.Fatal(err)
	}
}

func simulateTrafficStageProcessExit(t *testing.T, stage *HostDeleteStage) {
	t.Helper()
	if err := stage.tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	stage.done = true
	stage.traffic.wmu.Unlock()
}
