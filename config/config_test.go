package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func clearPostgresComponents(t *testing.T) {
	t.Helper()
	for _, key := range []string{"ARTEX_PG_HOST", "ARTEX_PG_PORT", "ARTEX_PG_USER", "ARTEX_PG_PASSWORD", "ARTEX_PG_DBNAME", "ARTEX_PG_SSLMODE"} {
		t.Setenv(key, "")
	}
}

func TestPostgresDSNPrecedence(t *testing.T) {
	clearPostgresComponents(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	os.WriteFile(cfgPath, []byte(`{"database":{"host":"10.1.2.3","port":6000,"user":"u","password":"p","dbname":"d","sslmode":"require"}}`), 0o644)
	t.Setenv("ARTEX_CONFIG", cfgPath)

	// no env DSN → built from config file fields
	t.Setenv("ARTEX_PG_DSN", "")
	got, _, err := PostgresDSN()
	want := "postgres://u:p@10.1.2.3:6000/d?sslmode=require"
	if err != nil || got != want {
		t.Fatalf("from file: got %q err %v want %q", got, err, want)
	}

	// env wins over config file
	t.Setenv("ARTEX_PG_DSN", "postgres://envwins/x")
	if got, _, err := PostgresDSN(); err != nil || got != "postgres://envwins/x" {
		t.Fatalf("env should win, got %q err %v", got, err)
	}

	// no env, no file → error (no built-in default)
	t.Setenv("ARTEX_PG_DSN", "")
	t.Setenv("ARTEX_CONFIG", filepath.Join(dir, "nope.json"))
	if got, _, err := PostgresDSN(); err == nil {
		t.Fatalf("missing config should error, got %q", got)
	}

	// full dsn in config file is used verbatim
	os.WriteFile(cfgPath, []byte(`{"database":{"dsn":"postgres://full/dsn"}}`), 0o644)
	t.Setenv("ARTEX_CONFIG", cfgPath)
	if got, _, err := PostgresDSN(); err != nil || got != "postgres://full/dsn" {
		t.Fatalf("file dsn verbatim, got %q err %v", got, err)
	}
}

// A config file that exists but is not valid JSON must fail loudly, not run with
// defaults (or report "no database config") as if the file were absent.
func TestMalformedConfigIsAnError(t *testing.T) {
	clearPostgresComponents(t)
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	// What install.sh used to write for a password containing a double quote.
	os.WriteFile(cfgPath, []byte(`{"database":{"host":"h","password":"a"b","dbname":"d"},"skill_dir":"/s"}`), 0o644)
	t.Setenv("ARTEX_CONFIG", cfgPath)
	t.Setenv("ARTEX_PG_DSN", "")
	t.Setenv("ARTEX_SKILL_DIR", "")

	if _, _, err := PostgresDSN(); err == nil || !strings.Contains(err.Error(), "parse config file") {
		t.Fatalf("PostgresDSN: want a parse error, got %v", err)
	}
	if d, err := SkillDir(); err == nil {
		t.Fatalf("SkillDir: want a parse error, got dir %q", d)
	}
	if _, err := Load(); err == nil {
		t.Fatal("Load: want a parse error")
	}
}

func TestPostgresEnvComponentsPreserveCredentials(t *testing.T) {
	clearPostgresComponents(t)
	t.Setenv("ARTEX_PG_DSN", "")
	t.Setenv("ARTEX_CONFIG", filepath.Join(t.TempDir(), "missing.json"))
	t.Setenv("ARTEX_PG_HOST", "postgres")
	t.Setenv("ARTEX_PG_PORT", "6000")
	t.Setenv("ARTEX_PG_USER", "user:@/%#?")
	t.Setenv("ARTEX_PG_DBNAME", "db/#?%")
	t.Setenv("ARTEX_PG_SSLMODE", "disable")
	for _, password := range []string{"plain", "pa#ss", "pa/ss", "pa?ss", "pa%ss", "pa%2Fss", "pa:ss@host", " ' $ &+\\\n\t "} {
		t.Run(password, func(t *testing.T) {
			t.Setenv("ARTEX_PG_PASSWORD", password)
			dsn, source, err := PostgresDSN()
			if err != nil {
				t.Fatal(err)
			}
			cfg, err := pgx.ParseConfig(dsn)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Password != password || cfg.User != "user:@/%#?" || cfg.Database != "db/#?%" || cfg.Host != "postgres" || cfg.Port != 6000 {
				t.Fatalf("connection components changed during DSN encoding: %+v", cfg.Config)
			}
			if source != "environment variables ARTEX_PG_*" {
				t.Fatalf("unexpected source: %s", source)
			}
		})
	}
}

func TestPostgresEnvComponentPrecedence(t *testing.T) {
	clearPostgresComponents(t)
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"database":{"dsn":"postgres://fileuser:filepass@filehost/filedb"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ARTEX_CONFIG", path)
	t.Setenv("ARTEX_PG_DSN", "")
	t.Setenv("ARTEX_PG_HOST", "envhost")
	dsn, _, err := PostgresDSN()
	if err != nil || dsn != "postgres://envhost:5432/?sslmode=disable" {
		t.Fatalf("standalone components must override file without merging: %q %v", dsn, err)
	}
	t.Setenv("ARTEX_PG_PORT", "invalid")
	t.Setenv("ARTEX_PG_DSN", "postgres://explicit/db")
	if dsn, _, err := PostgresDSN(); err != nil || dsn != "postgres://explicit/db" {
		t.Fatalf("explicit DSN must take precedence over components: %q %v", dsn, err)
	}
	t.Setenv("ARTEX_PG_DSN", "")
	if _, _, err := PostgresDSN(); err == nil {
		t.Fatal("invalid component configuration must fail instead of using file DSN")
	}
	t.Setenv("ARTEX_PG_HOST", "")
	t.Setenv("ARTEX_PG_PORT", "")
	if dsn, _, err := PostgresDSN(); err != nil || dsn != "postgres://fileuser:filepass@filehost/filedb" {
		t.Fatalf("empty component environment must preserve file fallback: %q %v", dsn, err)
	}
}

func TestPostgresEnvComponentValidation(t *testing.T) {
	clearPostgresComponents(t)
	t.Setenv("ARTEX_PG_DSN", "")
	t.Setenv("ARTEX_CONFIG", filepath.Join(t.TempDir(), "missing.json"))
	t.Setenv("ARTEX_PG_HOST", "::1")
	for _, port := range []string{"0", "-1", "65536", "not-a-port"} {
		t.Setenv("ARTEX_PG_PORT", port)
		if _, _, err := PostgresDSN(); err == nil || !strings.Contains(err.Error(), "ARTEX_PG_PORT") {
			t.Fatalf("port %q should fail validation: %v", port, err)
		}
	}
	t.Setenv("ARTEX_PG_PORT", "")
	if dsn, _, err := PostgresDSN(); err != nil || dsn != "postgres://[::1]:5432/?sslmode=disable" {
		t.Fatalf("IPv6/default port: %q %v", dsn, err)
	}
	t.Setenv("ARTEX_PG_HOST", "")
	t.Setenv("ARTEX_PG_PASSWORD", "password-only")
	if _, _, err := PostgresDSN(); err == nil {
		t.Fatal("password alone must not select an implicit database endpoint")
	}
}
