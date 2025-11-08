package main

import "testing"

func TestBuildDSNFromEnv(t *testing.T) {
	t.Setenv("POSTGRES_DB", "bench")
	t.Setenv("POSTGRES_HOST", "db.example")
	t.Setenv("POSTGRES_PORT", "15432")
	t.Setenv("POSTGRES_USER", "bench_user")
	t.Setenv("POSTGRES_PASSWORD", "s3cr3t")

	got, err := buildDSNFromEnv()
	if err != nil {
		t.Fatalf("buildDSNFromEnv returned error: %v", err)
	}
	want := "host=db.example port=15432 user=bench_user password=s3cr3t dbname=bench sslmode=disable"
	if got != want {
		t.Fatalf("unexpected DSN. got %q want %q", got, want)
	}
}

func TestBuildDSNFromEnvUsesDatabaseURL(t *testing.T) {
	t.Setenv("POSTGRES_DB", "")
	t.Setenv("DATABASE_URL", "postgres://user:secret@localhost/testdb")

	got, err := buildDSNFromEnv()
	if err != nil {
		t.Fatalf("buildDSNFromEnv returned error: %v", err)
	}
	if got != "postgres://user:secret@localhost/testdb" {
		t.Fatalf("unexpected DSN: %q", got)
	}
}

func TestBuildDSNFromEnvMissingConfig(t *testing.T) {
	t.Setenv("POSTGRES_DB", "")
	t.Setenv("DATABASE_URL", "")

	if _, err := buildDSNFromEnv(); err == nil {
		t.Fatalf("expected error when config missing")
	}
}

func TestWindowLimitOffset(t *testing.T) {
	limit, offset := windowLimitOffset(3, 5)
	if limit != 5 || offset != 10 {
		t.Fatalf("unexpected limit/offset: %d %d", limit, offset)
	}
	limit, offset = windowLimitOffset(-1, 0)
	if limit != 1 || offset != 0 {
		t.Fatalf("expected defaults, got %d %d", limit, offset)
	}
}

func TestNormalizePositiveInt(t *testing.T) {
	if got := normalizePositiveInt(7, 1); got != 7 {
		t.Fatalf("expected 7, got %d", got)
	}
	if got := normalizePositiveInt(0, 2); got != 2 {
		t.Fatalf("expected fallback 2, got %d", got)
	}
}
