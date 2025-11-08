package main

import (
	"encoding/json"
	"testing"
)

func TestParseInt64Numeric(t *testing.T) {
	raw := json.RawMessage("12345")
	v, err := parseInt64(raw)
	if err != nil {
		t.Fatalf("parseInt64 error: %v", err)
	}
	if v != 12345 {
		t.Fatalf("expected 12345, got %d", v)
	}
}

func TestParseInt64String(t *testing.T) {
	raw := json.RawMessage(`"67890"`)
	v, err := parseInt64(raw)
	if err != nil {
		t.Fatalf("parseInt64 error: %v", err)
	}
	if v != 67890 {
		t.Fatalf("expected 67890, got %d", v)
	}
}

func TestParseInt64Invalid(t *testing.T) {
	raw := json.RawMessage(`{"oops":1}`)
	if _, err := parseInt64(raw); err == nil {
		t.Fatalf("expected error for invalid payload")
	}
}
