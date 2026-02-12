package main

import (
	"bufio"
	"bytes"
	"io"
	"testing"
)

func TestWriteCommand(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	rw := bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(nil)), bufio.NewWriter(buf))

	if err := writeCommand(rw, "PING", "foo", "bar"); err != nil {
		t.Fatalf("writeCommand error: %v", err)
	}
	got := buf.String()
	want := "*3\r\n$4\r\nPING\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
	if got != want {
		t.Fatalf("unexpected redis command. got %q want %q", got, want)
	}
}

func TestReadBRPOPMultiBulk(t *testing.T) {
	payload := "*2\r\n$5\r\nqueue\r\n$11\r\n{\"foo\":\"bar\"}\r\n"
	rw := bufio.NewReadWriter(bufio.NewReader(bytes.NewBufferString(payload)), bufio.NewWriter(io.Discard))

	key, msg, err := readBRPOP(rw)
	if err != nil {
		t.Fatalf("readBRPOP error: %v", err)
	}
	if key != "queue" {
		t.Fatalf("expected key \"queue\", got %q", key)
	}
	if msg != "{\"foo\":\"bar\"}" {
		t.Fatalf("unexpected payload: %q", msg)
	}
}

func TestReadBRPOPTimeout(t *testing.T) {
	for _, payload := range []string{
		"$-1\r\n",  // bulk nil
		"*-1\r\n",  // array nil (Redis BRPOP timeout)
		"*0\r\n",   // empty array (defensive)
	} {
		rw := bufio.NewReadWriter(bufio.NewReader(bytes.NewBufferString(payload)), bufio.NewWriter(io.Discard))

		key, msg, err := readBRPOP(rw)
		if err != nil {
			t.Fatalf("readBRPOP error for %q: %v", payload, err)
		}
		if key != "" || msg != "" {
			t.Fatalf("expected empty timeout result for %q, got %q %q", payload, key, msg)
		}
	}
}
