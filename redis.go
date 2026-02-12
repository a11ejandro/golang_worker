package main

import (
    "bufio"
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "strconv"
)

type sidekiqJob struct {
    Class string            `json:"class"`
    Args  []json.RawMessage `json:"args"`
    Queue string            `json:"queue"`
}

func writeCommand(w *bufio.ReadWriter, cmd string, args ...string) error {
    if _, err := fmt.Fprintf(w, "*%d\r\n", 1+len(args)); err != nil {
        return err
    }
    if err := writeBulk(w, cmd); err != nil {
        return err
    }
    for _, a := range args {
        if err := writeBulk(w, a); err != nil {
            return err
        }
    }
    return w.Flush()
}

func writeBulk(w *bufio.ReadWriter, s string) error {
    if _, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(s), s); err != nil {
        return err
    }
    return nil
}

var ioEOF = errors.New("eof")

func readLine(r *bufio.Reader) (string, error) {
    b, err := r.ReadBytes('\n')
    if err != nil {
        return "", ioEOF
    }
    if len(b) >= 2 && b[len(b)-2] == '\r' {
        b = b[:len(b)-2]
    }
    return string(b), nil
}

func readOK(rw *bufio.ReadWriter) error {
    line, err := readLine(rw.Reader)
    if err != nil {
        return err
    }
    if len(line) > 0 && line[0] == '+' {
        return nil
    }
    return fmt.Errorf("redis not OK: %s", line)
}

func readBRPOP(rw *bufio.ReadWriter) (key string, payload string, err error) {
    line, err2 := readLine(rw.Reader)
    if err2 != nil {
        return "", "", err2
    }
    if len(line) == 0 {
        return "", "", fmt.Errorf("empty reply")
    }
    switch line[0] {
    case '*':
        n, err := parseArrayLen(line)
        if err != nil {
            return "", "", err
        }
        // Redis returns *-1 for nil (timeout) on BRPOP.
        if n <= 0 {
            return "", "", nil
        }
        if n != 2 {
            // Defensive: consume elements to keep stream aligned.
            for i := 0; i < n; i++ {
                if _, err := readBulkString(rw.Reader); err != nil {
                    return "", "", err
                }
            }
            return "", "", fmt.Errorf("unexpected BRPOP array length: %d", n)
        }
        key, err = readBulkString(rw.Reader)
        if err != nil {
            return "", "", err
        }
        payload, err = readBulkString(rw.Reader)
        if err != nil {
            return "", "", err
        }
        return key, payload, nil
    case '$':
        if line == "$-1" {
            return "", "", nil
        }
        l, _ := strconv.Atoi(line[1:])
        buf := make([]byte, l)
        if _, err := rw.Reader.Read(buf); err != nil {
            return "", "", ioEOF
        }
        rw.Reader.ReadByte()
        rw.Reader.ReadByte()
        return "", string(buf), nil
    case '-':
        return "", "", fmt.Errorf("redis error: %s", line)
    default:
        return "", "", fmt.Errorf("unexpected reply: %s", line)
    }
}

func parseArrayLen(line string) (int, error) {
    if len(line) < 2 || line[0] != '*' {
        return 0, fmt.Errorf("invalid array reply: %q", line)
    }
    n, err := strconv.Atoi(line[1:])
    if err != nil {
        return 0, fmt.Errorf("invalid array length: %q", line)
    }
    return n, nil
}

func readBulkString(r *bufio.Reader) (string, error) {
    header, err := readLine(r)
    if err != nil {
        return "", err
    }
    if len(header) == 0 || header[0] != '$' {
        return "", fmt.Errorf("expected bulk string, got %q", header)
    }
    if header == "$-1" {
        return "", nil
    }
    l, err := strconv.Atoi(header[1:])
    if err != nil {
        return "", fmt.Errorf("invalid bulk length: %q", header)
    }
    buf := make([]byte, l)
    if _, err := io.ReadFull(r, buf); err != nil {
        return "", ioEOF
    }
    // consume CRLF
    if _, err := r.ReadByte(); err != nil {
        return "", ioEOF
    }
    if _, err := r.ReadByte(); err != nil {
        return "", ioEOF
    }
    return string(buf), nil
}
