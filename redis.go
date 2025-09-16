package main

import (
    "bufio"
    "encoding/json"
    "errors"
    "fmt"
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
        _, _ = readLine(rw.Reader)
        key, _ = readLine(rw.Reader)
        _, _ = readLine(rw.Reader)
        payload, _ = readLine(rw.Reader)
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
