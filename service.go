package main

import (
    "bufio"
    "database/sql"
    "encoding/json"
    "fmt"
    "log"
    "net"
    "net/url"
    "os"
    "runtime"
    "strconv"
    "strings"
    "time"
)

func processTestRun(db *sql.DB, testRunID int64) error {
    if !existsTestRun(db, testRunID) {
        return fmt.Errorf("test_runs id %d not found", testRunID)
    }
    values, err := fetchSamples(db)
    if err != nil {
        return fmt.Errorf("fetch samples failed: %w", err)
    }
    var before, after runtime.MemStats
    runtime.ReadMemStats(&before)
    start := time.Now()
    stats := calculateStatistics(values)
    elapsed := time.Since(start).Seconds()
    runtime.ReadMemStats(&after)
    memBytes := float64(after.TotalAlloc - before.TotalAlloc)
    if err := insertTestResult(db, testRunID, stats, elapsed, memBytes); err != nil {
        return fmt.Errorf("insert test_result failed: %w", err)
    }
    log.Printf("processed test_run=%d duration=%.6fs memory_bytes=%.0f\n", testRunID, elapsed, memBytes)
    return nil
}

func runService(db *sql.DB) {
    redisURL := os.Getenv("REDIS_URL")
    if redisURL == "" {
        redisURL = "redis://localhost:6379/0"
    }
    u, err := url.Parse(redisURL)
    if err != nil {
        log.Fatalf("invalid REDIS_URL: %v", err)
    }
    host := u.Host
    if host == "" && u.Scheme == "unix" {
        log.Fatal("unix sockets not supported by this worker")
    }
    password, _ := u.User.Password()
    dbIndex := 0
    if parts := strings.TrimPrefix(u.Path, "/"); parts != "" {
        if i, err := strconv.Atoi(parts); err == nil {
            dbIndex = i
        }
    }
    qname := os.Getenv("WORKER_QUEUE")
    if qname == "" { qname = "default" }
    queue := "queue:" + qname

    for {
        conn, err := net.DialTimeout("tcp", host, 5*time.Second)
        if err != nil {
            log.Printf("redis connect failed: %v; retrying in 2s", err)
            time.Sleep(2 * time.Second)
            continue
        }
        rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

        if password != "" {
            if err := writeCommand(rw, "AUTH", password); err != nil || readOK(rw) != nil {
                log.Printf("redis auth failed: %v", err)
                conn.Close()
                time.Sleep(2 * time.Second)
                continue
            }
        }
        if dbIndex != 0 {
            if err := writeCommand(rw, "SELECT", strconv.Itoa(dbIndex)); err != nil || readOK(rw) != nil {
                log.Printf("redis select failed: %v", err)
                conn.Close()
                time.Sleep(2 * time.Second)
                continue
            }
        }

        for {
            if err := writeCommand(rw, "BRPOP", queue, "5"); err != nil {
                log.Printf("redis write error: %v", err)
                break
            }
            key, payload, err := readBRPOP(rw)
            if err != nil {
                if err != ioEOF {
                    log.Printf("redis read error: %v", err)
                }
                break
            }
            if key == "" && payload == "" {
                continue // timeout
            }
            var job sidekiqJob
            if err := json.Unmarshal([]byte(payload), &job); err != nil {
                log.Printf("invalid job json: %v", err)
                continue
            }
            if job.Class != "RubyWorker" && job.Class != "GoWorker" {
                log.Printf("skipping job class=%s", job.Class)
                continue
            }
            var id int64
            if len(job.Args) > 0 {
                id, _ = parseInt64(job.Args[0])
            }
            if id == 0 {
                log.Printf("job missing test_run_id: %s", payload)
                continue
            }
            if err := processTestRun(db, id); err != nil {
                log.Printf("process error: %v", err)
            }
        }
        conn.Close()
        time.Sleep(1 * time.Second)
    }
}
