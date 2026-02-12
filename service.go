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
	"strconv"
	"strings"
	"time"
)

func processTestRun(db *sql.DB, testRunID int64) error {
	if !existsTestRun(db, testRunID) {
		return fmt.Errorf("test_runs id %d not found", testRunID)
	}
	page, perPage, err := fetchTaskWindow(db, testRunID)
	if err != nil {
		return fmt.Errorf("fetch task window failed: %w", err)
	}

	values, err := fetchSamples(db, page, perPage)
	if err != nil {
		return fmt.Errorf("fetch samples failed: %w", err)
	}
	stats, elapsed, peak := measurePeakResidentMemory(func() (Stats, float64) {
		start := time.Now()
		stats := calculateStatistics(values)
		return stats, time.Since(start).Seconds()
	})

	if err := insertTestResult(db, testRunID, stats, elapsed, peak); err != nil {
		return fmt.Errorf("insert test_result failed: %w", err)
	}
	log.Printf("processed test_run=%d duration=%.6fs memory_bytes=%.0f\n", testRunID, elapsed, peak)
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
	if qname == "" {
		qname = "default"
	}
	queue := "queue:" + qname

	log.Printf("[go_worker] starting service redis=%s queue=%s", redisURL, queue)

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

		log.Printf("[go_worker] connected redis_host=%s db=%d listening=%s", host, dbIndex, queue)
		lastHeartbeat := time.Now()

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
				if time.Since(lastHeartbeat) >= 60*time.Second {
					log.Printf("[go_worker] idle (no jobs) queue=%s", queue)
					lastHeartbeat = time.Now()
				}
				continue // timeout
			}
			lastHeartbeat = time.Now()
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
			log.Printf("[go_worker] popped key=%s job_queue=%s class=%s test_run_id=%d", key, job.Queue, job.Class, id)
			if err := processTestRun(db, id); err != nil {
				log.Printf("[go_worker] process error key=%s class=%s test_run_id=%d err=%v", key, job.Class, id, err)
			}
		}
		conn.Close()
		time.Sleep(1 * time.Second)
	}
}
