package main

import (
    "database/sql"
    "flag"
    "fmt"
    "log"

    "github.com/joho/godotenv"
)

func main() {
    // Load environment from .env files for local development.
    // Prefer the Rails app .env if present.
    _ = godotenv.Load("../benchmark_ui/.env")
    _ = godotenv.Load(".env")

    var testRunID int64
    var service bool
    flag.Int64Var(&testRunID, "test-run-id", 0, "ID of test_runs row to attach results to (omit to run service)")
    flag.BoolVar(&service, "service", false, "Run as background service listening to Sidekiq queue")
    flag.Parse()

    dsn, err := buildDSNFromEnv()
    if err != nil {
        log.Fatalf("database config error: %v", err)
    }
    db, err := sql.Open("postgres", dsn)
    if err != nil {
        log.Fatalf("connect error: %v", err)
    }
    defer db.Close()
    if err := db.Ping(); err != nil {
        log.Fatalf("database not reachable: %v", err)
    }

    if service || (testRunID == 0 && flag.NArg() == 0) {
        runService(db)
        return
    }

    if testRunID == 0 {
        if flag.NArg() > 0 {
            var v int64
            _, err := fmt.Sscan(flag.Arg(0), &v)
            if err == nil {
                testRunID = v
            }
        }
    }
    if testRunID == 0 {
        log.Fatal("missing --test-run-id <id> argument or --service")
    }

    if err := processTestRun(db, testRunID); err != nil {
        log.Fatal(err)
    }
}
