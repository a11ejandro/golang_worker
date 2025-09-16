package main

import (
    "database/sql"
    "errors"
    "fmt"
    "os"

    _ "github.com/lib/pq"
)

func buildDSNFromEnv() (string, error) {
    host := os.Getenv("POSTGRES_HOST")
    port := os.Getenv("POSTGRES_PORT")
    user := os.Getenv("POSTGRES_USER")
    pass := os.Getenv("POSTGRES_PASSWORD")
    dbname := os.Getenv("POSTGRES_DB")
    if dbname == "" {
        if url := os.Getenv("DATABASE_URL"); url != "" {
            return url, nil
        }
        return "", errors.New("POSTGRES_DB not set; set env vars or DATABASE_URL")
    }
    if host == "" {
        host = "localhost"
    }
    if port == "" {
        port = "5432"
    }
    dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, pass, dbname)
    return dsn, nil
}

func existsTestRun(db *sql.DB, id int64) bool {
    var exists bool
    err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM test_runs WHERE id = $1)", id).Scan(&exists)
    return err == nil && exists
}

func fetchSamples(db *sql.DB) ([]float64, error) {
    rows, err := db.Query("SELECT value FROM samples")
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    values := make([]float64, 0, 1024)
    for rows.Next() {
        var v sql.NullFloat64
        if err := rows.Scan(&v); err != nil {
            return nil, err
        }
        if v.Valid {
            values = append(values, v.Float64)
        }
    }
    return values, rows.Err()
}

func insertTestResult(db *sql.DB, testRunID int64, st Stats, durationSeconds float64, memoryBytes float64) error {
    const q = `
INSERT INTO test_results 
  (test_run_id, mean, median, q1, q3, min, max, standard_deviation, duration, memory, created_at, updated_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW(),NOW())
`
    _, err := db.Exec(q,
        testRunID,
        st.Mean, st.Median, st.Q1, st.Q3, st.Min, st.Max, st.StdDev,
        durationSeconds, memoryBytes,
    )
    return err
}

