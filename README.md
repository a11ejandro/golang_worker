# Go Worker

A minimal Go implementation mirroring the Ruby `RubyWorker#perform` to compute statistics over `samples` and persist a `test_results` row.

## Build

- Requires Go 1.22+
- From `golang_worker`:

```
go mod tidy   # downloads github.com/lib/pq
go build -o go_worker
```

## Configure

The worker loads environment from `.env` automatically (tries `../benchmark_ui/.env` then local `.env`). It reads Postgres settings from the same env vars used by Rails:

- `POSTGRES_HOST` (default: `localhost`)
- `POSTGRES_PORT` (default: `5432`)
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_DB` (e.g., `benchmark_development`)

Alternatively, set `DATABASE_URL` and omit the above.

Redis configuration:

- `REDIS_URL` (e.g., `redis://localhost:6379/0`)
- `WORKER_QUEUE` (default: `default`; set to `go` if you want a dedicated queue)

## Run

Provide the `test_runs.id` to attach results to:

```
./go_worker --test-run-id 123
# or positional
./go_worker 123
```

This will:

- SELECT all `value` from `samples`
- Compute min, q1, median, q3, max, mean, and population stddev
- Measure wall duration and allocated bytes during the computation
- INSERT a row into `test_results` with the computed fields and instrumentation

### Run as a background service (Sidekiq-compatible queue)

If you omit `--test-run-id` or pass `--service`, the worker will run as a background service that listens to the same Redis queue as Rails Sidekiq (default: `queue:default` from `REDIS_URL`). It consumes jobs enqueued by `RubyWorker.perform_async(test_run_id)` and processes them using the same logic as above, inserting a `test_results` row per job.

```
./go_worker --service
# or simply
./go_worker
```

Environment:

- `REDIS_URL` (e.g., `redis://localhost:6379/0`)
- `WORKER_QUEUE` (default: `default`) — set to `go` to isolate from Ruby Sidekiq
- The Postgres variables noted above

## Notes

- Standard deviation uses population variance (divide by n), matching the Ruby service.
- Memory uses the delta of Go `runtime.MemStats.TotalAlloc` (bytes) during computation. This is analogous to Ruby's MemoryProfiler total allocated bytes.
- Timestamps are set via `NOW()` on insert.
