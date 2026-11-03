# object-migration

Go service that migrates object storage between AWS S3, GCS, and on-prem SeaweedFS with zero downtime, using rclone.

The orchestrator drives this server through the common migration API shared with the DB and cache migration servers. App servers read the active bucket from this server and fall back to the other bucket while both copies exist.

Design: [architecture](https://softbank-2026-hackathon-foxtrot.github.io/design-docs/architecture/) · [object storage steps](https://softbank-2026-hackathon-foxtrot.github.io/design-docs/object-storage/)

## Status

Early. The HTTP server exposes `GET /healthz`. Migration types, phase rules, and an in-memory store are in place; the common API and the worker are next.

## Run

```sh
go run ./cmd/object-migration
curl localhost:8080/healthz
```

`ADDR` sets the listen address (default `:8080`).

## Test

```sh
go test ./...
```

## Layout

| Path | Purpose |
| --- | --- |
| `cmd/object-migration` | Entry point |
| `internal/api` | HTTP API |
| `internal/migration` | Migration types, phases, and which commands each phase accepts. No I/O |
| `internal/store` | Storage shared by the API and the worker. In-memory for now |

Planned packages: `internal/worker` (runs phases), `internal/eventsource` (SQS, Pub/Sub, SeaweedFS change feeds), `internal/copier` (rclone), `internal/routing` (active bucket for app servers).
