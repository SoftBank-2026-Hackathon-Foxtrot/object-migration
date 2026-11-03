# object-migration

Go service that migrates object storage between AWS S3, GCS, and on-prem SeaweedFS with zero downtime, using rclone.

The orchestrator drives this server through the common migration API shared with the DB and cache migration servers. App servers read the active bucket from this server and fall back to the other bucket while both copies exist.

Design: [architecture](https://softbank-2026-hackathon-foxtrot.github.io/design-docs/architecture/) · [object storage steps](https://softbank-2026-hackathon-foxtrot.github.io/design-docs/object-storage/)

## Status

M1: the common API and the worker run the whole phase flow with a fake engine. No real copying yet; that starts in M2 with rclone.

The status response shows, per migration:

- `phase` and `state`: the step it is in and whether that step is still working
- `replication.forward` and `replication.reverse`: whether each direction is in the initial copy, following changes (CDC), draining, stopped, or off
- `readiness.cutover`, `readiness.advance`, `readiness.rollback`: PASS, FAIL, or UNKNOWN for freezing, finalizing, and going back, with the checks behind each
- `allowedActions`: the commands accepted right now

## Run

```sh
FAKE_STEP_DELAY=500ms go run ./cmd/object-migration
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `ADDR` | `:8080` | Listen address |
| `POLL_INTERVAL` | `2s` | How often a waiting migration rechecks, and the retry backoff |
| `FAKE_STEP_DELAY` | `2s` | How long each fake engine step takes |

Drive a migration with curl:

```sh
ID=$(curl -s -X POST localhost:8080/v1/migrations \
  -d '{"source":{"kind":"s3","bucket":"src","ref":"env:AWS"},"target":{"kind":"gcs","bucket":"dst","ref":"env:GCP"}}' | jq -r .id)
curl -s localhost:8080/v1/migrations/$ID | jq '{phase,state,replication,allowedActions}'   # wait for READY
curl -s -X POST localhost:8080/v1/migrations/$ID/freeze
curl -s -X POST localhost:8080/v1/migrations/$ID/switch
curl -s -X POST localhost:8080/v1/migrations/$ID/resume       # wait for VERIFYING/succeeded
curl -s -X POST localhost:8080/v1/migrations/$ID/finalize
```

The API is described in [api/openapi.yaml](api/openapi.yaml).

## Test

```sh
go test -race ./...
```

## Layout

| Path | Purpose |
| --- | --- |
| `cmd/object-migration` | Entry point. Runs the API and the worker in one process |
| `api/openapi.yaml` | Common API draft shared with the DB and cache migration servers |
| `internal/api` | HTTP handlers, error mapping, Idempotency-Key replay |
| `internal/worker` | Runs each migration through its phases, one goroutine per migration |
| `internal/engine` | Storage work behind each phase. `Fake` for now |
| `internal/migration` | Migration types, phases, and which commands each phase accepts. No I/O |
| `internal/store` | Storage shared by the API and the worker. In-memory for now |

Planned packages: `internal/copier` (rclone), `internal/eventsource` (SQS, Pub/Sub, SeaweedFS change feeds), `internal/routing` (active bucket for app servers).
