# object-migration

Go service that migrates object storage between AWS S3, GCS, and on-prem SeaweedFS with zero downtime, using rclone.

The orchestrator drives this server through the common migration API shared with the DB and cache migration servers. App servers read the active bucket from this server and fall back to the other bucket while both copies exist.

Design: [architecture](https://softbank-2026-hackathon-foxtrot.github.io/design-docs/architecture/) · [object storage steps](https://softbank-2026-hackathon-foxtrot.github.io/design-docs/object-storage/)

## Status

M2: the rclone engine copies objects between real buckets. Tested S3 (`ap-northeast-1`) to GCS (`asia-northeast1`) with 1,000 objects.

- `INITIAL_SYNC` copies the whole source. Copying again skips objects that are already identical, so a copy that was stopped or crashed continues where it left off.
- `SYNCING` copies what is new every `SYNC_INTERVAL`. The migration is `READY` once a round copies nothing and every source object is identical in the target (size and MD5).
- Deletes in the source are not copied yet. Event-driven copying comes in M3.
- `freeze`, `switch` and `resume` only move the phase. The app keeps using the source until switching lands in M4, so nothing is copied back after resume and `replication.reverse` stays `OFF`.
- An `abort` during a copy stops it at once.

The status response shows, per migration:

- `phase` and `state`: the step it is in and whether that step is still working
- `replication.forward` and `replication.reverse`: whether each direction is in the initial copy, following changes (CDC), draining, stopped, or off
- `replication.initialCopy`: objects and bytes copied out of the total
- `readiness.cutover`, `readiness.advance`, `readiness.rollback`: PASS, FAIL, or UNKNOWN for freezing, finalizing, and going back, with the checks behind each
- `allowedActions`: the commands accepted right now

## Credentials

Each endpoint's `ref` says where the server finds its credentials. Requests never carry secrets.

| `ref` | s3, seaweedfs | gcs |
| --- | --- | --- |
| `default` | AWS SDK default chain (`AWS_PROFILE`, instance role, ...) | Application Default Credentials |
| `env:NAME` | `NAME_ACCESS_KEY_ID`, `NAME_SECRET_ACCESS_KEY`, optional `NAME_SESSION_TOKEN` | `NAME_CREDENTIALS_FILE`, a service account key file |

s3 endpoints also need `region`.

## Run

Copy between real buckets. With `aws login` and `gcloud auth application-default login` done:

```sh
ENGINE=rclone AWS_PROFILE=foxtrot go run ./cmd/object-migration
```

Or try the phase flow without storage:

```sh
ENGINE=fake FAKE_STEP_DELAY=500ms go run ./cmd/object-migration
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `ADDR` | `:8080` | Listen address |
| `ENGINE` | `rclone` | `rclone` copies real objects; `fake` only pretends |
| `POLL_INTERVAL` | `2s` | How often a waiting migration rechecks, and the retry backoff |
| `SYNC_INTERVAL` | `30s` (`2s` with fake) | Wait between sync rounds. Each rclone round lists both buckets |
| `COPY_TRANSFERS` | `16` | Objects copied at once |
| `COPY_CHECKERS` | `16` | Objects compared at once |
| `FAKE_STEP_DELAY` | `2s` | How long each fake engine step takes |

Drive a migration with curl:

```sh
BODY='{"source":{"kind":"s3","bucket":"foxtrot-om-src-test","region":"ap-northeast-1","ref":"default"},"target":{"kind":"gcs","bucket":"foxtrot-om-dst-test","ref":"default"}}'
curl -s -X POST localhost:8080/v1/preflight -d "$BODY" | jq        # can the server read both and write the target?
ID=$(curl -s -X POST localhost:8080/v1/migrations -d "$BODY" | jq -r .id)
curl -s localhost:8080/v1/migrations/$ID | jq '{phase,state,replication,readiness,allowedActions}'   # wait for READY
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
| `internal/engine` | Storage work behind each phase: `Rclone`, and `Fake` for trying the flow |
| `internal/copier` | rclone as a library: connect to a bucket, copy, compare, count, preflight probes |
| `internal/migration` | Migration types, phases, and which commands each phase accepts. No I/O |
| `internal/store` | Storage shared by the API and the worker. In-memory for now |

Planned packages: `internal/eventsource` (SQS, Pub/Sub, SeaweedFS change feeds), `internal/routing` (active bucket for app servers).
