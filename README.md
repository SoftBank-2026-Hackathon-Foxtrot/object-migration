# object-migration

Go service that migrates object storage between AWS S3, GCS, and on-prem SeaweedFS with zero downtime, using rclone.

The orchestrator drives this server through the common migration API shared with the DB and cache migration servers. App servers read the active bucket from this server and fall back to the other bucket while both copies exist.

Design: [architecture](https://softbank-2026-hackathon-foxtrot.github.io/design-docs/architecture/) · [object storage steps](https://softbank-2026-hackathon-foxtrot.github.io/design-docs/object-storage/)

## Status

Apps switch between AWS S3, GCS and on-prem SeaweedFS, or back, without a failed request. Each direction below was run on real buckets with 3 simulated app servers writing, reading each other's objects and deleting through the [app storage module](docs/app-storage.md) during freeze, switch, resume and finalize; every object written was in the bucket kept, every object deleted in neither.

| Direction | Change feed | Requests | Failed |
| --- | --- | --- | --- |
| S3 → GCS | SQS | 4,816 | 0 |
| S3 → GCS, rolled back | SQS | 1,405 | 0 |
| S3 → SeaweedFS | SQS | 3,110 | 0 |
| SeaweedFS → S3 | filer subscription | 2,444 | 0 |
| SeaweedFS → GCS | filer subscription | 2,393 | 0 |
| GCS → S3 | Pub/Sub | 1,980 | 0 |
| GCS → SeaweedFS | Pub/Sub | 3,426 | 0 |

Restarts: the server was killed with SIGKILL during the initial copy, while following changes, and between switch and resume, and started again each time, with apps working throughout:

| Direction | Change feed | State kept in | Requests | Failed | Missing | Deleted left |
| --- | --- | --- | --- | --- | --- | --- |
| SeaweedFS → GCS | filer subscription, resumed from the saved position | PostgreSQL (the earlier store) | 20,122 | 0 | 0 | 0 |
| GCS → SeaweedFS | Pub/Sub | PostgreSQL (the earlier store) | 8,553 | 0 | 0 | 0 |
| SeaweedFS → SeaweedFS | filer subscription, resumed from the saved position | PostgreSQL (the earlier store) | 4,179 | 0 | 0 | 0 |
| SeaweedFS → SeaweedFS | filer subscription, resumed from the saved position | DynamoDB | 4,491 | 0 | 0 | 0 |
| S3 → GCS | SQS | DynamoDB | 9,361 | 0 | 0 | 0 |

`scripts/scenario.sh` runs one direction end to end; `/v1/capabilities` lists every direction with its change feed and how objects are compared.

How a migration runs:

1. **Prepare.** The target must be empty, or hold only identical copies of source objects (`target_clean`). The server creates an SQS queue and adds a notification for every object created or removed in the source bucket, keeping the bucket's other notifications.
2. **`INITIAL_SYNC`** copies the whole source while changes wait in the queue. A copy that was stopped continues where it left off.
3. **`SYNCING` / `READY`.** Each changed key is made to match the source as its event arrives: copied if the source has it, deleted from the target if not. The whole source is copied again every `FULL_COPY_INTERVAL` and compared every minute; objects deleted from the source while their events were lost are deleted from the target too. A canary object written to the source under `.object-migration/` every 15 seconds proves events still arrive. `READY` needs the last canary within 10 s, at most 100 changes waiting, p95 lag at most 5 s, and a full comparison that finds nothing missing (except objects written in the last 30 s), different, or only in the target.
4. **`freeze`** has nothing to do: keys are never rewritten, so writes need not stop.
5. **`switch`** moves apps in two steps, each waiting until every live app applied it: first reads cover both buckets, then writes move to the target. An app that does not confirm in time leaves the migration `unknown` with `APPS_NOT_CONFIRMED`, and the switch keeps waiting.
6. **`resume`** handles the source changes still in the queue and copies the source once more.
7. **`VERIFYING`** checks every source object is in the target, and copies what apps write to the target back to the source, so a rollback loses nothing.
8. **`finalize`** stops reads from the source, then stops deletes to it. **`rollback`** moves writes back to the source while reads still cover both, copies the target's new objects back, then stops reading and deleting the target.
9. The queue, the notification and any canary are removed however the migration ends.

Apps record each key they delete with the server before deleting it from the buckets; every copy the server makes, in either direction, removes its copy of a recorded key. A copy and a delete can then finish in either order. Apps report a route as applied only once no request on an older one is running, and stop serving when they cannot refresh the route for 5 seconds during a migration, so the server never moves on while an app may still write where the old route said.

A switch or finalize that waits for an app that never confirms leaves the migration `unknown`; `rollback` may then replace it.

**Restarts.** With `DYNAMODB_TABLE` set, the server keeps in its own DynamoDB table every migration (phase, route generation, pending command), the keys apps deleted, the SeaweedFS feed's position, and the responses replayed for `Idempotency-Key`. A server that stopped, even killed mid-step, picks up every unfinished migration on start and runs its current step again:

- The initial copy continues: objects already in the target are compared, not copied again.
- SQS queues and Pub/Sub subscriptions held the changes meanwhile; the SeaweedFS feed reads again from the saved position. The first round after a restart copies the whole source and removes every key apps deleted so far, in case a copy cut short brought one back.
- A route step waits one `APP_TTL` before it trusts that it has heard from every app, since app reports are not kept. Apps keep working on their 5-second lease through a restart of a few seconds, and their deletes retry the record with the server meanwhile; through a longer one their requests fail until the server is back.

Run one server per table: app reports live in the process. The table belongs to this service alone; other migration servers keep their own.

Change feeds: an S3 source through SQS, a GCS source through Pub/Sub (set `GCP_PROJECT` to the platform's project, which holds the topic; the bucket's storage service agent needs `roles/pubsub.publisher` there, which the server grants on the topic when it may), a SeaweedFS source through its filer's metadata subscription (give the endpoint `filer`, the filer's gRPC address). The SeaweedFS feed needs nothing set up on the SeaweedFS side and resumes from the oldest change not yet handled after a lost connection. Sources without a feed (gcs when `GCP_PROJECT` is unset, seaweedfs without `filer`) are mirrored in full every `SYNC_INTERVAL` before the switch. S3 sends each object event to one destination, so a source bucket that already sends object events elsewhere (a Lambda, SNS topic or queue) cannot be followed; preflight reports it as `source_events`.

The status response shows, per migration:

- `phase` and `state`: the step it is in and whether that step is still working
- `replication.forward` and `replication.reverse`: whether each direction is in the initial copy, following changes (CDC), draining, stopped, or off
- `replication.initialCopy`: objects and bytes copied out of the total
- `readiness.cutover`, `readiness.advance`, `readiness.rollback`: PASS, FAIL, or UNKNOWN for freezing, finalizing, and going back, with the checks behind each
- `routing`: the route apps use now: its generation, where they write, read and delete
- `lifecycle`, `health`, `allowedActions`, `readiness`, `checks`, `observed`: the same, in the team's common form (below)
- `revision`: changes only when the phase changes or an action is accepted, so an orchestrator can send it back as `expectedRevision`
- `operations`: every accepted action and how it ended

## Orchestrator API

The orchestrator drives the server with the team's common migration API, draft 0.2: the same contract as the DB and Redis servers. Responses validate against that contract's schemas (checked against `db-migration-adaptor`'s `contract.json`), with this server's own fields added after the common ones.

| Step | Request | Here |
| --- | --- | --- |
| Plan | `POST /v1/migrations` with `workflowId`, `kind: OBJECT_STORAGE`, `profile: object-copy-route-v1`, `sourceRef`, `targetRef`, ... | 201, CREATED. Touches nothing. `sourceRef` and `targetRef` name buckets registered in `OBJECT_RESOURCES` as `resource:<name>` |
| `PREPARE` | `POST /v1/migrations/{id}/actions` | Checks access and that the target is clean, starts following the source's changes. PREPARED. A failed check fails the operation and leaves the plan in CREATED |
| `START_SYNC` | | Starts the full copy. SYNCING, then READY when the copy is caught up |
| `QUIESCE` | `transitionId`, `quiesceRef` | Records the cutover attempt. Apps keep writing. QUIESCED |
| `CUTOVER` | | Apps read both buckets, then write to the target, each step confirmed by every app. SWITCHED |
| `RESUME` | `expectedLocation: TARGET` | Starts copying the target's changes back to the source. ACTIVE |
| `ROLLBACK` | | Copies the target's new objects back and moves apps to the source. ROLLED_BACK_HELD |
| `RESUME` | `expectedLocation: SOURCE` | Confirms apps are on the source; changes nothing. RESTORED |
| `CANCEL` | | Before CUTOVER. CANCELLED; the target keeps what was copied |
| `RECONCILE` | | Compares the record with what apps report (route generation, apps not on it); changes nothing outside the record. After an unknown outcome it is the only action accepted until run |
| `FINALIZE` | | ACTIVE: stops reading and deleting from the source. RESTORED: closes the plan. COMPLETED |
| Result | `GET /v1/operations/{id}` | `RUNNING`, `SUCCEEDED`, `FAILED` or `UNKNOWN`, with `error` and `confirmedChanges` |

Every POST needs an `Idempotency-Key`; a repeat returns the first answer. Every action needs `expectedRevision`. One cutover attempt per migration: the first `QUIESCE` sets `transitionId`, and later held actions must repeat it. With `API_TOKEN` set, control routes need `Authorization: Bearer`; apps call `.../routing` and `.../deletes` with no token.

Where object storage differs from the DB and Redis servers:
- `QUIESCE` holds nothing and `quiesceRef` is kept but not checked: apps keep writing through the cutover.
- `readiness.advance` is never PASS from this server alone. `WORKLOAD_RUNNING` and `BUSINESS_PROBE` are UNKNOWN, as in the Redis server: they are the orchestrator's evidence.
- An unknown outcome means some apps did not confirm a new route in time. After `RECONCILE`, `ROLLBACK` is allowed; if the apps catch up, the worker finishes the command on its own.

The older forms still work for existing tools: `POST /v1/migrations` with inline `source` and `target` starts at once (202), and the path commands (`POST /v1/migrations/{id}/freeze` and the rest) run without the common checks.

## On-prem storage

On-prem buckets live in SeaweedFS, run from [deploy/seaweedfs](deploy/seaweedfs/compose.yaml) on the on-prem VM: one node with master, volume, filer and the S3 gateway. The migration server reaches the S3 API on 8333 and the filer's gRPC on 18888 over Tailscale. Keys go in `s3.json` next to it (see `s3.json.example`): one identity for this server and one for the app.

```json
{"kind": "seaweedfs", "bucket": "photos", "url": "http://100.64.0.7:8333", "filer": "100.64.0.7:18888", "ref": "env:SEAWEED"}
```

Tested both ways against SeaweedFS 4.48: AWS S3 to SeaweedFS and SeaweedFS to AWS S3, each through switch and finalize with 3 simulated apps and no failed request. `go test ./internal/copier ./internal/eventsource` also runs S3 compatibility and feed tests against a SeaweedFS when `OM_SEAWEEDFS_URL`, `OM_SEAWEEDFS_FILER`, `OM_SEAWEEDFS_SRC` and `OM_SEAWEEDFS_DST` are set.

## Credentials

Each endpoint's `ref` says where the server finds its credentials. Requests never carry secrets.

| `ref` | s3, seaweedfs | gcs |
| --- | --- | --- |
| `default` | AWS SDK default chain (`AWS_PROFILE`, instance role, ...) | Application Default Credentials |
| `env:NAME` | `NAME_ACCESS_KEY_ID`, `NAME_SECRET_ACCESS_KEY`, optional `NAME_SESSION_TOKEN` | `NAME_CREDENTIALS_FILE`, a service account key file |

s3 endpoints also need `region`.

The SQS queue is created with the server's own AWS credentials (the default chain), in the source bucket's region; it may be in another account than the bucket. The source `ref` needs `s3:GetBucketNotification` and `s3:PutBucketNotification` on the bucket and `sts:GetCallerIdentity`; the server needs `sqs:CreateQueue`, `sqs:SetQueueAttributes`, `sqs:GetQueueAttributes`, `sqs:GetQueueUrl`, `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:DeleteQueue` and `sqs:TagQueue`.

Set `AWS_PROFILE` when you run the server on a laptop: with `ref: default`, the default profile is what the server uses.

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
| `DYNAMODB_TABLE` | | DynamoDB table for the server's state, created with on-demand capacity if missing. Reached with the platform's AWS credentials (the SDK's default chain, e.g. `AWS_PROFILE`). Unset: state lives in memory and is lost on restart |
| `DYNAMODB_REGION` | `ap-northeast-2` | Region of `DYNAMODB_TABLE` |
| `API_TOKEN` | | Bearer token the orchestrator must send. Unset: control routes are open (logged as a warning) |
| `OBJECT_RESOURCES` | | JSON file of the buckets a plan may name: `{"aws-receipts": {"kind": "s3", "bucket": "...", "region": "ap-northeast-2", "ref": "default"}, ...}` |
| `DYNAMODB_ENDPOINT` | | Endpoint override, e.g. DynamoDB Local |
| `ENGINE` | `rclone` | `rclone` copies real objects; `fake` only pretends |
| `POLL_INTERVAL` | `2s` | How often a waiting migration rechecks, and the retry backoff |
| `SYNC_INTERVAL` | `30s` (`2s` with fake) | Wait between sync rounds for sources without a change feed. Each such round lists both buckets |
| `GCP_PROJECT` | | Platform project for the Pub/Sub topics that follow GCS sources. Unset: GCS sources are mirrored every round |
| `FULL_COPY_INTERVAL` | `5m` | How often a source with a change feed is copied in full again, in case an event was lost |
| `COPY_TRANSFERS` | `16` | Objects copied at once |
| `COPY_CHECKERS` | `16` | Objects compared at once |
| `APP_TTL` | `10s` | An app that has not asked for its route for this long is not waited for |
| `CONFIRM_TIMEOUT` | `30s` | How long a route step waits for apps when the command gives no `deadlineMs` |
| `LOG_LEVEL` | `info` | `debug` logs every change handled |
| `FAKE_STEP_DELAY` | `2s` | How long each fake engine step takes |

Drive a migration with curl:

```sh
BODY='{"source":{"kind":"s3","bucket":"foxtrot-om-src-test","region":"ap-northeast-1","ref":"default"},"target":{"kind":"gcs","bucket":"foxtrot-om-dst-test","ref":"default"}}'
curl -s -X POST localhost:8080/v1/preflight -d "$BODY" | jq        # can the server read both and write the target?
ID=$(curl -s -X POST localhost:8080/v1/migrations -d "$BODY" | jq -r .id)
curl -s localhost:8080/v1/migrations/$ID | jq '{lifecycle,phase,state,replication,readiness,allowedActions,revision}'   # wait for READY
curl -s -X POST localhost:8080/v1/migrations/$ID/freeze
curl -s -X POST localhost:8080/v1/migrations/$ID/switch
curl -s -X POST localhost:8080/v1/migrations/$ID/resume       # wait for VERIFYING/succeeded
curl -s -X POST localhost:8080/v1/migrations/$ID/finalize
```

Or as the orchestrator does, with a registered plan (`OBJECT_RESOURCES` set):

```sh
PLAN='{"workflowId":"wf-1","resourceId":"receipts","kind":"OBJECT_STORAGE","profile":"object-copy-route-v1","sourceRef":"resource:aws-receipts","targetRef":"resource:gcp-receipts","workloadScopeRef":"workload:demo","bindingRef":"binding:receipts","policy":{"rollbackMode":"REVERSE_REQUIRED","maxQuiesceSeconds":30}}'
ID=$(curl -s -X POST localhost:8080/v1/migrations -H 'Idempotency-Key: plan-1' -d "$PLAN" | jq -r .migrationId)
act() { curl -si -X POST localhost:8080/v1/migrations/$ID/actions -H "Idempotency-Key: $1-$(date +%s)" \
  -d "{\"action\":\"$1\",\"expectedRevision\":$(curl -s localhost:8080/v1/migrations/$ID | jq .revision)$2}"; }
act PREPARE       # then wait for lifecycle PREPARED
act START_SYNC    # then wait for READY
act QUIESCE ',"transitionId":"tx-1","quiesceRef":"q-1"'      # Location: /v1/operations/...
```

Simulate app servers during a migration with `appsim`: it writes, reads each other's objects and deletes through the app storage module, counts every failed request, and writes the keys it left and deleted to compare with the buckets. It reports request times per migration step (the route each app held when it sent the request) and the longest time an app went without a successful write, and logs every request to `requests.csv` in `-out`. With `-direct` it sends the same requests straight to one bucket, without a migration, as a baseline.

```sh
AWS_PROFILE=foxtrot go run ./cmd/appsim -migration $ID -apps 3 -for 5m
AWS_PROFILE=foxtrot go run ./cmd/appsim -direct s3 -bucket foxtrot-om-src-test -region ap-northeast-1 -for 3m
```

Measured on real buckets, S3 → GCS: [request latency per step](https://softbank-2026-hackathon-foxtrot.github.io/design-docs/object-storage/latency/).

The API is described in [api/openapi.yaml](api/openapi.yaml); what an app's storage module does is in [docs/app-storage.md](docs/app-storage.md).

## Test

```sh
go test -race ./...
```

The store tests also run against DynamoDB when `OM_DYNAMODB_ENDPOINT` names one; each test makes and drops its own table:

```sh
docker run -d --name om-dynamodb -p 58000:8000 amazon/dynamodb-local -jar DynamoDBLocal.jar -inMemory
OM_DYNAMODB_ENDPOINT=http://127.0.0.1:58000 go test -race ./internal/store
```

## Layout

| Path | Purpose |
| --- | --- |
| `cmd/object-migration` | Entry point. Runs the API and the worker in one process |
| `api/openapi.yaml` | Common API draft shared with the DB and cache migration servers |
| `internal/api` | HTTP handlers, error mapping, Idempotency-Key replay |
| `internal/worker` | Runs each migration through its phases, one goroutine per migration |
| `internal/engine` | Storage work behind each phase: `Rclone`, and `Fake` for trying the flow |
| `internal/copier` | rclone as a library: connect to a bucket, copy, compare, sync one key, preflight probes |
| `internal/eventsource` | Change feeds of source buckets: S3 through SQS, GCS through Pub/Sub, SeaweedFS through the filer |
| `internal/routing` | Which route each app applied, and the keys apps deleted |
| `client` | App storage module (Go reference implementation) |
| `cmd/appsim`, `internal/appsim` | Simulated app servers for testing a switch on real buckets |
| `internal/e2e` | API, worker and apps together on in-memory buckets |
| `internal/creds` | Resolves endpoint credential references |
| `internal/migration` | Migration types, phases, and which commands each phase accepts. No I/O |
| `internal/store` | Storage shared by the API and the worker: DynamoDB, or memory for tests and local runs |

Not yet: metrics and server-to-server authentication (M8).

## 마이그레이션 관측

추가 필드와 연결 방법은 [관측 연결](docs/migration-observability.md)을 참고하세요.
