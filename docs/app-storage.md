# App storage module

An app server that keeps objects in a bucket being migrated reads and writes them through a small module. The module asks the migration server where to write, in which order to read and where to delete, and follows changes within about a second. The reference implementation is the Go package [`client`](../client/client.go); this page is what a port to another language must do.

## Asking for the route

```http
GET /v1/migrations/{id}/routing?app={name}&applied={generation}
```

- `app` names this app server, unique among the app's servers (for example the host name).
- `applied` is the oldest route generation a request still running uses; `0` before the app has a route. A request that started on an older route can still write where that route said, so the app reports the new generation only once those requests ended.

```json
{
  "migrationId": "om-86c3bcc343a300dc",
  "phase": "SWITCHED",
  "generation": 3,
  "write": {"kind": "gcs", "bucket": "foxtrot-om-dst-test"},
  "read": [
    {"kind": "gcs", "bucket": "foxtrot-om-dst-test"},
    {"kind": "s3", "bucket": "foxtrot-om-src-test", "region": "ap-northeast-1"}
  ],
  "delete": [
    {"kind": "gcs", "bucket": "foxtrot-om-dst-test"},
    {"kind": "s3", "bucket": "foxtrot-om-src-test", "region": "ap-northeast-1"}
  ]
}
```

The response never holds credentials; the app reaches each bucket with its own.

1. Ask once at start-up, before serving requests.
2. Ask again every second, passing `applied`.
3. When the generation changed, use the new route for every request that starts from now on, then ask once more at once, so the server learns without waiting a second. A route step waits for every app that asked in the last 10 seconds (`APP_TTL`).
4. **Lease.** While the migration runs (phase not `COMPLETED`, `ROLLED_BACK`, `ABORTED` or `FAILED`), fail every request once the route has not been refreshed for 5 seconds. After 10 seconds the server stops waiting for an app it cannot hear, and the others may move on; an app still using the old route could then write where no one reads. The lease must stay shorter than `APP_TTL`. Once the migration is over the route no longer changes and needs no lease.
5. Use a route only once every bucket in it can be reached.

## Requests

| Operation | What to do |
| --- | --- |
| Write | Put the object in `write`. |
| Read | Try `read` in order. Not found in one bucket means try the next; not found in all of them means not found. Any other error is an error. |
| Delete | If `delete` lists more than one bucket, first `POST /v1/migrations/{id}/deletes` with `{"keys": ["<key>"]}` and expect `204`; if that fails, fail the delete. Then delete from every bucket in `delete`, in order. A bucket that does not hold the key is not an error. |

Recording the key first lets the migration server remove any copy of it that a copy running at the same time makes, whichever finishes last. It also means a delete that fails half way may still take effect: the server finishes it. Treat a failed delete as "maybe deleted" and retry it.

The migration server does not authenticate apps yet (planned with server-to-server tokens); keep its API on the private network.

Keys are never rewritten: a write always uses a new key. Objects under `.object-migration/` belong to the migration server; the app must ignore them.

## What the app sees during a migration

| Phase | Route generation | Write | Read | Delete |
| --- | --- | --- | --- | --- |
| Before the switch | 1 | source | source | target, source |
| Switch, step 1 | 2 | source | source, target | target, source |
| Switch, step 2 (`SWITCHED` onwards) | 3 | target | target, source | target, source |
| Finalize, step 1 | 4 | target | target | target, source |
| `COMPLETED` | 5 | target | target | target |

Rolling back goes the other way: writes back to the source while reads still cover both, then reads only the source, then deletes only from the source. Every step waits until every live app applied the previous one, so two apps are never more than one step apart and every object stays readable by every app.
