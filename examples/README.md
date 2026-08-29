# Examples

A runnable demo of one complete objlog cycle — write, read, retention, and
garbage collection — against a local object storage emulator. Nothing here
touches a cloud account.

## Run it with no setup

The GCS emulator can run inside the demo process, so this works on a clean
checkout with no docker and no credentials:

```sh
go run ./examples/demo -provider fake-gcs
```

## Run it against containers

```sh
docker compose -f examples/docker-compose.yml up -d

go run ./examples/demo -provider minio
go run ./examples/demo -provider azurite
STORAGE_EMULATOR_HOST=127.0.0.1:4443 go run ./examples/demo -provider fake-gcs

docker compose -f examples/docker-compose.yml down -v
```

| Provider | Emulator | Endpoint | objlog package |
| --- | --- | --- | --- |
| `-provider minio` | MinIO | `http://127.0.0.1:9000` | `objlog/s3` |
| `-provider azurite` | Azurite | `http://127.0.0.1:10000/devstoreaccount1` | `objlog/azure` |
| `-provider fake-gcs` | fake-gcs-server, container or in-process | `127.0.0.1:4443` | `objlog/gcs` |

MinIO and Azurite are pinned to the versions CI runs against. Each demo run
writes under its own `demo/<timestamp>` prefix, so runs never collide.

## What it does

```text
1. write        open a fenced writer, append records, flush to publish them
2. read         replay from an LSN, seek by timestamp, fetch one exact LSN
3. resume       save a cursor checkpoint as JSON, reopen it, carry on exactly
4. tail         follow the live tail while another goroutine appends and flushes
5. retention    request a boundary, let the writer apply it, watch old LSNs expire
6. gc           observe unreachable objects, wait out the grace period, delete
```

Sample run (`-provider fake-gcs -delete-delay 1s`):

```text
objlog demo
   provider        fake-gcs · in-process fake-gcs-server
   location        bucket objlog-demo · prefix demo/1787940703
   stream          demo/orders · partition 7
   settings        12 records · retain from LSN 6 · delete delay 1s

── 1 · write ───────────────────────────────────────────────
   appends are accepted locally; a flush is what publishes them

   writer          opened on partition 7, fenced through the catalog
   batching        1 record per segment (demo setting)
   appended        12 records, LSN 0 → 11 — none visible yet
   flush           published 12 segments
   head            nextLSN 12 · oldestLSN 0 · reachable 12
   bucket          13 objects · 7.7 KiB   12 segments · 1 catalog · 0 maintenance

── 2 · read ────────────────────────────────────────────────
   readers use the catalog, then range-read objects; the writer is never involved

   by LSN          from 0 → 12 records, "order-0000" … "order-0011"
   by timestamp    first record at or after base+600ms → LSN 6, "order-0006"
   by exact LSN    11 → found=true, "order-0011"

── 3 · resume ──────────────────────────────────────────────
   a cursor is a position you can persist and pick up in another process

   cursor          opened at LSN 0, 4 records per call
   next            4 records, "order-0000" … "order-0003" — position now 4
   checkpoint      {"version":1,"stream_id":"demo/orders","partition":7,"next_lsn":4}
                   persist the whole value — NextLSN alone is not resumable
   consumer exits  cursor closed; only those bytes survive
   resumed         4 records, "order-0004" … "order-0007" — nothing repeated, nothing skipped
   validated       against the live head: another stream, partition, or an
                   LSN below the retention floor is refused, not silently reset

── 4 · tail ────────────────────────────────────────────────
   a watch refreshes the catalog in the background; a tailer blocks until new records land

   watch           background catalog refresh started for partition 7
   tailer          waiting at LSN 12 — the log is caught up, so Next blocks
   first wake      after 1.003s, 3 record(s) — no polling loop in the caller
   writer          appended 3 more records and flushed while the tailer waited
   tailed          3 records over 1 wake(s), "late-0000" … "late-0002"
   position        tailer now at LSN 15
   bucket          16 objects · 9.5 KiB   15 segments · 1 catalog · 0 maintenance  (+3 segments)

── 5 · retention ───────────────────────────────────────────
   retention changes what readers can see; it deletes nothing

   request         before LSN 6, policy v1 — intent only, head unchanged
   writer applies  through its own fence → oldestLSN 0 → 6 · reachable 15 → 9
   read at LSN 0   LSNExpiredError: requested 0, oldest 6
   checkpoint      the one saved in step 3 (NextLSN 4) no longer resumes: expired below 6
   bucket          17 objects · 8.8 KiB   15 segments · 1 catalog · 1 maintenance  (+1 maintenance)
                   no segment was deleted — the head shrank as it dropped references,
                   and the one new object is the request itself

── 6 · garbage collection ──────────────────────────────────
   two passes: observe what became unreachable, delete it after a grace period

   pass 1 observe  wrote gc state naming the orphaned objects — still nothing deleted
   grace period    1s, so in-flight reads can finish
   pass 2 reclaim  completed 1 · failed 0 · deferred 0
   scrub           completed 1 · failed 0 — hunts orphans the catalog never referenced
   bucket          12 objects · 6.1 KiB    9 segments · 1 catalog · 2 maintenance  (-6 segments, +1 maintenance)

── summary ─────────────────────────────────────────────────
   objects         13 write → 16 tail → 17 retention → 12 gc
                   tail +3 segments; retention +1 maintenance; gc -6 segments, +1 maintenance
   bytes           7.7 KiB → 6.1 KiB (1.6 KiB reclaimed)
   records         15 written (3 of them while a tailer followed) · 9 still readable, from LSN 6
   stored in       bucket objlog-demo and nowhere else — no broker, no local state
```

Every phase ends with the same bucket line, split by what each object is for —
`segments` hold records, `catalog` is the head, `maintenance` is retention and GC
state. That split is what makes the totals read correctly: garbage collection
removed six segments while writing one state object of its own, so 14 objects
became 9, not 8. Add `-v` to list every object key and size behind those totals.

## Flags

| Flag | Default | Purpose |
| --- | --- | --- |
| `-provider` | `fake-gcs` | `fake-gcs`, `minio`, or `azurite` |
| `-partition` | `7` | partition to write and read |
| `-records` | `12` | records to append |
| `-retain-from` | half the records | retention boundary LSN |
| `-delete-delay` | `2s` | grace period before an unreachable object may be deleted |
| `-v` | off | print every object key and size after each phase |

The demo sets `BatchPolicy{MaxRecords: 1}` so every record becomes its own
segment object and the counts are easy to follow. Real writers batch by size,
count, or age — see the `BatchPolicy` fields.

## Going to a real bucket

Only the client wiring changes. `examples/demo/minio.go`, `azurite.go`, and
`fakegcs.go` each build a provider store in a dozen lines; point the same
constructor at a real endpoint with real credentials and the rest of the demo
runs unmodified.

## Environment overrides

| Variable | Applies to | Default |
| --- | --- | --- |
| `OBJLOG_MINIO_ENDPOINT` | minio | `http://127.0.0.1:9000` |
| `OBJLOG_MINIO_BUCKET` | minio | `objlog-demo` |
| `OBJLOG_MINIO_ACCESS_KEY` / `OBJLOG_MINIO_SECRET_KEY` | minio | `minioadmin` |
| `OBJLOG_MINIO_REGION` | minio | `us-east-1` |
| `OBJLOG_AZURITE_CONNECTION_STRING` | azurite | the well-known devstoreaccount1 string |
| `OBJLOG_AZURITE_CONTAINER` | azurite | `objlog-demo` |
| `STORAGE_EMULATOR_HOST` | fake-gcs | unset, meaning in-process |
