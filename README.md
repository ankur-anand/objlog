<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/images/objlog-mark-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="docs/images/objlog-mark.svg">
    <img src="docs/images/objlog-mark.svg" alt="objlog append stack mark" width="112">
  </picture>
</p>

# objlog

**An append-only log that lives in your object storage bucket. No broker.**

objlog is an embedded Go library for durable, partitioned logs stored directly
in S3, GCS, Azure Blob, or MinIO. It links into your process — writers and
readers run wherever your code already runs, talking to the bucket and nothing
else. A writer seals records into immutable segment objects and publishes a
small catalog update that makes the new range visible. Readers range-read those
objects straight from the bucket.

There is nothing else to run: no cluster to size, no coordinator to elect a
writer — the fence is a compare-and-swap in the bucket — no partition kept
resident while it is idle, and no local disk holding the only copy. The bucket
is the log.

The Go library writes it, but the byte layouts are published specifications
with conformance corpora, so a reader in any language can decode a stream
straight out of the bucket.

## Install

```sh
go get github.com/ankur-anand/objlog
```

Go 1.25 or newer.

## Try it first

One command runs a full cycle against a local emulator. No docker, no
credentials, no configuration — the GCS fake runs inside the demo process:

```sh
go run ./examples/demo -provider fake-gcs
```

```text
1 · write        open a fenced writer, append records, flush to publish them
2 · read         replay from an LSN, seek by timestamp, fetch one exact LSN
3 · resume       save a cursor checkpoint as JSON, reopen it, carry on exactly
4 · tail         follow the live tail while another goroutine appends and flushes
5 · retention    request a boundary, let the writer apply it, watch old LSNs expire
6 · gc           observe unreachable objects, wait the grace period, delete them
```

It narrates each step and ends with what actually happened in the bucket:

```text
── summary ─────────────────────────────────────────────────
   objects         13 write → 16 tail → 17 retention → 12 gc
                   tail +3 segments; retention +1 maintenance; gc -6 segments, +1 maintenance
   bytes           7.7 KiB → 6.1 KiB (1.6 KiB reclaimed)
   records         15 written (3 of them while a tailer followed) · 9 still readable, from LSN 6
   stored in       bucket objlog-demo and nowhere else — no broker, no local state
```

The same demo runs against containers, so you can watch it in a real
object store:

```sh
docker compose -f examples/docker-compose.yml up -d

go run ./examples/demo -provider minio      # S3 API
go run ./examples/demo -provider azurite    # Azure Blob
STORAGE_EMULATOR_HOST=127.0.0.1:4443 \
  go run ./examples/demo -provider fake-gcs # GCS
```

Add `-v` to print every object key and size as the bucket changes. Full
transcript, flags, and provider wiring: [`examples/`](examples/).

## Usage

Open a store for one bucket and stream, then open the log over it:

```go
import (
    "github.com/ankur-anand/objlog"
    objs3 "github.com/ankur-anand/objlog/s3"
)

store, err := objs3.New(objs3.Options{
    Client:   s3Client,
    Bucket:   "events",
    Prefix:   "prod",
    StreamID: "hosts/host-a/events",
})

log, err := objlog.Open(objlog.Options{Store: store})
defer log.Close()
```

### Write

A writer owns one partition and is fenced through the catalog: one writer at a
time, and a superseded writer is rejected at publish rather than racing the new
owner.

```go
w, err := log.OpenWriter(ctx, objlog.WriterOptions{
    Partition: 7,
    WriterID:  uuid.New(),
    Batch: objlog.BatchPolicy{
        MaxDelay:   time.Second,
        MaxBytes:   64 << 20,
        MaxRecords: 16_384,
    },
})
defer w.Abort(context.Background())

appended, err := w.Append(ctx, objlog.Record{
    TimestampMS: time.Now().UnixMilli(),
    Value:       []byte("hello"),
})

snapshot, err := w.Flush(ctx)
```

**Records are batched before anything is published.** `Append` accepts a record
into the active batch and hands back its LSN — that is local acceptance, not
durability. The batch is cut when the first `BatchPolicy` limit is reached:

| Limit | Cuts the batch when |
| --- | --- |
| `MaxDelay` | that long has passed since the batch's first record |
| `MaxBytes` | raw record bytes reach the threshold, measured before compression |
| `MaxRecords` | that many records have been accepted |

A cut batch is sealed into an immutable segment object, uploaded, and published
to the catalog in the background. Only after that publish are the records
visible to readers. `BackpressurePolicy` bounds how much cut-but-unpublished
work may queue; when the limit is reached, further cuts block until the
background drains.

Force the boundary when you need it: `Cut` rotates the active segment, `Flush`
publishes everything accepted so far and returns the new head, `Close` flushes
and releases the fence, `Abort` gives it up without publishing. To wait on
durability without polling, take `Committed()` before reading `State()`.

### Read

Readers never talk to the writer. They read the catalog for what is committed,
then range-read the segment objects themselves:

```go
batch, err := log.Reader().Partition(7).Read(ctx, objlog.ReadRequest{
    StartLSN:  appended.LSN,
    Limit:     1000,
    Freshness: objlog.FreshnessOnTail,
})

for _, r := range batch.Records {
    _ = r.LSN
    _ = r.Value
}
```

A reader is cheap and disposable — its caches can be thrown away and rebuilt
from the bucket. Cursors, timestamp seeks, and tailing are covered in
[Reading](#reading) below.

### Retention and GC

Logical retention and physical deletion are separate, and both are explicit:

1. `log.RequestRetention(...)` records monotonic intent. Visibility is
   unchanged.
2. The active writer applies the latest request through its own fence with
   `writer.ApplyRetention(...)`, advancing `OldestLSN`. Whole segments are
   kept, so the effective `OldestLSN` can be lower than the one requested.
3. `objlog/lifecycle` reclaims the now-unreachable objects after a grace
   period, under a shared delete rate limit. A slower `OperationScrub` pass
   finds orphaned segments and catalog pages.

```go
import "github.com/ankur-anand/objlog/lifecycle"

// 1. any process — record the boundary
_, err := log.RequestRetention(ctx, objlog.RetentionRequest{
    Partition: 7, PolicyVersion: 1, BeforeLSN: 1_000_000,
})

// 2. the partition's writer — apply it through the fence it already holds
applied, err := writer.ApplyRetention(ctx)
_ = applied.Snapshot.Head.OldestLSN

// 3. any process, on its own schedule — delete what is now unreachable
reclaimer, err := store.NewReclaimer(lifecycle.Options{DeleteDelay: 24 * time.Hour})
scheduler, err := lifecycle.NewScheduler(reclaimer, lifecycle.SchedulerOptions{})
summary, err := scheduler.Run(ctx, []lifecycle.Task{
    {Partition: 7, Operation: lifecycle.OperationReclaim},
})
```

Only step 2 goes through the writer, because only the fence holder may move the
head. The reclaimer never opens a writer session: it takes its own lease in the
bucket, so it can run as a separate maintenance process, a cron job, or a
sidecar, as long as it points at the same store. It can only ever delete what
the catalog head has already stopped referencing, so a reclaimer that runs
before step 2 finds nothing to do.

Nothing runs implicitly inside writers or readers. Partition discovery and the
recurring schedule belong to the caller.

## How it works

```text
  Append(record)
        |
        v
  fenced writer  ---- batches by size, count, or age
        |
        v
  immutable segment object  +  catalog page
        |
        v
  S3  ·  GCS  ·  Azure Blob  ·  MinIO
        |
        v
  readers range-read only the blocks they need
```

- **One fenced writer per partition.** The catalog fence is the arbiter: a
  superseded writer is rejected at publication, and it terminates rather than
  writing behind the new owner.
- **Dense LSNs.** Every record in a partition gets a gapless LSN and a
  timestamp. Order within a partition is strict.
- **Bounded catalog metadata.** Readers page through index references instead
  of loading a partition's whole history.
- **Direct reads.** A reader needs the catalog and the object store, nothing
  else. Caches are disposable; committed state is reconstructible from the
  bucket.
- **Immutable segments.** Nothing is rewritten in place — there is no segment
  rewrite and no event-level compaction. Retention publishes new metadata;
  objects are only ever added or deleted.

## Reading

| Need | Call |
| --- | --- |
| Replay from an LSN | `log.Reader().Partition(p).Read(ctx, objlog.ReadRequest{...})` |
| Resumable replay | `Cursor` / `ResumeCursor` with a `CursorCheckpoint` |
| Seek by wall-clock time | `reader.ConsumeFromTimestamp(ctx, ...)` |
| One record at an exact LSN | `reader.Fetch(ctx, ...)` |
| Follow the tail | `reader.Watch(ctx, ...)` and a `Tailer` |

`Freshness` decides when a read refreshes the catalog head: `FreshnessCached`
uses what is already known, `FreshnessOnTail` refreshes only on reaching the
cached tail, `FreshnessLatest` refreshes first. Concurrent refreshes of the
same partition share one catalog load.

## Providers

| Package | Storage |
| --- | --- |
| `objlog/s3` | S3-compatible: AWS S3, MinIO, and friends |
| `objlog/gcs` | Google Cloud Storage |
| `objlog/azure` | Azure Blob Storage |

Each provider exposes `New(Options)` and `NewReclaimer(...)`. The whole public
API is `objlog`, the one provider package you use, and `objlog/lifecycle`.

## Write a reader in any language

The format is the interface, and it is specified rather than implied. Nothing
about it is Go-specific: a reader in Rust, Python, Java, or C++ can list,
range-read, and decode a stream straight out of the bucket without linking this
library, running a sidecar, or calling a service. Object key derivation is part
of the specification too, so a foreign reader can find the objects as well as
parse them.

Two byte-level formats, each versioned, each frozen by a checked-in conformance
corpus:

| Format | Covers | Specification | Corpus |
| --- | --- | --- | --- |
| `segformat` v2 | segment objects: preamble, blocks, block index, records, trailer | [SPEC.md](internal/segformat/SPEC.md) | [`testdata/segformat/v2`](testdata/segformat/v2) |
| `catformat` v1 | catalog objects: the mutable head, immutable leaf and index pages | [SPEC.md](internal/catalog/blob/SPEC.md) | [`testdata/catformat/v1`](testdata/catformat/v1) |

Both are big-endian, both reject an unknown version instead of guessing at it,
and both ship a language-neutral `manifest.json` alongside the fixtures — the
expected decode result, with 64-bit values as decimal strings, hashes as hex,
and payloads as base64, so nothing is lost through a JSON float. LSNs above
2^53 are in the corpus precisely because that is where naive JSON readers break.

A new implementation proves itself the same way the Go one does: verify each
fixture's SHA-256, parse it against the spec, then compare every field with the
manifest. Passing only the uncompressed vector is not enough — a complete reader
clears both the CRC32C and the zstd/XXH64 fixtures. The Go decoder is fuzzed
against the same corpus continuously, so the vectors stay honest.

Conformance procedures:
[`segformat`](internal/segformat/COMPATIBILITY.md) ·
[`catformat`](internal/catalog/blob/COMPATIBILITY.md).

## Where a broker still fits

Kafka and friends are the better tool when messages must reach live consumers
with low latency, when consumer groups should divide shared work, or when the
whole stream is consumed as it arrives.

objlog is for history that is written once and reopened later: replay,
reprocessing, audit, and per-partition retention you control. They compose —
publish to the broker for live delivery, keep the durable history here.

## Docs

- [`docs/usage.md`](docs/usage.md) — the library guide: writers, readers,
  cursors, tailing, retention, metrics.
- [`examples/`](examples/) — the runnable demo above: write, read, cursor
  resume, live tail, retention, and GC against MinIO, Azurite, or
  fake-gcs-server, with a docker-compose for all three.
- [`internal/segformat/SPEC.md`](internal/segformat/SPEC.md) and
  [`COMPATIBILITY.md`](internal/segformat/COMPATIBILITY.md) — segment object
  bytes and the conformance procedure.
- [`internal/catalog/blob/SPEC.md`](internal/catalog/blob/SPEC.md) and
  [`COMPATIBILITY.md`](internal/catalog/blob/COMPATIBILITY.md) — catalog object
  bytes, key derivation, and its conformance procedure.
