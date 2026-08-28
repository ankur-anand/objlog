<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/images/objlog-mark-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="docs/images/objlog-mark.svg">
    <img src="docs/images/objlog-mark.svg" alt="objlog append stack mark" width="112">
  </picture>
</p>

# objlog

**An append-only log that lives in your object storage bucket.**

objlog is a Go library for durable, partitioned logs stored directly in S3,
GCS, Azure Blob, or MinIO. A writer seals records into immutable segment
objects and publishes a small catalog update that makes the new range visible.
Readers range-read those objects straight from the bucket.

There is no broker to operate, no partition to keep resident, and no local disk
that holds the only copy. The bucket is the log.

## Install

```sh
go get github.com/ankur-anand/objlog
```

Go 1.25 or newer.

## Quickstart

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

Write. One writer owns one partition:

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

appended, err := w.Append(ctx, objlog.Record{
    TimestampMS: time.Now().UnixMilli(),
    Value:       []byte("hello"),
})

_, err = w.Flush(ctx)
```

`Append` acknowledges local acceptance. Records become visible to readers once
a segment is cut and published — when `Flush`/`Close` returns, or when the
batch policy rolls a segment on its own.

Read. Readers never talk to the writer:

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

## Retention and GC

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
reclaimer, err := store.NewReclaimer(lifecycle.Options{DeleteDelay: 24 * time.Hour})
scheduler, err := lifecycle.NewScheduler(reclaimer, lifecycle.SchedulerOptions{})
summary, err := scheduler.Run(ctx, []lifecycle.Task{
    {Partition: 7, Operation: lifecycle.OperationReclaim},
})
```

Nothing runs implicitly inside writers or readers. Partition discovery and the
recurring schedule belong to the caller.

## Providers

| Package | Storage |
| --- | --- |
| `objlog/s3` | S3-compatible: AWS S3, MinIO, and friends |
| `objlog/gcs` | Google Cloud Storage |
| `objlog/azure` | Azure Blob Storage |

Each provider exposes `New(Options)` and `NewReclaimer(...)`. The whole public
API is `objlog`, the one provider package you use, and `objlog/lifecycle`.

## Segment format

Segments are a durable contract, not an implementation detail. The v2 format
covers uncompressed and zstd blocks, CRC32C and XXH64 hashes, block indexes,
headers, and LSNs above 2^53. A checked-in corpus with a language-neutral
`manifest.json` lets a reader written in another language verify itself, and
the Go decoder is fuzzed against the same fixtures.

See [`internal/segformat/COMPATIBILITY.md`](internal/segformat/COMPATIBILITY.md).

## Where a broker still fits

Kafka and friends are the better tool when messages must reach live consumers
with low latency, when consumer groups should divide shared work, or when the
whole stream is consumed as it arrives.

objlog is for history that is written once and reopened later: replay,
reprocessing, audit, and per-partition retention you control. They compose —
publish to the broker for live delivery, keep the durable history here.

## Status

Experimental. The storage engine runs on S3, GCS, Azure Blob, and MinIO with
immutable segment publication, fenced writers, bounded catalog metadata,
direct replay, retention, and garbage collection. The storage layout and the
Go API may still change before a stable release.

## Docs

- [`docs/usage.md`](docs/usage.md) — the library guide: writers, readers,
  cursors, tailing, retention, metrics.
- [`internal/segformat/COMPATIBILITY.md`](internal/segformat/COMPATIBILITY.md)
  — segment binary format and the cross-language corpus.
