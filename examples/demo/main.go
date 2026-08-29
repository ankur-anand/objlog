// Command demo runs one complete objlog cycle - write, read, retention, and
// garbage collection - against a local object storage emulator.
//
//	go run ./examples/demo -provider fake-gcs   # no docker needed
//	go run ./examples/demo -provider minio      # docker compose up minio
//	go run ./examples/demo -provider azurite    # docker compose up azurite
//
// Add -v to print every object key as the bucket changes.
// See examples/README.md for the emulators.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/google/uuid"

	"github.com/ankur-anand/objlog"
	"github.com/ankur-anand/objlog/lifecycle"
)

type config struct {
	provider    string
	partition   uint32
	records     int
	retainFrom  uint64
	deleteDelay time.Duration
	verbose     bool
}

func main() {
	var (
		cfg        config
		partition  = flag.Uint("partition", 7, "partition to write and read")
		retainFrom = flag.Uint64("retain-from", 0, "retention boundary LSN (default: half the records)")
	)
	flag.StringVar(&cfg.provider, "provider", "fake-gcs", "emulator to run against: fake-gcs, minio, or azurite")
	flag.IntVar(&cfg.records, "records", 12, "records to append")
	flag.DurationVar(&cfg.deleteDelay, "delete-delay", 2*time.Second, "grace period before an unreachable object may be deleted")
	flag.BoolVar(&cfg.verbose, "v", false, "print object keys after every phase")
	flag.Parse()

	cfg.partition = uint32(*partition)
	cfg.retainFrom = *retainFrom
	if cfg.records < 4 {
		fail(errors.New("-records must be at least 4"))
	}
	if cfg.retainFrom == 0 {
		cfg.retainFrom = uint64(cfg.records / 2)
	}
	if cfg.retainFrom >= uint64(cfg.records) {
		fail(fmt.Errorf("-retain-from %d must be below -records %d", cfg.retainFrom, cfg.records))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		fail(err)
	}
}

// demo carries the state each phase needs: one provider, one log, one writer.
type demo struct {
	cfg    config
	prefix string
	p      *provider
	log    *objlog.Log
	writer *objlog.Writer
	baseTS int64

	// checkpoint is the cursor position saved in the resume phase. Retention
	// later invalidates it, which is the failure a stalled consumer sees.
	checkpoint objlog.CursorCheckpoint

	// written counts every record appended, including the ones the tail phase
	// adds while a reader is already following.
	written int
}

// tailRecords are appended while the tailer is blocked, so the wake-up is real.
const tailRecords = 3

func run(ctx context.Context, cfg config) error {
	d := &demo{cfg: cfg, prefix: fmt.Sprintf("demo/%d", time.Now().Unix())}

	p, err := openProvider(ctx, d.cfg.provider, d.prefix)
	if err != nil {
		return err
	}
	d.p = p
	defer p.close()

	fmt.Println("objlog demo")
	row("provider", "%s · %s", p.name, p.where)
	row("location", "%s · prefix %s", p.container, d.prefix)
	row("stream", "%s · partition %d", streamID, d.cfg.partition)
	row("settings", "%d records · retain from LSN %d · delete delay %s",
		d.cfg.records, d.cfg.retainFrom, d.cfg.deleteDelay)

	d.log, err = objlog.Open(objlog.Options{Store: d.p.store})
	if err != nil {
		return fmt.Errorf("open log: %w", err)
	}
	defer d.log.Close()
	defer d.closeWriter()

	afterWrite, snapshot, err := d.write(ctx)
	if err != nil {
		return err
	}
	if err := d.read(ctx, snapshot); err != nil {
		return err
	}
	if err := d.resume(ctx); err != nil {
		return err
	}
	afterTail, err := d.tail(ctx, snapshot.Head.NextLSN, afterWrite)
	if err != nil {
		return err
	}
	retained, afterRetention, err := d.retain(ctx, afterTail)
	if err != nil {
		return err
	}
	afterGC, err := d.collect(ctx, afterRetention)
	if err != nil {
		return err
	}
	return d.summarize(ctx, afterWrite, afterTail, afterRetention, afterGC, retained)
}

func (d *demo) closeWriter() {
	if d.writer != nil {
		_ = d.writer.Abort(context.Background())
	}
}

func (d *demo) write(ctx context.Context) (ledger, objlog.Snapshot, error) {
	section(1, "write", "appends are accepted locally; a flush is what publishes them")

	writer, err := d.log.OpenWriter(ctx, objlog.WriterOptions{
		Partition: d.cfg.partition,
		WriterID:  uuid.New(),
		// One record per segment keeps the object count easy to follow. Real
		// writers batch by MaxDelay, MaxBytes, or MaxRecords.
		Batch: objlog.BatchPolicy{MaxRecords: 1},
	})
	if err != nil {
		return ledger{}, objlog.Snapshot{}, fmt.Errorf("open writer: %w", err)
	}
	d.writer = writer

	row("writer", "opened on partition %d, fenced through the catalog", d.cfg.partition)
	row("batching", "1 record per segment (demo setting)")

	base := time.Now().UnixMilli()
	d.baseTS = base
	for i := 0; i < d.cfg.records; i++ {
		if _, err := writer.Append(ctx, objlog.Record{
			TimestampMS: base + int64(i)*100,
			Headers:     []objlog.Header{{Key: []byte("source"), Value: []byte("demo")}},
			Value:       []byte(fmt.Sprintf("order-%04d", i)),
		}); err != nil {
			return ledger{}, objlog.Snapshot{}, fmt.Errorf("append %d: %w (writer: %v)", i, err, writer.Err())
		}
	}
	d.written = d.cfg.records
	row("appended", "%d records, LSN 0 → %d — none visible yet", d.cfg.records, d.cfg.records-1)

	snapshot, err := writer.Flush(ctx)
	if err != nil {
		return ledger{}, objlog.Snapshot{}, fmt.Errorf("flush: %w", err)
	}
	row("flush", "published %d segments", snapshot.Head.SegmentCount)
	row("head", "nextLSN %d · oldestLSN %d · reachable %d",
		snapshot.Head.NextLSN, snapshot.Head.OldestLSN, snapshot.Head.ReachableSegmentCount)

	state, err := d.snapshotBucket(ctx)
	if err != nil {
		return ledger{}, objlog.Snapshot{}, err
	}
	row("bucket", "%s", state)
	if d.cfg.verbose {
		keyList(state, d.prefix)
	}
	return state, snapshot, nil
}

func (d *demo) read(ctx context.Context, snapshot objlog.Snapshot) error {
	section(2, "read", "readers use the catalog, then range-read objects; the writer is never involved")

	reader := d.log.Reader()

	batch, err := reader.Partition(d.cfg.partition).Read(ctx, objlog.ReadRequest{
		StartLSN: 0, Limit: d.cfg.records, Freshness: objlog.FreshnessLatest,
	})
	if err != nil {
		return fmt.Errorf("read from LSN 0: %w", err)
	}
	row("by LSN", "from 0 → %d records, %q … %q",
		len(batch.Records), batch.Records[0].Value, batch.Records[len(batch.Records)-1].Value)

	offset := int64(d.cfg.records/2) * 100
	seek, err := reader.ConsumeFromTimestamp(ctx, objlog.ConsumeFromTimestampRequest{
		Partition: d.cfg.partition, TimestampMS: d.baseTS + offset, Limit: 3,
	})
	if err != nil {
		return fmt.Errorf("seek by timestamp: %w", err)
	}
	row("by timestamp", "first record at or after base+%dms → LSN %d, %q",
		offset, seek.Records[0].LSN, seek.Records[0].Value)

	last := snapshot.Head.NextLSN - 1
	one, err := reader.Fetch(ctx, objlog.FetchRequest{Partition: d.cfg.partition, LSN: last})
	if err != nil {
		return fmt.Errorf("fetch LSN %d: %w", last, err)
	}
	row("by exact LSN", "%d → found=%v, %q", last, one.Found, one.Record.Value)
	return nil
}

func (d *demo) resume(ctx context.Context) error {
	section(3, "resume", "a cursor is a position you can persist and pick up in another process")

	partition := d.log.Reader().Partition(d.cfg.partition)
	batch := max(2, d.cfg.records/3)

	cursor, err := partition.Cursor(objlog.CursorOptions{StartLSN: 0, Limit: batch})
	if err != nil {
		return fmt.Errorf("open cursor: %w", err)
	}
	row("cursor", "opened at LSN 0, %d records per call", batch)

	first, err := cursor.Next(ctx)
	if err != nil {
		_ = cursor.Close()
		return fmt.Errorf("cursor next: %w", err)
	}
	row("next", "%d records, %q … %q — position now %d",
		len(first.Records), first.Records[0].Value,
		first.Records[len(first.Records)-1].Value, cursor.Position())

	checkpoint, err := cursor.Checkpoint(ctx)
	if err != nil {
		_ = cursor.Close()
		return fmt.Errorf("checkpoint: %w", err)
	}
	encoded, err := json.Marshal(checkpoint)
	if err != nil {
		_ = cursor.Close()
		return fmt.Errorf("encode checkpoint: %w", err)
	}
	if err := cursor.Close(); err != nil {
		return fmt.Errorf("close cursor: %w", err)
	}
	row("checkpoint", "%s", encoded)
	row("", "persist the whole value — NextLSN alone is not resumable")
	row("consumer exits", "cursor closed; only those bytes survive")

	var restored objlog.CursorCheckpoint
	if err := json.Unmarshal(encoded, &restored); err != nil {
		return fmt.Errorf("decode checkpoint: %w", err)
	}
	resumed, err := partition.ResumeCursor(ctx, restored, objlog.CursorResumeOptions{Limit: batch})
	if err != nil {
		return fmt.Errorf("resume cursor: %w", err)
	}
	defer resumed.Close()

	second, err := resumed.Next(ctx)
	if err != nil {
		return fmt.Errorf("resumed next: %w", err)
	}
	row("resumed", "%d records, %q … %q — nothing repeated, nothing skipped",
		len(second.Records), second.Records[0].Value,
		second.Records[len(second.Records)-1].Value)
	row("validated", "against the live head: another stream, partition, or an")
	row("", "LSN below the retention floor is refused, not silently reset")

	d.checkpoint = restored
	return nil
}

func (d *demo) tail(ctx context.Context, from uint64, before ledger) (ledger, error) {
	section(4, "tail", "a watch refreshes the catalog in the background; a tailer blocks until new records land")

	watch, err := d.log.Reader().Watch(ctx, objlog.WatchOptions{
		Partitions: []uint32{d.cfg.partition},
	})
	if err != nil {
		return ledger{}, fmt.Errorf("open watch: %w", err)
	}
	defer watch.Close()
	row("watch", "background catalog refresh started for partition %d", d.cfg.partition)

	tailer, err := watch.Tail(objlog.TailOptions{
		Partition: d.cfg.partition,
		StartLSN:  from,
		Limit:     16,
	})
	if err != nil {
		return ledger{}, fmt.Errorf("open tailer: %w", err)
	}
	defer tailer.Close()
	row("tailer", "waiting at LSN %d — the log is caught up, so Next blocks", from)

	// Append from another goroutine once the tailer is parked, so the wake-up
	// is a real publish rather than a batch that was already sitting there.
	appended := make(chan error, 1)
	go func() {
		if err := sleep(ctx, 300*time.Millisecond); err != nil {
			appended <- err
			return
		}
		// Record timestamps must not go backwards within a partition, so the
		// late records continue the series the write phase started.
		for i := 0; i < tailRecords; i++ {
			if _, err := d.writer.Append(ctx, objlog.Record{
				TimestampMS: d.baseTS + int64(d.written+i)*100,
				Value:       []byte(fmt.Sprintf("late-%04d", i)),
			}); err != nil {
				appended <- err
				return
			}
		}
		_, err := d.writer.Flush(ctx)
		appended <- err
	}()

	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	start := time.Now()
	var (
		records []objlog.ReadRecord
		wakes   int
	)
	for len(records) < tailRecords {
		batch, err := tailer.Next(waitCtx)
		if err != nil {
			// A failure in the appending goroutine shows up here as a timeout;
			// report the real cause when there is one.
			select {
			case appendErr := <-appended:
				if appendErr != nil {
					return ledger{}, fmt.Errorf("append while tailing: %w", appendErr)
				}
			default:
			}
			return ledger{}, fmt.Errorf("tail next: %w", err)
		}
		wakes++
		if wakes == 1 {
			row("first wake", "after %s, %d record(s) — no polling loop in the caller",
				time.Since(start).Round(time.Millisecond), len(batch.Records))
		}
		records = append(records, batch.Records...)
	}
	if err := <-appended; err != nil {
		return ledger{}, fmt.Errorf("append while tailing: %w", err)
	}
	d.written += tailRecords

	row("writer", "appended %d more records and flushed while the tailer waited", tailRecords)
	row("tailed", "%d records over %d wake(s), %q … %q",
		len(records), wakes, records[0].Value, records[len(records)-1].Value)
	row("position", "tailer now at LSN %d", tailer.Position())

	state, err := d.snapshotBucket(ctx)
	if err != nil {
		return ledger{}, err
	}
	row("bucket", "%s  (%s)", state, state.delta(before))
	return state, nil
}

func (d *demo) retain(ctx context.Context, before ledger) (objlog.RetentionResult, ledger, error) {
	section(5, "retention", "retention changes what readers can see; it deletes nothing")

	head, err := d.log.Reader().Partition(d.cfg.partition).Head(ctx)
	if err != nil {
		return objlog.RetentionResult{}, ledger{}, fmt.Errorf("read head: %w", err)
	}

	if _, err := d.log.RequestRetention(ctx, objlog.RetentionRequest{
		Partition: d.cfg.partition, PolicyVersion: 1, BeforeLSN: d.cfg.retainFrom,
	}); err != nil {
		return objlog.RetentionResult{}, ledger{}, fmt.Errorf("request retention: %w", err)
	}
	row("request", "before LSN %d, policy v1 — intent only, head unchanged", d.cfg.retainFrom)

	applied, err := d.writer.ApplyRetention(ctx)
	if err != nil {
		return objlog.RetentionResult{}, ledger{}, fmt.Errorf("apply retention: %w", err)
	}
	row("writer applies", "through its own fence → oldestLSN %d → %d · reachable %d → %d",
		head.OldestLSN, applied.Snapshot.Head.OldestLSN,
		head.ReachableSegmentCount, applied.Snapshot.Head.ReachableSegmentCount)
	if applied.Snapshot.Head.OldestLSN < applied.RequestedLSN {
		row("", "kept a whole segment: oldestLSN %d is below the requested %d",
			applied.Snapshot.Head.OldestLSN, applied.RequestedLSN)
	}

	_, err = d.log.Reader().Partition(d.cfg.partition).Read(ctx, objlog.ReadRequest{
		StartLSN: 0, Limit: 1, Freshness: objlog.FreshnessLatest,
	})
	var expired objlog.LSNExpiredError
	switch {
	case err == nil:
		return objlog.RetentionResult{}, ledger{}, errors.New("read below the retention boundary unexpectedly succeeded")
	case !errors.As(err, &expired):
		return objlog.RetentionResult{}, ledger{}, fmt.Errorf("read below retention: %w", err)
	}
	row("read at LSN 0", "LSNExpiredError: requested %d, oldest %d", expired.Requested, expired.Oldest)

	_, resumeErr := d.log.Reader().Partition(d.cfg.partition).ResumeCursor(ctx, d.checkpoint,
		objlog.CursorResumeOptions{Limit: 1})
	var stale objlog.LSNExpiredError
	switch {
	case errors.As(resumeErr, &stale):
		row("checkpoint", "the one saved in step 3 (NextLSN %d) no longer resumes: expired below %d",
			d.checkpoint.NextLSN, stale.Oldest)
	case resumeErr != nil:
		return objlog.RetentionResult{}, ledger{}, fmt.Errorf("resume stale checkpoint: %w", resumeErr)
	default:
		row("checkpoint", "the one saved in step 3 (NextLSN %d) still resumes — it is above the floor",
			d.checkpoint.NextLSN)
	}

	state, err := d.snapshotBucket(ctx)
	if err != nil {
		return objlog.RetentionResult{}, ledger{}, err
	}
	row("bucket", "%s  (%s)", state, state.delta(before))
	row("", "no segment was deleted — the head shrank as it dropped references,")
	row("", "and the one new object is the request itself")
	if d.cfg.verbose {
		keyList(state, d.prefix)
	}
	return applied, state, nil
}

func (d *demo) collect(ctx context.Context, before ledger) (ledger, error) {
	section(6, "garbage collection", "two passes: observe what became unreachable, delete it after a grace period")

	reclaimer, err := d.p.store.NewReclaimer(lifecycle.Options{
		OwnerID:           uuid.New(),
		DeleteDelay:       d.cfg.deleteDelay,
		MaxObjectsPerRun:  512,
		MaxDeletesPerRun:  512,
		DeleteBatchSize:   8,
		DeleteConcurrency: 4,
	})
	if err != nil {
		return ledger{}, fmt.Errorf("new reclaimer: %w", err)
	}

	if _, err := reclaimer.RunPartition(ctx, d.cfg.partition); err != nil {
		return ledger{}, fmt.Errorf("reclaim (observe): %w", err)
	}
	row("pass 1 observe", "wrote gc state naming the orphaned objects — still nothing deleted")
	row("grace period", "%s, so in-flight reads can finish", d.cfg.deleteDelay)

	if err := sleep(ctx, d.cfg.deleteDelay+250*time.Millisecond); err != nil {
		return ledger{}, err
	}

	scheduler, err := lifecycle.NewScheduler(reclaimer, lifecycle.SchedulerOptions{
		MaxConcurrentPartitions: 1,
		PartitionRunTimeout:     30 * time.Second,
	})
	if err != nil {
		return ledger{}, fmt.Errorf("new scheduler: %w", err)
	}
	summary, err := scheduler.Run(ctx, []lifecycle.Task{
		{Partition: d.cfg.partition, Operation: lifecycle.OperationReclaim},
	})
	if err != nil {
		return ledger{}, fmt.Errorf("reclaim (delete): %w", err)
	}
	row("pass 2 reclaim", "completed %d · failed %d · deferred %d",
		summary.Completed, summary.Failed, summary.Deferred)

	scrub, err := scheduler.Run(ctx, []lifecycle.Task{
		{Partition: d.cfg.partition, Operation: lifecycle.OperationScrub},
	})
	if err != nil {
		return ledger{}, fmt.Errorf("scrub: %w", err)
	}
	row("scrub", "completed %d · failed %d — hunts orphans the catalog never referenced",
		scrub.Completed, scrub.Failed)

	state, err := d.snapshotBucket(ctx)
	if err != nil {
		return ledger{}, err
	}
	row("bucket", "%s  (%s)", state, state.delta(before))
	if d.cfg.verbose {
		keyList(state, d.prefix)
	}
	return state, nil
}

func (d *demo) summarize(ctx context.Context, afterWrite, afterTail, afterRetention, afterGC ledger, retained objlog.RetentionResult) error {
	sectionPlain("summary")

	tail, err := d.log.Reader().Partition(d.cfg.partition).Read(ctx, objlog.ReadRequest{
		StartLSN: retained.Snapshot.Head.OldestLSN, Limit: d.written, Freshness: objlog.FreshnessLatest,
	})
	if err != nil {
		return fmt.Errorf("read retained range: %w", err)
	}

	row("objects", "%d write → %d tail → %d retention → %d gc",
		afterWrite.total(), afterTail.total(), afterRetention.total(), afterGC.total())
	row("", "tail %s; retention %s; gc %s",
		afterTail.delta(afterWrite), afterRetention.delta(afterTail), afterGC.delta(afterRetention))
	row("bytes", "%s → %s (%s reclaimed)",
		humanBytes(afterWrite.bytes), humanBytes(afterGC.bytes),
		humanBytes(afterWrite.bytes-afterGC.bytes))
	row("records", "%d written (%d of them while a tailer followed) · %d still readable, from LSN %d",
		d.written, tailRecords, len(tail.Records), retained.Snapshot.Head.OldestLSN)
	row("stored in", "%s and nowhere else — no broker, no local state", d.p.container)
	fmt.Println()
	return nil
}

func (d *demo) snapshotBucket(ctx context.Context) (ledger, error) {
	objects, err := d.p.listObjects(ctx)
	if err != nil {
		return ledger{}, fmt.Errorf("list objects: %w", err)
	}
	return newLedger(objects), nil
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "demo: %v\n", err)
	os.Exit(1)
}
