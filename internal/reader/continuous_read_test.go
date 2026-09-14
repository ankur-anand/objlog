package reader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	blobcache "github.com/ankur-anand/objlog/internal/blob/cache"
	"github.com/ankur-anand/objlog/internal/catalog"
	"github.com/ankur-anand/objlog/internal/pmeta"
)

func TestCursorAutoReusesWholeSegmentAcrossBatches(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	segment := fixture.appendSegment(t, 0, 20)
	store := newCountingSegmentStore(fixture.store)
	r, err := New(fixture.catalog, store, Options{
		MaxRecordsPerBatch:     5,
		ReadStrategy:           ReadAuto,
		MaxWholeSegmentBytes:   segment.SizeBytes,
		WholeSegmentCacheBytes: segment.SizeBytes,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	cursor, err := r.Partition(fixture.partition).Cursor(CursorOptions{StartLSN: 0, Limit: 5})
	if err != nil {
		t.Fatalf("Cursor() error = %v", err)
	}

	for batch := 0; batch < 4; batch++ {
		result, err := cursor.Next(context.Background())
		if err != nil {
			t.Fatalf("Next(%d) error = %v", batch, err)
		}
		assertRecordsEqual(t, result.Records, fixture.records[batch*5:(batch+1)*5])
		if got := wholeCachePinnedRefs(r); got != 0 {
			t.Fatalf("pinned whole-cache refs after Next(%d) = %d, want 0", batch, got)
		}
	}
	if got := store.rangeReadCount(segment.URI, 0, segment.SizeBytes); got != 1 {
		t.Fatalf("whole-object reads = %d, want one cached read", got)
	}
	if got := store.readCount(); got != 1 {
		t.Fatalf("total object-store reads = %d, want cached whole-object reuse", got)
	}
}

func TestCursorSegmentHintAvoidsRepeatedCatalogListings(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	segment := fixture.appendSegment(t, 0, 20)
	cat := &countingReaderCatalog{Reader: fixture.catalog}
	r, err := New(cat, fixture.store, Options{
		MaxRecordsPerBatch:     5,
		ReadStrategy:           ReadAuto,
		MaxWholeSegmentBytes:   segment.SizeBytes,
		WholeSegmentCacheBytes: segment.SizeBytes,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	cursor, err := r.Partition(fixture.partition).Cursor(CursorOptions{StartLSN: 0, Limit: 5})
	if err != nil {
		t.Fatalf("Cursor() error = %v", err)
	}
	for batch := 0; batch < 4; batch++ {
		if _, err := cursor.Next(context.Background()); err != nil {
			t.Fatalf("Next(%d) error = %v", batch, err)
		}
	}
	if got := cat.ListCalls(); got != 1 {
		t.Fatalf("ListSegments calls = %d, want one segment resolution", got)
	}
}

func TestTailerAutoReusesWholeSegmentAcrossBatches(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	segment := fixture.appendSegment(t, 0, 20)
	store := newCountingSegmentStore(fixture.store)
	r, err := New(fixture.catalog, store, Options{
		MaxRecordsPerBatch:     5,
		ReadStrategy:           ReadAuto,
		MaxWholeSegmentBytes:   segment.SizeBytes,
		WholeSegmentCacheBytes: segment.SizeBytes,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = r.Close() }()
	watch, err := r.Watch(context.Background(), WatchOptions{Partitions: []uint32{fixture.partition}})
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	defer func() { _ = watch.Close() }()
	tailer, err := watch.Tail(TailOptions{Partition: fixture.partition, StartLSN: 0, Limit: 5})
	if err != nil {
		t.Fatalf("Tail() error = %v", err)
	}

	for batch := 0; batch < 4; batch++ {
		result, err := tailer.Next(context.Background())
		if err != nil {
			t.Fatalf("Next(%d) error = %v", batch, err)
		}
		assertRecordsEqual(t, result.Records, fixture.records[batch*5:(batch+1)*5])
		if got := wholeCachePinnedRefs(r); got != 0 {
			t.Fatalf("pinned whole-cache refs after Next(%d) = %d, want 0", batch, got)
		}
	}
	if got := store.rangeReadCount(segment.URI, 0, segment.SizeBytes); got != 1 {
		t.Fatalf("whole-object reads = %d, want one cached read", got)
	}
	if got := store.readCount(); got != 1 {
		t.Fatalf("total object-store reads = %d, want cached whole-object reuse", got)
	}
}

func TestTailerClearsSegmentHintWhenRetentionExpiresPosition(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	first := fixture.appendSegment(t, 0, 10)
	fixture.appendSegment(t, 10, 10)
	r := fixture.openReader(t, Options{
		MaxRecordsPerBatch:     5,
		ReadStrategy:           ReadAuto,
		MaxWholeSegmentBytes:   first.SizeBytes,
		WholeSegmentCacheBytes: first.SizeBytes,
	})
	defer func() { _ = r.Close() }()
	watch, err := r.Watch(context.Background(), WatchOptions{Partitions: []uint32{fixture.partition}})
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	tailer, err := watch.Tail(TailOptions{Partition: fixture.partition, StartLSN: 0, Limit: 5})
	if err != nil {
		t.Fatalf("Tail() error = %v", err)
	}
	if _, err := tailer.Next(context.Background()); err != nil {
		t.Fatalf("Next(first) error = %v", err)
	}
	if tailerSegmentHint(tailer) == nil {
		t.Fatal("first Next retained no segment hint")
	}

	request := catalog.RetentionRequest{
		Version:       catalog.RetentionRequestVersion,
		PolicyVersion: 1,
		BeforeLSN:     10,
		CreatedUnixMS: 1,
	}
	if _, err := fixture.catalog.RequestRetention(context.Background(), fixture.partition, request); err != nil {
		t.Fatalf("RequestRetention() error = %v", err)
	}
	retention := fixture.session.(catalog.RetentionWriterSession)
	if _, err := retention.ApplyPendingRetention(context.Background()); err != nil {
		t.Fatalf("ApplyPendingRetention() error = %v", err)
	}
	if _, err := r.refresh.refresh(context.Background(), fixture.partition); err != nil {
		t.Fatalf("refresh() error = %v", err)
	}

	if _, err := tailer.Next(context.Background()); !errors.Is(err, ErrLSNExpired) {
		t.Fatalf("Next(after retention) error = %v, want %v", err, ErrLSNExpired)
	}
	if hint := tailerSegmentHint(tailer); hint != nil {
		t.Fatalf("Tailer retained stale segment hint %+v after expiration", *hint)
	}
}

func TestCursorCancellationKeepsPositionWithoutPin(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	segment := fixture.appendSegment(t, 0, 10)
	r := fixture.openReader(t, Options{
		MaxRecordsPerBatch:     5,
		ReadStrategy:           ReadAuto,
		MaxWholeSegmentBytes:   segment.SizeBytes,
		WholeSegmentCacheBytes: segment.SizeBytes,
	})
	cursor, err := r.Partition(fixture.partition).Cursor(CursorOptions{StartLSN: 0, Limit: 5})
	if err != nil {
		t.Fatalf("Cursor() error = %v", err)
	}
	if _, err := cursor.Next(context.Background()); err != nil {
		t.Fatalf("Next(first) error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := cursor.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next(cancelled) error = %v, want %v", err, context.Canceled)
	}
	if got := cursor.Position(); got != 5 {
		t.Fatalf("cursor position = %d, want 5", got)
	}
	if got := wholeCachePinnedRefs(r); got != 0 {
		t.Fatalf("pinned whole-cache refs after cancellation = %d, want 0", got)
	}
}

func TestCursorPreservesCrossSegmentBatchIndependentOfStrategy(t *testing.T) {
	t.Parallel()

	for _, strategy := range []ReadStrategy{ReadRanges, ReadAuto} {
		strategy := strategy
		t.Run(strategyName(strategy), func(t *testing.T) {
			t.Parallel()
			fixture := newReaderFixture(t)
			first := fixture.appendSegment(t, 0, 10)
			second := fixture.appendSegment(t, 10, 10)
			store := newCountingSegmentStore(fixture.store)
			opts := Options{MaxRecordsPerBatch: 15, ReadStrategy: strategy}
			if strategy == ReadAuto {
				opts.MaxWholeSegmentBytes = max(first.SizeBytes, second.SizeBytes)
				opts.WholeSegmentCacheBytes = opts.MaxWholeSegmentBytes
			}
			r, err := New(fixture.catalog, store, opts)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			cursor, err := r.Partition(fixture.partition).Cursor(CursorOptions{StartLSN: 0, Limit: 15})
			if err != nil {
				t.Fatalf("Cursor() error = %v", err)
			}

			result, err := cursor.Next(context.Background())
			if err != nil {
				t.Fatalf("Next(first) error = %v", err)
			}
			assertRecordsEqual(t, result.Records, fixture.records[:15])
			if got := cursor.Position(); got != 15 {
				t.Fatalf("cursor position = %d, want 15", got)
			}
			if strategy == ReadAuto {
				if got := store.rangeReadCount(second.URI, 0, second.SizeBytes); got != 1 {
					t.Fatalf("second segment whole-object reads = %d, want 1", got)
				}
			}
		})
	}
}

func TestCursorInCallRetryAfterSecondSegmentFailureReusesFirstWholeObject(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	first := fixture.appendSegment(t, 0, 10)
	second := fixture.appendSegment(t, 10, 10)
	counting := newCountingSegmentStore(fixture.store)
	store := &failOnceRangeStore{
		inner: counting,
		key:   segmentReadKey{uri: second.URI, off: 0, n: second.SizeBytes},
	}
	r, err := New(fixture.catalog, store, Options{
		MaxRecordsPerBatch:     15,
		ReadStrategy:           ReadAuto,
		MaxWholeSegmentBytes:   max(first.SizeBytes, second.SizeBytes),
		WholeSegmentCacheBytes: first.SizeBytes + second.SizeBytes,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	cursor, err := r.Partition(fixture.partition).Cursor(CursorOptions{StartLSN: 0, Limit: 15})
	if err != nil {
		t.Fatalf("Cursor() error = %v", err)
	}

	result, err := cursor.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	assertRecordsEqual(t, result.Records, fixture.records[:15])
	if got := cursor.Position(); got != 15 {
		t.Fatalf("cursor position = %d, want 15", got)
	}
	if got := counting.rangeReadCount(first.URI, 0, first.SizeBytes); got != 1 {
		t.Fatalf("first segment whole-object reads = %d, want cached reuse on retry", got)
	}
}

func TestCursorReportsLatestCachedHeadInsideWholeSegment(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	first := fixture.appendSegment(t, 0, 10)
	r := fixture.openReader(t, Options{
		MaxRecordsPerBatch:     5,
		ReadStrategy:           ReadAuto,
		MaxWholeSegmentBytes:   first.SizeBytes,
		WholeSegmentCacheBytes: first.SizeBytes,
	})
	cursor, err := r.Partition(fixture.partition).Cursor(CursorOptions{StartLSN: 0, Limit: 5})
	if err != nil {
		t.Fatalf("Cursor() error = %v", err)
	}
	firstResult, err := cursor.Next(context.Background())
	if err != nil {
		t.Fatalf("Next(first) error = %v", err)
	}
	if firstResult.Head.NextLSN != 10 {
		t.Fatalf("first head next_lsn = %d, want 10", firstResult.Head.NextLSN)
	}

	fixture.appendSegment(t, 10, 10)
	if _, err := r.refresh.refresh(context.Background(), fixture.partition); err != nil {
		t.Fatalf("refresh() error = %v", err)
	}
	secondResult, err := cursor.Next(context.Background())
	if err != nil {
		t.Fatalf("Next(second) error = %v", err)
	}
	if secondResult.Head.NextLSN != 20 {
		t.Fatalf("second head next_lsn = %d, want latest cached head 20", secondResult.Head.NextLSN)
	}
	assertRecordsEqual(t, secondResult.Records, fixture.records[5:10])
}

func TestCursorHonorsCachedRetentionInsideWholeSegment(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	first := fixture.appendSegment(t, 0, 10)
	fixture.appendSegment(t, 10, 10)
	r := fixture.openReader(t, Options{
		MaxRecordsPerBatch:     5,
		ReadStrategy:           ReadAuto,
		MaxWholeSegmentBytes:   first.SizeBytes,
		WholeSegmentCacheBytes: first.SizeBytes,
	})
	cursor, err := r.Partition(fixture.partition).Cursor(CursorOptions{StartLSN: 0, Limit: 5})
	if err != nil {
		t.Fatalf("Cursor() error = %v", err)
	}
	if _, err := cursor.Next(context.Background()); err != nil {
		t.Fatalf("Next(first) error = %v", err)
	}

	request := catalog.RetentionRequest{
		Version:       catalog.RetentionRequestVersion,
		PolicyVersion: 1,
		BeforeLSN:     10,
		CreatedUnixMS: 1,
	}
	if _, err := fixture.catalog.RequestRetention(context.Background(), fixture.partition, request); err != nil {
		t.Fatalf("RequestRetention() error = %v", err)
	}
	retention := fixture.session.(catalog.RetentionWriterSession)
	if _, err := retention.ApplyPendingRetention(context.Background()); err != nil {
		t.Fatalf("ApplyPendingRetention() error = %v", err)
	}
	if _, err := r.refresh.refresh(context.Background(), fixture.partition); err != nil {
		t.Fatalf("refresh() error = %v", err)
	}

	if _, err := cursor.Next(context.Background()); !errors.Is(err, ErrLSNExpired) {
		t.Fatalf("Next() error = %v, want %v", err, ErrLSNExpired)
	}
}

func TestIdleCursorDoesNotPreventWholeCacheEviction(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	firstSegment := fixture.appendSegment(t, 0, 20)
	secondSegment := fixture.appendSegment(t, 20, 20)
	store := newCountingSegmentStore(fixture.store)
	budget := max(firstSegment.SizeBytes, secondSegment.SizeBytes)
	r, err := New(fixture.catalog, store, Options{
		MaxRecordsPerBatch:     15,
		ReadStrategy:           ReadAuto,
		MaxWholeSegmentBytes:   budget,
		WholeSegmentCacheBytes: budget,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	partition := r.Partition(fixture.partition)
	first, err := partition.Cursor(CursorOptions{StartLSN: 0, Limit: 15})
	if err != nil {
		t.Fatalf("Cursor(first) error = %v", err)
	}
	if _, err := first.Next(context.Background()); err != nil {
		t.Fatalf("first.Next() error = %v", err)
	}
	if got := wholeCachePinnedRefs(r); got != 0 {
		t.Fatalf("idle cursor pinned refs = %d, want 0", got)
	}

	second, err := partition.Cursor(CursorOptions{StartLSN: 20, Limit: 15})
	if err != nil {
		t.Fatalf("Cursor(second) error = %v", err)
	}
	if _, err := second.Next(context.Background()); err != nil {
		t.Fatalf("second.Next() error = %v", err)
	}
	if got := store.rangeReadCount(secondSegment.URI, 0, secondSegment.SizeBytes); got != 1 {
		t.Fatalf("second segment whole-object reads = %d, want 1 after evicting idle entry", got)
	}

	result, err := first.Next(context.Background())
	if err != nil {
		t.Fatalf("first.Next(after eviction) error = %v", err)
	}
	assertRecordsEqual(t, result.Records, fixture.records[15:30])
	if got := store.rangeReadCount(firstSegment.URI, 0, firstSegment.SizeBytes); got != 1 {
		t.Fatalf("first segment whole-object reads = %d, want no narrow-tail redownload", got)
	}
}

func TestWholeCacheBudgetFallbackEmitsMetric(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	firstSegment := fixture.appendSegment(t, 0, 10)
	secondSegment := fixture.appendSegment(t, 10, 10)
	store := newCountingSegmentStore(fixture.store)
	observer := make(metricChannel, 32)
	budget := max(firstSegment.SizeBytes, secondSegment.SizeBytes)
	r, err := New(fixture.catalog, store, Options{
		MaxRecordsPerBatch:     10,
		ReadStrategy:           ReadAuto,
		MaxWholeSegmentBytes:   budget,
		WholeSegmentCacheBytes: budget,
		Observer:               observer,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	lease, available, err := r.wholeSegments.Acquire(context.Background(), r.wholeStore, firstSegment, r.opts.SegmentOptions)
	if err != nil || !available {
		t.Fatalf("Acquire(first) = available:%t error:%v", available, err)
	}
	defer lease.Release()

	if _, err := r.Consume(context.Background(), ConsumeRequest{Partition: fixture.partition, StartLSN: 10, Limit: 10}); err != nil {
		t.Fatalf("Consume(second) error = %v", err)
	}
	found := false
	for len(observer) > 0 {
		event := <-observer
		if event.Name == MetricWholeReadFallback && event.SegmentURI == secondSegment.URI {
			found = true
		}
	}
	if !found {
		t.Fatal("cache-budget range fallback emitted no metric")
	}
}

func TestCursorForkCopiesHintWithoutPinningCache(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	segment := fixture.appendSegment(t, 0, 10)
	store := newCountingSegmentStore(fixture.store)
	r, err := New(fixture.catalog, store, Options{
		MaxRecordsPerBatch:     5,
		ReadStrategy:           ReadAuto,
		MaxWholeSegmentBytes:   segment.SizeBytes,
		WholeSegmentCacheBytes: segment.SizeBytes,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	cursor, err := r.Partition(fixture.partition).Cursor(CursorOptions{StartLSN: 0, Limit: 5})
	if err != nil {
		t.Fatalf("Cursor() error = %v", err)
	}
	if _, err := cursor.Next(context.Background()); err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	fork := cursor.Fork()
	if err := cursor.Close(); err != nil {
		t.Fatalf("cursor.Close() error = %v", err)
	}
	result, err := fork.Next(context.Background())
	if err != nil {
		t.Fatalf("fork.Next() error = %v", err)
	}
	assertRecordsEqual(t, result.Records, fixture.records[5:10])
	if got := store.rangeReadCount(segment.URI, 0, segment.SizeBytes); got != 1 {
		t.Fatalf("whole-object reads = %d, want cached reuse", got)
	}
	if got := wholeCachePinnedRefs(r); got != 0 {
		t.Fatalf("fork left pinned refs = %d, want 0", got)
	}
}

func TestTailerMetricObserverCanReadPosition(t *testing.T) {
	fixture := newReaderFixture(t)
	segment := fixture.appendSegment(t, 0, 10)
	var tailer *Tailer
	observed := make(chan struct{}, 1)
	observer := observerFunc(func(event MetricEvent) {
		if event.Name == MetricTailNext {
			_ = tailer.Position()
			observed <- struct{}{}
		}
	})
	r, err := New(fixture.catalog, fixture.store, Options{
		MaxRecordsPerBatch:     5,
		ReadStrategy:           ReadAuto,
		MaxWholeSegmentBytes:   segment.SizeBytes,
		WholeSegmentCacheBytes: segment.SizeBytes,
		Observer:               observer,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	watch, err := r.Watch(context.Background(), WatchOptions{Partitions: []uint32{fixture.partition}})
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	tailer, err = watch.Tail(TailOptions{Partition: fixture.partition, StartLSN: 0, Limit: 5})
	if err != nil {
		t.Fatalf("Tail() error = %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, nextErr := tailer.Next(context.Background())
		done <- nextErr
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Next() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Tailer.Next deadlocked while its observer called Position")
	}
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("tail-next metric was not observed")
	}
	_ = watch.Close()
	_ = r.Close()
}

func TestTailerCloseCancelsBlockedWholeRead(t *testing.T) {
	fixture := newReaderFixture(t)
	segment := fixture.appendSegment(t, 0, 10)
	store := &blockingSegmentStore{
		inner:   fixture.store,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	r, err := New(fixture.catalog, store, Options{
		MaxRecordsPerBatch:     5,
		ReadStrategy:           ReadAuto,
		MaxWholeSegmentBytes:   segment.SizeBytes,
		WholeSegmentCacheBytes: segment.SizeBytes,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	watch, err := r.Watch(context.Background(), WatchOptions{Partitions: []uint32{fixture.partition}})
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	tailer, err := watch.Tail(TailOptions{Partition: fixture.partition, StartLSN: 0, Limit: 5})
	if err != nil {
		t.Fatalf("Tail() error = %v", err)
	}
	nextDone := make(chan error, 1)
	go func() {
		_, nextErr := tailer.Next(context.Background())
		nextDone <- nextErr
	}()
	waitForLoadSignal(t, store.entered, "whole-segment read")

	closeDone := make(chan error, 1)
	go func() { closeDone <- tailer.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Tailer.Close() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Tailer.Close blocked behind an in-flight Next")
	}
	select {
	case err := <-nextDone:
		if !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("blocked Next error = %v, want tailer-closed error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Tailer.Close did not cancel the in-flight Next")
	}
	_ = watch.Close()
	_ = r.Close()
}

func TestTailerPositionDoesNotWaitForBlockedWholeRead(t *testing.T) {
	fixture := newReaderFixture(t)
	segment := fixture.appendSegment(t, 0, 10)
	store := &blockingSegmentStore{
		inner:   fixture.store,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	r, watch, tailer := newBlockedWholeReadTailer(t, fixture, store, segment)
	defer func() { _ = r.Close() }()

	nextDone := make(chan error, 1)
	go func() {
		_, nextErr := tailer.Next(context.Background())
		nextDone <- nextErr
	}()
	waitForLoadSignal(t, store.entered, "whole-segment read")

	positionDone := make(chan uint64, 1)
	go func() { positionDone <- tailer.Position() }()
	select {
	case position := <-positionDone:
		if position != 0 {
			t.Fatalf("Position() = %d, want 0", position)
		}
	case <-time.After(time.Second):
		t.Fatal("Tailer.Position blocked behind an in-flight Next")
	}

	_ = tailer.Close()
	if err := receiveLoadError(t, nextDone); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("blocked Next error = %v, want tailer-closed error", err)
	}
	_ = watch.Close()
}

func TestWatchCloseCancelsBlockedWholeRead(t *testing.T) {
	fixture := newReaderFixture(t)
	segment := fixture.appendSegment(t, 0, 10)
	store := &blockingSegmentStore{
		inner:   fixture.store,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	r, watch, tailer := newBlockedWholeReadTailer(t, fixture, store, segment)
	defer func() { _ = r.Close() }()

	nextDone := make(chan error, 1)
	go func() {
		_, nextErr := tailer.Next(context.Background())
		nextDone <- nextErr
	}()
	waitForLoadSignal(t, store.entered, "whole-segment read")

	if err := watch.Close(); err != nil {
		t.Fatalf("Watch.Close() error = %v", err)
	}
	if err := receiveLoadError(t, nextDone); !errors.Is(err, ErrWatchClosed) {
		t.Fatalf("blocked Next error = %v, want %v", err, ErrWatchClosed)
	}
}

func TestReaderCloseCancelsBlockedTailerWholeRead(t *testing.T) {
	fixture := newReaderFixture(t)
	segment := fixture.appendSegment(t, 0, 10)
	store := &blockingSegmentStore{
		inner:   fixture.store,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	r, _, tailer := newBlockedWholeReadTailer(t, fixture, store, segment)

	nextDone := make(chan error, 1)
	go func() {
		_, nextErr := tailer.Next(context.Background())
		nextDone <- nextErr
	}()
	waitForLoadSignal(t, store.entered, "whole-segment read")

	closeDone := make(chan error, 1)
	go func() { closeDone <- r.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Reader.Close() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Reader.Close blocked behind an in-flight Tailer.Next")
	}
	if err := receiveLoadError(t, nextDone); !errors.Is(err, ErrWatchClosed) {
		t.Fatalf("blocked Next error = %v, want %v", err, ErrWatchClosed)
	}
}

func newBlockedWholeReadTailer(t *testing.T, fixture *readerFixture, store SegmentStore, segment pmeta.SegmentRef) (*Reader, *Watch, *Tailer) {
	t.Helper()
	r, err := New(fixture.catalog, store, Options{
		MaxRecordsPerBatch:     5,
		ReadStrategy:           ReadAuto,
		MaxWholeSegmentBytes:   segment.SizeBytes,
		WholeSegmentCacheBytes: segment.SizeBytes,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	watch, err := r.Watch(context.Background(), WatchOptions{Partitions: []uint32{fixture.partition}})
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	tailer, err := watch.Tail(TailOptions{Partition: fixture.partition, StartLSN: 0, Limit: 5})
	if err != nil {
		t.Fatalf("Tail() error = %v", err)
	}
	return r, watch, tailer
}

func TestTailerResumesAfterPartitionIsReadded(t *testing.T) {
	fixture := newReaderFixture(t)
	r := fixture.openReader(t, Options{})
	defer func() { _ = r.Close() }()
	watch, err := r.Watch(context.Background(), WatchOptions{Partitions: []uint32{fixture.partition}})
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	defer func() { _ = watch.Close() }()
	tailer, err := watch.Tail(TailOptions{Partition: fixture.partition, StartLSN: 0, Limit: 1})
	if err != nil {
		t.Fatalf("Tail() error = %v", err)
	}

	first := make(chan error, 1)
	go func() {
		_, nextErr := tailer.Next(context.Background())
		first <- nextErr
	}()
	watch.RemovePartition(fixture.partition)
	select {
	case err := <-first:
		if !errors.Is(err, ErrPartitionNotWatched) {
			t.Fatalf("Next after RemovePartition error = %v, want %v", err, ErrPartitionNotWatched)
		}
	case <-time.After(time.Second):
		t.Fatal("RemovePartition did not unblock Tailer.Next")
	}

	if err := watch.AddPartition(fixture.partition); err != nil {
		t.Fatalf("AddPartition() error = %v", err)
	}
	fixture.appendSegment(t, 0, 1)
	result, err := tailer.Next(context.Background())
	if err != nil {
		t.Fatalf("Next after AddPartition error = %v", err)
	}
	assertRecordsEqual(t, result.Records, fixture.records)
}

func TestWholeReadBypassesRangeCache(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	segment := fixture.appendSegment(t, 0, 10)
	baseStore := newCountingSegmentStore(fixture.store)
	rangeCache := blobcache.NewLRU(segment.SizeBytes)
	hotKey := blobcache.Key{URI: "memory://hot-range", Off: 7, N: 1}
	rangeCache.Set(hotKey, []byte{1})
	cachedStore := blobcache.MustNewStore(baseStore, rangeCache)
	r, err := New(fixture.catalog, cachedStore, Options{
		MaxRecordsPerBatch:     5,
		ReadStrategy:           ReadAuto,
		MaxWholeSegmentBytes:   segment.SizeBytes,
		WholeSegmentCacheBytes: segment.SizeBytes,
		WholeSegmentStore:      baseStore,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	cursor, err := r.Partition(fixture.partition).Cursor(CursorOptions{StartLSN: 0, Limit: 5})
	if err != nil {
		t.Fatalf("Cursor() error = %v", err)
	}
	if _, err := cursor.Next(context.Background()); err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if _, ok := rangeCache.Get(hotKey); !ok {
		t.Fatal("whole-object read evicted an unrelated hot range")
	}
	wholeKey := blobcache.Key{URI: segment.URI, Off: 0, N: segment.SizeBytes}
	if _, ok := rangeCache.Get(wholeKey); ok {
		t.Fatal("whole object was inserted into the exact-range cache")
	}
}

func TestCursorWholeOpenMapsErrorAndObservesSegmentRead(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	segment := fixture.appendSegment(t, 0, 10)
	observer := make(metricChannel, 16)
	r, err := New(fixture.catalog, newTestSegmentStore(nil), Options{
		MaxRecordsPerBatch:     5,
		ReadStrategy:           ReadAuto,
		MaxWholeSegmentBytes:   segment.SizeBytes,
		WholeSegmentCacheBytes: segment.SizeBytes,
		Observer:               observer,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	cursor, err := r.Partition(fixture.partition).Cursor(CursorOptions{StartLSN: 0, Limit: 5})
	if err != nil {
		t.Fatalf("Cursor() error = %v", err)
	}
	_, err = cursor.Next(context.Background())
	if !errors.Is(err, ErrStoreRead) {
		t.Fatalf("Next() error = %v, want %v", err, ErrStoreRead)
	}
	found := false
	for len(observer) > 0 {
		event := <-observer
		if event.Name == MetricSegmentRead && event.SegmentURI == segment.URI {
			found = true
		}
	}
	if !found {
		t.Fatal("whole-open failure emitted no segment-read metric")
	}
}

func wholeCachePinnedRefs(r *Reader) int {
	r.wholeSegments.mu.Lock()
	defer r.wholeSegments.mu.Unlock()
	total := 0
	for _, entry := range r.wholeSegments.entries {
		total += entry.refs
	}
	return total
}

func tailerSegmentHint(tailer *Tailer) *pmeta.SegmentRef {
	tailer.mu.Lock()
	defer tailer.mu.Unlock()
	return cloneSegmentHint(tailer.segmentHint)
}

func strategyName(strategy ReadStrategy) string {
	switch strategy {
	case ReadRanges:
		return "ranges"
	case ReadWholeSegment:
		return "whole"
	case ReadAuto:
		return "auto"
	default:
		return "unknown"
	}
}

type observerFunc func(MetricEvent)

func (f observerFunc) Observe(event MetricEvent) {
	f(event)
}

type metricChannel chan MetricEvent

func (c metricChannel) Observe(event MetricEvent) {
	c <- event
}

type countingReaderCatalog struct {
	catalog.Reader
	mu        sync.Mutex
	listCalls int
}

type failOnceRangeStore struct {
	inner SegmentStore
	key   segmentReadKey
	once  sync.Once
}

func (s *failOnceRangeStore) ReadAt(ctx context.Context, uri string, off uint64, n uint64) ([]byte, error) {
	failed := false
	if (segmentReadKey{uri: uri, off: off, n: n}) == s.key {
		s.once.Do(func() { failed = true })
	}
	if failed {
		return nil, fmt.Errorf("injected read failure")
	}
	return s.inner.ReadAt(ctx, uri, off, n)
}

func (c *countingReaderCatalog) ListSegments(ctx context.Context, req catalog.ListSegmentsRequest) (pmeta.SegmentPage, error) {
	c.mu.Lock()
	c.listCalls++
	c.mu.Unlock()
	return c.Reader.ListSegments(ctx, req)
}

func (c *countingReaderCatalog) ListCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.listCalls
}

var _ Observer = observerFunc(nil)
var _ Observer = metricChannel(nil)
