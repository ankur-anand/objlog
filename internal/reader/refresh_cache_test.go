package reader

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/objlog/internal/catalog"
	"github.com/ankur-anand/objlog/internal/pmeta"
)

func TestNormalizeOptionsBoundsCachedPartitionHeads(t *testing.T) {
	normalized, err := normalizeOptions(Options{})
	if err != nil {
		t.Fatalf("normalizeOptions() error = %v", err)
	}
	if normalized.MaxCachedPartitionHeads != DefaultMaxCachedPartitionHeads {
		t.Fatalf("MaxCachedPartitionHeads = %d, want %d", normalized.MaxCachedPartitionHeads, DefaultMaxCachedPartitionHeads)
	}

	_, err = normalizeOptions(Options{MaxCachedPartitionHeads: -1})
	if !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("normalizeOptions(negative cache limit) error = %v, want %v", err, ErrInvalidOptions)
	}
}

func TestRefreshCoordinatorBoundsPassiveHeadsByLRU(t *testing.T) {
	cat := newHeadCacheCatalog()
	coordinator := newRefreshCoordinator(cat, RefreshPolicy{}, 2, nil)

	loadHead(t, coordinator, 1)
	loadHead(t, coordinator, 2)
	loadHead(t, coordinator, 1) // Partition 1 is now the most recently used.
	loadHead(t, coordinator, 3) // Partition 2 must be evicted.
	loadHead(t, coordinator, 1)
	loadHead(t, coordinator, 2)

	if calls := cat.loadCalls(1); calls != 1 {
		t.Fatalf("partition 1 LoadPartition() calls = %d, want 1", calls)
	}
	if calls := cat.loadCalls(2); calls != 2 {
		t.Fatalf("partition 2 LoadPartition() calls = %d, want 2 after LRU eviction", calls)
	}
	if calls := cat.loadCalls(3); calls != 1 {
		t.Fatalf("partition 3 LoadPartition() calls = %d, want 1", calls)
	}
}

func TestRefreshCoordinatorPinsWatchedHeadOutsidePassiveLimit(t *testing.T) {
	cat := newHeadCacheCatalog()
	coordinator := newRefreshCoordinator(cat, RefreshPolicy{}, 1, nil)
	coordinator.watchPartition(7)
	defer coordinator.unwatchPartition(7)

	loadHead(t, coordinator, 7)
	head, generation, ok := coordinator.snapshot(7)
	if !ok {
		t.Fatal("snapshot(7) not found")
	}
	changed, wait := coordinator.waitChannel(7, generation)
	if !wait {
		t.Fatal("waitChannel(7) did not return the active watch channel")
	}

	loadHead(t, coordinator, 8)
	loadHead(t, coordinator, 9)
	loadHead(t, coordinator, 7)
	if calls := cat.loadCalls(7); calls != 1 {
		t.Fatalf("watched partition LoadPartition() calls = %d, want 1", calls)
	}

	cat.setNextLSN(7, head.NextLSN+1)
	if _, err := coordinator.refresh(context.Background(), 7); err != nil {
		t.Fatalf("refresh(7) error = %v", err)
	}
	select {
	case <-changed:
	default:
		t.Fatal("watched partition head update did not wake waiter")
	}
}

func TestRefreshCoordinatorReusesUnchangedConditionalSnapshot(t *testing.T) {
	cat := newConditionalSnapshotCatalog(7, 10)
	coordinator := newRefreshCoordinator(cat, RefreshPolicy{PollInterval: time.Hour}, 1, nil)
	coordinator.watchPartition(7)
	defer coordinator.unwatchPartition(7)

	first := loadHead(t, coordinator, 7)
	_, generation, ok := coordinator.snapshot(7)
	if !ok {
		t.Fatal("initial conditional snapshot not cached")
	}
	changed, wait := coordinator.waitChannel(7, generation)
	if !wait {
		t.Fatal("initial watch channel missing")
	}

	unchanged, err := coordinator.refresh(context.Background(), 7)
	if err != nil {
		t.Fatalf("unchanged refresh error = %v", err)
	}
	if unchanged != first {
		t.Fatalf("unchanged refresh head = %+v, want %+v", unchanged, first)
	}
	if loads, refreshes := cat.calls(); loads != 1 || refreshes != 1 {
		t.Fatalf("conditional calls loads=%d refreshes=%d, want 1/1", loads, refreshes)
	}
	select {
	case <-changed:
		t.Fatal("unchanged conditional refresh woke watch")
	default:
	}
	if _, nextGeneration, _ := coordinator.snapshot(7); nextGeneration != generation {
		t.Fatalf("unchanged refresh generation=%d, want %d", nextGeneration, generation)
	}

	cat.setNextLSN(11)
	updated, err := coordinator.refresh(context.Background(), 7)
	if err != nil {
		t.Fatalf("changed refresh error = %v", err)
	}
	if updated.NextLSN != 11 {
		t.Fatalf("changed refresh next_lsn=%d, want 11", updated.NextLSN)
	}
	select {
	case <-changed:
	default:
		t.Fatal("changed conditional refresh did not wake watch")
	}
}

func TestRefreshCoordinatorFinalUnwatchWakesWaiterAndReleasesHead(t *testing.T) {
	cat := newHeadCacheCatalog()
	coordinator := newRefreshCoordinator(cat, RefreshPolicy{}, 1, nil)
	coordinator.watchPartition(7)
	loadHead(t, coordinator, 7)

	_, generation, ok := coordinator.snapshot(7)
	if !ok {
		t.Fatal("snapshot(7) not found")
	}
	changed, wait := coordinator.waitChannel(7, generation)
	if !wait {
		t.Fatal("waitChannel(7) did not return the active watch channel")
	}

	coordinator.unwatchPartition(7)
	select {
	case <-changed:
	default:
		t.Fatal("final unwatch did not wake waiter")
	}
	if _, wait := coordinator.waitChannel(7, generation); wait {
		t.Fatal("waitChannel(7) still waits after final unwatch")
	}

	loadHead(t, coordinator, 8)
	loadHead(t, coordinator, 7)
	if calls := cat.loadCalls(7); calls != 2 {
		t.Fatalf("released partition LoadPartition() calls = %d, want 2 after passive eviction", calls)
	}
}

func TestRefreshCoordinatorKeepsStateUntilFinalUnwatch(t *testing.T) {
	cat := newHeadCacheCatalog()
	coordinator := newRefreshCoordinator(cat, RefreshPolicy{}, 1, nil)
	coordinator.watchPartition(7)
	coordinator.watchPartition(7)
	loadHead(t, coordinator, 7)

	_, generation, ok := coordinator.snapshot(7)
	if !ok {
		t.Fatal("snapshot(7) not found")
	}
	changed, wait := coordinator.waitChannel(7, generation)
	if !wait {
		t.Fatal("waitChannel(7) did not return the active watch channel")
	}

	coordinator.unwatchPartition(7)
	if next, wait := coordinator.waitChannel(7, generation); !wait || next != changed {
		t.Fatal("first unwatch released a partition still owned by another watch")
	}

	coordinator.unwatchPartition(7)
	select {
	case <-changed:
	default:
		t.Fatal("final unwatch did not close the watch channel")
	}
}

func TestRefreshCoordinatorWaitChannelDoesNotCreateState(t *testing.T) {
	coordinator := newRefreshCoordinator(newHeadCacheCatalog(), RefreshPolicy{}, 1, nil)

	if _, wait := coordinator.waitChannel(7, 0); wait {
		t.Fatal("waitChannel() created notification state for an unwatched partition")
	}
}

func TestWatchRemovePartitionStopsBlockedWaiter(t *testing.T) {
	cat := newHeadCacheCatalog()
	r, err := New(cat, newTestSegmentStore(nil), Options{MaxCachedPartitionHeads: 1})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	watch, err := r.Watch(context.Background(), WatchOptions{Partitions: []uint32{7}})
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	defer watch.Close()
	loadHead(t, r.refresh, 7)
	_, generation, ok := r.refresh.snapshot(7)
	if !ok {
		t.Fatal("snapshot(7) not found")
	}

	done := make(chan error, 1)
	go func() {
		done <- watch.waitForAdvance(context.Background(), 7, generation)
	}()
	watch.RemovePartition(7)

	select {
	case err := <-done:
		if !errors.Is(err, ErrPartitionNotWatched) {
			t.Fatalf("waitForAdvance() error = %v, want %v", err, ErrPartitionNotWatched)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waitForAdvance() remained blocked after RemovePartition")
	}
}

func loadHead(t *testing.T, coordinator *refreshCoordinator, partition uint32) pmeta.PartitionHead {
	t.Helper()
	head, err := coordinator.head(context.Background(), partition)
	if err != nil {
		t.Fatalf("head(%d) error = %v", partition, err)
	}
	return head
}

type headCacheCatalog struct {
	mu      sync.Mutex
	calls   map[uint32]int
	nextLSN map[uint32]uint64
}

func newHeadCacheCatalog() *headCacheCatalog {
	return &headCacheCatalog{
		calls:   make(map[uint32]int),
		nextLSN: make(map[uint32]uint64),
	}
}

func (c *headCacheCatalog) LoadPartition(ctx context.Context, partition uint32) (pmeta.PartitionHead, error) {
	if err := ctx.Err(); err != nil {
		return pmeta.PartitionHead{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls[partition]++
	nextLSN := c.nextLSN[partition]
	if nextLSN == 0 {
		nextLSN = uint64(partition) + 1
	}
	return pmeta.PartitionHead{
		StreamID:    "stream-a",
		Partition:   partition,
		NextLSN:     nextLSN,
		WriterEpoch: 1,
	}, nil
}

func (c *headCacheCatalog) FindSegment(context.Context, uint32, uint64) (pmeta.SegmentRef, bool, error) {
	return pmeta.SegmentRef{}, false, nil
}

func (c *headCacheCatalog) LookupTimestamp(ctx context.Context, req catalog.TimestampLookupRequest) (catalog.TimestampLookupResult, error) {
	head, err := c.LoadPartition(ctx, req.Partition)
	return catalog.TimestampLookupResult{Head: head}, err
}

func (c *headCacheCatalog) ListSegments(context.Context, catalog.ListSegmentsRequest) (pmeta.SegmentPage, error) {
	return pmeta.SegmentPage{}, nil
}

func (c *headCacheCatalog) loadCalls(partition uint32) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[partition]
}

func (c *headCacheCatalog) setNextLSN(partition uint32, nextLSN uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextLSN[partition] = nextLSN
}

type conditionalSnapshotCatalog struct {
	mu        sync.Mutex
	partition uint32
	head      pmeta.PartitionHead
	version   uint64
	loads     int
	refreshes int
}

type conditionalTestSnapshot struct {
	owner   *conditionalSnapshotCatalog
	version uint64
	head    pmeta.PartitionHead
}

func (s *conditionalTestSnapshot) PartitionHead() pmeta.PartitionHead { return s.head }

func newConditionalSnapshotCatalog(partition uint32, nextLSN uint64) *conditionalSnapshotCatalog {
	return &conditionalSnapshotCatalog{
		partition: partition,
		head: pmeta.PartitionHead{
			StreamID:    "stream-a",
			Partition:   partition,
			NextLSN:     nextLSN,
			WriterEpoch: 1,
		},
		version: 1,
	}
}

func (c *conditionalSnapshotCatalog) LoadPartition(context.Context, uint32) (pmeta.PartitionHead, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.head, nil
}

func (c *conditionalSnapshotCatalog) LoadPartitionSnapshot(ctx context.Context, partition uint32) (catalog.PartitionSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.loads++
	return &conditionalTestSnapshot{owner: c, version: c.version, head: c.head}, nil
}

func (c *conditionalSnapshotCatalog) RefreshPartitionSnapshot(ctx context.Context, partition uint32, previous catalog.PartitionSnapshot) (catalog.PartitionSnapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	prior, ok := previous.(*conditionalTestSnapshot)
	if !ok || prior.owner != c || partition != c.partition {
		return nil, false, errors.New("incompatible snapshot")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.refreshes++
	if prior.version == c.version {
		return prior, false, nil
	}
	return &conditionalTestSnapshot{owner: c, version: c.version, head: c.head}, true, nil
}

func (*conditionalSnapshotCatalog) FindSegment(context.Context, uint32, uint64) (pmeta.SegmentRef, bool, error) {
	return pmeta.SegmentRef{}, false, nil
}

func (c *conditionalSnapshotCatalog) LookupTimestamp(context.Context, catalog.TimestampLookupRequest) (catalog.TimestampLookupResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return catalog.TimestampLookupResult{Head: c.head}, nil
}

func (*conditionalSnapshotCatalog) ListSegments(context.Context, catalog.ListSegmentsRequest) (pmeta.SegmentPage, error) {
	return pmeta.SegmentPage{}, nil
}

func (c *conditionalSnapshotCatalog) setNextLSN(nextLSN uint64) {
	c.mu.Lock()
	c.head.NextLSN = nextLSN
	c.version++
	c.mu.Unlock()
}

func (c *conditionalSnapshotCatalog) calls() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.loads, c.refreshes
}
