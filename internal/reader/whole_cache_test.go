package reader

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ankur-anand/objlog/internal/segreader"
)

func TestWholeSegmentCacheLeaderCancellationDoesNotPoisonFollower(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	segment := fixture.appendSegment(t, 0, 10)
	counting := newCountingSegmentStore(fixture.store)
	store := &blockingSegmentStore{
		inner:   counting,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	cache := newWholeSegmentCache(segment.SizeBytes)
	opts := segreader.DefaultOptions()
	key := segmentCacheKey{Ref: segment, Opts: opts}

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan wholeSegmentAcquireResult, 1)
	go func() {
		lease, available, err := cache.Acquire(leaderCtx, store, segment, opts)
		leaderDone <- wholeSegmentAcquireResult{lease: lease, available: available, err: err}
	}()
	waitForLoadSignal(t, store.entered, "leader whole-segment open")

	followerDone := make(chan wholeSegmentAcquireResult, 1)
	go func() {
		lease, available, err := cache.Acquire(context.Background(), store, segment, opts)
		followerDone <- wholeSegmentAcquireResult{lease: lease, available: available, err: err}
	}()
	waitForWholeSegmentWaiters(t, cache, key, 2)
	cancelLeader()
	leader := <-leaderDone
	if !errors.Is(leader.err, context.Canceled) {
		t.Fatalf("leader Acquire() error = %v, want context.Canceled", leader.err)
	}
	close(store.release)

	select {
	case follower := <-followerDone:
		if follower.err != nil || !follower.available || follower.lease == nil || follower.lease.Reader() == nil {
			t.Fatalf("follower Acquire() = %+v", follower)
		}
		follower.lease.Release()
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for follower whole-segment open")
	}
	if got := counting.rangeReadCount(segment.URI, 0, segment.SizeBytes); got != 1 {
		t.Fatalf("whole-object reads = %d, want one coalesced read", got)
	}
	cache.Close()
}

func waitForWholeSegmentWaiters(t *testing.T, cache *wholeSegmentCache, key segmentCacheKey, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		cache.mu.Lock()
		call := cache.inflight[key]
		got := 0
		if call != nil {
			got = call.waiters
		}
		cache.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d whole-segment waiters", want)
}

type wholeSegmentAcquireResult struct {
	lease     *wholeSegmentLease
	available bool
	err       error
}
