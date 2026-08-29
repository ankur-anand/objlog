package reader

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/objlog/internal/segreader"
)

func TestSegmentReaderCacheLeaderCancellationDoesNotPoisonFollower(t *testing.T) {
	t.Parallel()

	fixture := newReaderFixture(t)
	segment := fixture.appendSegment(t, 0, 20)
	store := &blockingSegmentStore{
		inner:   fixture.store,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	cache := MustNewSegmentReaderCache(8)
	opts := segreader.DefaultOptions()
	key := segmentCacheKey{Ref: segment, Opts: opts}

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, err := cache.Open(leaderCtx, store, segment, opts)
		leaderDone <- err
	}()
	waitForLoadSignal(t, store.entered, "leader segment open")

	followerDone := make(chan segmentOpenResult, 1)
	go func() {
		reader, err := cache.Open(context.Background(), store, segment, opts)
		followerDone <- segmentOpenResult{reader: reader, err: err}
	}()
	waitForSegmentOpenWaiters(t, cache, key, 2)
	cancelLeader()
	if err := receiveLoadError(t, leaderDone); !errors.Is(err, context.Canceled) {
		t.Fatalf("leader Open() error = %v, want context.Canceled", err)
	}
	close(store.release)
	select {
	case result := <-followerDone:
		if result.err != nil {
			t.Fatalf("follower Open() error = %v", result.err)
		}
		if result.reader == nil || result.reader.Ref() != segment {
			t.Fatalf("follower Open() reader = %v", result.reader)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for follower segment open")
	}
}

type blockingSegmentStore struct {
	inner   SegmentStore
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingSegmentStore) ReadAt(ctx context.Context, uri string, off uint64, n uint64) ([]byte, error) {
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
	return s.inner.ReadAt(ctx, uri, off, n)
}

func waitForSegmentOpenWaiters(t *testing.T, cache *SegmentReaderCache, key segmentCacheKey, want int) {
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
	t.Fatalf("timed out waiting for %d segment-open waiters", want)
}

type segmentOpenResult struct {
	reader *segreader.Reader
	err    error
}
