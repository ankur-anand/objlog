package reader

import (
	"container/list"
	"context"
	"sync"

	"github.com/ankur-anand/objlog/internal/pmeta"
	"github.com/ankur-anand/objlog/internal/segreader"
)

// wholeSegmentCache owns complete immutable segment readers under one byte
// budget. Active read calls pin entries; entries become ordinary LRU candidates
// as soon as those calls return. When every byte is temporarily pinned, Acquire
// reports unavailable so the caller can fall back to bounded range reads.
type wholeSegmentCache struct {
	mu       sync.Mutex
	maxBytes uint64
	bytes    uint64
	ll       list.List
	entries  map[segmentCacheKey]*wholeSegmentEntry
	inflight map[segmentCacheKey]*wholeSegmentCall
	active   sync.WaitGroup
	closed   bool
}

type wholeSegmentEntry struct {
	key    segmentCacheKey
	reader *segreader.Reader
	size   uint64
	refs   int
	elem   *list.Element
}

type wholeSegmentCall struct {
	ctx      context.Context
	cancel   context.CancelCauseFunc
	done     chan struct{}
	waiters  int
	finished bool
	reserved uint64
	entry    *wholeSegmentEntry
	err      error
}

// wholeSegmentLease pins one cache entry until the active read call invokes
// Release. Cursor and Tailer values never retain a lease between calls.
type wholeSegmentLease struct {
	cache *wholeSegmentCache
	entry *wholeSegmentEntry
	once  sync.Once
}

func newWholeSegmentCache(maxBytes uint64) *wholeSegmentCache {
	return &wholeSegmentCache{
		maxBytes: maxBytes,
		entries:  make(map[segmentCacheKey]*wholeSegmentEntry),
		inflight: make(map[segmentCacheKey]*wholeSegmentCall),
	}
}

func (c *wholeSegmentCache) Acquire(ctx context.Context, store SegmentStore, ref pmeta.SegmentRef, opts segreader.Options) (*wholeSegmentLease, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	key := segmentCacheKey{Ref: ref, Opts: opts}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, false, ErrClosed
	}
	if lease := c.acquireCachedLocked(key); lease != nil {
		c.mu.Unlock()
		return lease, true, nil
	}
	if call := c.inflight[key]; call != nil {
		call.waiters++
		c.mu.Unlock()
		return c.wait(ctx, key, call)
	}
	if !c.reserveLocked(ref.SizeBytes) {
		c.mu.Unlock()
		return nil, false, nil
	}

	loadCtx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	call := &wholeSegmentCall{
		ctx:      loadCtx,
		cancel:   cancel,
		done:     make(chan struct{}),
		waiters:  1,
		reserved: ref.SizeBytes,
	}
	c.inflight[key] = call
	c.active.Add(1)
	c.mu.Unlock()

	go c.run(key, call, store, ref, opts)
	return c.wait(ctx, key, call)
}

// AcquireCached pins ref only when its complete object is already resident. It
// lets Auto finish a segment from the shared object without turning a narrow
// tail read into another whole-object download after eviction.
func (c *wholeSegmentCache) AcquireCached(ref pmeta.SegmentRef, opts segreader.Options) (*wholeSegmentLease, bool, error) {
	key := segmentCacheKey{Ref: ref, Opts: opts}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, false, ErrClosed
	}
	lease := c.acquireCachedLocked(key)
	return lease, lease != nil, nil
}

func (c *wholeSegmentCache) acquireCachedLocked(key segmentCacheKey) *wholeSegmentLease {
	entry := c.entries[key]
	if entry == nil || entry.reader == nil {
		return nil
	}
	entry.refs++
	c.ll.MoveToFront(entry.elem)
	return &wholeSegmentLease{cache: c, entry: entry}
}

func (c *wholeSegmentCache) wait(ctx context.Context, key segmentCacheKey, call *wholeSegmentCall) (*wholeSegmentLease, bool, error) {
	select {
	case <-ctx.Done():
		c.releaseWaiter(key, call)
		return nil, false, ctx.Err()
	case <-call.done:
		c.mu.Lock()
		call.waiters--
		entry, err := call.entry, call.err
		c.mu.Unlock()
		if err != nil {
			return nil, false, err
		}
		if entry == nil {
			return nil, false, nil
		}
		return &wholeSegmentLease{cache: c, entry: entry}, true, nil
	}
}

func (c *wholeSegmentCache) releaseWaiter(key segmentCacheKey, call *wholeSegmentCall) {
	var cancel bool
	c.mu.Lock()
	call.waiters--
	if call.finished && call.entry != nil {
		call.entry.refs--
	}
	if call.waiters == 0 && !call.finished {
		if c.inflight[key] == call {
			delete(c.inflight, key)
		}
		// Keep the reservation until run exits. A store may take time to honor
		// cancellation and can still hold the full buffer meanwhile; releasing
		// bytes here would let concurrent loads exceed the configured bound.
		cancel = true
	}
	c.mu.Unlock()
	if cancel {
		call.cancel(errNoLoadWaiters)
	}
}

func (c *wholeSegmentCache) run(key segmentCacheKey, call *wholeSegmentCall, store SegmentStore, ref pmeta.SegmentRef, opts segreader.Options) {
	reader, err := segreader.OpenWhole(call.ctx, store, ref, opts)
	if cause := context.Cause(call.ctx); cause != nil {
		reader = nil
		err = cause
	}

	c.mu.Lock()
	current := c.inflight[key] == call
	if current {
		delete(c.inflight, key)
	}
	if err == nil && current && !c.closed && call.waiters > 0 {
		entry := &wholeSegmentEntry{
			key:    key,
			reader: reader,
			size:   call.reserved,
			refs:   call.waiters,
		}
		entry.elem = c.ll.PushFront(entry)
		c.entries[key] = entry
		call.entry = entry
	} else {
		c.bytes -= call.reserved
	}
	call.err = err
	call.finished = true
	close(call.done)
	c.mu.Unlock()

	call.cancel(context.Canceled)
	c.active.Done()
}

// reserveLocked evicts only unpinned entries. It never exceeds maxBytes, so
// the sum of cached and active complete objects has a hard upper bound.
func (c *wholeSegmentCache) reserveLocked(size uint64) bool {
	if size == 0 || size > c.maxBytes {
		return false
	}
	for c.bytes > c.maxBytes-size {
		var victim *wholeSegmentEntry
		for elem := c.ll.Back(); elem != nil; elem = elem.Prev() {
			entry := elem.Value.(*wholeSegmentEntry)
			if entry.refs == 0 {
				victim = entry
				break
			}
		}
		if victim == nil {
			return false
		}
		c.removeEntryLocked(victim)
	}
	c.bytes += size
	return true
}

func (c *wholeSegmentCache) removeEntryLocked(entry *wholeSegmentEntry) {
	delete(c.entries, entry.key)
	c.ll.Remove(entry.elem)
	c.bytes -= entry.size
	entry.reader = nil
}

func (l *wholeSegmentLease) Reader() *segreader.Reader {
	if l == nil || l.cache == nil || l.entry == nil {
		return nil
	}
	l.cache.mu.Lock()
	defer l.cache.mu.Unlock()
	return l.entry.reader
}

func (l *wholeSegmentLease) Release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		l.cache.mu.Lock()
		if l.entry.refs > 0 {
			l.entry.refs--
		}
		if l.entry.elem != nil && l.entry.reader != nil {
			l.cache.ll.MoveToFront(l.entry.elem)
		}
		l.cache.mu.Unlock()
	})
}

func (c *wholeSegmentCache) Bytes() uint64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bytes
}

// Close revokes every cached reader and cancels unfinished opens.
func (c *wholeSegmentCache) Close() {
	if c == nil {
		return
	}
	var calls []*wholeSegmentCall
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		for _, entry := range c.entries {
			c.bytes -= entry.size
			entry.reader = nil
		}
		clear(c.entries)
		c.ll.Init()
		for _, call := range c.inflight {
			calls = append(calls, call)
		}
		// In-flight reservations, including calls whose last waiter cancelled,
		// remain counted until their workers observe cancellation and return.
	}
	c.mu.Unlock()

	for _, call := range calls {
		call.cancel(ErrClosed)
	}
	c.active.Wait()
}
