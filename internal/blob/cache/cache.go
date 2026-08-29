package cache

import (
	"container/list"
	"context"
	"fmt"
	"sync"

	"github.com/ankur-anand/objlog/internal/segreader"
)

type Key struct {
	URI string
	Off uint64
	N   uint64
}

type Cache interface {
	Get(Key) ([]byte, bool)
	Set(Key, []byte)
}

// Store decorates a segreader.SegmentStore with immutable range caching.
type Store struct {
	inner segreader.SegmentStore
	cache Cache

	mu       sync.Mutex
	inflight map[Key]*inflightRead
}

var _ segreader.SegmentStore = (*Store)(nil)

type inflightRead struct {
	ctx      context.Context
	cancel   context.CancelCauseFunc
	done     chan struct{}
	waiters  int
	finished bool
	body     []byte
	err      error
}

func NewStore(inner segreader.SegmentStore, cache Cache) (*Store, error) {
	if inner == nil {
		return nil, fmt.Errorf("blob/cache: nil inner store")
	}
	if cache == nil {
		return nil, fmt.Errorf("blob/cache: nil cache")
	}
	return &Store{
		inner:    inner,
		cache:    cache,
		inflight: make(map[Key]*inflightRead),
	}, nil
}

func MustNewStore(inner segreader.SegmentStore, cache Cache) *Store {
	store, err := NewStore(inner, cache)
	if err != nil {
		panic(err)
	}
	return store
}

func (s *Store) ReadAt(ctx context.Context, uri string, off uint64, n uint64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if n == 0 {
		return []byte{}, nil
	}
	key := Key{URI: uri, Off: off, N: n}
	if body, ok := s.cache.Get(key); ok && uint64(len(body)) == n {
		return body, nil
	}

	call, leader := s.begin(ctx, key)
	if leader {
		go s.run(key, call, uri, off, n)
	}
	return s.wait(ctx, key, call)
}

func (s *Store) run(key Key, call *inflightRead, uri string, off uint64, n uint64) {
	body, err := s.inner.ReadAt(call.ctx, uri, off, n)
	if err == nil && uint64(len(body)) != n {
		err = fmt.Errorf("blob/cache: short range read uri=%q offset=%d length=%d got=%d", uri, off, n, len(body))
	}
	if err == nil {
		s.cache.Set(key, body)
	}
	if cause := context.Cause(call.ctx); cause != nil {
		body = nil
		err = cause
	}
	s.finish(key, call, body, err)
}

// ClearRangeCache releases cached ranges when the configured cache supports
// it. In-flight reads are not interrupted.
func (s *Store) ClearRangeCache() {
	if cache, ok := s.cache.(interface{ Clear() }); ok {
		cache.Clear()
	}
}

func (s *Store) begin(ctx context.Context, key Key) (*inflightRead, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if call, ok := s.inflight[key]; ok {
		call.waiters++
		return call, false
	}
	callCtx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	call := &inflightRead{
		ctx:     callCtx,
		cancel:  cancel,
		done:    make(chan struct{}),
		waiters: 1,
	}
	s.inflight[key] = call
	return call, true
}

func (s *Store) wait(ctx context.Context, key Key, call *inflightRead) ([]byte, error) {
	select {
	case <-call.done:
		s.releaseWaiter(key, call)
		if call.err != nil {
			return nil, call.err
		}
		return append([]byte(nil), call.body...), nil
	case <-ctx.Done():
		s.releaseWaiter(key, call)
		return nil, ctx.Err()
	}
}

func (s *Store) releaseWaiter(key Key, call *inflightRead) {
	var cancel bool
	s.mu.Lock()
	call.waiters--
	if call.waiters == 0 && !call.finished {
		if s.inflight[key] == call {
			delete(s.inflight, key)
		}
		cancel = true
	}
	s.mu.Unlock()
	if cancel {
		call.cancel(context.Canceled)
	}
}

func (s *Store) finish(key Key, call *inflightRead, body []byte, err error) {
	s.mu.Lock()
	call.body = append([]byte(nil), body...)
	call.err = err
	call.finished = true
	if current := s.inflight[key]; current == call {
		delete(s.inflight, key)
	}
	close(call.done)
	s.mu.Unlock()
	call.cancel(context.Canceled)
}

type LRU struct {
	mu       sync.Mutex
	maxBytes uint64
	bytes    uint64
	ll       *list.List
	items    map[Key]*list.Element
}

type entry struct {
	key   Key
	value []byte
	size  uint64
}

func NewLRU(maxBytes uint64) *LRU {
	return &LRU{
		maxBytes: maxBytes,
		ll:       list.New(),
		items:    make(map[Key]*list.Element),
	}
}

func (c *LRU) Get(key Key) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(elem)
	ent := elem.Value.(*entry)
	return append([]byte(nil), ent.value...), true
}

func (c *LRU) Set(key Key, value []byte) {
	if c == nil {
		return
	}
	size := uint64(len(value))
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.items[key]; ok {
		c.removeElement(existing)
	}
	if c.maxBytes == 0 || size == 0 || size > c.maxBytes {
		return
	}
	ent := &entry{
		key:   key,
		value: append([]byte(nil), value...),
		size:  size,
	}
	c.items[key] = c.ll.PushFront(ent)
	c.bytes += size
	c.evict()
}

func (c *LRU) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

func (c *LRU) Bytes() uint64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bytes
}

func (c *LRU) MaxBytes() uint64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxBytes
}

// Clear releases every cached range while retaining the configured budget.
func (c *LRU) Clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.ll.Init()
	clear(c.items)
	c.bytes = 0
	c.mu.Unlock()
}

func (c *LRU) evict() {
	for c.bytes > c.maxBytes {
		back := c.ll.Back()
		if back == nil {
			return
		}
		c.removeElement(back)
	}
}

func (c *LRU) removeElement(elem *list.Element) {
	c.ll.Remove(elem)
	ent := elem.Value.(*entry)
	delete(c.items, ent.key)
	c.bytes -= ent.size
}
