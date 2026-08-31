package blob

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/ankur-anand/objlog/internal/blobstore"
	pcatalog "github.com/ankur-anand/objlog/internal/catalog"
)

func TestPartitionSnapshotRefreshUsesConditionalHeadRead(t *testing.T) {
	ctx := context.Background()
	backend := &conditionalCountingBackend{Backend: NewMemoryBackend()}
	cat, err := New(backend, Options{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	writer, err := cat.OpenWriter(ctx, 1, [16]byte{1})
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	if _, err := writer.AppendSegment(ctx, testSegmentRef(1, 0, 9, writer.Epoch())); err != nil {
		t.Fatalf("AppendSegment(first) error = %v", err)
	}

	backend.reset()
	snapshot, err := cat.LoadPartitionSnapshot(ctx, 1)
	if err != nil {
		t.Fatalf("LoadPartitionSnapshot() error = %v", err)
	}
	if full, conditional, bodyBytes := backend.counts(); full != 1 || conditional != 0 || bodyBytes == 0 {
		t.Fatalf("initial load full=%d conditional=%d body_bytes=%d", full, conditional, bodyBytes)
	}

	backend.reset()
	unchanged, changed, err := cat.RefreshPartitionSnapshot(ctx, 1, snapshot)
	if err != nil {
		t.Fatalf("RefreshPartitionSnapshot(unchanged) error = %v", err)
	}
	if changed || unchanged != snapshot {
		t.Fatalf("unchanged refresh snapshot=%T changed=%v", unchanged, changed)
	}
	if full, conditional, bodyBytes := backend.counts(); full != 0 || conditional != 1 || bodyBytes != 0 {
		t.Fatalf("unchanged refresh full=%d conditional=%d body_bytes=%d", full, conditional, bodyBytes)
	}

	if _, err := writer.AppendSegment(ctx, testSegmentRef(1, 10, 19, writer.Epoch())); err != nil {
		t.Fatalf("AppendSegment(second) error = %v", err)
	}
	updated, changed, err := cat.RefreshPartitionSnapshot(ctx, 1, snapshot)
	if err != nil {
		t.Fatalf("RefreshPartitionSnapshot(changed) error = %v", err)
	}
	if !changed || updated.PartitionHead().NextLSN != 20 {
		t.Fatalf("changed refresh head=%+v changed=%v", updated.PartitionHead(), changed)
	}
	if full, conditional, bodyBytes := backend.counts(); full != 0 || conditional != 2 || bodyBytes == 0 {
		t.Fatalf("changed refresh totals full=%d conditional=%d body_bytes=%d", full, conditional, bodyBytes)
	}
}

func BenchmarkUnchangedPartitionSnapshotRefresh(b *testing.B) {
	ctx := context.Background()
	cat, err := NewMemory(Options{})
	if err != nil {
		b.Fatalf("NewMemory() error = %v", err)
	}
	writer, err := cat.OpenWriter(ctx, 1, [16]byte{1})
	if err != nil {
		b.Fatalf("OpenWriter() error = %v", err)
	}
	if _, err := writer.AppendSegment(ctx, testSegmentRef(1, 0, 9, writer.Epoch())); err != nil {
		b.Fatalf("AppendSegment() error = %v", err)
	}
	snapshot, err := cat.LoadPartitionSnapshot(ctx, 1)
	if err != nil {
		b.Fatalf("LoadPartitionSnapshot() error = %v", err)
	}

	b.Run("unconditional_load", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := cat.LoadPartitionSnapshot(ctx, 1); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("conditional_unchanged", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, changed, err := cat.RefreshPartitionSnapshot(ctx, 1, snapshot); err != nil {
				b.Fatal(err)
			} else if changed {
				b.Fatal("unchanged snapshot reported changed")
			}
		}
	})
}

type conditionalCountingBackend struct {
	Backend
	fullGets        atomic.Int64
	conditionalGets atomic.Int64
	bodyBytes       atomic.Int64
}

var _ blobstore.ConditionalGetter = (*conditionalCountingBackend)(nil)
var _ pcatalog.SnapshotReader = (*Catalog)(nil)

func (b *conditionalCountingBackend) Get(ctx context.Context, key string) (Object, error) {
	object, err := b.Backend.Get(ctx, key)
	b.fullGets.Add(1)
	b.bodyBytes.Add(int64(len(object.Body)))
	return object, err
}

func (b *conditionalCountingBackend) GetIfChanged(ctx context.Context, key string, token string) (Object, bool, error) {
	object, changed, err := blobstore.GetIfChanged(ctx, b.Backend, key, token)
	b.conditionalGets.Add(1)
	b.bodyBytes.Add(int64(len(object.Body)))
	return object, changed, err
}

func (b *conditionalCountingBackend) reset() {
	b.fullGets.Store(0)
	b.conditionalGets.Store(0)
	b.bodyBytes.Store(0)
}

func (b *conditionalCountingBackend) counts() (int64, int64, int64) {
	return b.fullGets.Load(), b.conditionalGets.Load(), b.bodyBytes.Load()
}
