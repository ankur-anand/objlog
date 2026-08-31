package memory

import (
	"bytes"
	"context"
	"testing"
)

func TestGetIfChanged(t *testing.T) {
	ctx := context.Background()
	store := New()
	created, swapped, err := store.CompareAndSwap(ctx, "head.plc", "", []byte("generation-1"))
	if err != nil || !swapped {
		t.Fatalf("CompareAndSwap(create) object=%+v swapped=%v error=%v", created, swapped, err)
	}

	unchanged, changed, err := store.GetIfChanged(ctx, created.Key, created.Token)
	if err != nil {
		t.Fatalf("GetIfChanged(unchanged) error = %v", err)
	}
	if changed || unchanged.Token != created.Token || len(unchanged.Body) != 0 {
		t.Fatalf("GetIfChanged(unchanged) object=%+v changed=%v", unchanged, changed)
	}

	updated, swapped, err := store.CompareAndSwap(ctx, created.Key, created.Token, []byte("generation-2"))
	if err != nil || !swapped {
		t.Fatalf("CompareAndSwap(update) object=%+v swapped=%v error=%v", updated, swapped, err)
	}
	loaded, changed, err := store.GetIfChanged(ctx, created.Key, created.Token)
	if err != nil {
		t.Fatalf("GetIfChanged(changed) error = %v", err)
	}
	if !changed || loaded.Token != updated.Token || !bytes.Equal(loaded.Body, updated.Body) {
		t.Fatalf("GetIfChanged(changed) object=%+v changed=%v want=%+v", loaded, changed, updated)
	}
}

var benchmarkObjectBody []byte

func BenchmarkUnchangedHeadRead140KiB(b *testing.B) {
	ctx := context.Background()
	store := New()
	body := bytes.Repeat([]byte{0xa5}, 140<<10)
	created, swapped, err := store.CompareAndSwap(ctx, "head.plc", "", body)
	if err != nil || !swapped {
		b.Fatalf("CompareAndSwap(create) swapped=%v error=%v", swapped, err)
	}

	b.Run("unconditional", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(len(body)), "body-B/op")
		for i := 0; i < b.N; i++ {
			object, err := store.Get(ctx, created.Key)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkObjectBody = object.Body
		}
	})

	b.Run("conditional_unchanged", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(0, "body-B/op")
		for i := 0; i < b.N; i++ {
			object, changed, err := store.GetIfChanged(ctx, created.Key, created.Token)
			if err != nil {
				b.Fatal(err)
			}
			if changed {
				b.Fatal("unchanged object reported changed")
			}
			benchmarkObjectBody = object.Body
		}
	})
}
