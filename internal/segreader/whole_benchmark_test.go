package segreader

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/ankur-anand/objlog/internal/segformat"
)

var benchmarkRecordCount int

func BenchmarkSegmentReadStrategy4MiB(b *testing.B) {
	for _, codec := range []segformat.Codec{segformat.CodecNone, segformat.CodecZstd} {
		fixture := buildSegmentWithBlockSize(b, codec, segformat.HashXXH64, 4_000, 1, 1, 1_000, 1<<20)
		for _, workload := range []struct {
			name  string
			limit int
		}{
			{name: "all", limit: 0},
			{name: "one", limit: 1},
		} {
			for _, strategy := range []struct {
				name string
				open func(context.Context, SegmentStore) (*Reader, error)
			}{
				{
					name: "ranges",
					open: func(ctx context.Context, store SegmentStore) (*Reader, error) {
						return Open(ctx, store, fixture.ref, DefaultOptions())
					},
				},
				{
					name: "whole",
					open: func(ctx context.Context, store SegmentStore) (*Reader, error) {
						return OpenWhole(ctx, store, fixture.ref, DefaultOptions())
					},
				},
			} {
				name := fmt.Sprintf("%s/%s/%s", codec, workload.name, strategy.name)
				b.Run(name, func(b *testing.B) {
					store := &benchmarkSegmentStore{uri: fixture.ref.URI, body: fixture.object}
					ctx := context.Background()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						reader, err := strategy.open(ctx, store)
						if err != nil {
							b.Fatal(err)
						}
						records, err := reader.Read(ctx, fixture.ref.BaseLSN, workload.limit)
						if err != nil {
							b.Fatal(err)
						}
						benchmarkRecordCount = len(records)
					}
					b.ReportMetric(float64(store.reads.Load())/float64(b.N), "store_reads/op")
					b.ReportMetric(float64(store.bytes.Load())/float64(b.N), "store_bytes/op")
				})
			}
		}
	}
}

type benchmarkSegmentStore struct {
	uri   string
	body  []byte
	reads atomic.Uint64
	bytes atomic.Uint64
}

func (s *benchmarkSegmentStore) ReadAt(ctx context.Context, uri string, off uint64, n uint64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if uri != s.uri {
		return nil, fmt.Errorf("segment not found: %s", uri)
	}
	if off > uint64(len(s.body)) || n > uint64(len(s.body))-off {
		return nil, fmt.Errorf("range offset=%d length=%d beyond object size=%d", off, n, len(s.body))
	}
	s.reads.Add(1)
	s.bytes.Add(n)
	return append([]byte(nil), s.body[off:off+n]...), nil
}
