package reader

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/ankur-anand/objlog/internal/segformat"
)

var benchmarkContinuousRecords int

// BenchmarkContinuousRead4MiB compares the original stateless range cursor
// with the Auto whole-object LRU. A fresh Reader per iteration keeps both sides
// cold so the measurement includes exactly one replay of the segment.
func BenchmarkContinuousRead4MiB(b *testing.B) {
	for _, codec := range []segformat.Codec{segformat.CodecNone, segformat.CodecZstd} {
		fixture := newReaderFixture(b)
		segment := fixture.appendSegmentWithOptions(b, 0, 4_000, 1_000, 1<<20, codec)

		b.Run(fmt.Sprintf("%s/pre_range_cursor", codec), func(b *testing.B) {
			store := &benchmarkReaderStore{inner: fixture.store}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r, err := New(fixture.catalog, store, Options{
					MaxRecordsPerBatch: 1_000,
					ReadStrategy:       ReadRanges,
				})
				if err != nil {
					b.Fatal(err)
				}
				cursor, err := r.Partition(fixture.partition).Cursor(CursorOptions{StartLSN: segment.BaseLSN, Limit: 1_000})
				if err != nil {
					b.Fatal(err)
				}
				total := 0
				for cursor.Position() <= segment.LastLSN {
					result, err := cursor.Next(ctx)
					if err != nil {
						b.Fatal(err)
					}
					total += len(result.Records)
				}
				benchmarkContinuousRecords = total
				_ = cursor.Close()
				_ = r.Close()
			}
			reportContinuousStoreMetrics(b, store)
		})

		b.Run(fmt.Sprintf("%s/post_whole_cache", codec), func(b *testing.B) {
			store := &benchmarkReaderStore{inner: fixture.store}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r, err := New(fixture.catalog, store, Options{
					MaxRecordsPerBatch: 1_000,
					ReadStrategy:       ReadAuto,
				})
				if err != nil {
					b.Fatal(err)
				}
				cursor, err := r.Partition(fixture.partition).Cursor(CursorOptions{StartLSN: segment.BaseLSN, Limit: 1_000})
				if err != nil {
					b.Fatal(err)
				}
				total := 0
				for cursor.Position() <= segment.LastLSN {
					result, err := cursor.Next(ctx)
					if err != nil {
						b.Fatal(err)
					}
					total += len(result.Records)
				}
				benchmarkContinuousRecords = total
				_ = cursor.Close()
				_ = r.Close()
			}
			reportContinuousStoreMetrics(b, store)
		})
	}
}

func reportContinuousStoreMetrics(b *testing.B, store *benchmarkReaderStore) {
	b.Helper()
	b.ReportMetric(float64(store.reads.Load())/float64(b.N), "store_reads/op")
	b.ReportMetric(float64(store.bytes.Load())/float64(b.N), "store_bytes/op")
}

type benchmarkReaderStore struct {
	inner SegmentStore
	reads atomic.Uint64
	bytes atomic.Uint64
}

func (s *benchmarkReaderStore) ReadAt(ctx context.Context, uri string, off uint64, n uint64) ([]byte, error) {
	body, err := s.inner.ReadAt(ctx, uri, off, n)
	if err != nil {
		return nil, err
	}
	s.reads.Add(1)
	s.bytes.Add(n)
	return body, nil
}
