package reader

import (
	"sync"
	"time"

	"github.com/ankur-anand/objlog/internal/catalog"
	"github.com/ankur-anand/objlog/internal/pmeta"
	"github.com/ankur-anand/objlog/internal/segformat"
	"github.com/ankur-anand/objlog/internal/segreader"
)

const (
	DefaultMaxRecordsPerBatch                  = 1024
	DefaultMaxCachedPartitionHeads             = 16_384
	DefaultMaxWholeSegmentBytes                = uint64(8 << 20)
	DefaultWholeSegmentCacheBytes              = uint64(256 << 20)
	DefaultWholeSegmentThresholdPercent        = 50
	CursorCheckpointVersion             uint16 = 1
)

// ReadStrategy controls how immutable segment objects are fetched.
type ReadStrategy uint8

const (
	// ReadRanges preserves the bounded range-read behavior.
	ReadRanges ReadStrategy = iota
	// ReadWholeSegment fetches eligible segment objects in one request.
	ReadWholeSegment
	// ReadAuto loads whole objects for broad one-shot reads and broad remaining
	// cursor/tailer spans. Point and narrow cache misses use ranges, while any
	// read may reuse a compatible whole object already resident in memory.
	ReadAuto
)

type SegmentStore = segreader.SegmentStore

type Options struct {
	MaxRecordsPerBatch           int
	MaxCachedPartitionHeads      int
	ReadStrategy                 ReadStrategy
	MaxWholeSegmentBytes         uint64
	WholeSegmentCacheBytes       uint64
	WholeSegmentThresholdPercent int
	SegmentOptions               segreader.Options
	// WholeSegmentStore bypasses decorators intended for small exact ranges.
	// When nil, New uses SegmentStore for both range and whole-object reads.
	WholeSegmentStore SegmentStore
	// SegmentCache is cleared by Reader.Close. Do not share it with a Reader
	// whose lifecycle is independent.
	SegmentCache *SegmentReaderCache
	Refresh      RefreshPolicy
	Observer     Observer
}

type Reader struct {
	catalog       catalog.Reader
	store         SegmentStore
	wholeStore    SegmentStore
	wholeSegments *wholeSegmentCache
	opts          Options
	refresh       *refreshCoordinator

	lifecycleMu sync.Mutex
	closed      bool
	watches     map[*Watch]struct{}
}

type Record struct {
	Partition   uint32
	LSN         uint64
	TimestampMS int64
	Headers     []segformat.Header
	Value       []byte
}

type MetricName string

const (
	MetricHead           MetricName = "reader.head"
	MetricRead           MetricName = "reader.read"
	MetricFetch          MetricName = "reader.fetch"
	MetricTimestampRead  MetricName = "reader.timestamp_read"
	MetricTailNext       MetricName = "reader.tail_next"
	MetricCatalogRefresh MetricName = "reader.catalog_refresh"
	MetricSegmentRead    MetricName = "reader.segment_read"
	// MetricWholeReadFallback reports that a whole-object read selected by the
	// strategy used ranges because active whole reads occupied the cache budget.
	MetricWholeReadFallback MetricName = "reader.whole_read_fallback"
)

type MetricEvent struct {
	Name      MetricName
	Partition uint32

	StartLSN uint64
	NextLSN  uint64
	Limit    int
	Records  int

	SegmentURI string
	Duration   time.Duration
	Err        error
}

type Observer interface {
	Observe(MetricEvent)
}

func (r Record) Clone() Record {
	out := r
	out.Headers = segformat.CloneHeaders(r.Headers)
	if len(r.Value) > 0 {
		out.Value = append([]byte(nil), r.Value...)
	}
	return out
}

type ConsumeRequest struct {
	Partition uint32
	StartLSN  uint64
	Limit     int
}

type ConsumeAfterRequest struct {
	Partition     uint32
	StartAfterLSN uint64
	Limit         int
}

type ConsumeFromTimestampRequest struct {
	Partition   uint32
	TimestampMS int64
	Limit       int
}

type FetchRequest struct {
	Partition uint32
	LSN       uint64
}

type FetchResult struct {
	Record Record
	Found  bool
	Head   pmeta.PartitionHead
}

type ConsumeResult struct {
	Records []Record
	NextLSN uint64
	Head    pmeta.PartitionHead
}

// Freshness controls how a passive read consults catalog state.
type Freshness int

const (
	// FreshnessDefault uses FreshnessOnTail.
	FreshnessDefault Freshness = iota

	// FreshnessCached uses a cached partition head when one exists. If the
	// partition has not been seen before, the reader loads the head once.
	FreshnessCached

	// FreshnessOnTail refreshes the partition head only when the requested LSN
	// is at or beyond the cached tail. This is the default for Read and Cursor.
	FreshnessOnTail

	// FreshnessLatest refreshes the partition head before every read.
	FreshnessLatest
)

// RefreshPolicy controls explicit Watch background refresh. Passive reads do
// not start background polling.
type RefreshPolicy struct {
	// PollInterval is how often the shared reader refresh loop checks watched
	// partitions.
	PollInterval time.Duration

	// MaxConcurrentRefreshes limits concurrent catalog head refreshes. Actual
	// concurrency is capped by the number of watched partitions.
	MaxConcurrentRefreshes int

	// RefreshTimeout bounds one shared catalog head refresh. Zero uses the
	// default timeout.
	RefreshTimeout time.Duration
}

// ReadRequest reads committed records from one partition reader.
type ReadRequest struct {
	StartLSN  uint64
	Limit     int
	Freshness Freshness
}

// ReadResult is returned by one-shot reads, cursors, and tailers.
type ReadResult = ConsumeResult

// CursorOptions configures a passive replay cursor.
type CursorOptions struct {
	StartLSN uint64
	Limit    int
}

// CursorResumeOptions configures a cursor restored from a durable checkpoint.
type CursorResumeOptions struct {
	Limit int
}

// CursorCheckpoint is a durable next-read position bound to one stream
// partition. Persist the complete value, not NextLSN by itself.
type CursorCheckpoint struct {
	Version   uint16 `json:"version"`
	StreamID  string `json:"stream_id"`
	Partition uint32 `json:"partition"`
	NextLSN   uint64 `json:"next_lsn"`
}

// WatchOptions explicitly starts background catalog refresh for partitions.
type WatchOptions struct {
	Partitions []uint32
}

// TailOptions configures a tail cursor attached to a Watch.
type TailOptions struct {
	Partition uint32
	StartLSN  uint64
	Limit     int
}
