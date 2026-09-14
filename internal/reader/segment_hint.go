package reader

import "github.com/ankur-anand/objlog/internal/pmeta"

// A segment hint is immutable catalog metadata remembered by a Cursor or
// Tailer. It avoids another ListSegments call inside the same segment while
// leaving complete object bytes under normal shared-cache eviction.
func segmentHintContains(hint *pmeta.SegmentRef, partition uint32, lsn uint64) bool {
	return hint != nil &&
		hint.Partition == partition &&
		lsn >= hint.BaseLSN &&
		lsn <= hint.LastLSN
}

func cloneSegmentHint(hint *pmeta.SegmentRef) *pmeta.SegmentRef {
	if hint == nil {
		return nil
	}
	clone := *hint
	return &clone
}

func newSegmentHint(segment pmeta.SegmentRef) *pmeta.SegmentRef {
	return cloneSegmentHint(&segment)
}
