package reader

import "github.com/ankur-anand/objlog/internal/pmeta"

type readIntent uint8

const (
	readIntentSequential readIntent = iota
	readIntentPoint
	readIntentContinuous
)

func chooseReadStrategy(opts Options, segment pmeta.SegmentRef, fromLSN uint64, limit int, intent readIntent) ReadStrategy {
	if opts.ReadStrategy == ReadRanges {
		return ReadRanges
	}
	if segment.SizeBytes > opts.MaxWholeSegmentBytes {
		return ReadRanges
	}
	if opts.ReadStrategy == ReadWholeSegment {
		return ReadWholeSegment
	}
	if opts.ReadStrategy != ReadAuto || intent == readIntentPoint {
		return ReadRanges
	}
	if limit <= 0 || fromLSN < segment.BaseLSN || fromLSN > segment.LastLSN {
		return ReadRanges
	}

	available := segment.LastLSN - fromLSN + 1
	wanted := min(uint64(limit), available)
	if intent == readIntentContinuous {
		// A cursor or tailer can reuse the object across later Next calls, so its
		// useful span is the complete segment remainder rather than this call's
		// batch limit. This also avoids whole-fetching a segment when a resumed
		// consumer has only a small tail left.
		wanted = available
	}
	if wanted*100 >= uint64(segment.RecordCount)*uint64(opts.WholeSegmentThresholdPercent) {
		return ReadWholeSegment
	}
	return ReadRanges
}
