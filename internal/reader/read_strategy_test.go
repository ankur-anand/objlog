package reader

import (
	"testing"

	"github.com/ankur-anand/objlog/internal/pmeta"
)

func TestChooseReadStrategy(t *testing.T) {
	t.Parallel()

	segment := pmeta.SegmentRef{
		BaseLSN:     100,
		LastLSN:     199,
		RecordCount: 100,
		SizeBytes:   4 << 20,
	}
	auto := Options{
		ReadStrategy:                 ReadAuto,
		MaxWholeSegmentBytes:         8 << 20,
		WholeSegmentThresholdPercent: 50,
	}

	tests := []struct {
		name   string
		opts   Options
		from   uint64
		limit  int
		intent readIntent
		want   ReadStrategy
	}{
		{name: "default ranges", opts: Options{}, from: 100, limit: 100, intent: readIntentSequential, want: ReadRanges},
		{name: "auto broad", opts: auto, from: 100, limit: 50, intent: readIntentSequential, want: ReadWholeSegment},
		{name: "auto narrow", opts: auto, from: 100, limit: 49, intent: readIntentSequential, want: ReadRanges},
		{name: "auto point", opts: auto, from: 100, limit: 100, intent: readIntentPoint, want: ReadRanges},
		{name: "auto continuous broad remainder", opts: auto, from: 100, limit: 1, intent: readIntentContinuous, want: ReadWholeSegment},
		{name: "auto continuous narrow remainder", opts: auto, from: 175, limit: 1, intent: readIntentContinuous, want: ReadRanges},
		{name: "auto partial tail", opts: auto, from: 175, limit: 25, intent: readIntentSequential, want: ReadRanges},
		{name: "explicit whole", opts: Options{ReadStrategy: ReadWholeSegment, MaxWholeSegmentBytes: 8 << 20}, from: 100, limit: 1, intent: readIntentPoint, want: ReadWholeSegment},
		{name: "whole over byte limit", opts: Options{ReadStrategy: ReadWholeSegment, MaxWholeSegmentBytes: 2 << 20}, from: 100, limit: 100, intent: readIntentSequential, want: ReadRanges},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := chooseReadStrategy(tt.opts, segment, tt.from, tt.limit, tt.intent); got != tt.want {
				t.Fatalf("chooseReadStrategy() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestNormalizeOptionsRejectsInvalidReadStrategy(t *testing.T) {
	t.Parallel()

	if _, err := normalizeOptions(Options{ReadStrategy: ReadStrategy(255)}); err == nil {
		t.Fatal("normalizeOptions() error = nil, want invalid strategy")
	}
}

func TestNormalizeOptionsRejectsInvalidWholeSegmentThreshold(t *testing.T) {
	t.Parallel()

	if _, err := normalizeOptions(Options{WholeSegmentThresholdPercent: 101}); err == nil {
		t.Fatal("normalizeOptions() error = nil, want invalid threshold")
	}
}

func TestNormalizeOptionsRejectsWholeObjectLargerThanCache(t *testing.T) {
	t.Parallel()

	_, err := normalizeOptions(Options{
		ReadStrategy:           ReadWholeSegment,
		MaxWholeSegmentBytes:   8 << 20,
		WholeSegmentCacheBytes: 4 << 20,
	})
	if err == nil {
		t.Fatal("normalizeOptions() error = nil, want incompatible whole-object limits")
	}
}
