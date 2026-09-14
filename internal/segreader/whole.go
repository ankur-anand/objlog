package segreader

import (
	"context"
	"fmt"

	"github.com/ankur-anand/objlog/internal/pmeta"
)

// OpenWhole fetches one immutable segment object, then parses metadata and
// opens blocks from that in-memory object. Compressed blocks are still opened
// one at a time by Scanner.
func OpenWhole(ctx context.Context, store SegmentStore, ref pmeta.SegmentRef, opts Options) (*Reader, error) {
	normalized, err := validateOpen(store, ref, opts)
	if err != nil {
		return nil, err
	}
	body, err := readAtExact(ctx, store, ref.URI, 0, ref.SizeBytes)
	if err != nil {
		return nil, err
	}
	return open(ctx, wholeSegmentStore{uri: ref.URI, body: body}, ref, normalized, true)
}

type wholeSegmentStore struct {
	uri  string
	body []byte
}

func (s wholeSegmentStore) ReadAt(ctx context.Context, uri string, off uint64, n uint64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if uri != s.uri {
		return nil, fmt.Errorf("segment not found: %s", uri)
	}
	if off > uint64(len(s.body)) || n > uint64(len(s.body))-off {
		return nil, fmt.Errorf("range offset=%d length=%d beyond object size=%d", off, n, len(s.body))
	}
	return s.body[off : off+n], nil
}
