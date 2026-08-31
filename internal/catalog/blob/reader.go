package blob

import (
	"context"
	"fmt"

	csession "github.com/ankur-anand/objlog/internal/catalog"
	"github.com/ankur-anand/objlog/internal/catalog/blob/internal/catengine"
	"github.com/ankur-anand/objlog/internal/pmeta"
)

var _ csession.Reader = (*Catalog)(nil)
var _ csession.SnapshotReader = (*Catalog)(nil)

type partitionSnapshot struct {
	catalog *Catalog
	head    catengine.HeadSnapshot
}

func (s *partitionSnapshot) PartitionHead() pmeta.PartitionHead {
	if s == nil {
		return pmeta.PartitionHead{}
	}
	return s.head.State
}

func (c *Catalog) LoadPartition(ctx context.Context, partition uint32) (pmeta.PartitionHead, error) {
	return c.engine.LoadPartition(ctx, partition)
}

func (c *Catalog) LoadPartitionSnapshot(ctx context.Context, partition uint32) (csession.PartitionSnapshot, error) {
	head, err := c.engine.LoadHeadSnapshot(ctx, partition)
	if err != nil {
		return nil, err
	}
	return &partitionSnapshot{catalog: c, head: head}, nil
}

func (c *Catalog) RefreshPartitionSnapshot(ctx context.Context, partition uint32, previous csession.PartitionSnapshot) (csession.PartitionSnapshot, bool, error) {
	prior, ok := previous.(*partitionSnapshot)
	if !ok || prior == nil || prior.catalog != c {
		return nil, false, fmt.Errorf("%w: incompatible partition snapshot", csession.ErrInvalidRequest)
	}
	head, changed, err := c.engine.RefreshHeadSnapshot(ctx, partition, prior.head)
	if err != nil {
		return nil, false, err
	}
	if !changed {
		return prior, false, nil
	}
	return &partitionSnapshot{catalog: c, head: head}, true, nil
}

func (c *Catalog) FindSegment(ctx context.Context, partition uint32, lsn uint64) (pmeta.SegmentRef, bool, error) {
	return c.engine.FindSegment(ctx, partition, lsn)
}

func (c *Catalog) LookupTimestamp(ctx context.Context, req csession.TimestampLookupRequest) (csession.TimestampLookupResult, error) {
	head, segment, found, err := c.engine.LookupTimestampSnapshot(ctx, req.Partition, req.TimestampMS)
	if err != nil {
		return csession.TimestampLookupResult{}, err
	}
	return csession.TimestampLookupResult{Head: head, Segment: segment, Found: found}, nil
}

func (c *Catalog) ListSegments(ctx context.Context, req csession.ListSegmentsRequest) (pmeta.SegmentPage, error) {
	return c.engine.ListSegments(ctx, req.Partition, req.FromLSN, req.NormalizedLimit())
}
