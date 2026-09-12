package blob

import (
	"context"
	"fmt"

	csession "github.com/ankur-anand/objlog/internal/catalog"
	"github.com/ankur-anand/objlog/internal/catalog/blob/internal/catengine"
	"github.com/ankur-anand/objlog/internal/pmeta"
)

type writerSession struct {
	cat     *Catalog
	session *catengine.Session
}

var _ csession.WriterSession = (*writerSession)(nil)
var _ csession.RetentionWriterSession = (*writerSession)(nil)
var _ csession.RetentionReconciler = (*writerSession)(nil)

func (s *writerSession) Head() pmeta.PartitionHead { return s.session.Head() }
func (s *writerSession) Epoch() uint64             { return s.session.Epoch() }
func (s *writerSession) WriterID() [16]byte        { return s.session.WriterID() }

func (s *writerSession) AppendSegment(ctx context.Context, segment pmeta.SegmentRef) (pmeta.PartitionHead, error) {
	return s.session.AppendSegment(ctx, segment)
}

func (s *writerSession) ApplyPendingRetention(ctx context.Context) (csession.RetentionApplyResult, error) {
	// Finish the recorded request before consulting a potentially newer mailbox.
	if result, pending, err := s.ReconcilePendingRetention(ctx); pending || err != nil {
		return result, err
	}
	if s.session.IsStale() {
		return csession.RetentionApplyResult{}, fmt.Errorf("%w: partition=%d", csession.ErrStaleWriter, s.session.Partition())
	}
	request, found, err := s.cat.LoadRetentionRequest(ctx, s.session.Partition())
	if err != nil {
		return csession.RetentionApplyResult{}, err
	}
	return s.session.ApplyPendingRetention(ctx, request, found)
}

func (s *writerSession) ReconcilePendingRetention(ctx context.Context) (csession.RetentionApplyResult, bool, error) {
	return s.session.ReconcilePendingRetention(ctx)
}
