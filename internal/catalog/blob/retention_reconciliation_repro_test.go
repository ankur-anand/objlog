package blob

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	pcatalog "github.com/ankur-anand/objlog/internal/catalog"
	"github.com/ankur-anand/objlog/internal/catalog/writeradapter"
	"github.com/ankur-anand/objlog/internal/keylayout"
	"github.com/ankur-anand/objlog/internal/segwriter"
	plwriter "github.com/ankur-anand/objlog/internal/writer"
)

// TestBlobCatalogWriterRetentionInterleavings requires a lost retention response
// to remain recoverable when other session operations run before its retry.
func TestBlobCatalogWriterRetentionInterleavings(t *testing.T) {
	for _, tc := range []struct {
		name          string
		reappend      bool
		publish       bool
		fullRetention bool
	}{
		{name: "immediate_retry"},
		{name: "reappend_before_retry", reappend: true},
		{name: "fresh_append_before_retry", publish: true},
		{name: "refresh_then_fresh_append", reappend: true, publish: true},
		{name: "full_retention_then_fresh_append", publish: true, fullRetention: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			trace := &retentionReproCASBackend{Backend: NewMemoryBackend()}
			backend := &casFaultBackend{Backend: trace}
			opts := commitRecoveryTestOptions()
			// Cancellation must win the backoff deterministically; no sleep is
			// needed to schedule an operation between retention and its retry.
			opts.WriterCommitInitialBackoff = time.Hour
			opts.WriterCommitMaxBackoff = time.Hour
			request := pcatalog.RetentionRequest{
				Version: pcatalog.RetentionRequestVersion, PolicyVersion: 1,
				BeforeLSN: 5, CreatedUnixMS: 1,
			}
			if tc.fullRetention {
				request.BeforeLSN = 100
			}
			cat, inner, w := newRetentionRecoveryWriter(t, backend, opts, request)
			last := inner.Head().LastSegment

			applyCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			trace.reset()
			backend.arm(casFaultAfterApplyOnce, false, func() error {
				cancel()
				return nil
			})
			_, err := w.ApplyPendingRetention(applyCtx)
			t.Logf("retention call: %v", err)
			if !errors.Is(err, pcatalog.ErrCommitIndeterminate) || !errors.Is(err, context.Canceled) {
				t.Fatalf("retention error = %v, want indeterminate canceled commit", err)
			}
			if err := w.Err(); err != nil {
				t.Fatalf("Writer.Err() before intervening operation = %v, want nil", err)
			}
			durable, err := cat.LoadPartition(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("after lost response: durable policy=%d, session policy=%d, writer policy=%d",
				durable.AppliedRetentionVersion, inner.Head().AppliedRetentionVersion, w.State().Snapshot.Head.AppliedRetentionVersion)
			if durable.AppliedRetentionVersion != 1 || durable.AppliedRetentionLSN != min(request.BeforeLSN, 10) ||
				inner.Head().AppliedRetentionVersion != 0 || w.State().Snapshot.Head.AppliedRetentionVersion != 0 {
				t.Fatal("fault did not produce a landed retention with stale local snapshots")
			}
			if calls, _ := backend.stats(); calls != 1 {
				t.Fatalf("retention head CAS calls = %d, want 1", calls)
			}

			if tc.reappend {
				if _, err := inner.AppendSegment(ctx, last); err != nil {
					t.Fatalf("reappend = %v", err)
				}
				t.Logf("after reappend: session policy=%d, writer policy=%d",
					inner.Head().AppliedRetentionVersion, w.State().Snapshot.Head.AppliedRetentionVersion)
			}
			wantCalls, wantNextLSN := 1, uint64(10)
			if tc.publish {
				wantCalls, wantNextLSN = 2, 11
				if _, err := w.Append(ctx, plwriter.Record{TimestampMS: 10, Value: []byte("next")}); err != nil {
					t.Fatalf("Append() = %v", err)
				}
				if _, err := w.Flush(ctx); err != nil {
					t.Errorf("Flush() = %v, want successful publication", err)
				}
			}
			result, retryErr := w.ApplyPendingRetention(ctx)
			t.Logf("retention retry: applied=%v err=%v; Writer.Err()=%v", result.Applied, retryErr, w.Err())
			if retryErr != nil {
				t.Errorf("retention retry = %v, want nil", retryErr)
			}
			if err := w.Err(); err != nil {
				t.Errorf("Writer.Err() = %v, want nil", err)
			}
			for i, call := range trace.snapshot() {
				t.Logf("head CAS %d: expected token=%q, returned token=%q, swapped=%v, storage error=%v",
					i+1, call.expectedToken, call.returnedToken, call.swapped, call.err)
				if !call.swapped || call.err != nil {
					t.Errorf("head CAS %d did not succeed", i+1)
				}
			}
			if calls, callbackErr := backend.stats(); calls != wantCalls || callbackErr != nil {
				t.Errorf("head CAS calls=%d callbackErr=%v, want %d nil", calls, callbackErr, wantCalls)
			}
			durable, err = cat.LoadPartition(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("final durable head: next LSN=%d, applied retention policy=%d", durable.NextLSN, durable.AppliedRetentionVersion)
			if durable.NextLSN != wantNextLSN || durable.AppliedRetentionVersion != 1 {
				t.Errorf("durable next LSN=%d policy=%d, want %d 1", durable.NextLSN, durable.AppliedRetentionVersion, wantNextLSN)
			}
			if retryErr == nil {
				if result.Applied == tc.publish {
					t.Errorf("retry applied=%v, want %v (publication acknowledges retention first)", result.Applied, !tc.publish)
				}
				if result.Snapshot.Head != durable {
					t.Error("reconciled writer snapshot differs from durable head")
				}
				noOp, err := w.ApplyPendingRetention(ctx)
				if err != nil || noOp.Applied || noOp.Snapshot != result.Snapshot {
					t.Errorf("subsequent retention = %+v, err=%v, want unchanged no-op", noOp, err)
				}
				if calls, _ := backend.stats(); calls != wantCalls {
					t.Errorf("head CAS calls after no-op=%d, want %d", calls, wantCalls)
				}
			}
		})
	}
}

func TestBlobCatalogRetentionRecoveryBeforeUnappliedCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	backend := &casFaultBackend{Backend: NewMemoryBackend()}
	opts := commitRecoveryTestOptions()
	opts.WriterCommitMaxAttempts = 1
	request := pcatalog.RetentionRequest{Version: pcatalog.RetentionRequestVersion, PolicyVersion: 1, BeforeLSN: 100}
	cat, _, w := newRetentionRecoveryWriter(t, backend, opts, request)
	// The CAS never reaches storage, and its follow-up observation also fails.
	backend.arm(casFaultBeforeApplyOnce, true, nil)
	if _, err := w.ApplyPendingRetention(ctx); !errors.Is(err, plwriter.ErrRetentionIndeterminate) {
		t.Fatalf("ApplyPendingRetention() = %v, want unknown outcome", err)
	}
	backend.mu.Lock()
	backend.failGets = false
	backend.mu.Unlock()
	durable, err := cat.LoadPartition(ctx, 1)
	if err != nil || durable.AppliedRetentionVersion != 0 {
		t.Fatalf("before recovery head=%+v err=%v, want unapplied retention", durable, err)
	}
	if _, err := w.Append(ctx, plwriter.Record{TimestampMS: 10, Value: []byte("next")}); err != nil {
		t.Fatal(err)
	}
	state, err := w.Flush(ctx)
	if err != nil || w.Err() != nil {
		t.Fatalf("Flush()=%v Writer.Err()=%v", err, w.Err())
	}
	if state.Head.NextLSN != 11 || state.Head.OldestLSN != 10 || state.Head.AppliedRetentionLSN != 10 || state.Head.AppliedRetentionVersion != 1 {
		t.Fatalf("recovered state=%+v", state)
	}
	if calls, _ := backend.stats(); calls != 3 {
		t.Fatalf("head CAS calls=%d, want failed retention + replay + append", calls)
	}
}

func TestBlobCatalogSessionAppendReconcilesRetentionWithoutConsumingResult(t *testing.T) {
	ctx := context.Background()
	backend := &casFaultBackend{Backend: NewMemoryBackend()}
	opts := commitRecoveryTestOptions()
	opts.WriterCommitInitialBackoff, opts.WriterCommitMaxBackoff = time.Hour, time.Hour
	request := pcatalog.RetentionRequest{Version: pcatalog.RetentionRequestVersion, PolicyVersion: 1, BeforeLSN: 100}
	_, inner, _ := newRetentionRecoveryWriter(t, backend, opts, request)
	retention := inner.(pcatalog.RetentionWriterSession)
	applyCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	backend.arm(casFaultAfterApplyOnce, false, func() error { cancel(); return nil })
	if _, err := retention.ApplyPendingRetention(applyCtx); !errors.Is(err, pcatalog.ErrCommitIndeterminate) {
		t.Fatal(err)
	}
	// Exercise the catalog Session itself, without the writer's recovery hook.
	state, err := inner.AppendSegment(ctx, testSegmentRef(1, 10, 19, inner.Epoch()))
	if err != nil || state.NextLSN != 20 || state.OldestLSN != 10 || state.AppliedRetentionVersion != 1 {
		t.Fatalf("append after unknown retention: state=%+v err=%v", state, err)
	}
	result, err := retention.ApplyPendingRetention(ctx)
	if err != nil || !result.Applied || result.Request != request || result.Head != state {
		t.Fatalf("retention acknowledgement=%+v err=%v", result, err)
	}
	if calls, _ := backend.stats(); calls != 2 {
		t.Fatalf("head CAS calls=%d, want original retention and fresh append", calls)
	}
	gets := backend.getCount()
	_, pending, err := inner.(pcatalog.RetentionReconciler).ReconcilePendingRetention(ctx)
	if err != nil || pending || backend.getCount() != gets {
		t.Fatalf("idle reconciliation: pending=%v err=%v reads=%d, want false nil %d", pending, err, backend.getCount(), gets)
	}
}

func TestBlobCatalogRetentionRecoveryDoesNotPollNewMailbox(t *testing.T) {
	for _, publish := range []bool{false, true} {
		name := "explicit_retry"
		if publish {
			name = "background_recovery"
		}
		t.Run(name, func(t *testing.T) {
			ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			backend := &casFaultBackend{Backend: NewMemoryBackend()}
			opts := commitRecoveryTestOptions()
			opts.WriterCommitInitialBackoff, opts.WriterCommitMaxBackoff = time.Hour, time.Hour
			first := pcatalog.RetentionRequest{Version: pcatalog.RetentionRequestVersion, PolicyVersion: 1, BeforeLSN: 5}
			cat, _, w := newRetentionRecoveryWriter(t, backend, opts, first)
			applyCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			backend.arm(casFaultAfterApplyOnce, false, func() error { cancel(); return nil })
			if _, err := w.ApplyPendingRetention(applyCtx); !errors.Is(err, plwriter.ErrRetentionIndeterminate) {
				t.Fatal(err)
			}
			second := pcatalog.RetentionRequest{Version: pcatalog.RetentionRequestVersion, PolicyVersion: 2, BeforeLSN: 10}
			if _, err := cat.RequestRetention(ctx, 1, second); err != nil {
				t.Fatal(err)
			}
			if publish {
				if _, err := w.Append(ctx, plwriter.Record{TimestampMS: 10, Value: []byte("next")}); err != nil {
					t.Fatal(err)
				}
				if _, err := w.Flush(ctx); err != nil {
					t.Fatal(err)
				}
			} else {
				result, err := w.ApplyPendingRetention(ctx)
				if err != nil || !result.Applied || result.PolicyVersion != first.PolicyVersion {
					t.Fatalf("original request acknowledgement=%+v err=%v", result, err)
				}
			}
			if got := w.State().Snapshot.Head.AppliedRetentionVersion; got != first.PolicyVersion {
				t.Fatalf("recovery applied policy=%d, want original policy 1", got)
			}
			wantCalls := 2 // Original head mutation and newer mailbox request.
			if publish {
				wantCalls++
			}
			if calls, _ := backend.stats(); calls != wantCalls {
				t.Fatalf("CAS calls=%d, want %d without a newer retention mutation", calls, wantCalls)
			}
			// Only a subsequent explicit retention call consumes the newer policy.
			result, err := w.ApplyPendingRetention(ctx)
			if err != nil || !result.Applied || result.PolicyVersion != second.PolicyVersion || w.Err() != nil {
				t.Fatalf("new request result=%+v err=%v Writer.Err()=%v", result, err, w.Err())
			}
		})
	}
}

func TestBlobCatalogRetentionRecoveryPreservesQueueDuringReadFailure(t *testing.T) {
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	reads := &retentionRecoveryReadBackend{Backend: NewMemoryBackend(), attempted: make(chan struct{}, 1)}
	backend := &casFaultBackend{Backend: reads}
	opts := commitRecoveryTestOptions()
	opts.WriterCommitInitialBackoff, opts.WriterCommitMaxBackoff = time.Hour, time.Hour
	request := pcatalog.RetentionRequest{Version: pcatalog.RetentionRequestVersion, PolicyVersion: 1, BeforeLSN: 5}
	_, _, w := newRetentionRecoveryWriter(t, backend, opts, request)
	applyCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	backend.arm(casFaultAfterApplyOnce, false, func() error { cancel(); return nil })
	if _, err := w.ApplyPendingRetention(applyCtx); !errors.Is(err, plwriter.ErrRetentionIndeterminate) {
		t.Fatal(err)
	}
	reads.setFail(true)
	for i := 0; i < 2; i++ {
		if _, err := w.Append(ctx, plwriter.Record{TimestampMS: int64(10 + i), Value: []byte("queued")}); err != nil {
			t.Fatal(err)
		}
		if err := w.Cut(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// A second read attempt proves that the first failed recovery was retried.
	for i := 0; i < 2; i++ {
		select {
		case <-reads.attempted:
		case <-ctx.Done():
			t.Fatal("reconciliation did not retry")
		}
	}
	if w.Err() != nil || w.State().InflightSegments != 2 || w.State().Snapshot.Head.NextLSN != 10 {
		t.Fatalf("unresolved recovery dropped work: state=%+v err=%v", w.State(), w.Err())
	}
	if calls, _ := backend.stats(); calls != 1 {
		t.Fatalf("CAS calls during unresolved recovery=%d, want only the original retention", calls)
	}
	reads.setFail(false)
	state, err := w.Flush(ctx)
	if err != nil || state.Head.NextLSN != 12 || state.Head.SegmentCount != 3 || state.Head.AppliedRetentionVersion != 1 || w.Err() != nil {
		t.Fatalf("recovery failed to drain queue: state=%+v err=%v Writer.Err()=%v", state, err, w.Err())
	}
	if calls, _ := backend.stats(); calls != 3 {
		t.Fatalf("CAS calls=%d, want retention and two queued appends", calls)
	}
}

func TestBlobCatalogRetentionRecoveryRejectsSuccessorFence(t *testing.T) {
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	backend := &casFaultBackend{Backend: NewMemoryBackend()}
	opts := commitRecoveryTestOptions()
	opts.WriterCommitInitialBackoff, opts.WriterCommitMaxBackoff = time.Hour, time.Hour
	request := pcatalog.RetentionRequest{Version: pcatalog.RetentionRequestVersion, PolicyVersion: 1, BeforeLSN: 5}
	cat, _, w := newRetentionRecoveryWriter(t, backend, opts, request)
	applyCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	backend.arm(casFaultAfterApplyOnce, false, func() error { cancel(); return nil })
	if _, err := w.ApplyPendingRetention(applyCtx); !errors.Is(err, plwriter.ErrRetentionIndeterminate) {
		t.Fatal(err)
	}
	if _, err := cat.OpenWriter(ctx, 1, [16]byte{2}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(ctx, plwriter.Record{TimestampMS: 10, Value: []byte("fenced")}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Flush(ctx); !errors.Is(err, plwriter.ErrStaleWriter) || !errors.Is(w.Err(), plwriter.ErrStaleWriter) {
		t.Fatalf("Flush()=%v Writer.Err()=%v, want stale writer", err, w.Err())
	}
	if calls, _ := backend.stats(); calls != 2 {
		t.Fatalf("CAS calls=%d, want retention and takeover only", calls)
	}
	durable, err := cat.LoadPartition(ctx, 1)
	if err != nil || durable.NextLSN != 10 || durable.WriterEpoch != 2 || durable.AppliedRetentionVersion != 1 {
		t.Fatalf("successor head=%+v err=%v", durable, err)
	}
}

type retentionRecoveryReadBackend struct {
	Backend
	mu        sync.Mutex
	fail      bool
	attempted chan struct{}
}

func (b *retentionRecoveryReadBackend) setFail(fail bool) {
	b.mu.Lock()
	b.fail = fail
	b.mu.Unlock()
}

func (b *retentionRecoveryReadBackend) Get(ctx context.Context, key string) (Object, error) {
	b.mu.Lock()
	fail := b.fail
	b.mu.Unlock()
	if fail {
		select {
		case b.attempted <- struct{}{}:
		default:
		}
		return Object{}, errInjectedCAS
	}
	return b.Backend.Get(ctx, key)
}

// This wrapper records the actual storage outcome before casFaultBackend hides
// the successful retention response. After reset, only head CASes are issued.
type retentionReproCASBackend struct {
	Backend
	mu    sync.Mutex
	calls []retentionReproCAS
}

type retentionReproCAS struct {
	expectedToken string
	returnedToken string
	swapped       bool
	err           error
}

func (b *retentionReproCASBackend) CompareAndSwap(ctx context.Context, key, expectedToken string, body []byte) (Object, bool, error) {
	object, swapped, err := b.Backend.CompareAndSwap(ctx, key, expectedToken, body)
	b.mu.Lock()
	b.calls = append(b.calls, retentionReproCAS{expectedToken, object.Token, swapped, err})
	b.mu.Unlock()
	return object, swapped, err
}

func (b *retentionReproCASBackend) reset() {
	b.mu.Lock()
	b.calls = nil
	b.mu.Unlock()
}

func (b *retentionReproCASBackend) snapshot() []retentionReproCAS {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]retentionReproCAS(nil), b.calls...)
}

func newRetentionRecoveryWriter(t *testing.T, backend Backend, opts Options, request pcatalog.RetentionRequest) (*Catalog, pcatalog.WriterSession, *plwriter.Writer) {
	t.Helper()
	ctx := context.Background()
	cat, err := New(backend, opts)
	if err != nil {
		t.Fatal(err)
	}
	inner, err := cat.OpenWriter(ctx, 1, [16]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := inner.AppendSegment(ctx, testSegmentRef(1, 0, 9, inner.Epoch())); err != nil {
		t.Fatal(err)
	}
	if _, err := cat.RequestRetention(ctx, 1, request); err != nil {
		t.Fatal(err)
	}
	adapter, err := writeradapter.New(inner)
	if err != nil {
		t.Fatal(err)
	}
	writerOpts := plwriter.DefaultOptions(plwriter.SinkFactoryFunc(func(_ context.Context, info plwriter.SegmentInfo) (segwriter.Sink, error) {
		uri := keylayout.SegmentObjectKey("objlog", info.StreamID, info.Partition, info.BaseLSN, info.WriterEpoch, info.SegmentUUID)
		return segwriter.NewMemorySink(uri), nil
	}))
	writerOpts.Session = adapter
	w, err := plwriter.New(writerOpts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Abort(context.Background()) })
	return cat, inner, w
}
