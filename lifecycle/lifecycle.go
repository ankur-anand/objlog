// Package lifecycle drives physical object reclamation for an objlog store.
// It is explicitly scheduled; writers and readers never start it.
//
// A reclaimer is created from a provider store, never constructed directly:
//
//	reclaimer, err := store.NewReclaimer(lifecycle.Options{
//		DeleteDelay: 24 * time.Hour,
//	})
//	scheduler, err := lifecycle.NewScheduler(reclaimer, lifecycle.SchedulerOptions{})
//	result, err := scheduler.Run(ctx, tasks)
//
// The caller owns partition discovery and the recurring schedule.
package lifecycle

import (
	intlifecycle "github.com/ankur-anand/objlog/internal/lifecycle"
)

// Reclaimer applies retention and reclaims unreachable objects for one stream.
// Obtain one from a provider store's NewReclaimer.
type Reclaimer = intlifecycle.Reclaimer

// Options configures a reclaimer. StreamID and CatalogPrefix are set by the
// provider store; the remaining fields bound one pass.
type Options = intlifecycle.Options

// Result reports the work one RunPartition or ScrubPartition call performed.
type Result = intlifecycle.Result

// Operation selects the maintenance work a scheduled task performs.
type Operation = intlifecycle.Operation

const (
	// OperationReclaim applies ordered retention and stale staging cleanup.
	OperationReclaim = intlifecycle.OperationReclaim
	// OperationScrub discovers segment and catalog-page orphans.
	OperationScrub = intlifecycle.OperationScrub
)

// Runner performs one partition pass. *Reclaimer satisfies it.
type Runner = intlifecycle.Runner

// Task is one scheduled unit of maintenance work.
type Task = intlifecycle.Task

// Scheduler runs bounded maintenance passes across partitions.
type Scheduler = intlifecycle.Scheduler

// SchedulerOptions bounds concurrency, retries, and per-pass duration.
type SchedulerOptions = intlifecycle.SchedulerOptions

// ScheduleResult reports the outcome of one finite Run call.
type ScheduleResult = intlifecycle.ScheduleResult

// SchedulerEvent is one observable scheduler transition.
type SchedulerEvent = intlifecycle.SchedulerEvent

// SchedulerObserver receives scheduler events.
type SchedulerObserver = intlifecycle.SchedulerObserver

// SchedulerObserverFunc adapts a function to SchedulerObserver.
type SchedulerObserverFunc = intlifecycle.SchedulerObserverFunc

// DeleteRateLimiter coordinates delete throughput across reclaimers.
type DeleteRateLimiter = intlifecycle.DeleteRateLimiter

// TokenBucketDeleteLimiter is a shared token-bucket DeleteRateLimiter.
type TokenBucketDeleteLimiter = intlifecycle.TokenBucketDeleteLimiter

// Errors reported by reclaimer and scheduler calls.
var (
	ErrInvalidOptions = intlifecycle.ErrInvalidOptions
	ErrLeaseHeld      = intlifecycle.ErrLeaseHeld
	ErrLeaseLost      = intlifecycle.ErrLeaseLost
	ErrCorruptState   = intlifecycle.ErrCorruptState
)

// Reclaimer defaults applied to zero-valued Options fields.
const (
	DefaultDeleteDelay       = intlifecycle.DefaultDeleteDelay
	DefaultMaxPassDuration   = intlifecycle.DefaultMaxPassDuration
	DefaultListPageSize      = intlifecycle.DefaultListPageSize
	DefaultMaxObjectsPerRun  = intlifecycle.DefaultMaxObjectsPerRun
	DefaultMaxDeletesPerRun  = intlifecycle.DefaultMaxDeletesPerRun
	DefaultMaxDeleteBytes    = intlifecycle.DefaultMaxDeleteBytes
	DefaultDeleteBatchSize   = intlifecycle.DefaultDeleteBatchSize
	DefaultDeleteConcurrency = intlifecycle.DefaultDeleteConcurrency
	DefaultMaxQuarantine     = intlifecycle.DefaultMaxQuarantine
	DefaultCASAttempts       = intlifecycle.DefaultCASAttempts
)

// Scheduler defaults applied to zero-valued SchedulerOptions fields.
const (
	DefaultMaxConcurrentPartitions = intlifecycle.DefaultMaxConcurrentPartitions
	DefaultPartitionRunTimeout     = intlifecycle.DefaultPartitionRunTimeout
	DefaultMaxPassesPerTask        = intlifecycle.DefaultMaxPassesPerTask
	DefaultSchedulerMaxAttempts    = intlifecycle.DefaultSchedulerMaxAttempts
	DefaultRetryInitialBackoff     = intlifecycle.DefaultRetryInitialBackoff
	DefaultRetryMaxBackoff         = intlifecycle.DefaultRetryMaxBackoff
	DefaultRetryJitterFraction     = intlifecycle.DefaultRetryJitterFraction
	DefaultContinuationDelay       = intlifecycle.DefaultContinuationDelay
)

// NewScheduler builds a scheduler that drives runner across partitions.
func NewScheduler(runner Runner, opts SchedulerOptions) (*Scheduler, error) {
	return intlifecycle.NewScheduler(runner, opts)
}

// NewTokenBucketDeleteLimiter builds a shared delete rate limiter.
func NewTokenBucketDeleteLimiter(objectsPerSecond float64, burst int) (*TokenBucketDeleteLimiter, error) {
	return intlifecycle.NewTokenBucketDeleteLimiter(objectsPerSecond, burst)
}
