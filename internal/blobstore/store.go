// Package blobstore defines the conditional object operations shared by
// Objlog metadata services.
package blobstore

import (
	"context"
	"errors"
	"strings"
	"time"
)

var (
	ErrObjectNotFound    = errors.New("blobstore: object not found")
	ErrImmutableConflict = errors.New("blobstore: immutable object conflict")
	ErrInvalidRequest    = errors.New("blobstore: invalid request")
)

const (
	// Object-store list APIs do not share one maximum page size. The common
	// contract uses S3's 1,000-key ceiling so callers observe the same bounded
	// behavior on every backend.
	DefaultListLimit  = 1000
	MaxListLimit      = 1000
	JSONContentType   = "application/json"
	BinaryContentType = "application/octet-stream"
)

// ContentTypeForKey preserves JSON for mutable maintenance/control objects
// while identifying binary catalog-format objects by their reserved suffix.
func ContentTypeForKey(key string) string {
	if strings.HasSuffix(key, ".plc") {
		return BinaryContentType
	}
	return JSONContentType
}

// Store is the conditional object protocol used by metadata layers.
//
// Put creates an immutable object. Repeating Put with identical bytes is
// idempotent; writing different bytes to the same key returns
// ErrImmutableConflict.
//
// CompareAndSwap updates a mutable object. An empty expectedToken means
// create-if-absent. On a failed comparison, implementations return the current
// object when it still exists so callers can reconcile without another Get.
type Store interface {
	Get(ctx context.Context, key string) (Object, error)
	Put(ctx context.Context, key string, body []byte) (Object, error)
	CompareAndSwap(ctx context.Context, key string, expectedToken string, body []byte) (Object, bool, error)
	List(ctx context.Context, opts ListOptions) (ObjectPage, error)
	Delete(ctx context.Context, key string) error
}

// ConditionalGetter is implemented by stores that can avoid returning an
// object's body when its provider token has not changed. The token is opaque
// and must have been returned by the same store for the same key.
//
// GetIfChanged returns changed=false with a zero-body object when token still
// identifies the current object. A changed or missing object is reported using
// the same object and error contracts as Get.
type ConditionalGetter interface {
	GetIfChanged(ctx context.Context, key string, token string) (object Object, changed bool, err error)
}

// GetIfChanged uses a provider's conditional read when available. Stores that
// do not implement ConditionalGetter retain the ordinary Get behavior.
func GetIfChanged(ctx context.Context, store Store, key string, token string) (Object, bool, error) {
	if conditional, ok := store.(ConditionalGetter); ok && token != "" {
		return conditional.GetIfChanged(ctx, key, token)
	}
	object, err := store.Get(ctx, key)
	return object, err == nil, err
}

type Object struct {
	Key       string
	Body      []byte
	Token     string
	CreatedAt time.Time
}

type ObjectInfo struct {
	Key       string
	Token     string
	SizeBytes int
	CreatedAt time.Time
}

type ListOptions struct {
	Prefix string

	// AfterKey is an exclusive, lexicographic lower bound. It does not need to
	// name an existing object. Unlike provider continuation tokens, it is safe
	// to persist across process restarts and object deletions.
	AfterKey string

	Limit int
}

func (o ListOptions) NormalizedLimit() int {
	switch {
	case o.Limit <= 0:
		return DefaultListLimit
	case o.Limit > MaxListLimit:
		return MaxListLimit
	default:
		return o.Limit
	}
}

type ObjectPage struct {
	Objects      []ObjectInfo
	NextAfterKey string
	HasMore      bool
}
