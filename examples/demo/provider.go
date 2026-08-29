package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ankur-anand/objlog"
	"github.com/ankur-anand/objlog/lifecycle"
)

// streamID scopes every object this demo writes.
const streamID = "demo/orders"

// object is one stored object: its key and how many bytes it occupies.
type object struct {
	key  string
	size int64
}

// store is what a provider package gives you: an objlog.Store plus a reclaimer
// factory for physical garbage collection.
type store interface {
	objlog.Store
	NewReclaimer(opts lifecycle.Options) (*lifecycle.Reclaimer, error)
}

// provider is one emulator wired up and ready to use.
type provider struct {
	name      string
	where     string // human-readable endpoint
	container string // bucket or container name
	store     store

	// listObjects returns every object currently under the demo prefix, so what
	// retention and garbage collection do is visible rather than assumed.
	listObjects func(ctx context.Context) ([]object, error)

	close func()
}

func openProvider(ctx context.Context, name, prefix string) (*provider, error) {
	switch name {
	case "fake-gcs", "gcs":
		return openFakeGCS(ctx, prefix)
	case "minio", "s3":
		return openMinIO(ctx, prefix)
	case "azurite", "azure":
		return openAzurite(ctx, prefix)
	default:
		return nil, fmt.Errorf("unknown -provider %q: want fake-gcs, minio, or azurite", name)
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
