package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	objgcs "github.com/ankur-anand/objlog/gcs"
)

// openFakeGCS wires objlog to fake-gcs-server. With STORAGE_EMULATOR_HOST set
// it uses the container from docker-compose; otherwise it starts the same fake
// in-process, so this provider needs no docker at all.
func openFakeGCS(ctx context.Context, prefix string) (*provider, error) {
	const bucket = "objlog-demo"

	var (
		client *storage.Client
		where  string
		stop   = func() {}
	)

	if host := os.Getenv("STORAGE_EMULATOR_HOST"); host != "" {
		c, err := storage.NewClient(ctx, option.WithoutAuthentication())
		if err != nil {
			return nil, fmt.Errorf("gcs client for %s: %w", host, err)
		}
		if err := c.Bucket(bucket).Create(ctx, "objlog-demo", nil); err != nil && !bucketConflict(err) {
			return nil, fmt.Errorf("create bucket %q at %s: %w (is fake-gcs-server running?)", bucket, host, err)
		}
		client, where = c, host
		stop = func() { _ = c.Close() }
	} else {
		server, err := fakestorage.NewServerWithOptions(fakestorage.Options{NoListener: true})
		if err != nil {
			return nil, fmt.Errorf("start in-process fake-gcs: %w", err)
		}
		server.CreateBucket(bucket)
		client, where = server.Client(), "in-process fake-gcs-server"
		stop = func() {
			_ = client.Close()
			server.Stop()
		}
	}

	st, err := objgcs.New(objgcs.Options{
		Client:   client,
		Bucket:   bucket,
		Prefix:   prefix,
		StreamID: streamID,
	})
	if err != nil {
		stop()
		return nil, fmt.Errorf("gcs.New: %w", err)
	}

	list := func(ctx context.Context) ([]object, error) {
		var objects []object
		it := client.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: prefix})
		for {
			attrs, err := it.Next()
			if err != nil {
				if errors.Is(err, iterator.Done) {
					return objects, nil
				}
				return nil, err
			}
			objects = append(objects, object{key: attrs.Name, size: attrs.Size})
		}
	}

	return &provider{
		name:        "fake-gcs",
		where:       where,
		container:   "bucket " + bucket,
		store:       st,
		listObjects: list,
		close:       stop,
	}, nil
}

func bucketConflict(err error) bool {
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		return apiErr.Code == 409
	}
	return false
}
