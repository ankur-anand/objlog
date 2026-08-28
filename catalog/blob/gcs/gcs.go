package gcs

import (
	"cloud.google.com/go/storage"
	"github.com/ankur-anand/objlog/catalog/blob"
	blobstoregcs "github.com/ankur-anand/objlog/internal/blobstore/gcs"
)

type Backend = blobstoregcs.Backend
type Options = blob.Options

func New(client *storage.Client, bucket string, opts Options) (*blob.Catalog, error) {
	backend, err := blobstoregcs.New(client, bucket)
	if err != nil {
		return nil, err
	}
	return blob.New(backend, opts)
}

func NewBackend(client *storage.Client, bucket string) (*Backend, error) {
	return blobstoregcs.New(client, bucket)
}
