package blobstore_test

import (
	"github.com/ankur-anand/objlog/internal/blobstore"
	blobazure "github.com/ankur-anand/objlog/internal/blobstore/azure"
	blobgcs "github.com/ankur-anand/objlog/internal/blobstore/gcs"
	blobs3 "github.com/ankur-anand/objlog/internal/blobstore/s3"
)

var (
	_ blobstore.Store = (*blobs3.Backend)(nil)
	_ blobstore.Store = (*blobgcs.Backend)(nil)
	_ blobstore.Store = (*blobazure.Backend)(nil)
)
