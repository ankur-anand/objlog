package objlog

import (
	"github.com/ankur-anand/objlog/catalog"
	"github.com/ankur-anand/objlog/reader"
	"github.com/ankur-anand/objlog/writer"
)

// Store is a complete objlog backend.
//
// Provider packages such as objlog/s3, objlog/gcs, and
// objlog/azure implement this interface by wiring together a catalog,
// segment sink, and segment source.
type Store interface {
	WriterManager() catalog.WriterManager
	RetentionManager() catalog.RetentionManager
	ReaderCatalog() catalog.Reader
	SinkFactory() writer.SinkFactory
	SegmentStore() reader.SegmentStore
}
