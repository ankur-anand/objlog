package segreader

import "errors"

var (
	ErrInvalidOptions = errors.New("objlog/segreader: invalid options")
	ErrInvalidSegment = errors.New("objlog/segreader: invalid segment")
	ErrStoreRead      = errors.New("objlog/segreader: store read failed")
	ErrCorruptData    = errors.New("objlog/segreader: corrupt data")
)
