package reader

import (
	"errors"
	"fmt"
)

var (
	ErrClosed             = errors.New("objlog/reader: reader closed")
	ErrInvalidOptions     = errors.New("objlog/reader: invalid options")
	ErrInvalidRequest     = errors.New("objlog/reader: invalid request")
	ErrLSNExpired         = errors.New("objlog/reader: lsn expired")
	ErrLSNExhausted       = errors.New("objlog/reader: lsn exhausted")
	ErrCorruptData        = errors.New("objlog/reader: corrupt data")
	ErrStoreRead          = errors.New("objlog/reader: store read failed")
	ErrCheckpointInvalid  = errors.New("objlog/reader: invalid cursor checkpoint")
	ErrCheckpointMismatch = errors.New("objlog/reader: cursor checkpoint mismatch")
	ErrCheckpointAhead    = errors.New("objlog/reader: cursor checkpoint is ahead of head")
)

type LSNExpiredError struct {
	Requested uint64
	Oldest    uint64
	HeadNext  uint64
}

func (e LSNExpiredError) Error() string {
	return fmt.Sprintf("%v: requested=%d oldest=%d head_next=%d", ErrLSNExpired, e.Requested, e.Oldest, e.HeadNext)
}

func (e LSNExpiredError) Unwrap() error {
	return ErrLSNExpired
}
