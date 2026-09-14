package segblock

import (
	"fmt"

	"github.com/ankur-anand/objlog/internal/segformat"
)

func Open(codec segformat.Codec, hashAlgo segformat.HashAlgo, preamble segformat.BlockPreamble, stored []byte) ([]byte, error) {
	return open(codec, hashAlgo, preamble, stored, false)
}

// OpenBorrowed verifies and opens stored while borrowing its immutable bytes.
// For CodecNone the returned raw block aliases stored; callers must keep stored
// alive and unchanged for the lifetime of the returned bytes. Compressed
// codecs still return an owned decompression buffer.
func OpenBorrowed(codec segformat.Codec, hashAlgo segformat.HashAlgo, preamble segformat.BlockPreamble, stored []byte) ([]byte, error) {
	return open(codec, hashAlgo, preamble, stored, true)
}

func open(codec segformat.Codec, hashAlgo segformat.HashAlgo, preamble segformat.BlockPreamble, stored []byte, borrowStored bool) ([]byte, error) {
	if err := codec.Validate(); err != nil {
		return nil, err
	}
	if err := hashAlgo.Validate(); err != nil {
		return nil, err
	}
	if err := preamble.Validate(); err != nil {
		return nil, err
	}
	if len(stored) != int(preamble.StoredSize) {
		return nil, fmt.Errorf("%w: stored_size=%d want=%d", segformat.ErrInvalidSegment, len(stored), preamble.StoredSize)
	}
	gotHash, err := segformat.HashBytes(hashAlgo, stored)
	if err != nil {
		return nil, err
	}
	if gotHash != preamble.BlockHash {
		return nil, fmt.Errorf("%w: block hash got=%x want=%x", segformat.ErrIntegrityMismatch, gotHash, preamble.BlockHash)
	}
	raw, err := decodeStored(codec, stored, preamble.RawSize, borrowStored)
	if err != nil {
		return nil, err
	}
	if len(raw) != int(preamble.RawSize) {
		return nil, fmt.Errorf("%w: raw_size=%d want=%d", segformat.ErrInvalidSegment, len(raw), preamble.RawSize)
	}
	return raw, nil
}

func decodeStored(codec segformat.Codec, stored []byte, rawSize uint32, borrowStored bool) ([]byte, error) {
	switch codec {
	case segformat.CodecNone:
		if borrowStored {
			return stored, nil
		}
		return append([]byte(nil), stored...), nil
	case segformat.CodecZstd:
		dec := getZstdDecoder()
		defer putZstdDecoder(dec)
		raw, err := dec.DecodeAll(stored, make([]byte, 0, rawSize))
		if err != nil {
			return nil, fmt.Errorf("decode zstd block: %w", err)
		}
		return raw, nil
	default:
		return nil, fmt.Errorf("%w: %d", segformat.ErrUnsupportedCodec, uint16(codec))
	}
}
