package tape9

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

const (
	framedZstdHeaderSize            = 4
	defaultFramedZstdRawWindowBytes = 7 * 512 * 1024
)

type zstdEncoder interface {
	EncodeAll(src, dst []byte) []byte
}

type zstdDecoder interface {
	DecodeAll(input, dst []byte) ([]byte, error)
}

func newFramedZstdEncoder() (*zstd.Encoder, error) {
	return zstd.NewWriter(nil)
}

func newFramedZstdDecoder() (*zstd.Decoder, error) {
	return zstd.NewReader(nil)
}

func encodeFramedZstdChunk(raw []byte, encoder zstdEncoder) []byte {
	compressed := encoder.EncodeAll(raw, nil)
	framed := make([]byte, framedZstdHeaderSize+len(compressed))
	binary.BigEndian.PutUint32(framed[:framedZstdHeaderSize], uint32(len(compressed)))
	copy(framed[framedZstdHeaderSize:], compressed)
	return framed
}

func decodeFramedZstdSegment(segment []byte, decoder zstdDecoder, w io.Writer) error {
	for len(segment) > 0 {
		if len(segment) < framedZstdHeaderSize {
			return fmt.Errorf("invalid framed-zstd segment: truncated header")
		}

		compressedLen := int(binary.BigEndian.Uint32(segment[:framedZstdHeaderSize]))
		segment = segment[framedZstdHeaderSize:]
		if compressedLen <= 0 {
			return fmt.Errorf("invalid framed-zstd segment: invalid compressed length %d", compressedLen)
		}
		if len(segment) < compressedLen {
			return fmt.Errorf("invalid framed-zstd segment: truncated payload")
		}

		raw, err := decoder.DecodeAll(segment[:compressedLen], nil)
		if err != nil {
			return err
		}
		if err := writePayload(w, raw); err != nil {
			return err
		}
		segment = segment[compressedLen:]
	}

	return nil
}
