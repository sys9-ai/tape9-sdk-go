package tape9

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

const (
	framedZstdHeaderSize            = 4
	defaultFramedZstdRawWindowBytes = 7 * 512 * 1024
	// Match the server's accepted decoded size for ordinary compressed chunks.
	maxFramedZstdDecodedBytes = 64 * 1024 * 1024
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
	return zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(maxFramedZstdDecodedBytes))
}

func encodeFramedZstdChunk(raw []byte, encoder zstdEncoder) []byte {
	compressed := encoder.EncodeAll(raw, nil)
	framed := make([]byte, framedZstdHeaderSize+len(compressed))
	binary.BigEndian.PutUint32(framed[:framedZstdHeaderSize], uint32(len(compressed)))
	copy(framed[framedZstdHeaderSize:], compressed)
	return framed
}

func decodeFramedZstdSegment(segment []byte, decoder zstdDecoder, w io.Writer) error {
	return decodeFramedZstdReader(bytes.NewReader(segment), decoder, w)
}

func decodeFramedZstdReader(r io.Reader, decoder zstdDecoder, w io.Writer) error {
	header := make([]byte, framedZstdHeaderSize)
	for {
		_, err := io.ReadFull(r, header)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("invalid framed-zstd segment: truncated header")
		}

		compressedLen := int(binary.BigEndian.Uint32(header))
		if compressedLen <= 0 {
			return fmt.Errorf("invalid framed-zstd segment: invalid compressed length %d", compressedLen)
		}
		if compressedLen > defaultUploadChunkSizeBytes-framedZstdHeaderSize {
			return fmt.Errorf("invalid framed-zstd segment: compressed length %d exceeds maximum", compressedLen)
		}

		compressed := make([]byte, compressedLen)
		if _, err := io.ReadFull(r, compressed); err != nil {
			return fmt.Errorf("invalid framed-zstd segment: truncated payload")
		}

		raw, err := decoder.DecodeAll(compressed, nil)
		if err != nil {
			return err
		}
		if err := writePayload(w, raw); err != nil {
			return err
		}
	}
}
