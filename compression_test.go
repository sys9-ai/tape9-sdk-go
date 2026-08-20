package tape9

import (
	"bytes"
	"io"
	"testing"
)

func TestParseCompression(t *testing.T) {
	got, err := ParseCompression("")
	if err != nil {
		t.Fatalf("ParseCompression(\"\"): %v", err)
	}
	if got != CompressionNone {
		t.Fatalf("ParseCompression(\"\") = %q, want %q", got, CompressionNone)
	}

	got, err = ParseCompression(" zstd ")
	if err != nil {
		t.Fatalf("ParseCompression(\" zstd \"): %v", err)
	}
	if got != CompressionZstd {
		t.Fatalf("ParseCompression(\" zstd \") = %q, want %q", got, CompressionZstd)
	}

	_, err = ParseCompression("brotli")
	if err == nil || err.Error() != `invalid compression: "brotli"` {
		t.Fatalf("ParseCompression(\"brotli\") error = %v, want %q", err, `invalid compression: "brotli"`)
	}
}

func TestFramedZstdRejectsDecodedChunkAboveRawWindow(t *testing.T) {
	encoder, err := newFramedZstdEncoder()
	if err != nil {
		t.Fatalf("new encoder: %v", err)
	}
	defer encoder.Close()

	decoder, err := newFramedZstdDecoder()
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	defer decoder.Close()

	framed := encodeFramedZstdChunk(bytes.Repeat([]byte("a"), defaultFramedZstdRawWindowBytes+1), encoder)
	if err := decodeFramedZstdSegment(framed, decoder, io.Discard); err == nil {
		t.Fatal("decode oversized chunk succeeded")
	}
}
