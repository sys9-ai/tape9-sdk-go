package tape9

import (
	"bytes"
	"testing"
)

func encodeSegmentPayload(t *testing.T, payloads [][]byte) []byte {
	t.Helper()

	var buf bytes.Buffer
	for _, payload := range payloads {
		if _, err := buf.Write(payload); err != nil {
			t.Fatalf("write payload: %v", err)
		}
	}

	return buf.Bytes()
}

func encodeFramedZstdSegment(t *testing.T, payloads [][]byte) []byte {
	t.Helper()

	encoder, err := newFramedZstdEncoder()
	if err != nil {
		t.Fatalf("new encoder: %v", err)
	}
	defer encoder.Close()

	frames := make([][]byte, 0, len(payloads))
	for _, payload := range payloads {
		frames = append(frames, encodeFramedZstdChunk(payload, encoder))
	}
	return encodeSegmentPayload(t, frames)
}
