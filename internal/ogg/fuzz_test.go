package ogg

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func FuzzScanner(f *testing.F) {
	good := vorbisStream(1, 44100, 3, "ARTIST=A", "TITLE=B")
	f.Add(good, uint8(0))
	f.Add(good, uint8(1))
	f.Add(append(bytes.Clone(good), opusStream(2, 2, "ARTIST=C")...), uint8(7))
	f.Add(good[:len(good)/2], uint8(3))
	f.Add([]byte("OggSOggS\x00\x02OggS"), uint8(2))
	if b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "chained.ogg")); err == nil {
		f.Add(b[:min(len(b), 20000)], uint8(40))
	}
	f.Fuzz(func(t *testing.T, data []byte, chunk uint8) {
		r := scanWith(Options{MaxHeaderBytes: 4096}, data, func(int) int { return int(chunk) }, true)
		checkContiguous(t, r, data)
		// Chunking must not change what is reported.
		whole := scanWith(Options{MaxHeaderBytes: 4096}, data, nil, true)
		if len(whole.segs) != len(r.segs) || len(whole.tracks) != len(r.tracks) {
			t.Fatalf("chunk=%d: %d/%d segments, %d/%d tracks", chunk, len(r.segs), len(whole.segs), len(r.tracks), len(whole.tracks))
		}
	})
}
