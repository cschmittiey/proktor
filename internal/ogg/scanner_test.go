package ogg

import (
	"bytes"
	"math/rand"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

var chainedWant = []pair{
	{"New Order", "Blue Monday"},
	{"Röyksopp", "Eple"},
	{"坂本龍一", "Merry Christmas Mr. Lawrence"},
	{"Aphex Twin", "Xtal"},
	{"DJ Überlong Name feat. Ünïcödé", "Track with = equals sign"},
}

func TestFixtureChained(t *testing.T) {
	data := readFixture(t, "chained.ogg")
	r := scanWith(Options{}, data, nil, true)
	checkContiguous(t, r, data)
	if len(r.errs) != 0 {
		t.Errorf("unexpected errors: %v", r.errs)
	}
	if got := pairs(r.tracks); !reflect.DeepEqual(got, chainedWant) {
		t.Fatalf("tracks = %+v, want %+v", got, chainedWant)
	}
	byOffset := map[int64]Segment{}
	for _, s := range r.segs {
		byOffset[s.Offset] = s
	}
	seen := map[uint32]bool{}
	var last int64 = -1
	for i, tr := range r.tracks {
		if tr.SampleRate != 44100 {
			t.Errorf("track %d: SampleRate=%d", i, tr.SampleRate)
		}
		if tr.Offset <= last {
			t.Errorf("track %d: offset %d not increasing", i, tr.Offset)
		}
		last = tr.Offset
		if string(data[tr.Offset:tr.Offset+4]) != "OggS" {
			t.Errorf("track %d: offset %d is not a page", i, tr.Offset)
		}
		seg, ok := byOffset[tr.Offset]
		if !ok || !seg.Page || !seg.BOS || seg.Serial != tr.Serial {
			t.Errorf("track %d: no matching BOS page at offset %d: %+v", i, tr.Offset, seg)
		}
		if seen[tr.Serial] {
			t.Errorf("track %d: duplicate serial %d", i, tr.Serial)
		}
		seen[tr.Serial] = true
		if tr.Vendor == "" || len(tr.Comments) < 2 {
			t.Errorf("track %d: vendor %q comments %q", i, tr.Vendor, tr.Comments)
		}
		if tr.Comments[0] != "ARTIST="+tr.Artist && !contains(tr.Comments, "ARTIST="+tr.Artist) {
			t.Errorf("track %d: comments %q lack artist", i, tr.Comments)
		}
	}
	// Exactly the pages before the first audio page (granule 0) of each
	// stream are headers, and they start at the BOS page.
	state := map[uint32]string{} // serial -> "hdr" | "audio"
	for _, p := range r.pages() {
		wantHeader := p.Granule == 0
		if p.Header != wantHeader {
			t.Errorf("page at %d (serial %d seq %d granule %d): Header=%v", p.Offset, p.Serial, p.Sequence, p.Granule, p.Header)
		}
		if p.BOS && !p.Header {
			t.Errorf("BOS page at %d not Header", p.Offset)
		}
		if p.Header && state[p.Serial] == "audio" {
			t.Errorf("header page after audio at %d", p.Offset)
		}
		if p.Header {
			state[p.Serial] = "hdr"
		} else {
			state[p.Serial] = "audio"
		}
	}
	if len(r.errs) != 0 {
		t.Errorf("errors: %v", r.errs)
	}
	// OnTrack comes after the segment of the page that completed the header.
	for i, n := range r.segsAtTrack {
		last := r.segs[n-1]
		if !last.Page || last.Serial != r.tracks[i].Serial || !last.Header || last.BOS {
			t.Errorf("track %d fired after unexpected segment %+v", i, last)
		}
	}
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

// TestTestdata runs over every committed capture, including a real
// testdata/traktor-capture.ogg once it is added.
func TestTestdata(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "..", "testdata", "*.ogg"))
	if err != nil || len(files) == 0 {
		t.Skip("no testdata/*.ogg fixtures")
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			data := readFixture(t, filepath.Base(f))
			r := scanWith(Options{}, data, nil, true)
			checkContiguous(t, r, data)
			if len(r.tracks) < 1 {
				t.Fatalf("no tracks detected (errors: %v)", r.errs)
			}
			for _, tr := range r.tracks {
				t.Logf("track at %d: %q - %q (%d Hz)", tr.Offset, tr.Artist, tr.Title, tr.SampleRate)
			}
			if filepath.Base(f) == "chained.ogg" {
				if got := pairs(r.tracks); !reflect.DeepEqual(got, chainedWant) {
					t.Errorf("tracks = %+v, want %+v", got, chainedWant)
				}
			}
		})
	}
}

func TestChunkingInvariant(t *testing.T) {
	data := readFixture(t, "chained.ogg")
	want := scanWith(Options{}, data, nil, true)
	cases := map[string]func(int) int{"1-byte": oneByte}
	for seed := int64(1); seed <= 4; seed++ {
		cases[strings.Repeat("r", int(seed))] = randomChunks(seed, 3000)
	}
	cases["tiny-random"] = randomChunks(99, 40)
	for name, ch := range cases {
		t.Run(name, func(t *testing.T) {
			got := scanWith(Options{}, data, ch, true)
			if !reflect.DeepEqual(got.segs, want.segs) {
				t.Fatalf("segments differ (got %d, want %d)", len(got.segs), len(want.segs))
			}
			if !reflect.DeepEqual(got.tracks, want.tracks) {
				t.Fatalf("tracks differ: %+v vs %+v", got.tracks, want.tracks)
			}
			if len(got.errs) != 0 {
				t.Fatalf("errors: %v", got.errs)
			}
		})
	}
}

func TestChunkingInvariantWithGarbage(t *testing.T) {
	var data []byte
	data = append(data, junkBytes(1, 100)...)
	data = append(data, vorbisStream(1, 44100, 3, "ARTIST=A", "TITLE=One")...)
	data = append(data, junkBytes(2, 10000)...)
	data = append(data, "Og"...)
	data = append(data, "OggS\x01"...)
	data = append(data, vorbisStream(2, 48000, 3, "ARTIST=B", "TITLE=Two")...)
	data = append(data, junkBytes(3, 7)...)
	want := scanWith(Options{}, data, nil, true)
	checkContiguous(t, want, data)
	for name, ch := range map[string]func(int) int{"1-byte": oneByte, "random": randomChunks(5, 500)} {
		got := scanWith(Options{}, data, ch, true)
		if !reflect.DeepEqual(got.segs, want.segs) || !reflect.DeepEqual(got.tracks, want.tracks) {
			t.Errorf("%s: output differs from single write", name)
		}
	}
}

// junkBytes returns deterministic bytes that cannot contain a capture pattern.
func junkBytes(seed int64, n int) []byte {
	rng := rand.New(rand.NewSource(seed))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rng.Intn(256))
		if b[i] == 'O' {
			b[i] = 'o'
		}
	}
	return b
}

func TestGarbage(t *testing.T) {
	s1 := vorbisStream(11, 44100, 4, "ARTIST=One", "TITLE=First")
	s2 := vorbisStream(22, 44100, 4, "ARTIST=Two", "TITLE=Second")
	s3 := vorbisStream(33, 44100, 4, "ARTIST=Three", "TITLE=Third")
	pages := func(s []byte) [][]byte {
		var out [][]byte
		for len(s) > 0 {
			n := 27 + int(s[26])
			for _, lv := range s[27:n] {
				n += int(lv)
			}
			out = append(out, s[:n])
			s = s[n:]
		}
		return out
	}

	t.Run("junk before and between pages", func(t *testing.T) {
		var data []byte
		data = append(data, junkBytes(1, 333)...)
		for i, p := range pages(s1) {
			data = append(data, p...)
			if i == 2 {
				data = append(data, junkBytes(2, 50)...)
			}
		}
		data = append(data, junkBytes(3, 9000)...)
		data = append(data, s2...)
		data = append(data, s3...)
		r := scanAll(data)
		checkContiguous(t, r, data)
		if got, want := len(r.tracks), 3; got != want {
			t.Fatalf("tracks = %d, want %d: %+v", got, want, pairs(r.tracks))
		}
		if len(r.errs) == 0 {
			t.Error("OnError never called")
		}
		for _, tr := range r.tracks {
			if string(data[tr.Offset:tr.Offset+4]) != "OggS" {
				t.Errorf("track offset %d does not point at a page", tr.Offset)
			}
		}
	})

	t.Run("corrupted audio page", func(t *testing.T) {
		pp := pages(s2)
		bad := bytes.Clone(pp[3])
		bad[len(bad)-5] ^= 0xff
		var data []byte
		data = append(data, s1...)
		for i, p := range pp {
			if i == 3 {
				p = bad
			}
			data = append(data, p...)
		}
		data = append(data, s3...)
		r := scanAll(data)
		checkContiguous(t, r, data)
		if len(r.tracks) != 3 {
			t.Fatalf("tracks = %+v", pairs(r.tracks))
		}
		if len(r.errs) != 1 || !strings.Contains(r.errs[0].Error(), "CRC") {
			t.Errorf("errors = %v, want one CRC error", r.errs)
		}
		npages := len(pages(s1)) + len(pages(s2)) - 1 + len(pages(s3))
		if got := len(r.pages()); got != npages {
			t.Errorf("pages = %d, want %d", got, npages)
		}
		var badSeg *Segment
		for i, s := range r.segs {
			if !s.Page && bytes.Equal(s.Data, bad) {
				badSeg = &r.segs[i]
			}
		}
		if badSeg == nil {
			t.Error("corrupted page not emitted as one non-page segment")
		}
	})

	t.Run("corrupted BOS loses only that track", func(t *testing.T) {
		pp := pages(s2)
		pp[0] = bytes.Clone(pp[0])
		pp[0][30] ^= 1
		var data []byte
		data = append(data, s1...)
		data = append(data, bytes.Join(pp, nil)...)
		data = append(data, s3...)
		r := scanAll(data)
		checkContiguous(t, r, data)
		want := []pair{{"One", "First"}, {"Three", "Third"}}
		if got := pairs(r.tracks); !reflect.DeepEqual(got, want) {
			t.Errorf("tracks = %+v, want %+v", got, want)
		}
		if len(r.errs) == 0 {
			t.Error("OnError never called")
		}
	})

	t.Run("corrupted CRC field", func(t *testing.T) {
		data := bytes.Clone(s1)
		data[22] ^= 0x80 // BOS page checksum
		data = append(data, s2...)
		r := scanAll(data)
		checkContiguous(t, r, data)
		if got := pairs(r.tracks); !reflect.DeepEqual(got, []pair{{"Two", "Second"}}) {
			t.Errorf("tracks = %+v", got)
		}
	})

	t.Run("false capture patterns", func(t *testing.T) {
		var data []byte
		data = append(data, "OggS"...)                          // truncated header, immediately followed by...
		data = append(data, "OggSOggS\x00\x02"...)              // ...more of the same
		data = append(data, bytes.Repeat([]byte("OggS"), 5)...) //
		data = append(data, "OggS\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff"...)
		data = append(data, s1...)
		r := scanAll(data)
		checkContiguous(t, r, data)
		if len(r.tracks) != 1 || len(r.errs) == 0 {
			t.Errorf("tracks=%+v errs=%v", pairs(r.tracks), r.errs)
		}
	})

	t.Run("no ogg at all", func(t *testing.T) {
		data := junkBytes(9, 100000)
		r := scanAll(data)
		checkContiguous(t, r, data)
		if len(r.tracks) != 0 || len(r.pages()) != 0 {
			t.Error("found structure in junk")
		}
		if len(r.errs) != 1 {
			t.Errorf("got %d errors for one junk run, want 1", len(r.errs))
		}
		for _, s := range r.segs {
			if len(s.Data) > maxJunk {
				t.Fatalf("junk segment of %d bytes", len(s.Data))
			}
		}
	})
}

func TestTruncated(t *testing.T) {
	data := vorbisStream(7, 44100, 3, "ARTIST=Cut", "TITLE=Short")
	for cut := 0; cut <= len(data); cut++ {
		in := data[:cut]
		r := scanWith(Options{}, in, nil, false)
		pre := len(r.bytes())
		if cut-pre > maxPageSize {
			t.Fatalf("cut %d: %d bytes held back before Flush", cut, cut-pre)
		}
		r = scanWith(Options{}, in, nil, true)
		checkContiguous(t, r, in)
	}

	// Flush emits a trailing partial page as a non-page segment.
	cut := len(data) - 10
	r := scanWith(Options{}, data[:cut], nil, false)
	before := len(r.segs)
	var rr result
	opts := Options{
		OnSegment: func(s Segment) { rr.segs = append(rr.segs, s) },
		OnError:   func(err error) { rr.errs = append(rr.errs, err) },
	}
	sc := NewScanner(opts)
	sc.Write(data[:cut])
	n := len(rr.segs)
	if n != before {
		t.Fatalf("segments before flush: %d vs %d", n, before)
	}
	sc.Flush()
	if len(rr.segs) != n+1 || rr.segs[n].Page {
		t.Fatalf("Flush: want exactly one extra non-page segment, got %d", len(rr.segs)-n)
	}
	if len(rr.errs) == 0 {
		t.Error("truncation not reported")
	}
	rr.segs = nil
	sc.Flush() // idempotent
	if len(rr.segs) != 0 {
		t.Error("second Flush emitted data")
	}
}

func TestFlushFindsPageBehindFalseCapture(t *testing.T) {
	// A fake header claiming a huge page swallows a real page behind it
	// until Flush.
	fake := append([]byte("OggS\x00\x00"), make([]byte, 20)...)
	fake = append(fake, 255) // 255 lacing values follow
	fake = append(fake, bytes.Repeat([]byte{255}, 255)...)
	real := vorbisStream(5, 44100, 1, "ARTIST=Behind", "TITLE=Fake")
	data := append(fake, real...)
	r := scanWith(Options{}, data, nil, false)
	if len(r.tracks) != 0 {
		t.Fatal("track before enough data")
	}
	checkContiguous(t, scanAll(data), data)
	r = scanAll(data)
	if got := pairs(r.tracks); !reflect.DeepEqual(got, []pair{{"Behind", "Fake"}}) {
		t.Errorf("tracks = %+v", got)
	}
}

func TestHeaderFlags(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		data := vorbisStream(1, 44100, 3, "ARTIST=A", "TITLE=B")
		r := scanAll(data)
		checkContiguous(t, r, data)
		var got []bool
		for _, p := range r.pages() {
			got = append(got, p.Header)
		}
		if want := []bool{true, true, false, false, false}; !reflect.DeepEqual(got, want) {
			t.Errorf("Header = %v, want %v", got, want)
		}
		p := r.pages()
		if !p[0].BOS || p[0].EOS || !p[4].EOS || p[4].BOS {
			t.Error("BOS/EOS flags wrong")
		}
		if p[2].Granule != 1024 || p[2].Sequence != 2 || p[2].Serial != 1 {
			t.Errorf("page fields: %+v", p[2])
		}
	})

	t.Run("comment spanning pages", func(t *testing.T) {
		big := strings.Repeat("x", 150_000)
		data := vorbisStream(1, 44100, 2, "ARTIST=A", "TITLE=B", "COVER="+big)
		r := scanAll(data)
		checkContiguous(t, r, data)
		ps := r.pages()
		if len(ps) < 5 {
			t.Fatalf("expected the comment to span several pages, got %d pages", len(ps))
		}
		for i, p := range ps {
			// Everything up to and including the page with granule 0 that
			// completes setup is a header; the last two are audio.
			want := i < len(ps)-2
			if p.Header != want {
				t.Errorf("page %d: Header=%v, want %v (granule %d)", i, p.Header, want, p.Granule)
			}
		}
		if len(r.tracks) != 1 || r.tracks[0].Title != "B" || len(r.tracks[0].Comments) != 3 {
			t.Fatalf("tracks = %+v", r.tracks)
		}
		// The track fires after the segment of the page completing the comment.
		if n := r.segsAtTrack[0]; n != len(ps)-2 {
			t.Errorf("track fired after %d segments, want %d", n, len(ps)-2)
		}
	})

	t.Run("chained with no EOS and serial reuse", func(t *testing.T) {
		a := vorbisStream(1, 44100, 2, "ARTIST=A", "TITLE=1")
		a = a[:bytes.LastIndex(a, []byte("OggS"))] // drop EOS page
		data := append(a, vorbisStream(1, 44100, 2, "ARTIST=A", "TITLE=2")...)
		r := scanAll(data)
		checkContiguous(t, r, data)
		if got := pairs(r.tracks); !reflect.DeepEqual(got, []pair{{"A", "1"}, {"A", "2"}}) {
			t.Errorf("tracks = %+v", got)
		}
		var hdr int
		for _, p := range r.pages() {
			if p.Header {
				hdr++
			}
		}
		if hdr != 4 {
			t.Errorf("%d header pages, want 4", hdr)
		}
	})

	t.Run("multiplexed", func(t *testing.T) {
		// Two grouped streams: both BOS pages first, then interleaved.
		mk := func(serial uint32, comments ...string) *muxer {
			m := newMuxer(serial)
			m.packet(vorbisIdent(44100))
			m.flush(0, false)
			return m
		}
		a, b := mk(1), mk(2)
		var data []byte
		data = append(data, a.out...)
		data = append(data, b.out...)
		a.out, b.out = nil, nil
		a.packet(vorbisComment("v", "ARTIST=A", "TITLE=a"))
		a.packet(vorbisSetup())
		a.flush(0, false)
		b.packet(vorbisComment("v", "ARTIST=B", "TITLE=b"))
		b.packet(vorbisSetup())
		b.flush(0, false)
		a.packet([]byte("audio-a"))
		a.flush(100, false)
		b.packet([]byte("audio-b"))
		b.flush(100, false)
		a.packet([]byte("audio-a2"))
		a.flush(200, true)
		b.packet([]byte("audio-b2"))
		b.flush(200, true)
		data = append(data, a.out...)
		data = append(data, b.out...)
		// interleave the page lists the simple way: pages are already in order a..., b...
		r := scanAll(data)
		checkContiguous(t, r, data)
		if got := pairs(r.tracks); !reflect.DeepEqual(got, []pair{{"A", "a"}, {"B", "b"}}) {
			t.Errorf("tracks = %+v", got)
		}
		for _, p := range r.pages() {
			if want := p.Granule == 0; p.Header != want {
				t.Errorf("serial %d seq %d: Header=%v", p.Serial, p.Sequence, p.Header)
			}
		}
	})
}

func TestOversizeCommentHeader(t *testing.T) {
	good1 := vorbisStream(1, 44100, 2, "ARTIST=One", "TITLE=First")
	huge := vorbisStream(2, 44100, 2, "ARTIST=Two", "TITLE=Second", "COVERART="+strings.Repeat("A", 5000))
	good3 := vorbisStream(3, 44100, 2, "ARTIST=Three", "TITLE=Third")
	data := append(append(bytes.Clone(good1), huge...), good3...)

	for name, ch := range map[string]func(int) int{"whole": nil, "1-byte": oneByte} {
		t.Run(name, func(t *testing.T) {
			r := scanWith(Options{MaxHeaderBytes: 1024}, data, ch, true)
			checkContiguous(t, r, data)
			want := []pair{{"One", "First"}, {"Three", "Third"}}
			if got := pairs(r.tracks); !reflect.DeepEqual(got, want) {
				t.Errorf("tracks = %+v, want %+v", got, want)
			}
			if len(r.errs) != 1 || !strings.Contains(r.errs[0].Error(), "exceeds") {
				t.Errorf("errors = %v", r.errs)
			}
			for _, p := range r.pages() {
				if p.Header != (p.Granule == 0) {
					t.Errorf("serial %d seq %d: Header=%v", p.Serial, p.Sequence, p.Header)
				}
			}
		})
	}

	t.Run("default cap", func(t *testing.T) {
		big := vorbisStream(2, 44100, 1, "ARTIST=X", "TITLE=Y", "COVERART="+strings.Repeat("A", DefaultMaxHeaderBytes))
		in := append(bytes.Clone(good1), big...)
		in = append(in, good3...)
		r := scanAll(in)
		checkContiguous(t, r, in)
		if got := len(r.tracks); got != 2 || len(r.errs) != 1 {
			t.Errorf("tracks=%d errs=%v", got, r.errs)
		}
	})

	t.Run("under cap works", func(t *testing.T) {
		r := scanWith(Options{MaxHeaderBytes: 10000}, data, nil, true)
		if len(r.tracks) != 3 || len(r.errs) != 0 {
			t.Errorf("tracks=%d errs=%v", len(r.tracks), r.errs)
		}
	})
}

func TestMalformedComments(t *testing.T) {
	le := func(n uint32) []byte { return []byte{byte(n), byte(n >> 8), byte(n >> 16), byte(n >> 24)} }
	cat := func(parts ...[]byte) []byte { return bytes.Join(parts, nil) }
	magic := []byte("\x03vorbis")
	cases := map[string][]byte{
		"empty":                magic,
		"short vendor length":  cat(magic, []byte{1, 0}),
		"vendor too long":      cat(magic, le(1000), []byte("abc")),
		"vendor max uint32":    cat(magic, le(0xffffffff), []byte("abc")),
		"no count":             cat(magic, le(1), []byte("v")),
		"count too large":      cat(magic, le(0), le(1000000)),
		"count max uint32":     cat(magic, le(0), le(0xffffffff)),
		"comment len too long": cat(magic, le(0), le(1), le(50), []byte("ARTIST=x")),
		"comment len max":      cat(magic, le(0), le(1), le(0xffffffff), []byte("ARTIST=x")),
		"second comment cut":   cat(magic, le(0), le(2), le(8), []byte("ARTIST=x"), le(3)),
		"wrong packet type":    []byte("\x04vorbis\x00\x00\x00\x00\x00\x00\x00\x00"),
	}
	for name, pkt := range cases {
		t.Run(name, func(t *testing.T) {
			if _, _, err := ParseVorbisComments(pkt); err == nil {
				t.Error("want error")
			}
			data := vorbisStreamFrom(1, 44100, 1, pkt)
			data = append(data, vorbisStream(2, 44100, 1, "ARTIST=ok", "TITLE=fine")...)
			r := scanAll(data)
			checkContiguous(t, r, data)
			if got := pairs(r.tracks); !reflect.DeepEqual(got, []pair{{"ok", "fine"}}) {
				t.Errorf("tracks = %+v", got)
			}
			if len(r.errs) != 1 {
				t.Errorf("errors = %v", r.errs)
			}
		})
	}
}

func TestCommentSemantics(t *testing.T) {
	data := vorbisStream(1, 32000, 1,
		"artist=First", "Title=Only", "ARTIST=Second", "ALBUM=Ignored", "noequals", "TITLE=", "ArTiSt=Third")
	r := scanAll(data)
	if len(r.tracks) != 1 {
		t.Fatalf("tracks = %+v", r.tracks)
	}
	tr := r.tracks[0]
	if tr.Artist != "First, Second, Third" || tr.Title != "Only" {
		t.Errorf("artist=%q title=%q", tr.Artist, tr.Title)
	}
	if tr.Vendor != "proktor-test" || tr.SampleRate != 32000 || tr.Serial != 1 || tr.Offset != 0 {
		t.Errorf("track = %+v", tr)
	}
	if len(tr.Comments) != 7 || tr.Comments[4] != "noequals" {
		t.Errorf("comments = %q", tr.Comments)
	}

	// No artist/title at all is still a track (the caller decides).
	r = scanAll(vorbisStream(1, 44100, 1, "ALBUM=x"))
	if len(r.tracks) != 1 || r.tracks[0].Artist != "" || r.tracks[0].Title != "" {
		t.Errorf("tracks = %+v", r.tracks)
	}

	// Invalid UTF-8 never reaches Artist/Title.
	r = scanAll(vorbisStream(1, 44100, 1, "ARTIST=a\xffb", "TITLE=ok"))
	if len(r.tracks) != 1 || r.tracks[0].Artist != "a�b" || r.tracks[0].Comments[0] != "ARTIST=a\xffb" {
		t.Errorf("tracks = %+v", r.tracks)
	}
}

func TestOpus(t *testing.T) {
	data := opusStream(9, 3, "ARTIST=Opus Artist", "TITLE=Opus Title")
	data = append(data, vorbisStream(10, 44100, 2, "ARTIST=V", "TITLE=T")...)
	r := scanAll(data)
	checkContiguous(t, r, data)
	if got := pairs(r.tracks); !reflect.DeepEqual(got, []pair{{"Opus Artist", "Opus Title"}, {"V", "T"}}) {
		t.Fatalf("tracks = %+v", got)
	}
	if r.tracks[0].SampleRate != 44100 || r.tracks[0].Vendor != "proktor-test-opus" {
		t.Errorf("track = %+v", r.tracks[0])
	}
	var got []bool
	for _, p := range r.pages() {
		if p.Serial == 9 {
			got = append(got, p.Header)
		}
	}
	if want := []bool{true, true, false, false, false}; !reflect.DeepEqual(got, want) {
		t.Errorf("opus Header = %v, want %v", got, want)
	}
}

func TestUnknownCodec(t *testing.T) {
	m := newMuxer(4)
	m.packet([]byte("fLaC-not-really"))
	m.flush(0, false)
	m.packet([]byte("data"))
	m.flush(10, true)
	r := scanAll(m.out)
	checkContiguous(t, r, m.out)
	ps := r.pages()
	if len(r.tracks) != 0 || len(ps) != 2 || !ps[0].Header || ps[1].Header {
		t.Errorf("tracks=%v pages=%+v", r.tracks, ps)
	}
}

func TestMidStreamJoin(t *testing.T) {
	// Starting in the middle of a stream (no BOS seen): everything passes
	// through, nothing is a header, and the next stream is picked up.
	a := vorbisStream(1, 44100, 4, "ARTIST=A", "TITLE=1")
	i := bytes.Index(a[10:], []byte("OggS")) + 10 // second page onward
	data := append(bytes.Clone(a[i+1:]), vorbisStream(2, 44100, 1, "ARTIST=B", "TITLE=2")...)
	r := scanAll(data)
	checkContiguous(t, r, data)
	if got := pairs(r.tracks); !reflect.DeepEqual(got, []pair{{"B", "2"}}) {
		t.Errorf("tracks = %+v", got)
	}
}

func TestPageCRC(t *testing.T) {
	// A page whose CRC field is garbage hashes the same as with zeros.
	p := buildPage(2, 0, 1, 0, []byte{3}, []byte("abc"))
	want := PageCRC(p)
	q := bytes.Clone(p)
	copy(q[22:26], "\xde\xad\xbe\xef")
	if PageCRC(q) != want {
		t.Error("CRC depends on CRC field")
	}
	// Known vector: CRC-32/Ogg of "123456789" with init 0 is 0x89a1897f
	// (the plain MSB-first poly 0x04c11db7 / init 0 / no xor-out check value).
	if got := crcUpdate(0, []byte("123456789")); got != 0x89a1897f {
		t.Errorf("crc check value = %#x", got)
	}
}
