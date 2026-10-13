package ogg

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// muxer builds valid Ogg pages for one logical stream from packets.
type muxer struct {
	serial uint32
	seq    uint32
	bos    bool // next page carries BOS
	cont   bool // next page continues a packet
	lace   []byte
	body   []byte
	out    []byte
}

func newMuxer(serial uint32) *muxer { return &muxer{serial: serial, bos: true} }

// packet appends a packet, splitting it across pages when the lacing table
// fills up.
func (m *muxer) packet(p []byte) {
	for {
		n := min(len(p), 255)
		if len(m.lace) == 255 {
			m.emit(-1, false)
			m.cont = true
		}
		m.lace = append(m.lace, byte(n))
		m.body = append(m.body, p[:n]...)
		p = p[n:]
		if n < 255 {
			return
		}
	}
}

// emit closes the current page.
func (m *muxer) emit(granule int64, eos bool) {
	var flags byte
	if m.cont {
		flags |= 1
	}
	if m.bos {
		flags |= 2
	}
	if eos {
		flags |= 4
	}
	m.out = append(m.out, buildPage(flags, granule, m.serial, m.seq, m.lace, m.body)...)
	m.seq++
	m.bos, m.cont = false, false
	m.lace, m.body = nil, nil
}

// flush closes the current page at a packet boundary.
func (m *muxer) flush(granule int64, eos bool) { m.emit(granule, eos) }

func buildPage(flags byte, granule int64, serial, seq uint32, lace, body []byte) []byte {
	le := binary.LittleEndian
	p := make([]byte, 27, 27+len(lace)+len(body))
	copy(p, "OggS")
	p[5] = flags
	le.PutUint64(p[6:], uint64(granule))
	le.PutUint32(p[14:], serial)
	le.PutUint32(p[18:], seq)
	p[26] = byte(len(lace))
	p = append(p, lace...)
	p = append(p, body...)
	le.PutUint32(p[22:], PageCRC(p))
	return p
}

func vorbisIdent(rate uint32) []byte {
	p := make([]byte, 30)
	copy(p, "\x01vorbis")
	binary.LittleEndian.PutUint32(p[7:], 0) // version
	p[11] = 2                               // channels
	binary.LittleEndian.PutUint32(p[12:], rate)
	p[28] = 0xb8 // blocksizes
	p[29] = 1    // framing
	return p
}

func commentBody(vendor string, comments []string) []byte {
	le := binary.LittleEndian
	b := le.AppendUint32(nil, uint32(len(vendor)))
	b = append(b, vendor...)
	b = le.AppendUint32(b, uint32(len(comments)))
	for _, c := range comments {
		b = le.AppendUint32(b, uint32(len(c)))
		b = append(b, c...)
	}
	return b
}

func vorbisComment(vendor string, comments ...string) []byte {
	return append(append([]byte("\x03vorbis"), commentBody(vendor, comments)...), 1)
}

func vorbisSetup() []byte {
	return append([]byte("\x05vorbis"), bytes.Repeat([]byte{0x5a}, 300)...)
}

// vorbisStream builds a complete logical stream: BOS ident page, one page
// with comment+setup, nAudio audio pages and an EOS page.
func vorbisStream(serial uint32, rate uint32, nAudio int, comments ...string) []byte {
	return vorbisStreamFrom(serial, rate, nAudio, vorbisComment("proktor-test", comments...))
}

func vorbisStreamFrom(serial uint32, rate uint32, nAudio int, commentPacket []byte) []byte {
	m := newMuxer(serial)
	m.packet(vorbisIdent(rate))
	m.flush(0, false)
	m.packet(commentPacket)
	m.packet(vorbisSetup())
	m.flush(0, false)
	for i := range nAudio {
		m.packet(bytes.Repeat([]byte{byte(i)}, 200+i))
		m.packet(bytes.Repeat([]byte{byte(i)}, 90))
		m.flush(int64(1024*(i+1)), i == nAudio-1)
	}
	return m.out
}

func opusStream(serial uint32, nAudio int, comments ...string) []byte {
	head := make([]byte, 19)
	copy(head, "OpusHead")
	head[8] = 1
	head[9] = 2
	binary.LittleEndian.PutUint32(head[12:], 44100)
	tags := append([]byte("OpusTags"), commentBody("proktor-test-opus", comments)...)
	m := newMuxer(serial)
	m.packet(head)
	m.flush(0, false)
	m.packet(tags)
	m.flush(0, false)
	for i := range nAudio {
		m.packet(bytes.Repeat([]byte{byte(i)}, 120))
		m.flush(int64(960*(i+1)), i == nAudio-1)
	}
	return m.out
}

// result collects everything a Scanner reports.
type result struct {
	segs   []Segment
	tracks []Track
	errs   []error
	// segsAtTrack[i] is how many segments had been emitted when track i fired.
	segsAtTrack []int
}

func (r *result) bytes() []byte {
	var b []byte
	for _, s := range r.segs {
		b = append(b, s.Data...)
	}
	return b
}

func (r *result) pages() []Segment {
	var p []Segment
	for _, s := range r.segs {
		if s.Page {
			p = append(p, s)
		}
	}
	return p
}

// scanWith feeds data through a Scanner in the chunks chunker produces, then
// flushes if flush is set.
func scanWith(opts Options, data []byte, chunker func(remaining int) int, flush bool) *result {
	r := &result{}
	opts.OnSegment = func(s Segment) { r.segs = append(r.segs, s) }
	opts.OnTrack = func(t Track) {
		r.tracks = append(r.tracks, t)
		r.segsAtTrack = append(r.segsAtTrack, len(r.segs))
	}
	opts.OnError = func(err error) { r.errs = append(r.errs, err) }
	s := NewScanner(opts)
	for len(data) > 0 {
		n := 0
		if chunker != nil {
			n = chunker(len(data))
		}
		if n <= 0 || n > len(data) {
			n = len(data)
		}
		if got, err := s.Write(data[:n]); got != n || err != nil {
			panic("Write must return len(p), nil")
		}
		data = data[n:]
	}
	if flush {
		s.Flush()
	}
	return r
}

func scanAll(data []byte) *result { return scanWith(Options{}, data, nil, true) }

func oneByte(int) int { return 1 }

func randomChunks(seed int64, max int) func(int) int {
	rng := rand.New(rand.NewSource(seed))
	return func(int) int { return 1 + rng.Intn(max) }
}

// checkContiguous asserts the segments tile the input exactly.
func checkContiguous(t testing.TB, r *result, input []byte) {
	t.Helper()
	var off int64
	for i, s := range r.segs {
		if s.Offset != off {
			t.Fatalf("segment %d: Offset=%d, want %d", i, s.Offset, off)
		}
		if len(s.Data) == 0 {
			t.Fatalf("segment %d is empty", i)
		}
		if s.Page {
			if len(s.Data) < 27 || string(s.Data[:4]) != "OggS" {
				t.Fatalf("segment %d: Page without OggS header", i)
			}
		} else if s.BOS || s.EOS || s.Header || s.Serial != 0 || s.Granule != 0 || s.Sequence != 0 {
			t.Fatalf("segment %d: non-page segment has page fields set: %+v", i, s)
		}
		off += int64(len(s.Data))
	}
	if !bytes.Equal(r.bytes(), input) {
		t.Fatalf("bytes out (%d) != bytes in (%d)", len(r.bytes()), len(input))
	}
}

func readFixture(t testing.TB, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", name))
	if err != nil {
		t.Skipf("fixture not available: %v", err)
	}
	return b
}

type pair struct{ Artist, Title string }

func pairs(tracks []Track) []pair {
	var p []pair
	for _, t := range tracks {
		p = append(p, pair{t.Artist, t.Title})
	}
	return p
}
