package ogg

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	// DefaultMaxHeaderBytes is the default Options.MaxHeaderBytes.
	DefaultMaxHeaderBytes = 1 << 20

	// maxPageSize is the largest possible Ogg page: 27 header bytes, 255
	// lacing values and 255 full segments.
	maxPageSize = 27 + 255 + 255*255

	// maxJunk bounds how many unparseable bytes are coalesced into one
	// non-page Segment.
	maxJunk = 4096

	// writeChunk bounds how much of a single Write is buffered at once.
	writeChunk = 32 << 10

	// maxStreams bounds the number of logical streams tracked at once.
	maxStreams = 8
)

var capture = []byte("OggS")

// Options configures a Scanner.
type Options struct {
	// OnSegment receives every input byte exactly once, in order. Required.
	OnSegment func(Segment)
	// OnTrack is called when a logical stream's comment header has been
	// decoded, after the OnSegment call for the page that completed it.
	OnTrack func(Track)
	// OnError reports parse problems (bad CRC, resync, oversize or
	// malformed headers). They are never fatal: bytes keep flowing.
	OnError func(error)
	// MaxHeaderBytes caps the bytes buffered for one header packet. A
	// stream whose comment header exceeds it yields no Track. Zero means
	// DefaultMaxHeaderBytes.
	MaxHeaderBytes int
}

// Scanner splits a byte stream into Ogg pages and decodes track metadata.
// It never modifies or drops bytes. A Scanner is not safe for concurrent
// use; callbacks run synchronously inside Write and Flush.
//
// The scanner buffers at most one maximum-size page plus 32 KiB of input,
// plus up to Options.MaxHeaderBytes while reassembling a header packet.
// Unparseable bytes are coalesced into non-page Segments of up to 4 KiB, so
// a short run of junk is held back until the next page or Flush.
type Scanner struct {
	opts Options

	buf []byte // unconsumed input
	pos int64  // stream offset of buf[0]

	junk    []byte // skipped bytes not yet emitted; directly precede buf
	junkOff int64
	inJunk  bool // an error was already reported for the current junk run

	streams []*logical
}

// NewScanner returns a Scanner reporting to the callbacks in opts.
func NewScanner(opts Options) *Scanner {
	if opts.MaxHeaderBytes <= 0 {
		opts.MaxHeaderBytes = DefaultMaxHeaderBytes
	}
	if opts.OnSegment == nil {
		opts.OnSegment = func(Segment) {}
	}
	return &Scanner{opts: opts}
}

// Write feeds bytes to the scanner. It always returns len(p), nil.
func (s *Scanner) Write(p []byte) (int, error) {
	n := len(p)
	for len(p) > 0 {
		c := min(len(p), writeChunk)
		s.buf = append(s.buf, p[:c]...)
		p = p[c:]
		s.scan(false)
	}
	return n, nil
}

// Flush emits any buffered bytes that do not (yet) form a complete page as
// non-page Segments. Complete pages hidden behind a false capture pattern
// are still recognised. Call it at end of stream.
func (s *Scanner) Flush() {
	s.scan(true)
	s.flushJunk()
}

func (s *Scanner) reportf(format string, args ...any) {
	if s.opts.OnError != nil {
		s.opts.OnError(fmt.Errorf(format, args...))
	}
}

// scan consumes as much of s.buf as can be decided. With final set, bytes
// that would otherwise wait for more data are treated as unparseable.
func (s *Scanner) scan(final bool) {
	i := 0
loop:
	for i < len(s.buf) {
		b := s.buf[i:]
		if len(b) < len(capture) && bytes.HasPrefix(capture, b) {
			// Possibly the start of a capture pattern.
			if !final {
				break
			}
			i += s.skip(b, final, "truncated page")
			continue
		}
		if !bytes.HasPrefix(b, capture) {
			i += s.skip(b, final, "no capture pattern")
			continue
		}
		if len(b) >= 5 && b[4] != 0 {
			i += s.skip(b, final, fmt.Sprintf("unsupported stream structure version %d", b[4]))
			continue
		}
		if len(b) < 27 || len(b) < 27+int(b[26]) {
			if !final {
				break loop
			}
			i += s.skip(b, final, "truncated page")
			continue
		}
		hl := 27 + int(b[26])
		total := hl
		for _, lv := range b[27:hl] {
			total += int(lv)
		}
		if len(b) < total {
			if !final {
				break loop
			}
			i += s.skip(b, final, "truncated page")
			continue
		}
		page := b[:total]
		if got, want := PageCRC(page), binary.LittleEndian.Uint32(page[22:26]); got != want {
			i += s.skip(b, final, fmt.Sprintf("bad page CRC (computed %#08x, header says %#08x)", got, want))
			continue
		}
		s.emitPage(page, hl)
		i += total
		s.pos += int64(total)
	}
	// Compact: drop consumed bytes (pos already advanced for pages; junk
	// advanced it itself).
	n := copy(s.buf, s.buf[i:])
	s.buf = s.buf[:n]
}

// skip moves the leading bytes of b, which cannot start a valid page, into
// the junk run up to the next possible capture pattern, and returns how many
// bytes it consumed from b. The caller's loop index advances by that amount
// and skip advances s.pos itself.
func (s *Scanner) skip(b []byte, final bool, reason string) int {
	j := bytes.Index(b[1:], capture)
	switch {
	case j >= 0:
		j++
	case final:
		j = len(b)
	default:
		// Keep a trailing partial capture pattern for the next Write.
		j = len(b)
		for k := min(len(capture)-1, len(b)-1); k >= 1; k-- {
			if bytes.HasSuffix(b, capture[:k]) {
				j = len(b) - k
				break
			}
		}
	}
	if !s.inJunk {
		s.inJunk = true
		s.reportf("ogg: resync at offset %d: %s", s.pos, reason)
	}
	if len(s.junk) == 0 {
		s.junkOff = s.pos
	}
	s.junk = append(s.junk, b[:j]...)
	s.pos += int64(j)
	for len(s.junk) >= maxJunk {
		s.emitJunk(maxJunk)
	}
	return j
}

func (s *Scanner) emitJunk(n int) {
	data := make([]byte, n)
	copy(data, s.junk)
	s.opts.OnSegment(Segment{Data: data, Offset: s.junkOff})
	s.junkOff += int64(n)
	m := copy(s.junk, s.junk[n:])
	s.junk = s.junk[:m]
}

func (s *Scanner) flushJunk() {
	if len(s.junk) > 0 {
		s.emitJunk(len(s.junk))
	}
}

// emitPage reports one validated page; hl is its header length. s.pos is
// still the page's offset.
func (s *Scanner) emitPage(page []byte, hl int) {
	s.flushJunk()
	s.inJunk = false

	le := binary.LittleEndian
	flags := page[5]
	seg := Segment{
		Data:     append([]byte(nil), page...),
		Offset:   s.pos,
		Page:     true,
		BOS:      flags&0x02 != 0,
		EOS:      flags&0x04 != 0,
		Serial:   le.Uint32(page[14:18]),
		Granule:  int64(le.Uint64(page[6:14])),
		Sequence: le.Uint32(page[18:22]),
	}

	var track *Track
	l := s.stream(seg.Serial, seg.BOS)
	if l != nil {
		var header bool
		header, track = l.feed(flags&0x01 != 0, page[27:hl], page[hl:])
		seg.Header = header || seg.BOS
		if seg.EOS || l.finished {
			s.dropStream(l)
		}
	} else {
		seg.Header = seg.BOS
	}

	s.opts.OnSegment(seg)
	if track != nil && s.opts.OnTrack != nil {
		s.opts.OnTrack(*track)
	}
}

// stream returns the state for serial. On a BOS page it starts a fresh one.
func (s *Scanner) stream(serial uint32, bos bool) *logical {
	if !bos {
		for _, l := range s.streams {
			if l.serial == serial {
				return l
			}
		}
		return nil
	}
	for _, l := range s.streams {
		if l.serial == serial {
			s.dropStream(l)
			break
		}
	}
	if len(s.streams) >= maxStreams {
		s.streams = append(s.streams[:0], s.streams[1:]...)
	}
	l := newLogical(serial, s.pos, s.opts.MaxHeaderBytes, s.opts.OnError)
	s.streams = append(s.streams, l)
	return l
}

func (s *Scanner) dropStream(l *logical) {
	for i, x := range s.streams {
		if x == l {
			s.streams = append(s.streams[:i], s.streams[i+1:]...)
			return
		}
	}
}
