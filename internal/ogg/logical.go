package ogg

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type codec int

const (
	codecUnknown codec = iota // first packet not complete yet
	codecVorbis
	codecOpus
	codecOther // recognised as neither; no metadata
)

// logical tracks one logical bitstream from its BOS page until its header
// packets have all been seen. It reassembles packets across pages just far
// enough to decode the identification and comment headers; later audio
// packets are only counted, never buffered.
type logical struct {
	serial   uint32
	offset   int64 // offset of the BOS page
	maxBytes int   // cap on one buffered header packet
	report   func(error)

	codec      codec
	needed     int // number of header packets in this codec
	done       int // packets completed so far
	inPacket   bool
	drop       bool   // current packet is not being buffered
	cur        []byte // bytes of the current packet (when !drop)
	failed     bool   // metadata given up on
	sampleRate uint32
	finished   bool // all header packets seen
}

func newLogical(serial uint32, offset int64, maxBytes int, report func(error)) *logical {
	return &logical{serial: serial, offset: offset, maxBytes: maxBytes, report: report, needed: 1}
}

// wants reports whether packet idx must be buffered: only the identification
// (0) and comment (1) headers are decoded.
func (l *logical) wants(idx int) bool { return idx < 2 && idx < l.needed }

func (l *logical) errorf(format string, args ...any) {
	if l.report != nil {
		l.report(fmt.Errorf("ogg: stream %#08x: %s", l.serial, fmt.Sprintf(format, args...)))
	}
}

// feed consumes one page's packet data. cont is the page's "continued
// packet" flag, lace its lacing values and body the page body. It reports
// whether the page carried only header packets and, when the page completed
// a decodable comment header, the resulting track.
func (l *logical) feed(cont bool, lace, body []byte) (header bool, track *Track) {
	if l.finished {
		return false, nil
	}
	if !cont && l.inPacket {
		// The previous page ended mid-packet but this one does not continue
		// it: pages were lost. Count the lost packet to keep indices honest.
		l.endPacket(false)
	}
	pos := 0
	for i, lv := range lace {
		seg := body[pos : pos+int(lv)]
		pos += int(lv)
		if !l.inPacket {
			l.inPacket = true
			l.cur = l.cur[:0]
			// A continuation with no packet in progress means we never saw
			// the start of this packet.
			l.drop = !l.wants(l.done) || (i == 0 && cont)
		}
		if !l.drop {
			if len(l.cur)+len(seg) > l.maxBytes {
				l.drop = true
				l.cur = nil
				if !l.failed {
					l.failed = true
					l.errorf("header packet exceeds %d bytes; ignoring this stream's metadata", l.maxBytes)
				}
			} else {
				l.cur = append(l.cur, seg...)
			}
		}
		if lv < 255 {
			if tr := l.endPacket(!l.drop); tr != nil {
				track = tr
			}
		}
	}
	// Index of the last packet this page touched.
	switch {
	case len(lace) == 0:
		header = l.done < l.needed
	case l.inPacket:
		header = l.done < l.needed
	default:
		header = l.done-1 < l.needed
	}
	if l.done >= l.needed {
		l.finished = true
		l.cur = nil
	}
	return header, track
}

// endPacket finishes the current packet. decode is false for packets that
// were lost or not buffered.
func (l *logical) endPacket(decode bool) *Track {
	var track *Track
	if decode {
		switch l.done {
		case 0:
			l.identify(l.cur)
		case 1:
			track = l.decodeComments(l.cur)
		}
	}
	l.done++
	l.inPacket = false
	l.drop = false
	l.cur = l.cur[:0]
	return track
}

// identify decodes the first packet, selecting the codec and sample rate.
func (l *logical) identify(p []byte) {
	switch {
	case bytes.HasPrefix(p, vorbisIdentMagic):
		l.codec, l.needed = codecVorbis, 3
		if len(p) < 30 {
			l.errorf("short Vorbis identification header (%d bytes)", len(p))
			return
		}
		l.sampleRate = binary.LittleEndian.Uint32(p[12:16])
	case bytes.HasPrefix(p, opusHeadMagic):
		l.codec, l.needed = codecOpus, 2
		if len(p) < 19 {
			l.errorf("short OpusHead (%d bytes)", len(p))
			return
		}
		l.sampleRate = binary.LittleEndian.Uint32(p[12:16])
		if l.sampleRate == 0 {
			l.sampleRate = 48000
		}
	default:
		l.codec, l.needed = codecOther, 1
	}
}

// decodeComments decodes the second packet into a Track.
func (l *logical) decodeComments(p []byte) *Track {
	if l.failed {
		return nil
	}
	var (
		vendor   string
		comments []string
		err      error
	)
	switch l.codec {
	case codecVorbis:
		vendor, comments, err = ParseVorbisComments(p)
	case codecOpus:
		vendor, comments, err = ParseOpusTags(p)
	default:
		return nil
	}
	if err != nil {
		l.errorf("bad comment header: %v", err)
		return nil
	}
	artist, title := artistTitle(comments)
	return &Track{
		Artist:     artist,
		Title:      title,
		Vendor:     vendor,
		Comments:   comments,
		Serial:     l.serial,
		Offset:     l.offset,
		SampleRate: l.sampleRate,
	}
}
