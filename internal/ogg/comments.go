package ogg

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

// Packet magics.
var (
	vorbisIdentMagic   = []byte("\x01vorbis")
	vorbisCommentMagic = []byte("\x03vorbis")
	opusHeadMagic      = []byte("OpusHead")
	opusTagsMagic      = []byte("OpusTags")
)

// maxPreallocComments bounds the up-front allocation for the comment list so
// a hostile count field cannot trigger a large allocation.
const maxPreallocComments = 256

// parseCommentBody decodes the part shared by Vorbis and Opus comment
// headers: a length-prefixed vendor string followed by a count and that many
// length-prefixed "KEY=value" strings, all little-endian. Anything after the
// last comment (the Vorbis framing bit) is ignored. Every length is checked
// against the bytes that remain.
func parseCommentBody(b []byte) (vendor string, comments []string, err error) {
	le := binary.LittleEndian
	if len(b) < 4 {
		return "", nil, errors.New("truncated vendor length")
	}
	n := uint64(le.Uint32(b))
	b = b[4:]
	if n > uint64(len(b)) {
		return "", nil, fmt.Errorf("vendor length %d exceeds %d remaining bytes", n, len(b))
	}
	vendor = string(b[:n])
	b = b[n:]
	if len(b) < 4 {
		return vendor, nil, errors.New("truncated comment count")
	}
	count := uint64(le.Uint32(b))
	b = b[4:]
	if count > uint64(len(b))/4 {
		return vendor, nil, fmt.Errorf("comment count %d cannot fit in %d remaining bytes", count, len(b))
	}
	comments = make([]string, 0, min(count, maxPreallocComments))
	for i := uint64(0); i < count; i++ {
		if len(b) < 4 {
			return vendor, nil, fmt.Errorf("comment %d: truncated length", i)
		}
		n := uint64(le.Uint32(b))
		b = b[4:]
		if n > uint64(len(b)) {
			return vendor, nil, fmt.Errorf("comment %d: length %d exceeds %d remaining bytes", i, n, len(b))
		}
		comments = append(comments, string(b[:n]))
		b = b[n:]
	}
	return vendor, comments, nil
}

// ParseVorbisComments decodes a complete Vorbis comment header packet
// (starting with "\x03vorbis"). It returns the vendor string and the raw
// "KEY=value" comments in order.
func ParseVorbisComments(packet []byte) (vendor string, comments []string, err error) {
	if !bytes.HasPrefix(packet, vorbisCommentMagic) {
		return "", nil, errors.New("not a Vorbis comment header")
	}
	return parseCommentBody(packet[len(vorbisCommentMagic):])
}

// ParseOpusTags decodes a complete Opus comment header packet (starting with
// "OpusTags").
func ParseOpusTags(packet []byte) (vendor string, comments []string, err error) {
	if !bytes.HasPrefix(packet, opusTagsMagic) {
		return "", nil, errors.New("not an OpusTags header")
	}
	return parseCommentBody(packet[len(opusTagsMagic):])
}

// artistTitle extracts the ARTIST and TITLE fields from raw comments. Keys
// are matched case-insensitively; repeated keys are joined with ", ".
func artistTitle(comments []string) (artist, title string) {
	var artists, titles []string
	for _, c := range comments {
		key, val, ok := strings.Cut(c, "=")
		if !ok || val == "" {
			continue
		}
		val = strings.ToValidUTF8(val, "�")
		switch {
		case strings.EqualFold(key, "ARTIST"):
			artists = append(artists, val)
		case strings.EqualFold(key, "TITLE"):
			titles = append(titles, val)
		}
	}
	return strings.Join(artists, ", "), strings.Join(titles, ", ")
}
