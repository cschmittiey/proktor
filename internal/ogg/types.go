// Package ogg splits a live Ogg byte stream into pages without modifying it
// and extracts Vorbis comment metadata from each chained logical bitstream.
package ogg

// Segment is a contiguous run of input bytes as seen by the Scanner.
// Concatenating the Data of every emitted Segment, in order, reproduces the
// scanner's input byte for byte.
type Segment struct {
	// Data is a freshly allocated slice. Receivers may retain it but must
	// not modify it (it is shared between all consumers of a session).
	Data []byte
	// Offset is the byte offset of Data[0] from the start of the input.
	Offset int64
	// Page is true when Data is exactly one complete, CRC-valid Ogg page.
	// When false, Data is unparseable bytes skipped while resyncing (or a
	// trailing partial page flushed at end of stream) and every other
	// field below is zero.
	Page bool
	// BOS / EOS are the page's beginning/end-of-stream flags.
	BOS, EOS bool
	// Serial is the logical bitstream serial number.
	Serial uint32
	// Granule is the page's granule position (-1 if no packet ends on it).
	Granule int64
	// Sequence is the page sequence number within its logical stream.
	Sequence uint32
	// Header is true for pages that carry only codec header packets of the
	// current logical stream (the BOS page and following pages up to, not
	// including, the first page with audio data). A relay reconnecting
	// mid-track can replay these pages to give upstream a valid start.
	Header bool
}

// Track is the metadata of one logical bitstream, decoded from its Vorbis
// comment header.
type Track struct {
	Artist string
	Title  string
	// Vendor is the encoder vendor string from the comment header.
	Vendor string
	// Comments are the raw "KEY=value" user comments, in stream order.
	Comments []string
	// Serial is the logical bitstream serial number.
	Serial uint32
	// Offset is the byte offset of the stream's BOS page in the input.
	Offset int64
	// SampleRate is from the Vorbis identification header (0 if unknown).
	SampleRate uint32
}
