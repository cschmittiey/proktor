package recorder

import (
	"time"

	"github.com/cschmittiey/proktor/internal/ogg"
)

const fallbackSampleRate = 44100

// timeline converts positions in a chained Ogg stream to audio time.
// Granule positions restart in every logical bitstream, so the audio time of
// a stream's start is the sum of the durations of all earlier streams, each
// being its final granule divided by its sample rate.
//
// It is only used from the session's ingest goroutine.
type timeline struct {
	total time.Duration // summed duration of finished streams
	bad   bool          // some finished stream had no usable granule
	cur   *streamPos
}

type streamPos struct {
	serial     uint32
	startAudio time.Duration
	last       int64 // last granule seen, -1 if none
	rate       uint32
	partial    bool // joined mid-stream (no BOS seen)
}

// page feeds one segment.
func (t *timeline) page(seg ogg.Segment) {
	if !seg.Page {
		return
	}
	switch {
	case seg.BOS:
		t.finish()
		t.cur = &streamPos{serial: seg.Serial, startAudio: t.total, last: -1}
	case t.cur == nil:
		t.cur = &streamPos{serial: seg.Serial, last: -1, partial: true}
	}
	if t.cur.serial == seg.Serial && seg.Granule >= 0 {
		t.cur.last = seg.Granule
	}
}

// finish folds the current stream's duration into total.
func (t *timeline) finish() {
	c := t.cur
	if c == nil {
		return
	}
	if c.last < 0 || c.partial {
		t.bad = true
		return
	}
	t.total += granuleDuration(c.last, c.rate)
}

// startOf reports the audio time at which the stream with the given serial
// began, if it is the current stream and the timeline is trustworthy. It also
// records the stream's sample rate.
func (t *timeline) startOf(serial, rate uint32) (time.Duration, bool) {
	c := t.cur
	if c == nil || c.serial != serial {
		return 0, false
	}
	if rate > 0 {
		c.rate = rate
	}
	if t.bad || c.partial {
		return 0, false
	}
	return c.startAudio, true
}

func granuleDuration(g int64, rate uint32) time.Duration {
	if rate == 0 {
		rate = fallbackSampleRate
	}
	r := int64(rate)
	return time.Duration(g/r)*time.Second + time.Duration((g%r)*int64(time.Second)/r)
}
