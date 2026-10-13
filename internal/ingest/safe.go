package ingest

import (
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/cschmittiey/proktor/internal/ogg"
	"github.com/cschmittiey/proktor/internal/stream"
)

func typeName(v any) string { return fmt.Sprintf("%T", v) }
func itoa(i int) string     { return strconv.Itoa(i) }

// safeHandler isolates a handler's panics from the session and from the other
// handlers. Handlers are used from a single goroutine, so no locking.
type safeHandler struct {
	h      stream.Handler
	log    *slog.Logger
	name   string
	panics int
}

func (s *safeHandler) recovered(method string) {
	r := recover()
	if r == nil {
		return
	}
	s.panics++
	// Log the first panic and then every 1000th so a handler that panics on
	// every segment cannot flood the log.
	if s.panics == 1 || s.panics%1000 == 0 {
		s.log.Error("stream handler panicked", "consumer", s.name, "method", method, "panic", r, "count", s.panics)
	}
}

func (s *safeHandler) onSegment(seg ogg.Segment) {
	defer s.recovered("OnSegment")
	s.h.OnSegment(seg)
}

func (s *safeHandler) onTrack(t ogg.Track, at time.Time) {
	defer s.recovered("OnTrack")
	s.h.OnTrack(t, at)
}

func (s *safeHandler) onEnd(at time.Time, err error) {
	defer s.recovered("OnEnd")
	s.h.OnEnd(at, err)
}
