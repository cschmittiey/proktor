// Package stream defines how an ingested source session is fanned out to
// the components that consume it (track state, relay, recorder).
package stream

import (
	"time"

	"github.com/cschmittiey/proktor/internal/ogg"
)

// SessionInfo describes one source connection (one DJ broadcast).
type SessionInfo struct {
	// ID is unique per session for the lifetime of the process.
	ID         string
	Mount      string // e.g. "/live"
	RemoteAddr string
	StartedAt  time.Time

	ContentType string
	UserAgent   string

	// Icecast stream headers as sent by the source (empty if absent).
	IceName        string
	IceDescription string
	IceGenre       string
	IceURL         string
	IceBitrate     string
	IceAudioInfo   string
}

// Handler receives one session's data. Methods are called sequentially from
// the ingest goroutine for that session and must not block for long: a
// handler that does I/O must buffer internally.
type Handler interface {
	// OnSegment is called for every Segment, in order.
	OnSegment(seg ogg.Segment)
	// OnTrack is called when a new logical stream's metadata is decoded,
	// after the OnSegment call for the page that completed it.
	OnTrack(t ogg.Track, at time.Time)
	// OnEnd is called exactly once when the source disconnects or is
	// taken over. err is nil on a clean EOF.
	OnEnd(at time.Time, err error)
}

// Consumer is a component that wants to see sessions.
type Consumer interface {
	// StartSession is called when a source has been accepted, before any
	// data. It may return nil to ignore the session.
	StartSession(info SessionInfo) Handler
}

// ConsumerFunc adapts a function to Consumer.
type ConsumerFunc func(info SessionInfo) Handler

func (f ConsumerFunc) StartSession(info SessionInfo) Handler { return f(info) }
