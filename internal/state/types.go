// Package state owns the now-playing state: debouncing automatic track
// detections, applying manual overrides, the tracklist, and persistence.
package state

import (
	"errors"
	"time"
)

// SchemaVersion is the "v" field of State. Bump only on breaking changes.
const SchemaVersion = 1

// Modes.
const (
	ModeTrack = "track"
	ModeLive  = "live"
	ModeIdle  = "idle"
)

// Track sources.
const (
	SourceTraktor = "traktor"
	SourceManual  = "manual"
	SourceWebhook = "webhook"
)

// Track is one now-playing / history entry.
type Track struct {
	Artist    string    `json:"artist"`
	Title     string    `json:"title"`
	Source    string    `json:"source"`
	StartedAt time.Time `json:"started_at"`
}

// Pending is an automatic detection waiting out metadata_delay.
type Pending struct {
	Track
	DetectedAt time.Time `json:"detected_at"`
	PublishAt  time.Time `json:"publish_at"`
}

// State is the public state object shared by GET /now, GET /events and
// outgoing webhooks. Adding fields is fine; renaming/removing is breaking.
type State struct {
	V         int     `json:"v"`
	Mode      string  `json:"mode"`
	Track     *Track  `json:"track"`
	Performer *string `json:"performer"`
	EventName string  `json:"event_name"`
	// History is the last N tracks that were live, newest first. It does
	// not include the current track.
	History []Track `json:"history"`
	// Pending is the detected-but-not-yet-live track, if any.
	Pending   *Pending  `json:"pending"`
	Seq       uint64    `json:"seq"`
	UpdatedAt time.Time `json:"updated_at"`
}

// TracklistEntry is one line of tonight's tracklist (GET /tracks and the
// journal). Detected and published times are both kept so recordings can be
// cut accurately even when the display was delayed.
type TracklistEntry struct {
	Artist     string    `json:"artist"`
	Title      string    `json:"title"`
	Source     string    `json:"source"`
	Performer  string    `json:"performer,omitempty"`
	Mount      string    `json:"mount,omitempty"`
	DetectedAt time.Time `json:"detected_at"`
	// PublishedAt is nil if the track never went live (replaced while
	// pending, skipped, or suppressed by a pinned manual track).
	PublishedAt *time.Time `json:"published_at"`
}

// Detection is an automatic track change (from Traktor or a webhook source
// that should be debounced like one).
type Detection struct {
	Artist string
	Title  string
	Source string // SourceTraktor normally
	Mount  string
	At     time.Time
}

// Command actions for POST /now.
const (
	ActionSet       = "set"       // artist/title now (manual or webhook)
	ActionPerformer = "performer" // set/clear performer name, keep track
	ActionLive      = "live"      // live act: set performer, clear track
	ActionClear     = "clear"     // drop current and pending track
	ActionSkip      = "skip"      // drop pending track only
	ActionIdle      = "idle"      // nothing playing: clear everything
)

// Command is a manual / webhook instruction, the JSON body of POST /now.
type Command struct {
	Action    string  `json:"action"`
	Artist    string  `json:"artist,omitempty"`
	Title     string  `json:"title,omitempty"`
	Performer *string `json:"performer,omitempty"`
	// Source is "manual" (default) or "webhook" for ActionSet.
	Source string `json:"source,omitempty"`
}

// ErrInvalidCommand wraps validation errors from Machine.Apply (HTTP 400).
var ErrInvalidCommand = errors.New("invalid command")
