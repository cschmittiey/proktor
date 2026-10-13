// Package api is proktor's HTTP API: the main product surface that displays
// (the CRT terminal, browser pages, shell scripts) are built against.
//
// Reads are unauthenticated; writes need "Authorization: Bearer <token>".
// Every response carries "Access-Control-Allow-Origin: *" so browser-based
// displays work from any origin.
//
// Endpoints:
//
//	GET  /           embedded mobile control page
//	GET  /now        current state as JSON (state.State)
//	GET  /now.txt    current state as plain text (format below)
//	GET  /events     Server-Sent Events: "state" event on connect and on
//	                 every change, ":ping" comment every PingInterval
//	GET  /tracks     tonight's tracklist; ?format=txt for plain text
//	GET  /health     component health; always HTTP 200, see "ok" in the body
//	POST /now        manual set/performer/live/clear/skip/idle (token)
//
// # /now.txt format
//
// Plain text, UTF-8 as received, every line terminated by "\n":
//
//	mode track: line 1 "Artist - Title" (the " - " is omitted when one side
//	            is empty); line 2 is the performer, only when known
//	mode live:  one line "live: <performer>" ("live" if no performer is set)
//	mode idle:  one line with the event name, or "idle" if there is none
//
// # SSE format
//
//	retry: 2000
//
//	event: state
//	id: <seq>
//	data: <state JSON on a single line>
//
//	:ping
//
// # POST /now
//
// The body is a state.Command as JSON (max 64 KiB, unknown fields ignored).
// If "action" is empty but artist or title is present, "set" is assumed. The
// response is the new state JSON. Errors are {"error": "..."} with status
// 400 (bad JSON / invalid command), 401 (missing or wrong token), 403 (no
// token configured, so writes are disabled), 413 or 500.
package api

import (
	"crypto/sha256"
	"crypto/subtle"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/cschmittiey/proktor/internal/clock"
	"github.com/cschmittiey/proktor/internal/health"
	"github.com/cschmittiey/proktor/internal/state"
)

//go:embed control.html
var controlHTML []byte

const (
	maxBodyBytes     = 64 << 10
	defaultPingEvery = 15 * time.Second
	sseRetryMillis   = 2000
)

// Backend is satisfied by *state.Machine.
type Backend interface {
	Snapshot() state.State
	// Subscribe returns a channel receiving every state after subscribing,
	// and a cancel func that unsubscribes and closes the channel.
	Subscribe() (<-chan state.State, func())
	// Apply executes a command. Errors wrapping state.ErrInvalidCommand are
	// reported to the client as 400.
	Apply(cmd state.Command) (state.State, error)
	Tracklist() []state.TracklistEntry
}

// Options configures New.
type Options struct {
	Backend      Backend
	Token        string                    // bearer token for writes; "" disables writes (403)
	Health       map[string]health.Checker // component name -> checker
	Clock        clock.Clock               // default clock.Real()
	PingInterval time.Duration             // default 15s
	Logger       *slog.Logger              // default slog.Default()
}

type server struct {
	b         Backend
	tokenHash [sha256.Size]byte
	hasToken  bool
	health    map[string]health.Checker
	clk       clock.Clock
	ping      time.Duration
	log       *slog.Logger
}

// New returns the API handler.
func New(opts Options) http.Handler {
	s := &server{
		b:      opts.Backend,
		health: opts.Health,
		clk:    opts.Clock,
		ping:   opts.PingInterval,
		log:    opts.Logger,
	}
	if s.clk == nil {
		s.clk = clock.Real()
	}
	if s.ping <= 0 {
		s.ping = defaultPingEvery
	}
	if s.log == nil {
		s.log = slog.Default()
	}
	if opts.Token != "" {
		s.hasToken = true
		s.tokenHash = sha256.Sum256([]byte(opts.Token))
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.handleControl)
	mux.HandleFunc("GET /now", s.handleNow)
	mux.HandleFunc("GET /now.txt", s.handleNowTxt)
	mux.HandleFunc("GET /events", s.handleEvents)
	mux.HandleFunc("GET /tracks", s.handleTracks)
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("POST /now", s.handlePostNow)
	mux.HandleFunc("OPTIONS /now", s.handlePreflight)
	return cors(mux)
}

// cors adds the permissive CORS header to every response, including 404/405.
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		next.ServeHTTP(w, r)
	})
}

func (s *server) handlePreflight(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
	h.Set("Access-Control-Max-Age", "86400")
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleControl(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(controlHTML)
}

// prep normalises a state for the wire: history is always an array.
func prep(st state.State) state.State {
	if st.History == nil {
		st.History = []state.Track{}
	}
	return st
}

func (s *server) writeJSON(w http.ResponseWriter, code int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		s.log.Error("api: marshal response", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_, _ = w.Write(append(body, '\n'))
}

func (s *server) writeErr(w http.ResponseWriter, code int, msg string) {
	s.writeJSON(w, code, map[string]string{"error": msg})
}

func (s *server) writeText(w http.ResponseWriter, text string) {
	h := w.Header()
	h.Set("Content-Type", "text/plain; charset=utf-8")
	h.Set("Cache-Control", "no-store")
	_, _ = io.WriteString(w, text)
}

func (s *server) handleNow(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, prep(s.b.Snapshot()))
}

func (s *server) handleNowTxt(w http.ResponseWriter, r *http.Request) {
	s.writeText(w, nowText(s.b.Snapshot()))
}

func joinArtistTitle(artist, title string) string {
	switch {
	case artist != "" && title != "":
		return artist + " - " + title
	case artist != "":
		return artist
	default:
		return title
	}
}

// nowText renders the /now.txt format.
func nowText(st state.State) string {
	performer := ""
	if st.Performer != nil {
		performer = *st.Performer
	}
	var sb strings.Builder
	switch {
	case st.Mode == state.ModeTrack && st.Track != nil:
		if line := joinArtistTitle(st.Track.Artist, st.Track.Title); line != "" {
			sb.WriteString(line + "\n")
		}
		if performer != "" {
			sb.WriteString(performer + "\n")
		}
	case st.Mode == state.ModeLive:
		if performer != "" {
			sb.WriteString("live: " + performer + "\n")
		} else {
			sb.WriteString("live\n")
		}
	default:
		if st.EventName != "" {
			sb.WriteString(st.EventName + "\n")
		} else {
			sb.WriteString("idle\n")
		}
	}
	return sb.String()
}

func (s *server) handleEvents(w http.ResponseWriter, r *http.Request) {
	rc := http.NewResponseController(w)
	// Subscribe first so nothing between snapshot and subscription is lost.
	ch, cancel := s.b.Subscribe()
	defer cancel()
	snap := s.b.Snapshot()

	ticker := s.clk.NewTicker(s.ping)
	defer ticker.Stop()

	// The stream is long-lived; lift any server-wide write deadline.
	_ = rc.SetWriteDeadline(time.Time{})

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-store")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	send := func(format string, args ...any) bool {
		if _, err := fmt.Fprintf(w, format, args...); err != nil {
			return false
		}
		return rc.Flush() == nil
	}
	sendState := func(st state.State) bool {
		data, err := json.Marshal(prep(st))
		if err != nil {
			s.log.Error("api: marshal sse state", "err", err)
			return true
		}
		return send("event: state\nid: %d\ndata: %s\n\n", st.Seq, data)
	}

	if !send("retry: %d\n\n", sseRetryMillis) || !sendState(snap) {
		return
	}
	last := snap.Seq

	for {
		select {
		case <-r.Context().Done():
			return
		case st, ok := <-ch:
			if !ok {
				return
			}
			if st.Seq <= last {
				continue
			}
			last = st.Seq
			if !sendState(st) {
				return
			}
		case <-ticker.C():
			if !send(":ping\n\n") {
				return
			}
		}
	}
}

func (s *server) handleTracks(w http.ResponseWriter, r *http.Request) {
	tl := s.b.Tracklist()
	if r.URL.Query().Get("format") == "txt" {
		var sb strings.Builder
		for _, e := range tl {
			sb.WriteString(e.DetectedAt.UTC().Format("15:04:05"))
			sb.WriteString("  ")
			sb.WriteString(joinArtistTitle(e.Artist, e.Title))
			if e.PublishedAt == nil {
				sb.WriteString("  (not shown)")
			}
			sb.WriteByte('\n')
		}
		s.writeText(w, sb.String())
		return
	}
	if tl == nil {
		tl = []state.TracklistEntry{}
	}
	s.writeJSON(w, http.StatusOK, tl)
}

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ok := true
	comps := make(map[string]health.Status, len(s.health))
	for name, c := range s.health {
		if c == nil {
			continue
		}
		st := c.Health()
		if !st.OK {
			ok = false
		}
		comps[name] = st
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"ok":         ok,
		"components": comps,
		"time":       s.clk.Now().UTC(),
	})
}

// authorized checks the bearer token in constant time.
func (s *server) authorized(r *http.Request) bool {
	const prefix = "bearer "
	h := r.Header.Get("Authorization")
	if len(h) < len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return false
	}
	got := sha256.Sum256([]byte(strings.TrimSpace(h[len(prefix):])))
	return subtle.ConstantTimeCompare(got[:], s.tokenHash[:]) == 1
}

func (s *server) handlePostNow(w http.ResponseWriter, r *http.Request) {
	if !s.hasToken {
		s.writeErr(w, http.StatusForbidden, "writes are disabled: no token configured")
		return
	}
	if !s.authorized(r) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="proktor"`)
		s.writeErr(w, http.StatusUnauthorized, "missing or invalid bearer token")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	var cmd state.Command
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		var tooBig *http.MaxBytesError
		switch {
		case errors.As(err, &tooBig):
			s.writeErr(w, http.StatusRequestEntityTooLarge, "request body too large")
		case errors.Is(err, io.EOF):
			s.writeErr(w, http.StatusBadRequest, "empty request body")
		default:
			s.writeErr(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		}
		return
	}
	if cmd.Action == "" && (cmd.Artist != "" || cmd.Title != "") {
		cmd.Action = state.ActionSet
	}

	st, err := s.b.Apply(cmd)
	if err != nil {
		if errors.Is(err, state.ErrInvalidCommand) {
			s.writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		s.log.Error("api: apply command", "action", cmd.Action, "err", err)
		s.writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}
	s.writeJSON(w, http.StatusOK, prep(st))
}
