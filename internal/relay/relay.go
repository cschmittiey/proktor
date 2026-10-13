// Package relay forwards ingested source sessions to zero or more upstream
// Icecast-compatible servers (Icecast 2.4, AzuraCast / Liquidsoap harbor),
// acting as an Icecast source client.
//
// # Model
//
// Every configured upstream has one worker goroutine and one bounded queue.
// An upstream only holds a connection while a source session is active on a
// matching mount (config.Upstream.SourceMount, empty = any mount). If several
// matching sessions are active the most recently started one wins; when it
// ends the relay falls back to the previous one. Segment delivery
// (Handler.OnSegment) never blocks and never does I/O: it appends to the
// queue, or, when the queue would exceed its byte budget, drops the queue,
// closes the upstream connection and lets the worker reconnect. Upstream
// failures therefore can not slow down ingest, recording or the display.
//
// # Stream sync (header replay)
//
// The spec says that a relay reconnecting mid-stream should "wait for the
// next BOS page" so upstream gets a valid stream start. For a long track that
// can be minutes. This package improves on that, using the same idea Icecast
// itself uses for late-joining listeners: it caches the header pages
// (ogg.Segment.Header) of the current logical stream, resetting the cache on
// every BOS page, and on (re)connect sends the cached header pages first and
// then continues from the next whole page. Only whole, CRC-valid pages
// (Segment.Page) are ever forwarded; unparseable bytes are skipped, so
// upstream always receives clean Ogg. If there is no header cache yet (the
// session has not produced a BOS page), the upstream waits for the next BOS
// page, exactly as the spec describes.
//
// # Reconnects and backoff
//
// Dial errors, non-2xx responses (401/403/...), write errors, the upstream
// closing the connection, and queue overflow all lead to an exponential
// backoff (MinBackoff doubling up to MaxBackoff, driven by Options.Clock;
// reset after a connection has streamed for 30s) and a reconnect, but only
// while a source is still active.
//
// # Hold (DJ handoff)
//
// With HoldSeconds == 0 the upstream connection is closed as soon as the
// source session ends. With HoldSeconds > 0 the connection is kept open and
// idle for that long; a new session on a matching mount that starts within
// the hold reuses the connection, and since a new session starts with a BOS
// page the Ogg stream simply chains. Note that the upstream server may cut an
// idle source sooner than HoldSeconds (Icecast's <source-timeout> defaults to
// 10 seconds); in that case the relay does not reconnect on its own while
// holding, it reconnects when the next session starts.
//
// # Protocol notes
//
// Only "http://" and "https://" upstreams are supported (https uses
// crypto/tls with Options.TLSConfig). The request is "PUT <mount> HTTP/1.1"
// with "Expect: 100-continue" (protocol "put") or "SOURCE <mount> HTTP/1.0"
// (protocol "source"). A "100 Continue" or "200 OK" response starts the
// stream; anything else is an error. The body is the raw Ogg bytes with no
// chunked encoding.
package relay

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/cschmittiey/proktor/internal/clock"
	"github.com/cschmittiey/proktor/internal/config"
	"github.com/cschmittiey/proktor/internal/health"
	"github.com/cschmittiey/proktor/internal/ogg"
	"github.com/cschmittiey/proktor/internal/stream"
)

// Defaults.
const (
	defaultBufferBytes = 4 << 20
	defaultMinBackoff  = time.Second
	defaultMaxBackoff  = 30 * time.Second
	defaultHandshake   = 10 * time.Second
	// stableAfter is how long a connection must have streamed for the
	// backoff to be reset when it later fails.
	stableAfter = 30 * time.Second
	// writeTimeout bounds a single write to upstream so a dead peer is
	// noticed even when the queue would take long to overflow.
	writeTimeout = 30 * time.Second
)

// Dialer opens network connections; *net.Dialer implements it.
type Dialer interface {
	DialContext(ctx context.Context, network, addr string) (net.Conn, error)
}

// Options are the relay's injected dependencies and tuning.
type Options struct {
	Clock  clock.Clock // default clock.Real(); used for backoff and hold timers
	Dialer Dialer      // default &net.Dialer{Timeout: 10s}
	Logger *slog.Logger
	// MinBackoff and MaxBackoff bound the reconnect delay (default 1s, 30s).
	MinBackoff, MaxBackoff time.Duration
	UserAgent              string // default "proktor"
	// HandshakeTimeout bounds dialing plus the wait for the upstream's
	// response to the request (default 10s). It uses real time.
	HandshakeTimeout time.Duration
	// TLSConfig is used for https:// upstreams (default: system roots).
	TLSConfig *tls.Config
}

// Relay is a stream.Consumer that relays sessions to upstream servers.
type Relay struct {
	opts Options
	log  *slog.Logger
	ups  []*upstream

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
}

var (
	_ stream.Consumer = (*Relay)(nil)
	_ health.Checker  = (*Relay)(nil)
)

// New creates a Relay and starts one worker per enabled upstream. The workers
// stay idle until a matching source session starts.
func New(upstreams []config.Upstream, opts Options) *Relay {
	if opts.Clock == nil {
		opts.Clock = clock.Real()
	}
	if opts.Dialer == nil {
		opts.Dialer = &net.Dialer{Timeout: 10 * time.Second}
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.MinBackoff <= 0 {
		opts.MinBackoff = defaultMinBackoff
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = defaultMaxBackoff
	}
	if opts.MaxBackoff < opts.MinBackoff {
		opts.MaxBackoff = opts.MinBackoff
	}
	if opts.UserAgent == "" {
		opts.UserAgent = "proktor"
	}
	if opts.HandshakeTimeout <= 0 {
		opts.HandshakeTimeout = defaultHandshake
	}
	r := &Relay{opts: opts, log: opts.Logger}
	r.ctx, r.cancel = context.WithCancel(context.Background())
	for i, c := range upstreams {
		if c.Disabled {
			continue
		}
		u := newUpstream(r, i, c)
		r.ups = append(r.ups, u)
		if u.cfgErr != nil {
			r.log.Error("relay: upstream misconfigured", "upstream", u.name, "err", u.cfgErr)
			continue
		}
		r.wg.Add(1)
		go u.run()
	}
	return r
}

// applyDefaults fills in the documented defaults of an upstream config.
func applyDefaults(c config.Upstream) config.Upstream {
	if c.Mount == "" {
		c.Mount = "/"
	}
	if !strings.HasPrefix(c.Mount, "/") {
		c.Mount = "/" + c.Mount
	}
	if c.Username == "" {
		c.Username = "source"
	}
	c.Protocol = strings.ToLower(c.Protocol)
	if c.Protocol != "source" {
		c.Protocol = "put"
	}
	if c.ContentType == "" {
		c.ContentType = "application/ogg"
	}
	if c.BufferBytes <= 0 {
		c.BufferBytes = defaultBufferBytes
	}
	c.AuthStyle = strings.ToLower(c.AuthStyle)
	if c.AuthStyle != "user_in_password" {
		c.AuthStyle = "basic"
	}
	return c
}

// StartSession implements stream.Consumer. It returns nil when no upstream
// is interested in the session's mount (or the relay is closed).
func (r *Relay) StartSession(info stream.SessionInfo) stream.Handler {
	if r.ctx.Err() != nil {
		return nil
	}
	h := &handler{}
	for _, u := range r.ups {
		if u.cfgErr != nil || !u.matches(info.Mount) {
			continue
		}
		h.targets = append(h.targets, target{u: u, s: u.begin(info)})
	}
	if len(h.targets) == 0 {
		return nil
	}
	return h
}

// Close stops all workers and closes all upstream connections. It is
// idempotent and safe to call concurrently.
func (r *Relay) Close() error {
	r.closeOnce.Do(func() {
		r.cancel()
		for _, u := range r.ups {
			u.shutdown()
		}
	})
	r.wg.Wait()
	return nil
}

// UpstreamStatus is the per-upstream detail reported by Health.
type UpstreamStatus struct {
	Name string `json:"name"`
	// Target is host:port plus mount (never credentials).
	Target string `json:"target"`
	// State is one of idle, connecting, streaming, backoff, holding.
	State       string     `json:"state"`
	LastError   string     `json:"last_error,omitempty"`
	LastErrorAt *time.Time `json:"last_error_at,omitempty"`
	BytesSent   int64      `json:"bytes_sent"`
	// Reconnects counts connection failures that led to a retry.
	Reconnects int64 `json:"reconnects"`
	// Overflows counts how often the queue overflowed and was dropped;
	// DroppedBytes is the number of queued bytes discarded by those.
	Overflows      int64      `json:"overflows"`
	DroppedBytes   int64      `json:"dropped_bytes"`
	ConnectedSince *time.Time `json:"connected_since,omitempty"`
}

// Health reports per-upstream status. OK is false while any upstream is
// failing (in backoff) or misconfigured.
func (r *Relay) Health() health.Status {
	st := health.Status{OK: true}
	details := make([]UpstreamStatus, 0, len(r.ups))
	for _, u := range r.ups {
		s := u.status()
		details = append(details, s)
		if s.State == stateBackoff || u.cfgErr != nil {
			st.OK = false
			st.Warnings = append(st.Warnings, fmt.Sprintf("upstream %s: %s", s.Name, s.LastError))
		}
	}
	st.Detail = details
	return st
}

type target struct {
	u *upstream
	s *usess
}

// handler is one session's view; it fans segments to the matching upstreams.
type handler struct {
	targets []target
	once    sync.Once
}

func (h *handler) OnSegment(seg ogg.Segment) {
	if !seg.Page {
		return // upstream only ever gets whole pages
	}
	for _, t := range h.targets {
		t.u.segment(t.s, seg)
	}
}

func (h *handler) OnTrack(ogg.Track, time.Time) {}

func (h *handler) OnEnd(time.Time, error) {
	h.once.Do(func() {
		for _, t := range h.targets {
			t.u.end(t.s)
		}
	})
}

// parseTarget validates an upstream URL and returns scheme-specific details.
func parseTarget(raw string) (host, hostname string, useTLS bool, err error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", false, fmt.Errorf("invalid url: %w", err)
	}
	switch u.Scheme {
	case "http":
	case "https":
		useTLS = true
	default:
		return "", "", false, fmt.Errorf("unsupported url scheme %q (want http or https)", u.Scheme)
	}
	hostname = u.Hostname()
	if hostname == "" {
		return "", "", false, fmt.Errorf("url %q has no host", raw)
	}
	port := u.Port()
	if port == "" {
		port = "80"
		if useTLS {
			port = "443"
		}
	}
	return net.JoinHostPort(hostname, port), hostname, useTLS, nil
}
