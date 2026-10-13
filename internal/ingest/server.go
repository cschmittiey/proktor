// Package ingest is a TCP server that speaks enough of the Icecast source
// protocol (SOURCE / PUT) for Traktor's broadcast feature. Accepted sessions
// are fanned out to stream.Consumers; the audio bytes are never modified.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cschmittiey/proktor/internal/clock"
	"github.com/cschmittiey/proktor/internal/config"
	"github.com/cschmittiey/proktor/internal/health"
	"github.com/cschmittiey/proktor/internal/stream"
)

// Errors passed to Handler.OnEnd when a session does not end with a clean EOF.
var (
	// ErrTakenOver: a new source connected to the mount with on_conflict=takeover.
	ErrTakenOver = errors.New("ingest: session taken over by a new source")
	// ErrServerClosed: the server is shutting down.
	ErrServerClosed = errors.New("ingest: server closed")
	// ErrIdleTimeout: the source sent no bytes for IdleTimeout.
	ErrIdleTimeout = errors.New("ingest: source idle timeout")
)

// Options configures a Server.
type Options struct {
	Mounts    []config.Mount // defaults already applied
	Consumers []stream.Consumer
	Clock     clock.Clock // default clock.Real()
	Logger    *slog.Logger

	// HeaderTimeout bounds reading the request head. Default 10s.
	HeaderTimeout time.Duration
	// IdleTimeout drops a source that sends no bytes for this long. Default 30s.
	IdleTimeout time.Duration
}

// Server is the Icecast-source-protocol ingest server.
type Server struct {
	opts   Options
	clock  clock.Clock
	log    *slog.Logger
	mounts map[string]*mount
	order  []string // mount paths in config order

	idSeq atomic.Uint64

	mu      sync.Mutex
	conns   map[net.Conn]struct{}
	closing bool
}

// mount is the per-mount state.
type mount struct {
	cfg config.Mount

	mu       sync.Mutex
	cur      *session
	sessions uint64 // total accepted so far
}

// New returns a Server. It does not listen until Serve is called.
func New(opts Options) *Server {
	if opts.Clock == nil {
		opts.Clock = clock.Real()
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.HeaderTimeout <= 0 {
		opts.HeaderTimeout = 10 * time.Second
	}
	if opts.IdleTimeout <= 0 {
		opts.IdleTimeout = 30 * time.Second
	}
	s := &Server{
		opts:   opts,
		clock:  opts.Clock,
		log:    opts.Logger,
		mounts: make(map[string]*mount),
		conns:  make(map[net.Conn]struct{}),
	}
	for _, m := range opts.Mounts {
		p := m.Path
		if !strings.HasPrefix(p, "/") {
			p = "/" + p
		}
		m.Path = p
		if _, dup := s.mounts[p]; dup {
			continue
		}
		s.mounts[p] = &mount{cfg: m}
		s.order = append(s.order, p)
	}
	return s
}

// Serve accepts connections on ln until ctx is cancelled, then closes the
// listener and every session and returns nil. Any other accept error is
// returned (after the same cleanup).
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			ln.Close()
		case <-stop:
		}
	}()

	var wg sync.WaitGroup
	var serveErr error
	var backoff time.Duration
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				if backoff == 0 {
					backoff = 5 * time.Millisecond
				} else if backoff < time.Second {
					backoff *= 2
				}
				s.log.Warn("accept failed, retrying", "err", err, "in", backoff)
				select {
				case <-time.After(backoff):
					continue
				case <-ctx.Done():
				}
				break
			}
			serveErr = err
			break
		}
		backoff = 0
		if !s.track(conn) {
			conn.Close()
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer s.untrack(conn)
			s.handleConn(ctx, conn)
		}()
	}
	ln.Close()
	s.closeAll()
	wg.Wait()
	if ctx.Err() != nil {
		return nil
	}
	return serveErr
}

func (s *Server) track(c net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return false
	}
	s.conns[c] = struct{}{}
	return true
}

func (s *Server) untrack(c net.Conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
	c.Close()
}

func (s *Server) closeAll() {
	s.mu.Lock()
	s.closing = true
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
}

// MountStatus is the per-mount detail reported by Health.
type MountStatus struct {
	Path      string `json:"path"`
	Connected bool   `json:"connected"`
	// SessionsTotal counts sources accepted on this mount since start.
	SessionsTotal uint64 `json:"sessions_total"`

	// The fields below are set only while connected.
	SessionID       string     `json:"session_id,omitempty"`
	RemoteAddr      string     `json:"remote_addr,omitempty"`
	IceName         string     `json:"ice_name,omitempty"`
	ContentType     string     `json:"content_type,omitempty"`
	UserAgent       string     `json:"user_agent,omitempty"`
	StartedAt       *time.Time `json:"started_at,omitempty"`
	BytesReceived   int64      `json:"bytes_received"`
	LastArtist      string     `json:"last_artist,omitempty"`
	LastTitle       string     `json:"last_title,omitempty"`
	TracksDetected  int        `json:"tracks_detected"`
	ParseErrors     int64      `json:"parse_errors"`
	LastTrackAt     *time.Time `json:"last_track_at,omitempty"`
	LastDataAt      *time.Time `json:"last_data_at,omitempty"`
	ContentTypeWarn bool       `json:"non_ogg,omitempty"`
}

// Detail is the Health().Detail payload.
type Detail struct {
	Mounts map[string]MountStatus `json:"mounts"`
}

// Health reports per-mount source status. It is always OK: having no source
// connected is normal, but is surfaced as a warning for status lines.
func (s *Server) Health() health.Status {
	d := Detail{Mounts: make(map[string]MountStatus, len(s.mounts))}
	var warns []string
	paths := append([]string(nil), s.order...)
	sort.Strings(paths)
	for _, p := range paths {
		m := s.mounts[p]
		m.mu.Lock()
		cur := m.cur
		total := m.sessions
		m.mu.Unlock()
		st := MountStatus{Path: p, SessionsTotal: total}
		if cur != nil {
			st = cur.status(st)
		} else {
			warns = append(warns, "no source on "+p)
		}
		d.Mounts[p] = st
	}
	return health.Status{OK: true, Warnings: warns, Detail: d}
}

func (s *Server) newSessionID() string {
	n := s.idSeq.Add(1)
	return fmt.Sprintf("%d-%s", n, randHex(3))
}
