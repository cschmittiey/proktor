package relay

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/cschmittiey/proktor/internal/clock"
	"github.com/cschmittiey/proktor/internal/config"
	"github.com/cschmittiey/proktor/internal/ogg"
	"github.com/cschmittiey/proktor/internal/stream"
)

// Worker states as reported in health. "holding" is derived: streaming with
// no active source while the hold timer runs.
const (
	stateIdle       = "idle"
	stateConnecting = "connecting"
	stateStreaming  = "streaming"
	stateBackoff    = "backoff"
	stateHolding    = "holding"
)

var errOverflow = errors.New("upstream too slow: queue overflow, dropping connection")

// usess is one source session as seen by one upstream. Its fields are
// protected by upstream.mu.
type usess struct {
	info stream.SessionInfo
	// hdr caches the header pages of the session's current logical stream
	// (reset on every BOS page).
	hdr   [][]byte
	ended bool
}

// link is one upstream connection's data path. queue, bytes, budget and
// waitBOS are protected by upstream.mu.
type link struct {
	ctx    context.Context
	cancel context.CancelFunc
	once   sync.Once
	cause  error // valid once ctx is done and the kill came from kill()

	queue [][]byte
	// bytes counts queued plus in-flight bytes; budget is the limit for it
	// (header replays raise the budget by their own size).
	bytes, budget int
	// waitBOS drops pages until the next BOS page.
	waitBOS bool
	wake    chan struct{}
}

func newLink(parent context.Context, budget int) *link {
	ctx, cancel := context.WithCancel(parent)
	return &link{ctx: ctx, cancel: cancel, budget: budget, wake: make(chan struct{}, 1)}
}

// kill ends the link with cause (the first call wins). It never blocks.
func (l *link) kill(cause error) {
	l.once.Do(func() {
		l.cause = cause
		l.cancel()
	})
}

type upstream struct {
	r    *Relay
	name string
	cfg  config.Upstream
	hold time.Duration
	log  *slog.Logger

	host     string // host:port
	hostname string
	useTLS   bool
	cfgErr   error

	changed chan struct{} // cap 1; wakes the worker to re-evaluate

	mu        sync.Mutex
	active    []*usess // matching sessions in start order; last is current
	lastInfo  stream.SessionInfo
	holding   bool
	holdGen   int
	holdTimer clock.Timer
	link      *link
	state     string
	closed    bool

	lastErr      string
	lastErrAt    time.Time
	bytesSent    int64
	reconnects   int64
	overflows    int64
	droppedBytes int64
	connSince    time.Time
}

func newUpstream(r *Relay, idx int, c config.Upstream) *upstream {
	c = applyDefaults(c)
	u := &upstream{
		r:       r,
		cfg:     c,
		hold:    time.Duration(c.HoldSeconds * float64(time.Second)),
		changed: make(chan struct{}, 1),
		state:   stateIdle,
	}
	u.host, u.hostname, u.useTLS, u.cfgErr = parseTarget(c.URL)
	u.name = c.Name
	if u.name == "" {
		u.name = fmt.Sprintf("upstream-%d", idx+1)
	}
	if u.cfgErr != nil {
		u.lastErr = u.cfgErr.Error()
	}
	u.log = r.log.With("upstream", u.name)
	return u
}

func normMount(m string) string {
	if !strings.HasPrefix(m, "/") {
		return "/" + m
	}
	return m
}

func (u *upstream) matches(mount string) bool {
	return u.cfg.SourceMount == "" || normMount(u.cfg.SourceMount) == normMount(mount)
}

// cur returns the followed session. u.mu must be held.
func (u *upstream) cur() *usess {
	if len(u.active) == 0 {
		return nil
	}
	return u.active[len(u.active)-1]
}

// wantedLocked reports whether the upstream should hold a connection.
func (u *upstream) wantedLocked() bool {
	return !u.closed && (len(u.active) > 0 || u.holding)
}

func (u *upstream) wanted() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.wantedLocked()
}

func (u *upstream) signal() {
	select {
	case u.changed <- struct{}{}:
	default:
	}
}

func (u *upstream) setState(s string) {
	u.mu.Lock()
	u.state = s
	u.mu.Unlock()
}

func (u *upstream) stopHoldLocked() {
	if u.holdTimer != nil {
		u.holdTimer.Stop()
		u.holdTimer = nil
	}
	u.holding = false
	u.holdGen++
}

// begin registers a new source session and makes it the followed one.
func (u *upstream) begin(info stream.SessionInfo) *usess {
	s := &usess{info: info}
	u.mu.Lock()
	defer u.mu.Unlock()
	u.active = append(u.active, s)
	u.lastInfo = info
	u.stopHoldLocked()
	u.switchedLocked()
	u.signal()
	return s
}

// end unregisters a session. Ending a session that is no longer the followed
// one (e.g. a taken-over one) changes nothing.
func (u *upstream) end(s *usess) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if s.ended {
		return
	}
	s.ended = true
	s.hdr = nil
	wasCur := u.cur() == s
	for i, a := range u.active {
		if a == s {
			u.active = append(u.active[:i:i], u.active[i+1:]...)
			break
		}
	}
	if !wasCur {
		return
	}
	if u.cur() != nil {
		u.switchedLocked() // fall back to the previous session
		u.signal()
		return
	}
	if u.hold > 0 && !u.closed && (u.state == stateStreaming || u.state == stateConnecting) {
		u.holding = true
		u.holdGen++
		gen := u.holdGen
		u.holdTimer = u.r.opts.Clock.AfterFunc(u.hold, func() { u.holdExpired(gen) })
	}
	u.signal()
}

func (u *upstream) holdExpired(gen int) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if gen != u.holdGen || !u.holding {
		return
	}
	u.holding = false
	u.holdTimer = nil
	u.signal()
}

// switchedLocked is called when the followed session changed while a link may
// be live: replay the new session's cached headers, or wait for its BOS.
func (u *upstream) switchedLocked() {
	l := u.link
	cur := u.cur()
	if l == nil || cur == nil {
		return
	}
	if len(cur.hdr) > 0 {
		l.waitBOS = false
		for _, p := range cur.hdr {
			l.queue = append(l.queue, p)
			l.bytes += len(p)
			l.budget += len(p)
		}
		select {
		case l.wake <- struct{}{}:
		default:
		}
		return
	}
	l.waitBOS = true
}

// segment feeds one page to the upstream. It never blocks.
func (u *upstream) segment(s *usess, seg ogg.Segment) {
	if !seg.Page {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if s.ended || u.closed {
		return
	}
	switch {
	case seg.BOS:
		s.hdr = [][]byte{seg.Data}
	case seg.Header && len(s.hdr) > 0:
		s.hdr = append(s.hdr, seg.Data)
	}
	if u.cur() != s {
		return
	}
	l := u.link
	if l == nil {
		return // not connected (yet): the header cache is all we need
	}
	if l.waitBOS {
		if !seg.BOS {
			return
		}
		l.waitBOS = false
	}
	if l.bytes+len(seg.Data) > l.budget {
		u.overflows++
		u.droppedBytes += int64(l.bytes + len(seg.Data))
		l.queue = nil
		l.bytes = 0
		u.link = nil
		l.kill(errOverflow)
		u.log.Warn("relay: queue overflow, dropping upstream connection", "budget", l.budget)
		return
	}
	l.queue = append(l.queue, seg.Data)
	l.bytes += len(seg.Data)
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// shutdown is called by Relay.Close.
func (u *upstream) shutdown() {
	u.mu.Lock()
	u.closed = true
	u.stopHoldLocked()
	l := u.link
	u.link = nil
	u.mu.Unlock()
	if l != nil {
		l.kill(nil)
	}
	u.signal()
}

func (u *upstream) status() UpstreamStatus {
	u.mu.Lock()
	defer u.mu.Unlock()
	st := UpstreamStatus{
		Name:         u.name,
		Target:       u.host + u.cfg.Mount,
		State:        u.state,
		LastError:    u.lastErr,
		BytesSent:    u.bytesSent,
		Reconnects:   u.reconnects,
		Overflows:    u.overflows,
		DroppedBytes: u.droppedBytes,
	}
	if u.cfgErr != nil {
		st.State = stateIdle
	}
	if u.state == stateStreaming && len(u.active) == 0 && u.holding {
		st.State = stateHolding
	}
	if !u.lastErrAt.IsZero() {
		t := u.lastErrAt
		st.LastErrorAt = &t
	}
	if u.state == stateStreaming && !u.connSince.IsZero() {
		t := u.connSince
		st.ConnectedSince = &t
	}
	return st
}

// ---------------------------------------------------------------- worker

func (u *upstream) run() {
	defer u.r.wg.Done()
	ctx := u.r.ctx
	clk := u.r.opts.Clock
	for {
		if !u.waitWanted() {
			return
		}
		backoff := u.r.opts.MinBackoff
		for u.wanted() {
			if ctx.Err() != nil {
				return
			}
			u.setState(stateConnecting)
			ran, err := u.attempt()
			if ctx.Err() != nil {
				return
			}
			if err == nil {
				continue // source went away; loop condition decides
			}
			if ran >= stableAfter {
				backoff = u.r.opts.MinBackoff
			}
			u.mu.Lock()
			u.lastErr = err.Error()
			u.lastErrAt = clk.Now()
			if u.cur() == nil && u.holding {
				// Lost the connection while only holding it open:
				// nothing to reconnect for.
				u.stopHoldLocked()
			}
			retry := u.wantedLocked()
			if retry {
				u.reconnects++
			}
			u.mu.Unlock()
			u.log.Warn("relay: upstream connection failed", "err", err, "retry", retry)
			if !retry {
				break
			}
			delay := backoff
			if backoff *= 2; backoff > u.r.opts.MaxBackoff {
				backoff = u.r.opts.MaxBackoff
			}
			// Register the timer before publishing the state so that
			// observers seeing "backoff" can rely on it.
			timer := clk.After(delay)
			u.setState(stateBackoff)
		wait:
			for {
				select {
				case <-timer:
					break wait
				case <-u.changed:
					if !u.wanted() {
						break wait
					}
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (u *upstream) waitWanted() bool {
	for {
		if u.r.ctx.Err() != nil {
			return false
		}
		u.mu.Lock()
		w := u.wantedLocked()
		if !w {
			u.state = stateIdle
		}
		u.mu.Unlock()
		if w {
			return true
		}
		select {
		case <-u.changed:
		case <-u.r.ctx.Done():
			return false
		}
	}
}

// attempt makes one connection and streams until it ends. A nil error means
// a clean end (the source is gone, or the relay is closing); ran is how long
// the connection streamed.
func (u *upstream) attempt() (ran time.Duration, err error) {
	opts := u.r.opts
	u.mu.Lock()
	info := u.lastInfo
	if c := u.cur(); c != nil {
		info = c.info
	}
	u.mu.Unlock()

	l := newLink(u.r.ctx, u.cfg.BufferBytes)
	defer func() {
		l.kill(nil) // closes the connection via the AfterFunc below
	}()

	conn, br, err := u.connect(l.ctx, info)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	// Attach the link. From here on OnSegment enqueues pages. Everything
	// from the header replay to the first live page happens under the
	// lock, so no page can fall in between.
	u.mu.Lock()
	if !u.wantedLocked() || u.r.ctx.Err() != nil {
		u.mu.Unlock()
		return 0, nil
	}
	if c := u.cur(); c != nil && len(c.hdr) > 0 {
		for _, p := range c.hdr {
			l.queue = append(l.queue, p)
			l.bytes += len(p)
			l.budget += len(p)
		}
	} else {
		l.waitBOS = true
	}
	u.link = l
	u.state = stateStreaming
	start := opts.Clock.Now()
	u.connSince = start
	u.mu.Unlock()
	u.log.Info("relay: upstream streaming", "target", u.host+u.cfg.Mount)

	go u.drain(l, br)

	err = u.pump(conn, l)

	u.mu.Lock()
	if u.link == l {
		u.link = nil
	}
	u.connSince = time.Time{}
	u.mu.Unlock()
	return opts.Clock.Now().Sub(start), err
}

// drain reads and discards whatever upstream sends after the handshake, so
// that a closed connection is noticed even while we are idle (hold). The
// first line of any data is kept for the error message.
func (u *upstream) drain(l *link, br *bufio.Reader) {
	buf := make([]byte, 4096)
	note := ""
	first := true
	for {
		n, err := br.Read(buf)
		if n > 0 && first {
			first = false
			if line, _, _ := bytes.Cut(buf[:n], []byte("\n")); bytes.HasPrefix(line, []byte("HTTP/")) {
				note = " after sending " + strings.TrimSpace(string(line))
			}
		}
		if err != nil {
			l.kill(fmt.Errorf("upstream closed the connection%s", note))
			return
		}
	}
}

// pump writes queued pages to conn until the link dies, the source goes away
// or a write fails.
func (u *upstream) pump(conn net.Conn, l *link) error {
	for {
		select {
		case <-l.ctx.Done():
			return u.linkErr(l)
		default:
		}
		u.mu.Lock()
		batch := l.queue
		l.queue = nil
		want := u.wantedLocked()
		u.mu.Unlock()

		if len(batch) > 0 {
			total := 0
			for _, b := range batch {
				total += len(b)
			}
			conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			bufs := net.Buffers(batch)
			n, err := bufs.WriteTo(conn)
			u.mu.Lock()
			u.bytesSent += n
			l.bytes -= total
			u.mu.Unlock()
			if err != nil {
				if l.ctx.Err() != nil {
					return u.linkErr(l)
				}
				return fmt.Errorf("write to upstream: %w", err)
			}
			continue
		}
		if !want {
			return nil
		}
		select {
		case <-l.wake:
		case <-u.changed:
		case <-l.ctx.Done():
			return u.linkErr(l)
		}
	}
}

// linkErr is the error for a link whose context is done.
func (u *upstream) linkErr(l *link) error {
	if u.r.ctx.Err() != nil {
		return nil // relay closing
	}
	if l.cause != nil {
		return l.cause
	}
	return errors.New("upstream connection closed")
}
