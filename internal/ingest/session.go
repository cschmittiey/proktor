package ingest

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http/httputil"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cschmittiey/proktor/internal/ogg"
	"github.com/cschmittiey/proktor/internal/stream"
)

const readBufSize = 32 << 10

// handleConn runs one connection from request head to disconnect.
func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	remote := conn.RemoteAddr().String()
	log := s.log.With("remote", remote)

	conn.SetReadDeadline(time.Now().Add(s.opts.HeaderTimeout))
	rd := &connReader{conn: conn, remaining: maxHeadBytes}
	br := bufio.NewReaderSize(rd, readBufSize)
	req, err := readRequest(br)
	if err != nil {
		if ctx.Err() == nil && !errors.Is(err, io.EOF) {
			log.Debug("bad request head", "err", err)
			writeResponse(conn, 400, "Bad Request", nil, "bad request\n")
		}
		return
	}

	switch req.method {
	case "SOURCE", "PUT":
	case "GET", "HEAD", "POST":
		writeResponse(conn, 405, "Method Not Allowed", []string{"Allow: SOURCE, PUT"},
			"proktor ingest port: broadcast here with an Icecast source client (SOURCE or PUT)\n")
		return
	default:
		writeResponse(conn, 400, "Bad Request", nil, "unsupported method\n")
		return
	}

	m := s.mounts[req.mount]
	if m == nil {
		log.Info("source rejected: unknown mount", "mount", req.mount)
		writeResponse(conn, 404, "Not Found", nil, "no such mount\n")
		return
	}
	if m.cfg.Password != "" {
		pw, ok := basicPassword(req.header("authorization"))
		if !ok || !passwordOK(m.cfg.Password, pw) {
			log.Info("source rejected: bad credentials", "mount", req.mount)
			writeResponse(conn, 401, "Unauthorized", []string{`WWW-Authenticate: Basic realm="proktor"`}, "authentication required\n")
			return
		}
	}

	sess := &session{
		srv:   s,
		mount: m,
		conn:  conn,
		done:  make(chan struct{}),
		info: stream.SessionInfo{
			ID:             s.newSessionID(),
			Mount:          m.cfg.Path,
			RemoteAddr:     remote,
			StartedAt:      s.clock.Now(),
			ContentType:    req.header("content-type"),
			UserAgent:      req.header("user-agent"),
			IceName:        req.header("ice-name", "icy-name"),
			IceDescription: req.header("ice-description", "icy-description"),
			IceGenre:       req.header("ice-genre", "icy-genre"),
			IceURL:         req.header("ice-url", "icy-url"),
			IceBitrate:     req.header("ice-bitrate", "icy-br"),
			IceAudioInfo:   req.header("ice-audio-info"),
		},
	}
	sess.log = log.With("mount", m.cfg.Path, "session", sess.info.ID)

	if !s.claim(ctx, m, sess) {
		if ctx.Err() == nil {
			log.Info("source rejected: mount in use", "mount", req.mount)
			writeResponse(conn, 403, "Forbidden", nil, "Mountpoint in use\n")
		}
		return
	}

	reply := "HTTP/1.0 200 OK\r\n\r\n"
	if strings.Contains(strings.ToLower(req.header("expect")), "100-continue") {
		reply = "HTTP/1.1 100 Continue\r\n\r\n"
	}
	if err := writeRaw(conn, reply); err != nil {
		sess.release()
		return
	}

	var body io.Reader = br
	if strings.Contains(strings.ToLower(req.header("transfer-encoding")), "chunked") {
		body = httputil.NewChunkedReader(br)
	}
	rd.idle = s.opts.IdleTimeout
	sess.run(ctx, body)
}

// claim makes sess the mount's active session. If the mount is busy it either
// returns false (reject) or ends the old session first (takeover). It returns
// false without claiming if ctx ends while waiting.
func (s *Server) claim(ctx context.Context, m *mount, sess *session) bool {
	for {
		m.mu.Lock()
		old := m.cur
		if old == nil {
			m.cur = sess
			m.sessions++
			m.mu.Unlock()
			return true
		}
		m.mu.Unlock()
		if m.cfg.OnConflict != "takeover" {
			return false
		}
		s.log.Info("taking over mount", "mount", m.cfg.Path, "old_session", old.info.ID, "new_remote", sess.info.RemoteAddr)
		old.stop(ErrTakenOver)
		select {
		case <-old.done:
		case <-ctx.Done():
			return false
		}
	}
}

// session is one accepted source connection.
type session struct {
	srv   *Server
	mount *mount
	conn  net.Conn
	info  stream.SessionInfo
	log   *slog.Logger
	done  chan struct{} // closed after OnEnd and mount release

	handlers []*safeHandler

	bytes      atomic.Int64
	parseErrs  atomic.Int64
	lastDataNs atomic.Int64

	mu         sync.Mutex
	stopErr    error
	artist     string
	title      string
	tracks     int
	lastTrack  time.Time
	nonOgg     bool
	endOnce    sync.Once
	releasedMu sync.Once
}

// stop ends the session from outside: the connection is closed and err is
// reported to the handlers' OnEnd (first reason wins).
func (se *session) stop(err error) {
	se.mu.Lock()
	if se.stopErr == nil {
		se.stopErr = err
	}
	se.mu.Unlock()
	se.conn.Close()
}

func (se *session) stopReason() error {
	se.mu.Lock()
	defer se.mu.Unlock()
	return se.stopErr
}

func (se *session) release() {
	se.releasedMu.Do(func() {
		se.mount.mu.Lock()
		if se.mount.cur == se {
			se.mount.cur = nil
		}
		se.mount.mu.Unlock()
		close(se.done)
	})
}

func (se *session) status(st MountStatus) MountStatus {
	se.mu.Lock()
	defer se.mu.Unlock()
	started := se.info.StartedAt
	st.Connected = true
	st.SessionID = se.info.ID
	st.RemoteAddr = se.info.RemoteAddr
	st.IceName = se.info.IceName
	st.ContentType = se.info.ContentType
	st.UserAgent = se.info.UserAgent
	st.StartedAt = &started
	st.BytesReceived = se.bytes.Load()
	st.LastArtist = se.artist
	st.LastTitle = se.title
	st.TracksDetected = se.tracks
	st.ParseErrors = se.parseErrs.Load()
	st.ContentTypeWarn = se.nonOgg
	if !se.lastTrack.IsZero() {
		t := se.lastTrack
		st.LastTrackAt = &t
	}
	if ns := se.lastDataNs.Load(); ns != 0 {
		t := time.Unix(0, ns).UTC()
		st.LastDataAt = &t
	}
	return st
}

// run is the session lifecycle: start consumers, pump bytes, end consumers.
func (se *session) run(ctx context.Context, body io.Reader) {
	srv := se.srv
	for i, c := range srv.opts.Consumers {
		if c == nil {
			continue
		}
		if h := se.startConsumer(i, c); h != nil {
			se.handlers = append(se.handlers, &safeHandler{h: h, log: se.log, name: consumerName(i, c)})
		}
	}

	ct := strings.ToLower(se.info.ContentType)
	if ct != "" && !strings.Contains(ct, "ogg") {
		se.nonOgg = true
		se.log.Warn("source is not Ogg: bytes are relayed and recorded but track metadata will not parse",
			"content_type", se.info.ContentType)
	}
	se.log.Info("source connected", "ice_name", se.info.IceName,
		"content_type", se.info.ContentType, "user_agent", se.info.UserAgent)

	scanner := ogg.NewScanner(ogg.Options{
		OnSegment: func(seg ogg.Segment) {
			for _, h := range se.handlers {
				h.onSegment(seg)
			}
		},
		OnTrack: se.onTrack,
		OnError: se.onParseError,
	})

	readErr := se.pump(body, scanner)
	se.flush(scanner)

	endErr := se.stopReason()
	if endErr == nil && readErr != nil {
		switch {
		case ctx.Err() != nil:
			endErr = ErrServerClosed
		case isTimeout(readErr):
			endErr = errors.Join(ErrIdleTimeout, readErr)
		default:
			endErr = readErr
		}
	}
	end := srv.clock.Now()
	se.endOnce.Do(func() {
		for _, h := range se.handlers {
			h.onEnd(end, endErr)
		}
	})
	se.release()

	attrs := []any{
		"duration", end.Sub(se.info.StartedAt).Round(time.Millisecond),
		"bytes", se.bytes.Load(),
		"tracks", se.trackCount(),
		"parse_errors", se.parseErrs.Load(),
	}
	if endErr != nil {
		attrs = append(attrs, "reason", endErr.Error())
	}
	se.log.Info("source disconnected", attrs...)
}

func (se *session) trackCount() int {
	se.mu.Lock()
	defer se.mu.Unlock()
	return se.tracks
}

func (se *session) startConsumer(i int, c stream.Consumer) (h stream.Handler) {
	defer func() {
		if r := recover(); r != nil {
			se.log.Error("consumer StartSession panicked", "consumer", consumerName(i, c), "panic", r)
			h = nil
		}
	}()
	return c.StartSession(se.info)
}

// pump reads body until EOF or error, feeding the scanner. A nil return means
// a clean EOF.
func (se *session) pump(body io.Reader, scanner *ogg.Scanner) (err error) {
	defer func() {
		if r := recover(); r != nil {
			se.log.Error("ingest scanner panicked", "panic", r)
			err = errors.New("internal error in stream scanner")
		}
	}()
	buf := make([]byte, readBufSize)
	for {
		n, rerr := body.Read(buf)
		if n > 0 {
			se.bytes.Add(int64(n))
			se.lastDataNs.Store(se.srv.clock.Now().UnixNano())
			scanner.Write(buf[:n])
		}
		if rerr != nil {
			if rerr == io.EOF {
				return nil
			}
			return rerr
		}
	}
}

func (se *session) flush(scanner *ogg.Scanner) {
	defer func() {
		if r := recover(); r != nil {
			se.log.Error("ingest scanner flush panicked", "panic", r)
		}
	}()
	scanner.Flush()
}

func (se *session) onTrack(t ogg.Track) {
	now := se.srv.clock.Now()
	se.mu.Lock()
	se.artist, se.title = t.Artist, t.Title
	se.tracks++
	se.lastTrack = now
	se.mu.Unlock()
	se.log.Info("track", "artist", t.Artist, "title", t.Title, "now_playing", nowPlaying(t))
	for _, h := range se.handlers {
		h.onTrack(t, now)
	}
}

func nowPlaying(t ogg.Track) string {
	switch {
	case t.Artist != "" && t.Title != "":
		return t.Artist + " - " + t.Title
	case t.Title != "":
		return t.Title
	default:
		return t.Artist
	}
}

func (se *session) onParseError(err error) {
	n := se.parseErrs.Add(1)
	// Rate limit: first few at warn, then every 100th.
	switch {
	case n <= 3:
		se.log.Warn("ogg parse problem (audio keeps flowing)", "err", err, "count", n)
	case n%100 == 0:
		se.log.Warn("ogg parse problems continue", "err", err, "count", n)
	default:
		se.log.Debug("ogg parse problem", "err", err, "count", n)
	}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func consumerName(i int, c stream.Consumer) string {
	return strings.TrimPrefix(typeName(c), "*") + "#" + itoa(i)
}
