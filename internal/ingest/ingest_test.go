package ingest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cschmittiey/proktor/internal/config"
	"github.com/cschmittiey/proktor/internal/ogg"
	"github.com/cschmittiey/proktor/internal/stream"
)

// ---- recording consumer ----

type recSession struct {
	Info   stream.SessionInfo
	mu     sync.Mutex
	data   bytes.Buffer
	segs   int
	tracks []ogg.Track
	ends   []error
	endAt  []time.Time
}

func (r *recSession) OnSegment(seg ogg.Segment) {
	r.mu.Lock()
	r.data.Write(seg.Data)
	r.segs++
	r.mu.Unlock()
}
func (r *recSession) OnTrack(t ogg.Track, at time.Time) {
	r.mu.Lock()
	r.tracks = append(r.tracks, t)
	r.mu.Unlock()
}
func (r *recSession) OnEnd(at time.Time, err error) {
	r.mu.Lock()
	r.ends = append(r.ends, err)
	r.endAt = append(r.endAt, at)
	r.mu.Unlock()
}
func (r *recSession) Data() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]byte(nil), r.data.Bytes()...)
}
func (r *recSession) Ends() []error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]error(nil), r.ends...)
}
func (r *recSession) Tracks() []ogg.Track {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ogg.Track(nil), r.tracks...)
}

type recorder struct {
	mu       sync.Mutex
	sessions []*recSession
	events   []string
}

func (c *recorder) StartSession(info stream.SessionInfo) stream.Handler {
	s := &recSession{Info: info}
	c.mu.Lock()
	c.sessions = append(c.sessions, s)
	c.events = append(c.events, "start:"+info.ID)
	c.mu.Unlock()
	return &eventHandler{recSession: s, c: c}
}

// eventHandler also logs OnEnd into the consumer's event list for ordering.
type eventHandler struct {
	*recSession
	c *recorder
}

func (h *eventHandler) OnEnd(at time.Time, err error) {
	h.c.mu.Lock()
	h.c.events = append(h.c.events, "end:"+h.Info.ID)
	h.c.mu.Unlock()
	h.recSession.OnEnd(at, err)
}

func (c *recorder) Sessions() []*recSession {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*recSession(nil), c.sessions...)
}
func (c *recorder) Events() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.events...)
}

// waitSession waits for the n-th (0-based) session to exist.
func (c *recorder) waitSession(t *testing.T, n int) *recSession {
	t.Helper()
	var s *recSession
	waitFor(t, fmt.Sprintf("session %d to start", n), func() bool {
		ss := c.Sessions()
		if len(ss) > n {
			s = ss[n]
			return true
		}
		return false
	})
	return s
}

// ---- helpers ----

// junk is non-Ogg data; the scanner emits unparseable bytes in 4096-byte runs.
var junk = bytes.Repeat([]byte("j"), 4096)

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func fixture(t testing.TB) []byte {
	t.Helper()
	b, err := os.ReadFile("../../testdata/chained.ogg")
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	return b
}

type testServer struct {
	*Server
	Addr   string
	cancel context.CancelFunc
	done   chan error
}

func startServer(t *testing.T, opts Options) *testServer {
	t.Helper()
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.Mounts == nil {
		opts.Mounts = []config.Mount{{Path: "/live", OnConflict: "reject"}}
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := New(opts)
	ctx, cancel := context.WithCancel(context.Background())
	ts := &testServer{Server: srv, Addr: ln.Addr().String(), cancel: cancel, done: make(chan error, 1)}
	go func() { ts.done <- srv.Serve(ctx, ln) }()
	t.Cleanup(func() { ts.stop(t) })
	return ts
}

func (ts *testServer) stop(t *testing.T) {
	t.Helper()
	ts.cancel()
	select {
	case err := <-ts.done:
		if err != nil {
			t.Errorf("Serve returned %v, want nil", err)
		}
		ts.done <- nil
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after cancel")
	}
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func basic(user, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

// readHead reads a response head (status line + headers) and returns the
// status line and lower-cased headers.
func readHead(t *testing.T, c net.Conn) (string, map[string]string, *bufio.Reader) {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReader(c)
	status, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("reading status: %v", err)
	}
	h := map[string]string{}
	for {
		l, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("reading headers: %v", err)
		}
		l = strings.TrimRight(l, "\r\n")
		if l == "" {
			break
		}
		k, v, _ := strings.Cut(l, ":")
		h[strings.ToLower(k)] = strings.TrimSpace(v)
	}
	c.SetReadDeadline(time.Time{})
	return strings.TrimRight(status, "\r\n"), h, br
}

// connect sends a request head and returns the response status line.
func connect(t *testing.T, addr, head string) (net.Conn, string, map[string]string) {
	t.Helper()
	c := dial(t, addr)
	if _, err := io.WriteString(c, head); err != nil {
		t.Fatal(err)
	}
	st, h, _ := readHead(t, c)
	return c, st, h
}

func sourceHead(mount, pass string, extra ...string) string {
	h := "SOURCE " + mount + " HTTP/1.0\r\n"
	if pass != "" {
		h += "Authorization: " + basic("source", pass) + "\r\n"
	}
	h += "User-Agent: test/1\r\nContent-Type: application/ogg\r\n"
	for _, e := range extra {
		h += e + "\r\n"
	}
	return h + "\r\n"
}

func writeChunked(t *testing.T, w io.Writer, data []byte, size int) {
	t.Helper()
	for len(data) > 0 {
		n := min(size, len(data))
		if _, err := fmt.Fprintf(w, "%x\r\n", n); err != nil {
			t.Fatal(err)
		}
		w.Write(data[:n])
		io.WriteString(w, "\r\n")
		data = data[n:]
	}
	io.WriteString(w, "0\r\n\r\n")
}

var wantTracks = [][2]string{
	{"New Order", "Blue Monday"},
	{"Röyksopp", "Eple"},
	{"坂本龍一", "Merry Christmas Mr. Lawrence"},
	{"Aphex Twin", "Xtal"},
	{"DJ Überlong Name feat. Ünïcödé", "Track with = equals sign"},
}

func checkTracks(t *testing.T, got []ogg.Track) {
	t.Helper()
	if len(got) != len(wantTracks) {
		t.Fatalf("got %d tracks, want %d: %+v", len(got), len(wantTracks), got)
	}
	for i, w := range wantTracks {
		if got[i].Artist != w[0] || got[i].Title != w[1] {
			t.Errorf("track %d = %q - %q, want %q - %q", i, got[i].Artist, got[i].Title, w[0], w[1])
		}
	}
}

// finish half-closes by closing the connection and waits for the session's OnEnd.
func finish(t *testing.T, c net.Conn, s *recSession) {
	t.Helper()
	c.Close()
	waitFor(t, "OnEnd", func() bool { return len(s.Ends()) > 0 })
}

// ---- protocol tests ----

func TestSourceHTTP10AndIceHeaders(t *testing.T) {
	rec := &recorder{}
	ts := startServer(t, Options{Consumers: []stream.Consumer{rec}})
	data := fixture(t)

	c, st, _ := connect(t, ts.Addr, sourceHead("/live?foo=bar", "",
		"ice-name: DJ Test", "ice-description: a set", "ice-genre: techno", "ice-url: http://x.invalid/",
		"ice-bitrate: 192", "ice-audio-info: ice-samplerate=44100;ice-bitrate=192"))
	if st != "HTTP/1.0 200 OK" {
		t.Fatalf("status = %q", st)
	}
	s := rec.waitSession(t, 0)
	c.Write(data)
	finish(t, c, s)

	info := s.Info
	if info.Mount != "/live" || info.IceName != "DJ Test" || info.IceDescription != "a set" ||
		info.IceGenre != "techno" || info.IceURL != "http://x.invalid/" || info.IceBitrate != "192" ||
		info.IceAudioInfo != "ice-samplerate=44100;ice-bitrate=192" ||
		info.ContentType != "application/ogg" || info.UserAgent != "test/1" ||
		info.ID == "" || info.RemoteAddr == "" || info.StartedAt.IsZero() {
		t.Errorf("SessionInfo = %+v", info)
	}
	if got := s.Data(); !bytes.Equal(got, data) {
		t.Errorf("delivered %d bytes, sent %d (equal=%v)", len(got), len(data), bytes.Equal(got, data))
	}
	checkTracks(t, s.Tracks())
	time.Sleep(20 * time.Millisecond)
	if n := len(s.Ends()); n != 1 {
		t.Errorf("OnEnd called %d times, want 1", n)
	}
	if err := s.Ends()[0]; err != nil {
		t.Errorf("OnEnd err = %v, want nil on clean EOF", err)
	}
}

func TestSourceICE10Legacy(t *testing.T) {
	rec := &recorder{}
	ts := startServer(t, Options{Consumers: []stream.Consumer{rec}})
	data := fixture(t)
	c, st, _ := connect(t, ts.Addr,
		"SOURCE /live ICE/1.0\r\nAuthorization: "+basic("source", "x")+"\r\nContent-Type: application/ogg\r\nicy-name: Legacy\r\nicy-genre: g\r\nicy-br: 128\r\n\r\n")
	if st != "HTTP/1.0 200 OK" {
		t.Fatalf("status = %q", st)
	}
	s := rec.waitSession(t, 0)
	c.Write(data)
	finish(t, c, s)
	if s.Info.IceName != "Legacy" || s.Info.IceGenre != "g" || s.Info.IceBitrate != "128" {
		t.Errorf("legacy icy-* headers not mapped: %+v", s.Info)
	}
	if !bytes.Equal(s.Data(), data) {
		t.Error("bytes differ")
	}
	checkTracks(t, s.Tracks())
}

func TestPutExpect100Continue(t *testing.T) {
	rec := &recorder{}
	ts := startServer(t, Options{Consumers: []stream.Consumer{rec}})
	data := fixture(t)
	c, st, _ := connect(t, ts.Addr,
		"PUT /live HTTP/1.1\r\nHost: x\r\nAuthorization: "+basic("source", "p")+"\r\nContent-Type: application/ogg\r\nExpect: 100-continue\r\nice-name: put\r\n\r\n")
	if st != "HTTP/1.1 100 Continue" {
		t.Fatalf("status = %q, want 100 Continue", st)
	}
	s := rec.waitSession(t, 0)
	c.Write(data)
	finish(t, c, s)
	if !bytes.Equal(s.Data(), data) {
		t.Error("bytes differ")
	}
	checkTracks(t, s.Tracks())
}

func TestPutWithoutExpectGets200(t *testing.T) {
	rec := &recorder{}
	ts := startServer(t, Options{Consumers: []stream.Consumer{rec}})
	c, st, _ := connect(t, ts.Addr, "PUT /live HTTP/1.1\r\nHost: x\r\nContent-Type: application/ogg\r\n\r\n")
	if st != "HTTP/1.0 200 OK" {
		t.Fatalf("status = %q", st)
	}
	s := rec.waitSession(t, 0)
	finish(t, c, s)
}

func TestPutChunked(t *testing.T) {
	rec := &recorder{}
	ts := startServer(t, Options{Consumers: []stream.Consumer{rec}})
	data := fixture(t)
	c, st, _ := connect(t, ts.Addr,
		"PUT /live HTTP/1.1\r\nHost: x\r\nContent-Type: application/ogg\r\nTransfer-Encoding: chunked\r\nExpect: 100-continue\r\n\r\n")
	if st != "HTTP/1.1 100 Continue" {
		t.Fatalf("status = %q", st)
	}
	s := rec.waitSession(t, 0)
	writeChunked(t, c, data, 1000)
	// The terminating chunk ends the body: OnEnd must come without closing.
	waitFor(t, "OnEnd after terminating chunk", func() bool { return len(s.Ends()) > 0 })
	if !bytes.Equal(s.Data(), data) {
		t.Errorf("chunked body decoded wrongly: got %d bytes want %d", len(s.Data()), len(data))
	}
	checkTracks(t, s.Tracks())
}

func TestAuth(t *testing.T) {
	rec := &recorder{}
	ts := startServer(t, Options{
		Consumers: []stream.Consumer{rec},
		Mounts: []config.Mount{
			{Path: "/live", Password: "secret", OnConflict: "reject"},
			{Path: "/open", OnConflict: "reject"},
		},
	})

	// no credentials
	_, st, h := connect(t, ts.Addr, sourceHead("/live", ""))
	if !strings.HasPrefix(st, "HTTP/1.0 401") {
		t.Errorf("no auth: status = %q", st)
	}
	if !strings.HasPrefix(h["www-authenticate"], "Basic") || !strings.Contains(h["www-authenticate"], `realm="proktor"`) {
		t.Errorf("WWW-Authenticate = %q", h["www-authenticate"])
	}
	// wrong password
	_, st, _ = connect(t, ts.Addr, sourceHead("/live", "nope"))
	if !strings.HasPrefix(st, "HTTP/1.0 401") {
		t.Errorf("wrong pw: status = %q", st)
	}
	// garbage authorization
	_, st, _ = connect(t, ts.Addr, sourceHead("/live", "", "Authorization: Basic !!!notbase64"))
	if !strings.HasPrefix(st, "HTTP/1.0 401") {
		t.Errorf("bad header: status = %q", st)
	}
	if n := len(rec.Sessions()); n != 0 {
		t.Fatalf("%d sessions started by rejected requests", n)
	}
	// right password, arbitrary username
	c, st, _ := connect(t, ts.Addr,
		"SOURCE /live HTTP/1.0\r\nAuthorization: "+basic("whoever", "secret")+"\r\n\r\n")
	if st != "HTTP/1.0 200 OK" {
		t.Errorf("good pw: status = %q", st)
	}
	s := rec.waitSession(t, 0)
	finish(t, c, s)
	// password with a colon in it
	ts2 := startServer(t, Options{Mounts: []config.Mount{{Path: "/live", Password: "a:b", OnConflict: "reject"}}})
	c2, st, _ := connect(t, ts2.Addr, sourceHead("/live", "a:b"))
	if st != "HTTP/1.0 200 OK" {
		t.Errorf("colon pw: status = %q", st)
	}
	c2.Close()
	// no password configured: anything (even no auth header) is accepted
	c3, st, _ := connect(t, ts.Addr, sourceHead("/open", ""))
	if st != "HTTP/1.0 200 OK" {
		t.Errorf("open mount, no auth: status = %q", st)
	}
	c3.Close()
	c4, st, _ := connect(t, ts.Addr, sourceHead("/open", "whatever"))
	if st != "HTTP/1.0 200 OK" {
		t.Errorf("open mount, some auth: status = %q", st)
	}
	c4.Close()
}

func TestUnknownMountAndMethods(t *testing.T) {
	ts := startServer(t, Options{})
	_, st, _ := connect(t, ts.Addr, sourceHead("/nope", ""))
	if !strings.HasPrefix(st, "HTTP/1.0 404") {
		t.Errorf("unknown mount: %q", st)
	}
	_, st, _ = connect(t, ts.Addr, "GET /live HTTP/1.1\r\nHost: x\r\n\r\n")
	if !strings.HasPrefix(st, "HTTP/1.0 405") {
		t.Errorf("GET: %q", st)
	}
	_, st, _ = connect(t, ts.Addr, "FROB /live HTTP/1.1\r\n\r\n")
	if !strings.HasPrefix(st, "HTTP/1.0 400") {
		t.Errorf("unknown method: %q", st)
	}
	_, st, _ = connect(t, ts.Addr, "complete nonsense here and more\r\n\r\n")
	if !strings.HasPrefix(st, "HTTP/1.0 400") {
		t.Errorf("nonsense: %q", st)
	}
}

func TestHeaderTimeout(t *testing.T) {
	ts := startServer(t, Options{HeaderTimeout: 100 * time.Millisecond})
	c := dial(t, ts.Addr)
	io.WriteString(c, "SOURCE /live HTT") // never finishes
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	b, _ := io.ReadAll(c) // server must close on us
	if !strings.HasPrefix(string(b), "HTTP/1.0 400") && len(b) != 0 {
		t.Errorf("unexpected reply %q", b)
	}
}

// ---- conflict handling ----

func TestBusyMountReject(t *testing.T) {
	rec := &recorder{}
	ts := startServer(t, Options{Consumers: []stream.Consumer{rec}})
	c1, st, _ := connect(t, ts.Addr, sourceHead("/live", ""))
	if st != "HTTP/1.0 200 OK" {
		t.Fatal(st)
	}
	s1 := rec.waitSession(t, 0)
	_, st, _ = connect(t, ts.Addr, sourceHead("/live", ""))
	if !strings.HasPrefix(st, "HTTP/1.0 403") {
		t.Errorf("second source: %q, want 403", st)
	}
	// First session is undisturbed and still gets data.
	c1.Write(junk)
	waitFor(t, "data on first session", func() bool { return bytes.Equal(s1.Data(), junk) })
	if len(s1.Ends()) != 0 {
		t.Error("first session ended")
	}
	if n := len(rec.Sessions()); n != 1 {
		t.Errorf("%d sessions, want 1", n)
	}
	// After the first leaves, the mount is free again.
	finish(t, c1, s1)
	waitFor(t, "mount free", func() bool {
		c, st, _ := connect(t, ts.Addr, sourceHead("/live", ""))
		c.Close()
		return st == "HTTP/1.0 200 OK"
	})
}

func TestBusyMountTakeover(t *testing.T) {
	rec := &recorder{}
	ts := startServer(t, Options{
		Consumers: []stream.Consumer{rec},
		Mounts:    []config.Mount{{Path: "/live", OnConflict: "takeover"}},
	})
	data := fixture(t)
	c1, _, _ := connect(t, ts.Addr, sourceHead("/live", "", "ice-name: old"))
	s1 := rec.waitSession(t, 0)
	c1.Write(junk)
	waitFor(t, "old data", func() bool { return bytes.Equal(s1.Data(), junk) })

	c2, st, _ := connect(t, ts.Addr, sourceHead("/live", "", "ice-name: new"))
	if st != "HTTP/1.0 200 OK" {
		t.Fatalf("takeover status = %q", st)
	}
	s2 := rec.waitSession(t, 1)
	waitFor(t, "old OnEnd", func() bool { return len(s1.Ends()) > 0 })
	if err := s1.Ends()[0]; !errors.Is(err, ErrTakenOver) {
		t.Errorf("old OnEnd err = %v, want ErrTakenOver", err)
	}
	// Old connection was closed by the server.
	c1.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadAll(c1); err != nil && !errors.Is(err, io.EOF) {
		// connection reset is also acceptable
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			t.Error("old connection still open")
		}
	}
	// Order: old session ended before the new one started.
	ev := rec.Events()
	want := []string{"start:" + s1.Info.ID, "end:" + s1.Info.ID, "start:" + s2.Info.ID}
	if fmt.Sprint(ev) != fmt.Sprint(want) {
		t.Errorf("events = %v, want %v", ev, want)
	}
	// New session works fully.
	c2.Write(data)
	finish(t, c2, s2)
	if !bytes.Equal(s2.Data(), data) {
		t.Error("new session bytes differ")
	}
	checkTracks(t, s2.Tracks())
	if n := len(s1.Ends()); n != 1 {
		t.Errorf("old OnEnd called %d times", n)
	}
	if !bytes.Equal(s1.Data(), junk) {
		t.Errorf("old session got %q after takeover", s1.Data())
	}
}

func TestMultipleMounts(t *testing.T) {
	rec := &recorder{}
	ts := startServer(t, Options{
		Consumers: []stream.Consumer{rec},
		Mounts:    []config.Mount{{Path: "/a", OnConflict: "reject"}, {Path: "/b", OnConflict: "reject"}},
	})
	ca, _, _ := connect(t, ts.Addr, sourceHead("/a", ""))
	cb, st, _ := connect(t, ts.Addr, sourceHead("/b", ""))
	if st != "HTTP/1.0 200 OK" {
		t.Fatal(st)
	}
	ja, jb := bytes.Repeat([]byte("A"), 4096), bytes.Repeat([]byte("B"), 8192)
	ca.Write(ja)
	cb.Write(jb)
	waitFor(t, "both sessions", func() bool { return len(rec.Sessions()) == 2 })
	byMount := map[string]*recSession{}
	for _, s := range rec.Sessions() {
		byMount[s.Info.Mount] = s
	}
	waitFor(t, "data", func() bool {
		return bytes.Equal(byMount["/a"].Data(), ja) && bytes.Equal(byMount["/b"].Data(), jb)
	})
}

// ---- data path ----

func TestGarbageBodyByteExact(t *testing.T) {
	rec := &recorder{}
	ts := startServer(t, Options{Consumers: []stream.Consumer{rec}})
	rng := rand.New(rand.NewSource(1))
	garbage := make([]byte, 200_000)
	rng.Read(garbage)
	// sprinkle fake capture patterns
	copy(garbage[1000:], "OggS")
	copy(garbage[50000:], "OggS\x00\x02")
	c, _, _ := connect(t, ts.Addr, sourceHead("/live", ""))
	s := rec.waitSession(t, 0)
	c.Write(garbage)
	finish(t, c, s)
	if !bytes.Equal(s.Data(), garbage) {
		t.Errorf("garbage not delivered byte-exact: got %d want %d", len(s.Data()), len(garbage))
	}
	if n := len(s.Tracks()); n != 0 {
		t.Errorf("%d tracks from garbage", n)
	}
}

func TestNonOggContentTypeStillFlows(t *testing.T) {
	rec := &recorder{}
	ts := startServer(t, Options{Consumers: []stream.Consumer{rec}})
	c, st, _ := connect(t, ts.Addr, "SOURCE /live HTTP/1.0\r\nContent-Type: audio/mpeg\r\n\r\n")
	if st != "HTTP/1.0 200 OK" {
		t.Fatal(st)
	}
	s := rec.waitSession(t, 0)
	payload := bytes.Repeat([]byte{0xff, 0xfb, 0x90, 0x00}, 5000)
	c.Write(payload)
	finish(t, c, s)
	if !bytes.Equal(s.Data(), payload) {
		t.Error("mp3 bytes differ")
	}
	if s.Info.ContentType != "audio/mpeg" {
		t.Errorf("content type %q", s.Info.ContentType)
	}
}

func TestFixtureSplitIntoTinyWrites(t *testing.T) {
	rec := &recorder{}
	ts := startServer(t, Options{Consumers: []stream.Consumer{rec}})
	data := fixture(t)
	c, _, _ := connect(t, ts.Addr, sourceHead("/live", ""))
	s := rec.waitSession(t, 0)
	r := rand.New(rand.NewSource(7))
	rest := data
	for len(rest) > 0 {
		n := min(1+r.Intn(900), len(rest))
		c.Write(rest[:n])
		rest = rest[n:]
	}
	finish(t, c, s)
	if !bytes.Equal(s.Data(), data) {
		t.Error("bytes differ")
	}
	checkTracks(t, s.Tracks())
}

// ---- lifecycle ----

func TestIdleTimeoutDropsSilentSource(t *testing.T) {
	rec := &recorder{}
	ts := startServer(t, Options{Consumers: []stream.Consumer{rec}, IdleTimeout: 150 * time.Millisecond})
	c, _, _ := connect(t, ts.Addr, sourceHead("/live", ""))
	s := rec.waitSession(t, 0)
	c.Write([]byte("some bytes"))
	waitFor(t, "OnEnd from idle timeout", func() bool { return len(s.Ends()) > 0 })
	if err := s.Ends()[0]; !errors.Is(err, ErrIdleTimeout) {
		t.Errorf("OnEnd err = %v, want ErrIdleTimeout", err)
	}
	if string(s.Data()) != "some bytes" {
		t.Errorf("data = %q", s.Data())
	}
	// connection closed: reading yields EOF
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Read(make([]byte, 1)); err == nil {
		t.Error("expected closed connection")
	}
	// mount is free again
	waitFor(t, "ts.Health disconnected", func() bool {
		return !ts.Health().Detail.(Detail).Mounts["/live"].Connected
	})
}

func TestIdleTimeoutResetByTraffic(t *testing.T) {
	rec := &recorder{}
	ts := startServer(t, Options{Consumers: []stream.Consumer{rec}, IdleTimeout: 300 * time.Millisecond})
	c, _, _ := connect(t, ts.Addr, sourceHead("/live", ""))
	s := rec.waitSession(t, 0)
	for i := 0; i < 8; i++ {
		c.Write([]byte("x"))
		time.Sleep(100 * time.Millisecond)
	}
	if len(s.Ends()) != 0 {
		t.Errorf("session dropped despite steady traffic: %v", s.Ends())
	}
}

func TestContextCancelClosesEverything(t *testing.T) {
	rec := &recorder{}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := New(Options{
		Consumers: []stream.Consumer{rec},
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Mounts:    []config.Mount{{Path: "/a", OnConflict: "reject"}, {Path: "/b", OnConflict: "reject"}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, ln) }()
	addr := ln.Addr().String()

	ca, _, _ := connect(t, addr, sourceHead("/a", ""))
	cb, _, _ := connect(t, addr, sourceHead("/b", ""))
	// an idle half-open connection that never sent a head
	cidle := dial(t, addr)
	ca.Write([]byte("1"))
	cb.Write([]byte("2"))
	waitFor(t, "sessions", func() bool { return len(rec.Sessions()) == 2 })

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Serve = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return")
	}
	for _, s := range rec.Sessions() {
		if n := len(s.Ends()); n != 1 {
			t.Errorf("session %s: OnEnd called %d times", s.Info.Mount, n)
		} else if !errors.Is(s.Ends()[0], ErrServerClosed) {
			t.Errorf("session %s: OnEnd err = %v, want ErrServerClosed", s.Info.Mount, s.Ends()[0])
		}
	}
	for _, c := range []net.Conn{ca, cb, cidle} {
		c.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := c.Read(make([]byte, 1)); err == nil {
			t.Error("connection still open after shutdown")
		} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
			t.Error("connection not closed after shutdown")
		}
	}
	if _, err := net.DialTimeout("tcp", addr, 500*time.Millisecond); err == nil {
		t.Error("listener still accepting")
	}
}

// ---- panicking handlers ----

type panicHandler struct{}

func (panicHandler) OnSegment(ogg.Segment)        { panic("boom segment") }
func (panicHandler) OnTrack(ogg.Track, time.Time) { panic("boom track") }
func (panicHandler) OnEnd(time.Time, error)       { panic("boom end") }

func TestPanickingHandlerIsolated(t *testing.T) {
	rec1, rec2 := &recorder{}, &recorder{}
	panicking := stream.ConsumerFunc(func(stream.SessionInfo) stream.Handler { return panicHandler{} })
	startPanics := stream.ConsumerFunc(func(stream.SessionInfo) stream.Handler { panic("boom start") })
	nilHandler := stream.ConsumerFunc(func(stream.SessionInfo) stream.Handler { return nil })
	ts := startServer(t, Options{Consumers: []stream.Consumer{rec1, panicking, startPanics, nilHandler, nil, rec2}})
	data := fixture(t)

	for round := 0; round < 2; round++ { // server survives and handles a second session
		c, st, _ := connect(t, ts.Addr, sourceHead("/live", ""))
		if st != "HTTP/1.0 200 OK" {
			t.Fatal(st)
		}
		for _, rec := range []*recorder{rec1, rec2} {
			s := rec.waitSession(t, round)
			if rec == rec1 {
				c.Write(data)
				finish(t, c, s)
			}
			waitFor(t, "OnEnd", func() bool { return len(s.Ends()) > 0 })
			if !bytes.Equal(s.Data(), data) {
				t.Errorf("round %d: consumer data differs (%d vs %d)", round, len(s.Data()), len(data))
			}
			checkTracks(t, s.Tracks())
			if n := len(s.Ends()); n != 1 {
				t.Errorf("OnEnd %d times", n)
			}
		}
		waitFor(t, "mount free", func() bool { return !ts.Health().Detail.(Detail).Mounts["/live"].Connected })
	}
}

// ---- health ----

func TestHealth(t *testing.T) {
	rec := &recorder{}
	ts := startServer(t, Options{
		Consumers: []stream.Consumer{rec},
		Mounts:    []config.Mount{{Path: "/live", OnConflict: "reject"}, {Path: "/b", OnConflict: "reject"}},
	})
	st := ts.Health()
	if !st.OK {
		t.Error("health not OK without source")
	}
	if fmt.Sprint(st.Warnings) != "[no source on /b no source on /live]" {
		t.Errorf("warnings = %v", st.Warnings)
	}
	if m := st.Detail.(Detail).Mounts["/live"]; m.Connected {
		t.Error("connected before any source")
	}

	data := fixture(t)
	c, _, _ := connect(t, ts.Addr, sourceHead("/live", "", "ice-name: DJ Health"))
	s := rec.waitSession(t, 0)
	c.Write(data)
	waitFor(t, "all tracks counted", func() bool {
		return ts.Health().Detail.(Detail).Mounts["/live"].TracksDetected == len(wantTracks)
	})
	st = ts.Health()
	m := st.Detail.(Detail).Mounts["/live"]
	if !st.OK || !m.Connected || m.IceName != "DJ Health" || m.SessionID != s.Info.ID ||
		m.RemoteAddr == "" || m.StartedAt == nil || m.LastArtist != wantTracks[4][0] || m.LastTitle != wantTracks[4][1] ||
		m.SessionsTotal != 1 {
		t.Errorf("mount status = %+v", m)
	}
	waitFor(t, "bytes received", func() bool {
		return ts.Health().Detail.(Detail).Mounts["/live"].BytesReceived == int64(len(data))
	})
	if len(st.Warnings) != 1 || st.Warnings[0] != "no source on /b" {
		t.Errorf("warnings = %v", st.Warnings)
	}
	finish(t, c, s)
	waitFor(t, "disconnected", func() bool { return !ts.Health().Detail.(Detail).Mounts["/live"].Connected })
	if m := ts.Health().Detail.(Detail).Mounts["/live"]; m.SessionsTotal != 1 {
		t.Errorf("sessions_total = %d", m.SessionsTotal)
	}
}
