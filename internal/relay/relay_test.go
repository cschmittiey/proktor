package relay

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cschmittiey/proktor/internal/clock"
	"github.com/cschmittiey/proktor/internal/config"
	"github.com/cschmittiey/proktor/internal/stream"
)

var t0 = time.Date(2026, 10, 13, 22, 0, 0, 0, time.UTC)

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// settle gives the relay a moment to (wrongly) do something; used only to
// assert that something does NOT happen.
func settle() { time.Sleep(60 * time.Millisecond) }

func newRelay(t *testing.T, clk clock.Clock, opts Options, ups ...config.Upstream) *Relay {
	t.Helper()
	opts.Clock = clk
	if opts.HandshakeTimeout == 0 {
		opts.HandshakeTimeout = 3 * time.Second
	}
	r := New(ups, opts)
	t.Cleanup(func() { r.Close() })
	return r
}

func ups(srv *fakeServer, mod func(*config.Upstream)) config.Upstream {
	u := config.Upstream{Name: "up", URL: srv.url(), Username: "dj", Password: "pw", Mount: "/radio"}
	if mod != nil {
		mod(&u)
	}
	return u
}

func status(r *Relay, i int) UpstreamStatus {
	return r.Health().Detail.([]UpstreamStatus)[i]
}

func waitState(t *testing.T, r *Relay, want string) {
	t.Helper()
	waitFor(t, "state "+want, func() bool { return status(r, 0).State == want })
}

func start(t *testing.T, r *Relay, id, mount string) stream.Handler {
	t.Helper()
	h := r.StartSession(stream.SessionInfo{ID: id, Mount: mount, StartedAt: t0})
	if h == nil {
		t.Fatalf("StartSession(%s %s) = nil", id, mount)
	}
	return h
}

func waitBytes(t *testing.T, c *fakeConn, want []byte) {
	t.Helper()
	waitFor(t, "upstream bytes", func() bool { return len(c.bytes()) >= len(want) })
	if got := c.bytes(); !bytes.Equal(got, want) {
		t.Fatalf("upstream received %d bytes, want %d (verbatim)\n got pages: %v\nwant pages: %v",
			len(got), len(want), describe(t, got), describe(t, want))
	}
}

func describe(t *testing.T, b []byte) []string {
	var out []string
	defer func() { recover() }()
	for _, p := range splitPagesNoFail(b) {
		out = append(out, string(rune('A'+p.fill%26)))
	}
	return out
}

func splitPagesNoFail(b []byte) []pageInfo {
	var out []pageInfo
	for len(b) >= 27 && string(b[:4]) == "OggS" {
		n := int(b[26])
		if len(b) < 27+n {
			break
		}
		size := 27 + n
		for _, x := range b[27 : 27+n] {
			size += int(x)
		}
		if len(b) < size {
			break
		}
		out = append(out, pageInfo{fill: b[27+n], raw: b[:size]})
		b = b[size:]
	}
	return out
}

func TestRequestFormats(t *testing.T) {
	basic := "Basic " + base64.StdEncoding.EncodeToString([]byte("dj:pw"))
	uip := "Basic " + base64.StdEncoding.EncodeToString([]byte("source:dj:pw"))
	cases := []struct {
		name, proto, auth string
		wantLine          string
		wantAuth          string
		expect            bool
		status            string
	}{
		{"put-basic", "put", "basic", "PUT /radio HTTP/1.1", basic, true, "HTTP/1.1 100 Continue"},
		{"put-200", "", "", "PUT /radio HTTP/1.1", basic, true, "HTTP/1.1 200 OK"},
		{"source-basic", "source", "basic", "SOURCE /radio HTTP/1.0", basic, false, "HTTP/1.0 200 OK"},
		{"put-uip", "put", "user_in_password", "PUT /radio HTTP/1.1", uip, true, "HTTP/1.1 100 Continue"},
		{"source-uip", "source", "user_in_password", "SOURCE /radio HTTP/1.0", uip, false, "HTTP/1.0 200 OK"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newFakeServer(t, func(int) behavior { return behavior{status: tc.status} })
			r := newRelay(t, clock.NewFake(t0), Options{UserAgent: "proktor-test"},
				ups(srv, func(u *config.Upstream) {
					u.Protocol, u.AuthStyle = tc.proto, tc.auth
					u.ContentType = "application/ogg"
				}))
			h := r.StartSession(stream.SessionInfo{
				ID: "s1", Mount: "/live",
				IceName: "DJ Example", IceDescription: "801 night", IceGenre: "techno", IceURL: "https://example.invalid/",
			})
			waitState(t, r, "streaming")
			bos, a1 := mkPage(1, 0, true, true, 40, 1), mkPage(1, 1, false, false, 60, 2)
			h.OnSegment(bos)
			h.OnSegment(a1)
			waitBytes(t, srv.conn(0), concat(bos, a1))

			c := srv.conn(0)
			if c.reqLine != tc.wantLine {
				t.Errorf("request line = %q, want %q", c.reqLine, tc.wantLine)
			}
			hd := c.header
			checks := map[string]string{
				"Authorization":   tc.wantAuth,
				"Content-Type":    "application/ogg",
				"User-Agent":      "proktor-test",
				"Ice-Public":      "0",
				"Ice-Name":        "DJ Example",
				"Ice-Description": "801 night",
				"Ice-Genre":       "techno",
				"Ice-Url":         "https://example.invalid/",
				"Host":            srv.addr(),
			}
			for k, v := range checks {
				if got := hd.Get(k); got != v {
					t.Errorf("header %s = %q, want %q", k, got, v)
				}
			}
			if got := hd.Get("Expect"); (got == "100-continue") != tc.expect {
				t.Errorf("Expect header = %q, want present=%v", got, tc.expect)
			}
		})
	}
}

func TestIceHeadersOmittedWhenEmptyAndSanitized(t *testing.T) {
	srv := newFakeServer(t, nil)
	r := newRelay(t, clock.NewFake(t0), Options{}, ups(srv, nil))
	r.StartSession(stream.SessionInfo{ID: "s", Mount: "/live", IceName: "evil\r\nX-Injected: 1"})
	waitFor(t, "conn", func() bool { return srv.numConns() == 1 })
	h := srv.conn(0).header
	if _, ok := h["Ice-Genre"]; ok {
		t.Error("empty ice-genre should not be sent")
	}
	if h.Get("X-Injected") != "" {
		t.Error("header injection through ice-name")
	}
	if !strings.Contains(h.Get("Ice-Name"), "X-Injected") {
		t.Errorf("sanitized name lost: %q", h.Get("Ice-Name"))
	}
}

func TestForwardsPagesVerbatimSkipsGarbage(t *testing.T) {
	srv := newFakeServer(t, nil)
	r := newRelay(t, clock.NewFake(t0), Options{}, ups(srv, nil))
	h := start(t, r, "s1", "/live")
	waitState(t, r, "streaming")

	bos := mkPage(7, 0, true, true, 30, 1)
	hdr := mkPage(7, 1, false, true, 50, 2)
	a1 := mkPage(7, 2, false, false, 70, 3)
	a2 := mkPage(7, 3, false, false, 90, 4)
	h.OnSegment(garbage(13))
	h.OnSegment(bos)
	h.OnSegment(garbage(5))
	h.OnSegment(hdr)
	h.OnSegment(a1)
	h.OnSegment(garbage(300))
	h.OnSegment(a2)
	want := concat(bos, hdr, a1, a2)
	waitBytes(t, srv.conn(0), want)
	if got := status(r, 0).BytesSent; got != int64(len(want)) {
		t.Errorf("bytes_sent = %d, want %d", got, len(want))
	}
	if n := len(splitPages(t, srv.conn(0).bytes())); n != 4 {
		t.Errorf("pages = %d", n)
	}
}

func TestReconnectReplaysHeadersThenPageAlignedContinuation(t *testing.T) {
	srv := newFakeServer(t, func(i int) behavior {
		if i == 0 {
			return behavior{dropAfter: 100}
		}
		return behavior{}
	})
	clk := clock.NewFake(t0)
	r := newRelay(t, clk, Options{}, ups(srv, nil))
	h := start(t, r, "s1", "/live")
	waitState(t, r, "streaming")

	bos := mkPage(1, 0, true, true, 40, 'a'-'A'+1)
	hdr := mkPage(1, 1, false, true, 80, 2)
	h.OnSegment(bos)
	h.OnSegment(hdr)
	for i := uint32(2); i < 6; i++ {
		h.OnSegment(mkPage(1, i, false, false, 100, 3))
	}
	waitState(t, r, "backoff")
	if st := status(r, 0); st.Reconnects != 1 || st.LastError == "" {
		t.Errorf("status after drop = %+v", st)
	}
	if r.Health().OK {
		t.Error("health should not be OK while in backoff")
	}
	// Pages produced while the upstream is down are not queued.
	missed := mkPage(1, 6, false, false, 100, 9)
	h.OnSegment(missed)

	clk.Advance(time.Second)
	waitFor(t, "second connection streaming", func() bool {
		return srv.numConns() == 2 && status(r, 0).State == "streaming"
	})
	b1, b2 := mkPage(1, 7, false, false, 100, 5), mkPage(1, 8, false, false, 100, 6)
	h.OnSegment(b1)
	h.OnSegment(b2)
	waitBytes(t, srv.conn(1), concat(bos, hdr, b1, b2))

	pages := splitPages(t, srv.conn(1).bytes())
	if !pages[0].bos || pages[1].seq != 1 || pages[2].seq != 7 {
		t.Errorf("unexpected page order: %+v", pages)
	}
	if st := status(r, 0); !r.Health().OK || st.ConnectedSince == nil {
		t.Errorf("status after reconnect = %+v", st)
	}
}

func TestMidStreamBOSResetsHeaderCache(t *testing.T) {
	srv := newFakeServer(t, func(i int) behavior {
		if i == 0 {
			return behavior{dropAfter: 1}
		}
		return behavior{}
	})
	clk := clock.NewFake(t0)
	r := newRelay(t, clk, Options{}, ups(srv, nil))
	h := start(t, r, "s1", "/live")
	waitState(t, r, "streaming")
	// The second logical stream replaces the first one's headers.
	h.OnSegment(mkPage(1, 0, true, true, 30, 1))
	h.OnSegment(mkPage(1, 1, false, true, 30, 2))
	h.OnSegment(mkPage(1, 2, false, false, 30, 3))
	bos2, hdr2 := mkPage(2, 0, true, true, 30, 4), mkPage(2, 1, false, true, 30, 5)
	h.OnSegment(bos2)
	h.OnSegment(hdr2)
	waitState(t, r, "backoff")
	clk.Advance(time.Second)
	waitFor(t, "reconnect", func() bool { return status(r, 0).State == "streaming" && srv.numConns() == 2 })
	tail := mkPage(2, 2, false, false, 30, 6)
	h.OnSegment(tail)
	waitBytes(t, srv.conn(1), concat(bos2, hdr2, tail))
}

func TestNoHeaderCacheWaitsForBOS(t *testing.T) {
	srv := newFakeServer(t, nil)
	r := newRelay(t, clock.NewFake(t0), Options{}, ups(srv, nil))
	h := start(t, r, "s1", "/live")
	waitState(t, r, "streaming")

	h.OnSegment(mkPage(1, 5, false, false, 50, 1))
	h.OnSegment(mkPage(1, 6, false, false, 50, 2))
	settle()
	if n := len(srv.conn(0).bytes()); n != 0 {
		t.Fatalf("upstream got %d bytes before any BOS", n)
	}
	bos, a := mkPage(2, 0, true, true, 30, 3), mkPage(2, 1, false, false, 50, 4)
	h.OnSegment(bos)
	h.OnSegment(a)
	waitBytes(t, srv.conn(0), concat(bos, a))
}

func TestNoSessionNoConnection(t *testing.T) {
	srv := newFakeServer(t, nil)
	r := newRelay(t, clock.NewFake(t0), Options{},
		ups(srv, func(u *config.Upstream) { u.SourceMount = "/live" }))
	settle()
	if srv.numConns() != 0 || status(r, 0).State != "idle" {
		t.Fatalf("connected without a session: conns=%d state=%s", srv.numConns(), status(r, 0).State)
	}
	if h := r.StartSession(stream.SessionInfo{ID: "x", Mount: "/other"}); h != nil {
		t.Fatal("session on a non-matching mount should be ignored")
	}
	settle()
	if srv.numConns() != 0 {
		t.Fatal("connected for a non-matching mount")
	}
	h := start(t, r, "s1", "live") // mount without slash still matches
	waitFor(t, "connection", func() bool { return srv.numConns() == 1 })
	h.OnEnd(t0, nil)
	waitFor(t, "disconnect", func() bool { return srv.conn(0).isClosed() })
	waitState(t, r, "idle")
}

func TestDisabledUpstreamsSkipped(t *testing.T) {
	srv := newFakeServer(t, nil)
	r := newRelay(t, clock.NewFake(t0), Options{}, ups(srv, func(u *config.Upstream) { u.Disabled = true }))
	if r.StartSession(stream.SessionInfo{ID: "s", Mount: "/live"}) != nil {
		t.Error("disabled upstream should not see sessions")
	}
	if d := r.Health().Detail.([]UpstreamStatus); len(d) != 0 {
		t.Errorf("detail = %+v", d)
	}
	settle()
	if srv.numConns() != 0 {
		t.Error("disabled upstream connected")
	}
}

func TestAuthRejectedBacksOffExponentially(t *testing.T) {
	srv := newFakeServer(t, func(i int) behavior {
		if i < 2 {
			return behavior{status: "HTTP/1.1 401 Unauthorized"}
		}
		return behavior{}
	})
	clk := clock.NewFake(t0)
	r := newRelay(t, clk, Options{}, ups(srv, nil))
	h := start(t, r, "s1", "/live")

	waitFor(t, "first rejection", func() bool {
		st := status(r, 0)
		return st.State == "backoff" && st.Reconnects == 1
	})
	st := status(r, 0)
	if !strings.Contains(st.LastError, "401") {
		t.Errorf("last_error = %q", st.LastError)
	}
	hs := r.Health()
	if hs.OK || len(hs.Warnings) != 1 || !strings.Contains(hs.Warnings[0], "401") {
		t.Errorf("health = %+v", hs)
	}
	clk.Advance(time.Second)
	waitFor(t, "second rejection", func() bool {
		st := status(r, 0)
		return srv.numConns() == 2 && st.State == "backoff" && st.Reconnects == 2
	})
	// The second delay is 2s: one second is not enough.
	clk.Advance(time.Second)
	settle()
	if srv.numConns() != 2 {
		t.Fatalf("reconnected too early (conns=%d)", srv.numConns())
	}
	clk.Advance(time.Second)
	waitState(t, r, "streaming")
	if srv.numConns() != 3 || !r.Health().OK {
		t.Fatalf("conns=%d health=%+v", srv.numConns(), r.Health())
	}
	bos := mkPage(1, 0, true, true, 30, 1)
	h.OnSegment(bos)
	waitBytes(t, srv.conn(2), concat(bos))
}

func TestBackoffResetsAfterStableConnection(t *testing.T) {
	srv := newFakeServer(t, func(i int) behavior {
		if i == 0 {
			return behavior{status: "HTTP/1.1 403 Forbidden"}
		}
		return behavior{}
	})
	clk := clock.NewFake(t0)
	r := newRelay(t, clk, Options{}, ups(srv, nil))
	start(t, r, "s1", "/live")
	waitFor(t, "rejection", func() bool { return status(r, 0).State == "backoff" })
	clk.Advance(time.Second)
	waitState(t, r, "streaming") // backoff is now 2s
	clk.Advance(40 * time.Second)
	srv.conn(1).conn.Close()
	waitFor(t, "second failure", func() bool {
		st := status(r, 0)
		return st.State == "backoff" && st.Reconnects == 2
	})
	// Stable for >30s, so the delay was reset to 1s rather than 2s.
	clk.Advance(time.Second)
	waitFor(t, "third connection", func() bool { return srv.numConns() == 3 })
}

func TestBackoffCapped(t *testing.T) {
	srv := newFakeServer(t, func(int) behavior { return behavior{status: "HTTP/1.1 503 Busy"} })
	clk := clock.NewFake(t0)
	r := newRelay(t, clk, Options{MinBackoff: time.Second, MaxBackoff: 4 * time.Second}, ups(srv, nil))
	start(t, r, "s1", "/live")
	for i, d := range []time.Duration{1, 2, 4, 4} {
		n := i + 1
		waitFor(t, "failure", func() bool {
			st := status(r, 0)
			return srv.numConns() == n && st.State == "backoff" && st.Reconnects == int64(n)
		})
		clk.Advance(d*time.Second - time.Millisecond)
		settle()
		if srv.numConns() != n {
			t.Fatalf("attempt %d: reconnected before %v", n, d*time.Second)
		}
		clk.Advance(time.Millisecond)
	}
}

func TestOverflowNeverBlocksAndReconnects(t *testing.T) {
	srv := newFakeServer(t, func(i int) behavior {
		if i == 0 {
			return behavior{noRead: true}
		}
		return behavior{}
	})
	clk := clock.NewFake(t0)
	r := newRelay(t, clk, Options{Dialer: smallBufDialer{}},
		ups(srv, func(u *config.Upstream) { u.BufferBytes = 32 << 10 }))
	h := start(t, r, "s1", "/live")
	waitState(t, r, "streaming")

	bos := mkPage(1, 0, true, true, 100, 1)
	h.OnSegment(bos)
	var worst time.Duration
	for i := uint32(1); i < 5000 && status(r, 0).Overflows == 0; i++ {
		p := mkPage(1, i, false, false, 4000, 2)
		begin := time.Now()
		h.OnSegment(p)
		if d := time.Since(begin); d > worst {
			worst = d
		}
	}
	if worst > 250*time.Millisecond {
		t.Fatalf("OnSegment blocked for %v", worst)
	}
	st := status(r, 0)
	if st.Overflows != 1 || st.DroppedBytes == 0 {
		t.Fatalf("overflow not counted: %+v", st)
	}
	waitState(t, r, "backoff")
	if !strings.Contains(status(r, 0).LastError, "overflow") {
		t.Errorf("last_error = %q", status(r, 0).LastError)
	}
	// Ingest keeps flowing while the upstream is down.
	for i := uint32(0); i < 50; i++ {
		h.OnSegment(mkPage(1, 6000+i, false, false, 4000, 3))
	}
	clk.Advance(time.Second)
	waitFor(t, "reconnect", func() bool { return srv.numConns() == 2 && status(r, 0).State == "streaming" })
	tail := mkPage(1, 7000, false, false, 100, 4)
	h.OnSegment(tail)
	waitBytes(t, srv.conn(1), concat(bos, tail)) // headers replayed, then a fresh page
}

type smallBufDialer struct{}

func (smallBufDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	c, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, network, addr)
	if err == nil {
		c.(*net.TCPConn).SetWriteBuffer(2048)
	}
	return c, err
}

func TestHoldKeepsConnectionAcrossHandoff(t *testing.T) {
	srv := newFakeServer(t, nil)
	clk := clock.NewFake(t0)
	r := newRelay(t, clk, Options{}, ups(srv, func(u *config.Upstream) { u.HoldSeconds = 5 }))

	a := start(t, r, "A", "/live")
	waitState(t, r, "streaming")
	abos, a1 := mkPage(1, 0, true, true, 40, 1), mkPage(1, 1, false, false, 60, 2)
	a.OnSegment(abos)
	a.OnSegment(a1)
	waitBytes(t, srv.conn(0), concat(abos, a1))

	a.OnEnd(t0, nil)
	if st := status(r, 0).State; st != "holding" {
		t.Fatalf("state after source end = %s, want holding", st)
	}
	clk.Advance(3 * time.Second)
	settle()
	if srv.conn(0).isClosed() || srv.numConns() != 1 {
		t.Fatal("connection dropped during hold")
	}

	b := start(t, r, "B", "/live")
	bbos, b1 := mkPage(2, 0, true, true, 40, 3), mkPage(2, 1, false, false, 60, 4)
	b.OnSegment(bbos)
	b.OnSegment(b1)
	waitBytes(t, srv.conn(0), concat(abos, a1, bbos, b1))

	// A's original hold deadline passes; B is streaming so nothing happens.
	clk.Advance(10 * time.Second)
	settle()
	if srv.conn(0).isClosed() || srv.numConns() != 1 || status(r, 0).State != "streaming" {
		t.Fatalf("stale hold timer closed the connection (state %s)", status(r, 0).State)
	}

	b.OnEnd(t0, nil)
	clk.Advance(4 * time.Second)
	settle()
	if srv.conn(0).isClosed() {
		t.Fatal("closed before hold expired")
	}
	clk.Advance(time.Second)
	waitFor(t, "hold expiry disconnect", func() bool { return srv.conn(0).isClosed() })
	waitState(t, r, "idle")
	if srv.numConns() != 1 {
		t.Fatalf("conns = %d", srv.numConns())
	}

	// A later session starts a fresh connection.
	start(t, r, "C", "/live")
	waitFor(t, "new connection", func() bool { return srv.numConns() == 2 })
}

func TestNoHoldDisconnectsImmediately(t *testing.T) {
	srv := newFakeServer(t, nil)
	r := newRelay(t, clock.NewFake(t0), Options{}, ups(srv, nil))
	h := start(t, r, "A", "/live")
	waitState(t, r, "streaming")
	bos := mkPage(1, 0, true, true, 40, 1)
	h.OnSegment(bos)
	waitBytes(t, srv.conn(0), concat(bos))
	h.OnEnd(t0, nil)
	waitFor(t, "disconnect", func() bool { return srv.conn(0).isClosed() })
	waitState(t, r, "idle")
	h.OnEnd(t0, nil) // exactly-once is enforced, a repeat is harmless
}

func TestLatestSessionWinsAndFallsBack(t *testing.T) {
	srv := newFakeServer(t, nil)
	r := newRelay(t, clock.NewFake(t0), Options{}, ups(srv, nil))
	a := start(t, r, "A", "/a")
	waitState(t, r, "streaming")
	abos := mkPage(1, 0, true, true, 30, 1)
	a.OnSegment(abos)
	waitBytes(t, srv.conn(0), concat(abos))

	b := start(t, r, "B", "/b")
	bbos, b1 := mkPage(2, 0, true, true, 30, 2), mkPage(2, 1, false, false, 30, 3)
	a.OnSegment(mkPage(1, 1, false, false, 30, 9)) // ignored: B is newer
	b.OnSegment(bbos)
	b.OnSegment(b1)
	waitBytes(t, srv.conn(0), concat(abos, bbos, b1))

	// B ends while A is still on air: fall back to A with its headers.
	b.OnEnd(t0, nil)
	a2 := mkPage(1, 2, false, false, 30, 4)
	a.OnSegment(a2)
	waitBytes(t, srv.conn(0), concat(abos, bbos, b1, abos, a2))
	if srv.conn(0).isClosed() || srv.numConns() != 1 {
		t.Fatal("connection should survive the fallback")
	}
	a.OnEnd(t0, nil)
	waitFor(t, "disconnect", func() bool { return srv.conn(0).isClosed() })
}

func TestTakeoverOnSameMountOldEndArrivesLate(t *testing.T) {
	srv := newFakeServer(t, nil)
	r := newRelay(t, clock.NewFake(t0), Options{}, ups(srv, nil))
	old := start(t, r, "old", "/live")
	waitState(t, r, "streaming")
	obos := mkPage(1, 0, true, true, 30, 1)
	old.OnSegment(obos)

	nw := start(t, r, "new", "/live") // takeover: begins before old.OnEnd
	nbos := mkPage(2, 0, true, true, 30, 2)
	nw.OnSegment(nbos)
	old.OnSegment(mkPage(1, 1, false, false, 30, 9)) // dying source, ignored
	old.OnEnd(t0, nil)
	n1 := mkPage(2, 1, false, false, 30, 3)
	nw.OnSegment(n1)
	waitBytes(t, srv.conn(0), concat(obos, nbos, n1))
	settle()
	if srv.conn(0).isClosed() || srv.numConns() != 1 {
		t.Fatal("old session's end tore down the new session's relay")
	}
}

func TestCloseIdempotent(t *testing.T) {
	srv := newFakeServer(t, nil)
	r := New([]config.Upstream{ups(srv, nil)}, Options{Clock: clock.NewFake(t0)})
	h := start(t, r, "A", "/live")
	waitState(t, r, "streaming")
	h.OnSegment(mkPage(1, 0, true, true, 30, 1))
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "conn closed", func() bool { return srv.conn(0).isClosed() })
	h.OnSegment(mkPage(1, 1, false, false, 30, 2)) // must not panic or block
	h.OnEnd(t0, nil)
	if r.StartSession(stream.SessionInfo{ID: "B", Mount: "/live"}) != nil {
		t.Error("StartSession after Close should return nil")
	}
	_ = r.Health()
}

func TestCloseWhileBackingOffAndIdle(t *testing.T) {
	srv := newFakeServer(t, func(int) behavior { return behavior{status: "HTTP/1.1 401 Unauthorized"} })
	idle := New([]config.Upstream{ups(srv, nil)}, Options{Clock: clock.NewFake(t0)})
	idle.Close()
	r := New([]config.Upstream{ups(srv, nil)}, Options{Clock: clock.NewFake(t0)})
	start(t, r, "A", "/live")
	waitState(t, r, "backoff")
	done := make(chan struct{})
	go func() { r.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close hung")
	}
}

func TestBadUpstreamURL(t *testing.T) {
	r := newRelay(t, clock.NewFake(t0), Options{}, config.Upstream{Name: "bad", URL: "ftp://x/"})
	if r.StartSession(stream.SessionInfo{ID: "s", Mount: "/live"}) != nil {
		t.Error("misconfigured upstream should ignore sessions")
	}
	hs := r.Health()
	if hs.OK || len(hs.Warnings) != 1 {
		t.Errorf("health = %+v", hs)
	}
}

func TestUpstreamClosesIdleConnectionDuringHold(t *testing.T) {
	srv := newFakeServer(t, nil)
	clk := clock.NewFake(t0)
	r := newRelay(t, clk, Options{}, ups(srv, func(u *config.Upstream) { u.HoldSeconds = 30 }))
	h := start(t, r, "A", "/live")
	waitState(t, r, "streaming")
	h.OnEnd(t0, nil)
	srv.conn(0).conn.Close() // e.g. Icecast source-timeout
	waitState(t, r, "idle")  // no pointless reconnect while merely holding
	settle()
	if srv.numConns() != 1 {
		t.Fatal("reconnected with no source active")
	}
	start(t, r, "B", "/live")
	waitFor(t, "fresh connection for the next session", func() bool { return srv.numConns() == 2 })
}

func TestHealthJSON(t *testing.T) {
	srv := newFakeServer(t, nil)
	r := newRelay(t, clock.NewFake(t0), Options{}, ups(srv, nil))
	b, err := json.Marshal(r.Health())
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{`"name":"up"`, `"state":"idle"`, `"bytes_sent":0`, `"reconnects":0`, `"overflows":0`} {
		if !strings.Contains(string(b), k) {
			t.Errorf("health JSON %s lacks %s", b, k)
		}
	}
	if strings.Contains(string(b), "pw") {
		t.Error("credentials leaked into health")
	}
}

func TestHTTPSUpstream(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, IsCA: true, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tln := tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}})
	srv := serveFake(t, tln, nil)

	r := newRelay(t, clock.NewFake(t0), Options{TLSConfig: &tls.Config{RootCAs: pool}},
		ups(srv, func(u *config.Upstream) { u.URL = "https://" + srv.addr() + "/" }))
	h := start(t, r, "A", "/live")
	waitState(t, r, "streaming")
	bos := mkPage(1, 0, true, true, 30, 1)
	h.OnSegment(bos)
	waitBytes(t, srv.conn(0), concat(bos))
}
