package simulate

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cschmittiey/proktor/internal/clock"
	"github.com/cschmittiey/proktor/internal/config"
	"github.com/cschmittiey/proktor/internal/ingest"
	"github.com/cschmittiey/proktor/internal/ogg"
	"github.com/cschmittiey/proktor/internal/stream"
)

const fixturePath = "../../testdata/chained.ogg"

type rec struct {
	mu       sync.Mutex
	sessions []*recSession
}
type recSession struct {
	rec    *rec
	Info   stream.SessionInfo
	data   bytes.Buffer
	tracks []ogg.Track
	ends   []error
}

func (r *rec) StartSession(info stream.SessionInfo) stream.Handler {
	s := &recSession{rec: r, Info: info}
	r.mu.Lock()
	r.sessions = append(r.sessions, s)
	r.mu.Unlock()
	return s
}
func (s *recSession) OnSegment(seg ogg.Segment) {
	s.rec.mu.Lock()
	s.data.Write(seg.Data)
	s.rec.mu.Unlock()
}
func (s *recSession) OnTrack(t ogg.Track, _ time.Time) {
	s.rec.mu.Lock()
	s.tracks = append(s.tracks, t)
	s.rec.mu.Unlock()
}
func (s *recSession) OnEnd(_ time.Time, err error) {
	s.rec.mu.Lock()
	s.ends = append(s.ends, err)
	s.rec.mu.Unlock()
}
func (r *rec) snapshot() (n int, data []byte, tracks []ogg.Track, ends int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n = len(r.sessions)
	for _, s := range r.sessions {
		data = append(data, s.data.Bytes()...)
		tracks = append(tracks, s.tracks...)
		ends += len(s.ends)
	}
	return
}
func (r *rec) first() *recSession {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.sessions) == 0 {
		return nil
	}
	return r.sessions[0]
}

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

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func startIngest(t *testing.T, mounts []config.Mount, c stream.Consumer) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := ingest.New(ingest.Options{Mounts: mounts, Consumers: []stream.Consumer{c}, Logger: quiet})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("ingest Serve did not return")
		}
	})
	return ln.Addr().String()
}

func liveMount(pw string) []config.Mount {
	return []config.Mount{{Path: "/live", Password: pw, OnConflict: "reject"}}
}

func fixture(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func checkFiveTracks(t *testing.T, tr []ogg.Track) {
	t.Helper()
	want := []string{"New Order - Blue Monday", "Röyksopp - Eple", "坂本龍一 - Merry Christmas Mr. Lawrence",
		"Aphex Twin - Xtal", "DJ Überlong Name feat. Ünïcödé - Track with = equals sign"}
	if len(tr) != len(want) {
		t.Fatalf("got %d tracks, want %d: %+v", len(tr), len(want), tr)
	}
	for i, w := range want {
		if got := tr[i].Artist + " - " + tr[i].Title; got != w {
			t.Errorf("track %d = %q, want %q", i, got, w)
		}
	}
}

func TestRunSpeedZeroDeliversEverything(t *testing.T) {
	for _, proto := range []string{"source", "put", ""} {
		t.Run("protocol="+proto, func(t *testing.T) {
			r := &rec{}
			addr := startIngest(t, liveMount("pw"), r)
			err := Run(context.Background(), Options{
				File: fixturePath, Addr: addr, Mount: "/live", Password: "pw",
				Protocol: proto, IceName: "Simulated DJ", Speed: 0, Logger: quiet,
			})
			if err != nil {
				t.Fatal(err)
			}
			waitFor(t, "session end", func() bool { _, _, _, e := r.snapshot(); return e == 1 })
			n, data, tracks, _ := r.snapshot()
			if n != 1 {
				t.Errorf("%d sessions", n)
			}
			if want := fixture(t); !bytes.Equal(data, want) {
				t.Errorf("delivered %d bytes, file is %d", len(data), len(want))
			}
			checkFiveTracks(t, tracks)
			s := r.first()
			if s.Info.IceName != "Simulated DJ" || s.Info.ContentType != "application/ogg" || s.Info.Mount != "/live" {
				t.Errorf("session info = %+v", s.Info)
			}
		})
	}
}

func TestRunBadPassword(t *testing.T) {
	r := &rec{}
	addr := startIngest(t, liveMount("right"), r)
	err := Run(context.Background(), Options{File: fixturePath, Addr: addr, Mount: "/live", Password: "wrong", Logger: quiet})
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("err = %v, want mention of 401", err)
	}
	if n, _, _, _ := r.snapshot(); n != 0 {
		t.Errorf("%d sessions after auth failure", n)
	}
}

func TestRunUnknownMountAndBadOptions(t *testing.T) {
	addr := startIngest(t, liveMount(""), &rec{})
	err := Run(context.Background(), Options{File: fixturePath, Addr: addr, Mount: "/nope", Logger: quiet})
	if err == nil || !strings.Contains(err.Error(), "404") {
		t.Errorf("unknown mount err = %v", err)
	}
	if err := Run(context.Background(), Options{File: fixturePath, Addr: addr, Protocol: "ftp", Logger: quiet}); err == nil {
		t.Error("want error for bad protocol")
	}
	if err := Run(context.Background(), Options{File: "/nonexistent.ogg", Addr: addr, Logger: quiet}); err == nil {
		t.Error("want error for missing file")
	}
	if err := Run(context.Background(), Options{File: fixturePath, Addr: "127.0.0.1:1", Logger: quiet}); err == nil {
		t.Error("want error when nothing is listening")
	}
}

// paced runs Run on a fake clock, advancing it whenever Run is waiting, and
// returns the total fake time that had to elapse.
func paced(t *testing.T, speed float64, r *rec, addr string, beforeFirstAdvance func()) time.Duration {
	t.Helper()
	clk := clock.NewFake(time.Unix(1_700_000_000, 0))
	done := make(chan error, 1)
	go func() {
		done <- Run(context.Background(), Options{File: fixturePath, Addr: addr, Mount: "/live",
			Speed: speed, Clock: clk, Logger: quiet})
	}()
	start := clk.Now()
	if beforeFirstAdvance != nil {
		waitFor(t, "Run to block on the clock", func() bool { return clk.Pending() > 0 })
		beforeFirstAdvance()
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			return clk.Now().Sub(start)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("paced Run did not finish")
		}
		if clk.Pending() > 0 {
			clk.Advance(50 * time.Millisecond)
		} else {
			time.Sleep(200 * time.Microsecond)
		}
	}
}

func TestRunPacesByAudioTime(t *testing.T) {
	file := fixture(t)
	r1 := &rec{}
	addr1 := startIngest(t, liveMount(""), r1)
	elapsed1 := paced(t, 1, r1, addr1, func() {
		// Blocked waiting for the clock: the whole file must not have been sent.
		time.Sleep(20 * time.Millisecond)
		_, data, _, _ := r1.snapshot()
		if len(data) >= len(file) {
			t.Errorf("entire file sent before any time passed (%d bytes)", len(data))
		}
	})
	if elapsed1 < time.Second {
		t.Fatalf("1x playback took only %v of audio time", elapsed1)
	}
	waitFor(t, "session end", func() bool { _, _, _, e := r1.snapshot(); return e == 1 })
	_, data, tracks, _ := r1.snapshot()
	if !bytes.Equal(data, file) {
		t.Error("paced replay changed bytes")
	}
	checkFiveTracks(t, tracks)

	// Chained streams restart granules; the running offset must make total
	// time cover all five tracks, i.e. be well above a single track's length.
	r2 := &rec{}
	addr2 := startIngest(t, liveMount(""), r2)
	elapsed2 := paced(t, 4, r2, addr2, nil)
	ratio := float64(elapsed1) / float64(elapsed2)
	if ratio < 3.3 || ratio > 4.7 {
		t.Errorf("4x speed: elapsed %v vs %v at 1x (ratio %.2f, want ~4)", elapsed2, elapsed1, ratio)
	}
	t.Logf("audio duration ~%v", elapsed1)
}

func TestRunLoopSingleConnectionThenCancel(t *testing.T) {
	r := &rec{}
	addr := startIngest(t, liveMount(""), r)
	file := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options{File: fixturePath, Addr: addr, Mount: "/live", Speed: 0, Loop: true, Logger: quiet})
	}()
	waitFor(t, "two loops of data", func() bool {
		_, data, tracks, _ := r.snapshot()
		return len(data) >= 2*len(file) && len(tracks) >= 10
	})
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run after cancel = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	waitFor(t, "session end", func() bool { _, _, _, e := r.snapshot(); return e == 1 })
	n, data, _, _ := r.snapshot()
	if n != 1 {
		t.Errorf("loop used %d connections, want 1", n)
	}
	if !bytes.HasPrefix(data, file) || !bytes.HasPrefix(data[len(file):], file) {
		t.Error("looped data is not the file repeated")
	}
}

func TestRunCancelWhilePaced(t *testing.T) {
	r := &rec{}
	addr := startIngest(t, liveMount(""), r)
	clk := clock.NewFake(time.Unix(1_700_000_000, 0))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options{File: fixturePath, Addr: addr, Mount: "/live", Speed: 1, Clock: clk, Logger: quiet})
	}()
	waitFor(t, "Run blocked on clock", func() bool { return clk.Pending() > 0 })
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestRunNoticesServerHangup(t *testing.T) {
	r := &rec{}
	// takeover mount: a second source kicks the simulator off while it is waiting on the clock.
	addr := startIngest(t, []config.Mount{{Path: "/live", OnConflict: "takeover"}}, r)
	clk := clock.NewFake(time.Unix(1_700_000_000, 0))
	done := make(chan error, 1)
	go func() {
		done <- Run(context.Background(), Options{File: fixturePath, Addr: addr, Mount: "/live", Speed: 1, Clock: clk, Logger: quiet})
	}()
	waitFor(t, "Run blocked on clock", func() bool { return clk.Pending() > 0 })
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	io.WriteString(c, "SOURCE /live HTTP/1.0\r\n\r\n")
	select {
	case err := <-done:
		if err == nil {
			t.Error("Run returned nil after the server hung up on it")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not notice the server hanging up")
	}
}
