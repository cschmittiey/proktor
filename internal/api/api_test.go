package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cschmittiey/proktor/internal/clock"
	"github.com/cschmittiey/proktor/internal/health"
	"github.com/cschmittiey/proktor/internal/state"
)

var update = flag.Bool("update", false, "regenerate golden files")

const wait = 5 * time.Second

var t0 = time.Date(2026, 10, 13, 22, 14, 5, 0, time.UTC)

func ptr[T any](v T) *T { return &v }

// fakeBackend is a minimal Backend.
type fakeBackend struct {
	mu        sync.Mutex
	st        state.State
	tracks    []state.TracklistEntry
	subs      map[int]chan state.State
	nextSub   int
	cancelled chan struct{} // receives once per cancel call
	applied   []state.Command
	applyFn   func(state.Command) (state.State, error)
	subbed    chan struct{} // receives once per Subscribe
}

func newBackend(st state.State) *fakeBackend {
	return &fakeBackend{
		st:        st,
		subs:      map[int]chan state.State{},
		cancelled: make(chan struct{}, 16),
		subbed:    make(chan struct{}, 16),
	}
}

func (f *fakeBackend) Snapshot() state.State {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.st
}

func (f *fakeBackend) Subscribe() (<-chan state.State, func()) {
	f.mu.Lock()
	id := f.nextSub
	f.nextSub++
	ch := make(chan state.State, 64)
	f.subs[id] = ch
	f.mu.Unlock()
	f.subbed <- struct{}{}
	var once sync.Once
	return ch, func() {
		once.Do(func() {
			f.mu.Lock()
			delete(f.subs, id)
			f.mu.Unlock()
			close(ch)
			f.cancelled <- struct{}{}
		})
	}
}

func (f *fakeBackend) Apply(cmd state.Command) (state.State, error) {
	f.mu.Lock()
	f.applied = append(f.applied, cmd)
	fn := f.applyFn
	st := f.st
	f.mu.Unlock()
	if fn != nil {
		return fn(cmd)
	}
	return st, nil
}

func (f *fakeBackend) Tracklist() []state.TracklistEntry {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tracks
}

// publish sets the state and broadcasts it.
func (f *fakeBackend) publish(st state.State) {
	f.mu.Lock()
	f.st = st
	for _, ch := range f.subs {
		ch <- st
	}
	f.mu.Unlock()
}

func (f *fakeBackend) lastApplied(t *testing.T) state.Command {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.applied) == 0 {
		t.Fatal("Apply was not called")
	}
	return f.applied[len(f.applied)-1]
}

func trackState() state.State {
	return state.State{
		V:         state.SchemaVersion,
		Mode:      state.ModeTrack,
		Track:     &state.Track{Artist: "New Order", Title: "Blue Monday", Source: state.SourceTraktor, StartedAt: t0},
		Performer: ptr("dj example"),
		EventName: "801 something",
		History: []state.Track{
			{Artist: "Kraftwerk", Title: "Computer Love", Source: state.SourceTraktor, StartedAt: t0.Add(-5 * time.Minute)},
			{Artist: "Étienne de Crécy", Title: "Am I Wrong", Source: state.SourceManual, StartedAt: t0.Add(-11 * time.Minute)},
		},
		Pending:   nil,
		Seq:       42,
		UpdatedAt: t0,
	}
}

func newServer(t *testing.T, opts Options) (*httptest.Server, *fakeBackend) {
	t.Helper()
	b, _ := opts.Backend.(*fakeBackend)
	if b == nil {
		b = newBackend(trackState())
		opts.Backend = b
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	srv := httptest.NewServer(New(opts))
	t.Cleanup(srv.Close)
	return srv, b
}

func get(t *testing.T, url string) (*http.Response, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp, string(body)
}

func TestNowGolden(t *testing.T) {
	srv, _ := newServer(t, Options{})
	resp, body := get(t, srv.URL+"/now")
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("content-type %q", ct)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
		t.Errorf("cache-control %q", cc)
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, []byte(body), "", "  "); err != nil {
		t.Fatalf("response is not JSON: %v\n%s", err, body)
	}
	buf.WriteByte('\n')
	const golden = "testdata/now.golden.json"
	if *update {
		if err := os.WriteFile(golden, buf.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("read golden (run with -update): %v", err)
	}
	if !bytes.Equal(want, buf.Bytes()) {
		t.Errorf("GET /now differs from %s (run with -update to regenerate)\n--- got ---\n%s--- want ---\n%s", golden, buf.Bytes(), want)
	}
}

func TestNowEmptyHistoryIsArray(t *testing.T) {
	b := newBackend(state.State{V: 1, Mode: state.ModeIdle, Seq: 1, UpdatedAt: t0})
	srv, _ := newServer(t, Options{Backend: b})
	_, body := get(t, srv.URL+"/now")
	if !strings.Contains(body, `"history":[]`) {
		t.Errorf("history should be [], got %s", body)
	}
}

func TestNowTxt(t *testing.T) {
	cases := []struct {
		name string
		st   state.State
		want string
	}{
		{"track+performer", trackState(), "New Order - Blue Monday\ndj example\n"},
		{"track no performer", state.State{Mode: state.ModeTrack, Track: &state.Track{Artist: "A", Title: "T"}}, "A - T\n"},
		{"track artist only", state.State{Mode: state.ModeTrack, Track: &state.Track{Artist: "A"}}, "A\n"},
		{"track title only", state.State{Mode: state.ModeTrack, Track: &state.Track{Title: "T"}, Performer: ptr("p")}, "T\np\n"},
		{"live", state.State{Mode: state.ModeLive, Performer: ptr("synth wizard"), EventName: "ev"}, "live: synth wizard\n"},
		{"live no performer", state.State{Mode: state.ModeLive}, "live\n"},
		{"idle event", state.State{Mode: state.ModeIdle, EventName: "801 something"}, "801 something\n"},
		{"idle", state.State{Mode: state.ModeIdle}, "idle\n"},
		{"utf8 passthrough", state.State{Mode: state.ModeTrack, Track: &state.Track{Artist: "Sigur Rós", Title: "Hoppípolla"}}, "Sigur Rós - Hoppípolla\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv, _ := newServer(t, Options{Backend: newBackend(c.st)})
			resp, body := get(t, srv.URL+"/now.txt")
			if ct := resp.Header.Get("Content-Type"); ct != "text/plain; charset=utf-8" {
				t.Errorf("content-type %q", ct)
			}
			if body != c.want {
				t.Errorf("got %q want %q", body, c.want)
			}
		})
	}
}

// sseReader parses an SSE response body.
type sseEvent struct{ event, id, data string }

type sseStream struct {
	lines chan string
}

func openSSE(t *testing.T, ctx context.Context, url string) (*sseStream, *http.Response) {
	t.Helper()
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	s := &sseStream{lines: make(chan string, 256)}
	go func() {
		defer close(s.lines)
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			s.lines <- sc.Text()
		}
	}()
	t.Cleanup(func() { resp.Body.Close() })
	return s, resp
}

func (s *sseStream) line(t *testing.T) string {
	t.Helper()
	select {
	case l, ok := <-s.lines:
		if !ok {
			t.Fatal("stream ended")
		}
		return l
	case <-time.After(wait):
		t.Fatal("timed out waiting for SSE line")
	}
	return ""
}

// next returns the next event, or a ping as event "ping". Handles retry:.
func (s *sseStream) next(t *testing.T) sseEvent {
	t.Helper()
	var ev sseEvent
	for {
		l := s.line(t)
		switch {
		case l == "":
			if ev.event != "" {
				return ev
			}
		case strings.HasPrefix(l, ":"):
			if l == ":ping" {
				return sseEvent{event: "ping"}
			}
		case strings.HasPrefix(l, "retry: "):
			// ignored
		case strings.HasPrefix(l, "event: "):
			ev.event = l[len("event: "):]
		case strings.HasPrefix(l, "id: "):
			ev.id = l[len("id: "):]
		case strings.HasPrefix(l, "data: "):
			ev.data = l[len("data: "):]
		default:
			t.Fatalf("unexpected SSE line %q", l)
		}
	}
}

func (s *sseStream) nextState(t *testing.T) (state.State, sseEvent) {
	t.Helper()
	ev := s.next(t)
	if ev.event != "state" {
		t.Fatalf("expected state event, got %+v", ev)
	}
	var st state.State
	if err := json.Unmarshal([]byte(ev.data), &st); err != nil {
		t.Fatalf("bad state json %q: %v", ev.data, err)
	}
	return st, ev
}

func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(wait):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestSSE(t *testing.T) {
	clk := clock.NewFake(t0)
	b := newBackend(trackState())
	srv, _ := newServer(t, Options{Backend: b, Clock: clk, PingInterval: 15 * time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, resp := openSSE(t, ctx, srv.URL+"/events")

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("content-type %q", ct)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
		t.Errorf("cache-control %q", cc)
	}
	if h := resp.Header.Get("X-Accel-Buffering"); h != "no" {
		t.Errorf("X-Accel-Buffering %q", h)
	}
	if h := resp.Header.Get("Access-Control-Allow-Origin"); h != "*" {
		t.Errorf("CORS header %q", h)
	}

	// retry hint comes first.
	if l := s.line(t); l != "retry: 2000" {
		t.Fatalf("first line %q", l)
	}
	if l := s.line(t); l != "" {
		t.Fatalf("expected blank line, got %q", l)
	}

	// Full state on connect.
	first, ev := s.nextState(t)
	if first.Seq != 42 || first.Track == nil || first.Track.Title != "Blue Monday" {
		t.Errorf("initial state wrong: %+v", first)
	}
	if ev.id != "42" {
		t.Errorf("id %q", ev.id)
	}
	waitFor(t, b.subbed, "subscribe")

	// A stale duplicate (same seq as snapshot) is skipped, then each new
	// state arrives exactly once, in order.
	b.mu.Lock()
	for _, ch := range b.subs {
		ch <- first // duplicate of the snapshot
	}
	b.mu.Unlock()
	for seq := uint64(43); seq <= 45; seq++ {
		st := trackState()
		st.Seq = seq
		st.Track = &state.Track{Artist: "A", Title: fmt.Sprintf("T%d", seq), Source: "manual", StartedAt: t0}
		b.publish(st)
	}
	for seq := uint64(43); seq <= 45; seq++ {
		got, ev := s.nextState(t)
		if got.Seq != seq || got.Track.Title != fmt.Sprintf("T%d", seq) || ev.id != fmt.Sprint(seq) {
			t.Fatalf("event for seq %d: got %+v (id %s)", seq, got, ev.id)
		}
	}

	// Ping after the fake clock advances past the interval.
	clk.Advance(15 * time.Second)
	if ev := s.next(t); ev.event != "ping" {
		t.Fatalf("expected ping, got %+v", ev)
	}
	clk.Advance(15 * time.Second)
	if ev := s.next(t); ev.event != "ping" {
		t.Fatalf("expected second ping, got %+v", ev)
	}

	// Disconnect cancels the subscription.
	select {
	case <-b.cancelled:
		t.Fatal("subscription cancelled too early")
	default:
	}
	cancel()
	waitFor(t, b.cancelled, "subscription cancel after disconnect")
}

func TestSSEDataIsSingleLine(t *testing.T) {
	st := trackState()
	st.Track.Title = "multi\nline"
	srv, _ := newServer(t, Options{Backend: newBackend(st), Clock: clock.NewFake(t0)})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, _ := openSSE(t, ctx, srv.URL+"/events")
	got, _ := s.nextState(t)
	if got.Track.Title != "multi\nline" {
		t.Errorf("title %q", got.Track.Title)
	}
}

func TestTracks(t *testing.T) {
	b := newBackend(trackState())
	pub := t0.Add(30 * time.Second)
	b.tracks = []state.TracklistEntry{
		{Artist: "Kraftwerk", Title: "Computer Love", Source: "traktor", DetectedAt: time.Date(2026, 10, 13, 21, 5, 9, 0, time.UTC), PublishedAt: &pub},
		{Artist: "Skipped", Title: "Oops", Source: "traktor", DetectedAt: time.Date(2026, 10, 13, 23, 0, 1, 0, time.FixedZone("x", 3600)), PublishedAt: nil},
		{Artist: "", Title: "Only Title", Source: "manual", DetectedAt: time.Date(2026, 10, 13, 23, 30, 0, 0, time.UTC), PublishedAt: &pub},
	}
	srv, _ := newServer(t, Options{Backend: b})

	resp, body := get(t, srv.URL+"/tracks")
	if ct := resp.Header.Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("content-type %q", ct)
	}
	var got []state.TracklistEntry
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[1].PublishedAt != nil || got[0].Artist != "Kraftwerk" {
		t.Errorf("tracks: %+v", got)
	}

	resp, body = get(t, srv.URL+"/tracks?format=txt")
	if ct := resp.Header.Get("Content-Type"); ct != "text/plain; charset=utf-8" {
		t.Errorf("content-type %q", ct)
	}
	want := "21:05:09  Kraftwerk - Computer Love\n" +
		"22:00:01  Skipped - Oops  (not shown)\n" +
		"23:30:00  Only Title\n"
	if body != want {
		t.Errorf("txt:\n%q\nwant\n%q", body, want)
	}
}

func TestTracksEmpty(t *testing.T) {
	srv, _ := newServer(t, Options{})
	_, body := get(t, srv.URL+"/tracks")
	if strings.TrimSpace(body) != "[]" {
		t.Errorf("empty tracklist should be [], got %q", body)
	}
	_, body = get(t, srv.URL+"/tracks?format=txt")
	if body != "" {
		t.Errorf("empty txt tracklist should be empty, got %q", body)
	}
}

func TestHealth(t *testing.T) {
	clk := clock.NewFake(t0)
	good := health.CheckerFunc(func() health.Status { return health.Status{OK: true, Detail: map[string]any{"mounts": 1}} })
	bad := health.CheckerFunc(func() health.Status {
		return health.Status{OK: false, Warnings: []string{"disk low: 1.2 GiB free"}}
	})

	type resp struct {
		OK         bool                     `json:"ok"`
		Components map[string]health.Status `json:"components"`
		Time       time.Time                `json:"time"`
	}
	fetch := func(comps map[string]health.Checker) (int, resp) {
		srv, _ := newServer(t, Options{Health: comps, Clock: clk})
		r, body := get(t, srv.URL+"/health")
		var out resp
		if err := json.Unmarshal([]byte(body), &out); err != nil {
			t.Fatalf("%v: %s", err, body)
		}
		return r.StatusCode, out
	}

	code, out := fetch(map[string]health.Checker{"ingest": good, "relay": good})
	if code != 200 || !out.OK || len(out.Components) != 2 || !out.Time.Equal(t0) {
		t.Errorf("all ok: %d %+v", code, out)
	}
	code, out = fetch(map[string]health.Checker{"ingest": good, "recorder": bad})
	if code != 200 {
		t.Errorf("unhealthy should still be 200, got %d", code)
	}
	if out.OK {
		t.Error("ok should be false when a component is not ok")
	}
	if !out.Components["ingest"].OK || out.Components["recorder"].OK {
		t.Errorf("components: %+v", out.Components)
	}
	if w := out.Components["recorder"].Warnings; len(w) != 1 || w[0] != "disk low: 1.2 GiB free" {
		t.Errorf("warnings: %v", w)
	}
	code, out = fetch(nil)
	if code != 200 || !out.OK || out.Components == nil {
		t.Errorf("no components: %d %+v", code, out)
	}
}

func post(t *testing.T, url, auth, body string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest("POST", url+"/now", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func errMsg(t *testing.T, body string) string {
	t.Helper()
	var e struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &e); err != nil || e.Error == "" {
		t.Fatalf("expected {\"error\": ...}, got %q", body)
	}
	return e.Error
}

func TestPostAuth(t *testing.T) {
	cases := []struct {
		name      string
		token     string
		auth      string
		wantCode  int
		wantApply bool
	}{
		{"no token configured", "", "Bearer anything", 403, false},
		{"no token configured, no header", "", "", 403, false},
		{"missing header", "s3cret", "", 401, false},
		{"wrong token", "s3cret", "Bearer nope", 401, false},
		{"wrong scheme", "s3cret", "Basic s3cret", 401, false},
		{"bare token", "s3cret", "s3cret", 401, false},
		{"prefix of token", "s3cret", "Bearer s3cre", 401, false},
		{"right token", "s3cret", "Bearer s3cret", 200, true},
		{"scheme case-insensitive", "s3cret", "bearer s3cret", 200, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv, b := newServer(t, Options{Token: c.token})
			resp, body := post(t, srv.URL, c.auth, `{"action":"clear"}`)
			if resp.StatusCode != c.wantCode {
				t.Fatalf("status %d want %d (%s)", resp.StatusCode, c.wantCode, body)
			}
			b.mu.Lock()
			n := len(b.applied)
			b.mu.Unlock()
			if (n > 0) != c.wantApply {
				t.Errorf("applied=%d want apply=%v", n, c.wantApply)
			}
			if c.wantCode != 200 {
				errMsg(t, body)
			}
			if c.wantCode == 401 && resp.Header.Get("WWW-Authenticate") == "" {
				t.Error("401 without WWW-Authenticate")
			}
			if resp.Header.Get("Access-Control-Allow-Origin") != "*" {
				t.Error("missing CORS header")
			}
		})
	}
}

func TestPostSuccessReturnsState(t *testing.T) {
	srv, b := newServer(t, Options{Token: "tok"})
	newState := trackState()
	newState.Seq = 99
	b.applyFn = func(c state.Command) (state.State, error) { return newState, nil }
	resp, body := post(t, srv.URL, "Bearer tok", `{"action":"set","artist":"A","title":"T","unknown_field":1}`)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	var got state.State
	if err := json.Unmarshal([]byte(body), &got); err != nil || got.Seq != 99 {
		t.Errorf("body %q err %v", body, err)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("content-type %q", ct)
	}
	cmd := b.lastApplied(t)
	if cmd.Action != "set" || cmd.Artist != "A" || cmd.Title != "T" {
		t.Errorf("cmd %+v", cmd)
	}
}

func TestPostErrors(t *testing.T) {
	srv, b := newServer(t, Options{Token: "tok"})

	for name, body := range map[string]string{
		"invalid json": `{"action":`,
		"not json":     `hello`,
		"empty":        ``,
		"wrong type":   `{"action": 5}`,
	} {
		resp, out := post(t, srv.URL, "Bearer tok", body)
		if resp.StatusCode != 400 {
			t.Errorf("%s: status %d", name, resp.StatusCode)
		}
		errMsg(t, out)
	}
	b.mu.Lock()
	if len(b.applied) != 0 {
		t.Errorf("Apply called for bad JSON: %v", b.applied)
	}
	b.mu.Unlock()

	// Invalid command (wrapped sentinel) -> 400 with message.
	b.applyFn = func(c state.Command) (state.State, error) {
		return state.State{}, fmt.Errorf("%w: unknown action %q", state.ErrInvalidCommand, c.Action)
	}
	resp, out := post(t, srv.URL, "Bearer tok", `{"action":"bogus"}`)
	if resp.StatusCode != 400 {
		t.Errorf("invalid command: status %d", resp.StatusCode)
	}
	if msg := errMsg(t, out); !strings.Contains(msg, "unknown action") {
		t.Errorf("message %q", msg)
	}

	// Other errors -> 500, without leaking internals.
	b.applyFn = func(c state.Command) (state.State, error) {
		return state.State{}, errors.New("disk exploded at /secret/path")
	}
	resp, out = post(t, srv.URL, "Bearer tok", `{"action":"clear"}`)
	if resp.StatusCode != 500 {
		t.Errorf("internal error: status %d", resp.StatusCode)
	}
	if msg := errMsg(t, out); strings.Contains(msg, "secret") {
		t.Errorf("500 leaks internals: %q", msg)
	}

	// Oversized body -> 413.
	big := `{"action":"set","artist":"` + strings.Repeat("x", 70<<10) + `"}`
	resp, _ = post(t, srv.URL, "Bearer tok", big)
	if resp.StatusCode != 413 {
		t.Errorf("oversized: status %d", resp.StatusCode)
	}
}

func TestPostActionlessSet(t *testing.T) {
	cases := []struct {
		body       string
		wantAction string
	}{
		{`{"artist":"A","title":"T"}`, "set"},
		{`{"title":"T"}`, "set"},
		{`{"artist":"A"}`, "set"},
		{`{"action":"performer","performer":"p"}`, "performer"},
		{`{}`, ""},
	}
	for _, c := range cases {
		srv, b := newServer(t, Options{Token: "tok"})
		resp, out := post(t, srv.URL, "Bearer tok", c.body)
		if resp.StatusCode != 200 {
			t.Fatalf("%s: status %d %s", c.body, resp.StatusCode, out)
		}
		if got := b.lastApplied(t).Action; got != c.wantAction {
			t.Errorf("%s: action %q want %q", c.body, got, c.wantAction)
		}
	}
}

func TestCORS(t *testing.T) {
	srv, _ := newServer(t, Options{Token: "tok"})

	req, _ := http.NewRequest("OPTIONS", srv.URL+"/now", nil)
	req.Header.Set("Origin", "http://display.local")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "authorization, content-type")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("preflight status %d", resp.StatusCode)
	}
	h := resp.Header
	if h.Get("Access-Control-Allow-Origin") != "*" {
		t.Errorf("allow-origin %q", h.Get("Access-Control-Allow-Origin"))
	}
	if !strings.Contains(h.Get("Access-Control-Allow-Methods"), "POST") {
		t.Errorf("allow-methods %q", h.Get("Access-Control-Allow-Methods"))
	}
	ah := strings.ToLower(h.Get("Access-Control-Allow-Headers"))
	if !strings.Contains(ah, "authorization") || !strings.Contains(ah, "content-type") {
		t.Errorf("allow-headers %q", ah)
	}

	for _, p := range []string{"/now", "/now.txt", "/tracks", "/health", "/", "/nope"} {
		r, _ := get(t, srv.URL+p)
		if r.Header.Get("Access-Control-Allow-Origin") != "*" {
			t.Errorf("GET %s: missing CORS header", p)
		}
	}
}

func TestRoutingErrors(t *testing.T) {
	srv, _ := newServer(t, Options{Token: "tok"})
	r, _ := get(t, srv.URL+"/nope")
	if r.StatusCode != 404 {
		t.Errorf("/nope: %d", r.StatusCode)
	}
	r, _ = get(t, srv.URL+"/nope/deeper")
	if r.StatusCode != 404 {
		t.Errorf("/nope/deeper: %d", r.StatusCode)
	}
	for _, c := range []struct{ method, path string }{
		{"DELETE", "/now"}, {"PUT", "/now"}, {"POST", "/now.txt"},
		{"POST", "/events"}, {"POST", "/tracks"}, {"POST", "/health"}, {"POST", "/"},
	} {
		req, _ := http.NewRequest(c.method, srv.URL+c.path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 405 {
			t.Errorf("%s %s: %d, want 405", c.method, c.path, resp.StatusCode)
		}
	}
}

func TestControlPage(t *testing.T) {
	srv, _ := newServer(t, Options{})
	resp, body := get(t, srv.URL+"/")
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Errorf("content-type %q", ct)
	}
	for _, want := range []string{"<!doctype html>", "EventSource", "/events", "Authorization"} {
		if !strings.Contains(body, want) {
			t.Errorf("control page lacks %q", want)
		}
	}
	// No external resources: the page must work on an isolated LAN.
	for _, bad := range []string{"http://", "https://", "//cdn"} {
		if strings.Contains(body, bad) {
			t.Errorf("control page references external resource %q", bad)
		}
	}
}
