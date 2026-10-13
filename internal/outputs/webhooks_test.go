package outputs

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cschmittiey/proktor/internal/config"
	"github.com/cschmittiey/proktor/internal/state"
)

const wait = 10 * time.Second // failure guard only, never a delay

type req struct {
	method, contentType, userAgent string
	body                           []byte
}

// testHook is an httptest server that records requests.
type testHook struct {
	*httptest.Server
	got     chan req
	entered chan struct{}
	status  atomic.Int32
	// block, when non-nil, holds every request until it is closed (or the
	// client gives up).
	block chan struct{}
}

func newTestHook(t *testing.T, block chan struct{}) *testHook {
	t.Helper()
	h := &testHook{got: make(chan req, 100), entered: make(chan struct{}, 100), block: block}
	h.status.Store(200)
	h.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		h.entered <- struct{}{}
		if h.block != nil {
			select {
			case <-h.block:
			case <-r.Context().Done():
			}
		}
		h.got <- req{r.Method, r.Header.Get("Content-Type"), r.Header.Get("User-Agent"), b}
		w.WriteHeader(int(h.status.Load()))
	}))
	t.Cleanup(h.Server.Close)
	return h
}

func (h *testHook) recv(t *testing.T) req {
	t.Helper()
	select {
	case r := <-h.got:
		return r
	case <-time.After(wait):
		t.Fatal("timed out waiting for a webhook request")
		return req{}
	}
}

type runner struct {
	w          *Webhooks
	src        *fakeSource
	dispatched chan struct{}
	done       []chan struct{}
	stop       func()
}

func startHooks(t *testing.T, cfgs []config.Webhook, src *fakeSource) *runner {
	t.Helper()
	w := NewWebhooks(cfgs, nil, quietLog)
	r := &runner{w: w, src: src, dispatched: make(chan struct{}, 100)}
	w.afterDispatch = func() { r.dispatched <- struct{}{} }
	for _, h := range w.hooks {
		ch := make(chan struct{}, 100)
		r.done = append(r.done, ch)
		h.onDone = func() { ch <- struct{}{} }
	}
	ctx, cancel := context.WithCancel(context.Background())
	res := make(chan error, 1)
	go func() { res <- w.Run(ctx, src) }()
	<-src.subscribed
	stopped := false
	r.stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-res:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(wait):
			t.Error("Run did not return")
		}
	}
	t.Cleanup(r.stop)
	return r
}

func (r *runner) change(t *testing.T, st state.State) {
	t.Helper()
	r.src.set(st)
	select {
	case <-r.dispatched:
	case <-time.After(wait):
		t.Fatal("state was not dispatched (subscription loop blocked?)")
	}
}

func (r *runner) awaitDone(t *testing.T, i int) {
	t.Helper()
	select {
	case <-r.done[i]:
	case <-time.After(wait):
		t.Fatalf("hook %d: no delivery attempt finished", i)
	}
}

func hookStatus(w *Webhooks) []WebhookStatus { return w.Health().Detail.([]WebhookStatus) }

func TestWebhooksOnePostPerChange(t *testing.T) {
	a, b := newTestHook(t, nil), newTestHook(t, nil)
	src := newFakeSource(trackState(0, "init", "init"))
	r := startHooks(t, []config.Webhook{{URL: a.URL}, {URL: b.URL + "/hook?token=s3cret"}}, src)

	for i := uint64(1); i <= 3; i++ {
		st := trackState(i, "Artist", "Title "+string(rune('A'+i)))
		r.change(t, st)
		want, _ := json.Marshal(st)
		for _, srv := range []*testHook{a, b} {
			got := srv.recv(t)
			if got.method != "POST" || got.contentType != "application/json" || got.userAgent != "proktor" {
				t.Errorf("request = %+v", got)
			}
			if string(got.body) != string(want) {
				t.Errorf("body = %s\nwant %s", got.body, want)
			}
		}
		r.awaitDone(t, 0)
		r.awaitDone(t, 1)
	}
	r.stop()
	for _, srv := range []*testHook{a, b} {
		select {
		case extra := <-srv.got:
			t.Errorf("unexpected extra request: %s", extra.body)
		default:
		}
	}
	for i, s := range hookStatus(r.w) {
		if s.Successes != 3 || s.Failures != 0 || s.LastStatus != 200 || s.LastSuccess == nil || s.Coalesced != 0 {
			t.Errorf("hook %d status = %+v", i, s)
		}
	}
	if h := r.w.Health(); !h.OK || len(h.Warnings) != 0 {
		t.Errorf("health = %+v", h)
	}
}

func TestWebhooksSlowEndpointDoesNotBlockOthersAndCoalesces(t *testing.T) {
	release := make(chan struct{})
	slow := newTestHook(t, release)
	fast := newTestHook(t, nil)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	src := newFakeSource(trackState(0, "init", "init"))
	r := startHooks(t, []config.Webhook{{URL: slow.URL, TimeoutSeconds: 30}, {URL: fast.URL}}, src)

	var sts []state.State
	for i := uint64(1); i <= 5; i++ {
		st := trackState(i, "A", "T"+string(rune('0'+i)))
		sts = append(sts, st)
		r.change(t, st)
		want, _ := json.Marshal(st)
		if got := fast.recv(t); string(got.body) != string(want) {
			t.Fatalf("fast got %s, want %s", got.body, want)
		}
		r.awaitDone(t, 1)
		if i == 1 {
			<-slow.entered // the slow request is now in flight
		}
	}
	h := hookStatus(r.w)
	if !h[0].InFlight || h[0].Coalesced != 4 || h[1].Coalesced != 0 {
		t.Fatalf("status = %+v", h)
	}

	close(release)
	r.awaitDone(t, 0) // first request completes
	r.awaitDone(t, 0) // then the latest pending one
	first, last := slow.recv(t), slow.recv(t)
	w1, _ := json.Marshal(sts[0])
	w5, _ := json.Marshal(sts[4])
	if string(first.body) != string(w1) || string(last.body) != string(w5) {
		t.Errorf("slow got %s then %s", first.body, last.body)
	}
	r.stop()
	select {
	case extra := <-slow.got:
		t.Errorf("unexpected extra request %s", extra.body)
	default:
	}
	if h := hookStatus(r.w); h[0].InFlight || h[0].Successes != 2 {
		t.Errorf("status = %+v", h[0])
	}
}

func TestWebhooksTimeoutHonoured(t *testing.T) {
	block := make(chan struct{})
	slow := newTestHook(t, block)
	t.Cleanup(func() { close(block) })
	src := newFakeSource(trackState(0, "i", "i"))
	r := startHooks(t, []config.Webhook{{URL: slow.URL, TimeoutSeconds: 0.05}}, src)

	begin := time.Now()
	r.change(t, trackState(1, "A", "B"))
	r.awaitDone(t, 0)
	if el := time.Since(begin); el > 5*time.Second {
		t.Errorf("took %v, timeout not honoured", el)
	}
	s := hookStatus(r.w)[0]
	if s.Failures != 1 || s.Successes != 0 || s.LastStatus != 0 || !strings.Contains(s.LastError, "deadline exceeded") || s.LastSuccess != nil || s.InFlight {
		t.Fatalf("status = %+v", s)
	}
	h := r.w.Health()
	if h.OK || len(h.Warnings) != 1 {
		t.Errorf("health = %+v", h)
	}
}

func TestWebhooksStatusCodesFailuresAndRecovery(t *testing.T) {
	bad := newTestHook(t, nil)
	bad.status.Store(500)
	dead := newTestHook(t, nil)
	deadURL := dead.URL + "/secret/path?key=abc"
	dead.Close() // connection refused
	good := newTestHook(t, nil)
	src := newFakeSource(trackState(0, "i", "i"))
	r := startHooks(t, []config.Webhook{{URL: bad.URL}, {URL: deadURL}, {URL: good.URL}, {URL: ""}, {URL: "ht!tp://%zz"}}, src)

	r.change(t, trackState(1, "A", "B"))
	for i := range 4 {
		r.awaitDone(t, i)
	}
	bad.recv(t)
	good.recv(t)
	st := hookStatus(r.w)
	if len(st) != 4 {
		t.Fatalf("empty url should be ignored; got %d hooks", len(st))
	}
	if st[0].LastStatus != 500 || st[0].Failures != 1 || st[0].LastError != "unexpected status 500" {
		t.Errorf("bad = %+v", st[0])
	}
	if st[1].Failures != 1 || st[1].LastStatus != 0 || st[1].LastError == "" {
		t.Errorf("dead = %+v", st[1])
	}
	if st[2].Successes != 1 || st[2].Failures != 0 {
		t.Errorf("good = %+v", st[2])
	}
	if st[3].Failures != 1 || st[3].LastError != "invalid url" {
		t.Errorf("invalid = %+v", st[3])
	}
	h := r.w.Health()
	b, _ := json.Marshal(h)
	if h.OK || len(h.Warnings) != 3 {
		t.Errorf("health = %s", b)
	}
	for _, leak := range []string{"secret/path", "abc", "%zz"} {
		if strings.Contains(string(b), leak) {
			t.Errorf("health leaks %q: %s", leak, b)
		}
	}

	// Recovery: latest attempt wins, counters keep history.
	bad.status.Store(204)
	r.change(t, trackState(2, "A", "C"))
	for i := range 4 {
		r.awaitDone(t, i)
	}
	st = hookStatus(r.w)
	if st[0].LastStatus != 204 || st[0].Failures != 1 || st[0].Successes != 1 || st[0].LastError != "" || st[0].LastSuccess == nil {
		t.Errorf("bad after recovery = %+v", st[0])
	}
}

func TestWebhooksDefaultTimeoutAndEmptyConfig(t *testing.T) {
	w := NewWebhooks([]config.Webhook{{URL: "http://example.invalid/"}, {URL: "http://x/", TimeoutSeconds: 1.5}}, nil, nil)
	if w.hooks[0].timeout != 5*time.Second || w.hooks[1].timeout != 1500*time.Millisecond {
		t.Errorf("timeouts = %v, %v", w.hooks[0].timeout, w.hooks[1].timeout)
	}
	empty := NewWebhooks(nil, nil, quietLog)
	if err := empty.Run(context.Background(), newFakeSource(state.State{})); err != nil {
		t.Error(err)
	}
	if h := empty.Health(); !h.OK {
		t.Error("no hooks should be healthy")
	}
}

func TestWebhooksRunCancelsInFlight(t *testing.T) {
	block := make(chan struct{})
	slow := newTestHook(t, block)
	t.Cleanup(func() { close(block) })
	src := newFakeSource(trackState(0, "i", "i"))
	r := startHooks(t, []config.Webhook{{URL: slow.URL, TimeoutSeconds: 60}}, src)
	r.change(t, trackState(1, "A", "B"))
	<-slow.entered
	begin := time.Now()
	r.stop() // must not wait the 60 s timeout
	if time.Since(begin) > 5*time.Second {
		t.Error("Run waited for the request timeout")
	}
}

func TestRedactURL(t *testing.T) {
	for in, want := range map[string]string{
		"http://host:8080":                "http://host:8080",
		"http://host/":                    "http://host/",
		"https://hooks.example/a/b?c=d":   "https://hooks.example/...",
		"https://user:pw@host/x":          "https://host/...",
		"nonsense":                        "(invalid url)",
		"http://[::1]:9/x":                "http://[::1]:9/...",
		"https://hooks.example/?k=secret": "https://hooks.example/...",
	} {
		if got := redactURL(in); got != want {
			t.Errorf("redactURL(%q) = %q, want %q", in, got, want)
		}
	}
}
