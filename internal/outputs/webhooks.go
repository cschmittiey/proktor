package outputs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/cschmittiey/proktor/internal/config"
	"github.com/cschmittiey/proktor/internal/health"
)

const defaultWebhookTimeout = 5 * time.Second

// Webhooks POSTs the state JSON to every configured URL on every change.
//
// Delivery is fire-and-forget and independent per URL: each URL has at most
// one request in flight plus the latest not-yet-sent state; states that
// arrive while a request is in flight replace the pending one (coalescing),
// so a slow endpoint never delays the others, the state subscription, or
// grows memory.
type Webhooks struct {
	hooks  []*hook
	client *http.Client
	log    *slog.Logger

	// Test seams.
	afterDispatch func() // after a state has been handed to every hook
}

type hook struct {
	idx     int
	url     string
	display string // URL with secrets stripped, for logs and health
	timeout time.Duration
	w       *Webhooks

	mu          sync.Mutex
	inflight    bool
	pending     []byte
	lastStatus  int
	lastError   string
	lastSuccess time.Time
	failures    uint64
	successes   uint64
	coalesced   uint64
	attempted   bool
	lastOK      bool

	onDone func() // test seam: after an attempt is recorded and in-flight state updated
}

// NewWebhooks builds the sender. client may be nil (a plain http.Client is
// used; per-request timeouts come from each webhook's TimeoutSeconds, default
// 5). Entries with an empty URL are ignored.
func NewWebhooks(cfgs []config.Webhook, client *http.Client, logger *slog.Logger) *Webhooks {
	if client == nil {
		client = &http.Client{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	w := &Webhooks{client: client, log: logger}
	for _, c := range cfgs {
		if c.URL == "" {
			logger.Warn("webhooks: ignoring webhook with empty url")
			continue
		}
		to := defaultWebhookTimeout
		if c.TimeoutSeconds > 0 {
			to = time.Duration(c.TimeoutSeconds * float64(time.Second))
		}
		w.hooks = append(w.hooks, &hook{idx: len(w.hooks), url: c.URL, display: redactURL(c.URL), timeout: to, w: w})
	}
	return w
}

// redactURL keeps only scheme and host: webhook URLs (Slack, Discord, ...)
// commonly carry their secret in the path or query, and /health is public.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "(invalid url)"
	}
	s := u.Scheme + "://" + u.Host
	if (u.Path != "" && u.Path != "/") || u.RawQuery != "" {
		s += "/..."
	} else if u.Path == "/" {
		s += "/"
	}
	return s
}

// Run subscribes to src and dispatches every change until ctx is done, then
// waits for in-flight requests (which are cancelled with ctx). Only changes
// are sent: the state current at start is not.
func (w *Webhooks) Run(ctx context.Context, src Source) error {
	if len(w.hooks) == 0 {
		return nil
	}
	ch, cancel := src.Subscribe()
	defer cancel()

	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		select {
		case <-ctx.Done():
			return nil
		case st, ok := <-ch:
			if !ok {
				return nil
			}
			body, err := json.Marshal(st)
			if err != nil {
				w.log.Error("webhooks: cannot encode state", "err", err)
				continue
			}
			for _, h := range w.hooks {
				h.submit(ctx, &wg, body)
			}
			if w.afterDispatch != nil {
				w.afterDispatch()
			}
		}
	}
}

// submit sends body now, or makes it the pending body if a request is in
// flight. It never blocks.
func (h *hook) submit(ctx context.Context, wg *sync.WaitGroup, body []byte) {
	h.mu.Lock()
	if h.inflight {
		h.pending = body
		h.coalesced++
		h.mu.Unlock()
		return
	}
	h.inflight = true
	h.mu.Unlock()
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.loop(ctx, body)
	}()
}

func (h *hook) loop(ctx context.Context, body []byte) {
	for body != nil {
		h.post(ctx, body)
		h.mu.Lock()
		body, h.pending = h.pending, nil
		if body == nil {
			h.inflight = false
		}
		h.mu.Unlock()
		if ctx.Err() != nil {
			h.mu.Lock()
			h.inflight = false
			h.pending = nil
			h.mu.Unlock()
			body = nil
		}
		if h.onDone != nil {
			h.onDone()
		}
	}
}

func (h *hook) post(ctx context.Context, body []byte) {
	rctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()
	status, err := h.do(rctx, body)

	h.mu.Lock()
	h.attempted = true
	h.lastStatus = status
	if err != nil {
		h.failures++
		h.lastOK = false
		h.lastError = err.Error()
	} else {
		h.successes++
		h.lastOK = true
		h.lastError = ""
		h.lastSuccess = time.Now()
	}
	h.mu.Unlock()
	if err != nil && ctx.Err() == nil {
		h.w.log.Warn("webhooks: delivery failed", "url", h.display, "status", status, "err", err)
	}
}

func (h *hook) do(ctx context.Context, body []byte) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader(body))
	if err != nil {
		return 0, errors.New("invalid url")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "proktor")
	resp, err := h.w.client.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err // the url.Error text repeats the (secret-bearing) URL
		}
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return resp.StatusCode, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return resp.StatusCode, nil
}

// WebhookStatus is the health detail for one webhook URL.
type WebhookStatus struct {
	Index int `json:"index"`
	// URL is scheme and host only; path and query may hold secrets.
	URL         string     `json:"url"`
	LastStatus  int        `json:"last_status"`
	LastError   string     `json:"last_error,omitempty"`
	LastSuccess *time.Time `json:"last_success,omitempty"`
	Failures    uint64     `json:"failures"`
	Successes   uint64     `json:"successes"`
	Coalesced   uint64     `json:"coalesced"`
	InFlight    bool       `json:"in_flight"`
}

// Health reports per-URL delivery status. It is not OK while the latest
// attempt for any URL failed.
func (w *Webhooks) Health() health.Status {
	st := health.Status{OK: true}
	details := make([]WebhookStatus, 0, len(w.hooks))
	for _, h := range w.hooks {
		h.mu.Lock()
		d := WebhookStatus{
			Index:      h.idx,
			URL:        h.display,
			LastStatus: h.lastStatus,
			LastError:  h.lastError,
			Failures:   h.failures,
			Successes:  h.successes,
			Coalesced:  h.coalesced,
			InFlight:   h.inflight,
		}
		if !h.lastSuccess.IsZero() {
			t := h.lastSuccess
			d.LastSuccess = &t
		}
		bad := h.attempted && !h.lastOK
		h.mu.Unlock()
		if bad {
			st.OK = false
			st.Warnings = append(st.Warnings, fmt.Sprintf("webhook %s: last delivery failed: %s", d.URL, d.LastError))
		}
		details = append(details, d)
	}
	st.Detail = details
	return st
}
