// Package config loads proktor's TOML config and applies CLI/env overrides.
//
// # Precedence
//
// Effective configuration is built in this order, later steps winning:
//
//	defaults < config file < environment (PROKTOR_*) < command-line flags
//
// The CLI is expected to do:
//
//	cfg, err := config.Load(path)   // defaults + file (ApplyDefaults already applied)
//	err = cfg.FromEnv(os.Environ()) // PROKTOR_* variables
//	err = cfg.Override(k, v)        // once per flag
//	err = cfg.Validate()            // only now
//
// Load does not call Validate, so that a value that is wrong in the file can
// still be fixed by an env var or flag. Override creates new array elements
// (one past the end) pre-filled with per-element defaults, so ApplyDefaults
// does not need to be called again after overrides; calling it again is
// harmless except that it re-fills fields deliberately blanked by an override
// (for example state.journal = "" to disable the journal).
//
// Every setting can be addressed by a dotted path made of the TOML key names,
// with array elements addressed by index: "http.token",
// "ingest.mount.0.password", "upstream.1.hold_seconds". Environment variable
// names are the same path upper-cased, prefixed with PROKTOR_ and with dots
// replaced by underscores: PROKTOR_UPSTREAM_1_HOLD_SECONDS. Use Paths to list
// every path.
package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
)

// Default values. Exported where other packages may want to mention them.
const (
	DefaultIngestListen  = "0.0.0.0:8000"
	DefaultHTTPListen    = "0.0.0.0:8080"
	DefaultMountPath     = "/live"
	DefaultOnConflict    = "reject"
	DefaultManualPin     = "until_next_auto"
	DefaultRecorderDir   = "./recordings"
	DefaultMinFreeMB     = 2048
	DefaultHistoryLength = 10
	DefaultJournal       = "./proktor-state.jsonl"

	DefaultUpstreamMount       = "/"
	DefaultUpstreamUsername    = "source"
	DefaultUpstreamProtocol    = "put"
	DefaultUpstreamContentType = "application/ogg"
	DefaultUpstreamAuthStyle   = "basic"
	DefaultUpstreamBufferBytes = 4 << 20

	DefaultWebhookTimeoutSeconds = 5
)

// Default returns a Config with every default applied: one ingest mount
// "/live" without a password, no upstreams, recorder disabled.
func Default() Config {
	var c Config
	c.ApplyDefaults()
	return c
}

// Load reads the TOML file at path, rejecting unknown keys, and applies
// defaults. An empty path returns Default(). Load does not validate.
func Load(path string) (Config, error) {
	if path == "" {
		return Default(), nil
	}
	var c Config
	md, err := toml.DecodeFile(path, &c)
	if err != nil {
		return Config{}, fmt.Errorf("config %s: %w", path, err)
	}
	if und := md.Undecoded(); len(und) > 0 {
		names := make([]string, len(und))
		for i, k := range und {
			names[i] = k.String()
		}
		return Config{}, fmt.Errorf("config %s: unknown key(s): %s", path, strings.Join(names, ", "))
	}
	c.ApplyDefaults()
	return c, nil
}

func setIfEmpty(s *string, v string) {
	if *s == "" {
		*s = v
	}
}

func slashPrefix(s string) string {
	if s != "" && !strings.HasPrefix(s, "/") {
		return "/" + s
	}
	return s
}

func applyMountDefaults(m *Mount) {
	m.Path = slashPrefix(m.Path)
	setIfEmpty(&m.OnConflict, DefaultOnConflict)
}

func applyUpstreamDefaults(u *Upstream) {
	setIfEmpty(&u.Mount, DefaultUpstreamMount)
	u.Mount = slashPrefix(u.Mount)
	u.SourceMount = slashPrefix(u.SourceMount)
	setIfEmpty(&u.Username, DefaultUpstreamUsername)
	setIfEmpty(&u.AuthStyle, DefaultUpstreamAuthStyle)
	setIfEmpty(&u.Protocol, DefaultUpstreamProtocol)
	setIfEmpty(&u.ContentType, DefaultUpstreamContentType)
	if u.BufferBytes == 0 {
		u.BufferBytes = DefaultUpstreamBufferBytes
	}
}

func applyWebhookDefaults(w *Webhook) {
	if w.TimeoutSeconds == 0 {
		w.TimeoutSeconds = DefaultWebhookTimeoutSeconds
	}
}

// ApplyDefaults fills zero values with defaults and normalises mount paths to
// start with "/". It is idempotent. Note that explicit zeros are
// indistinguishable from unset for fields with a non-zero default, so
// history_length = 0, min_free_mb = 0 and buffer_bytes = 0 in the file mean
// "default".
func (c *Config) ApplyDefaults() {
	setIfEmpty(&c.Ingest.Listen, DefaultIngestListen)
	if len(c.Ingest.Mount) == 0 {
		c.Ingest.Mount = []Mount{{Path: DefaultMountPath}}
	}
	for i := range c.Ingest.Mount {
		applyMountDefaults(&c.Ingest.Mount[i])
	}
	setIfEmpty(&c.Metadata.ManualPin, DefaultManualPin)
	for i := range c.Upstream {
		applyUpstreamDefaults(&c.Upstream[i])
	}
	setIfEmpty(&c.Recorder.Dir, DefaultRecorderDir)
	if c.Recorder.MinFreeMB == 0 {
		c.Recorder.MinFreeMB = DefaultMinFreeMB
	}
	setIfEmpty(&c.HTTP.Listen, DefaultHTTPListen)
	if c.HTTP.HistoryLength == 0 {
		c.HTTP.HistoryLength = DefaultHistoryLength
	}
	for i := range c.Webhook {
		applyWebhookDefaults(&c.Webhook[i])
	}
	setIfEmpty(&c.State.Journal, DefaultJournal)
}

// normalize re-applies only the path normalisation (leading "/"), used after
// Override so "ingest.mount.0.path=live" still works.
func (c *Config) normalize() {
	for i := range c.Ingest.Mount {
		c.Ingest.Mount[i].Path = slashPrefix(c.Ingest.Mount[i].Path)
	}
	for i := range c.Upstream {
		c.Upstream[i].Mount = slashPrefix(c.Upstream[i].Mount)
		c.Upstream[i].SourceMount = slashPrefix(c.Upstream[i].SourceMount)
	}
}

func oneOf(v string, allowed ...string) bool {
	for _, a := range allowed {
		if v == a {
			return true
		}
	}
	return false
}

func checkListen(field, addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s: %q is not a valid host:port (%v)", field, addr, err)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 0 || n > 65535 {
		return fmt.Errorf("%s: %q has an invalid port %q", field, addr, port)
	}
	return nil
}

// Validate checks the config and returns every problem found, joined with
// errors.Join, or nil.
func (c *Config) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if err := checkListen("ingest.listen", c.Ingest.Listen); err != nil {
		errs = append(errs, err)
	}
	if err := checkListen("http.listen", c.HTTP.Listen); err != nil {
		errs = append(errs, err)
	}

	if len(c.Ingest.Mount) == 0 {
		add("ingest.mount: at least one mount is required")
	}
	mounts := map[string]bool{}
	for i, m := range c.Ingest.Mount {
		f := fmt.Sprintf("ingest.mount[%d]", i)
		switch {
		case m.Path == "":
			add("%s.path: must not be empty", f)
		case mounts[m.Path]:
			add("%s.path: duplicate mount %q", f, m.Path)
		}
		mounts[m.Path] = true
		if !oneOf(m.OnConflict, "reject", "takeover") {
			add("%s.on_conflict: %q is not one of reject, takeover", f, m.OnConflict)
		}
	}

	if !oneOf(c.Metadata.ManualPin, "until_next_auto", "until_cleared") {
		add("metadata.manual_pin: %q is not one of until_next_auto, until_cleared", c.Metadata.ManualPin)
	}
	if c.Metadata.DelaySeconds < 0 {
		add("metadata.delay_seconds: must be >= 0, got %v", c.Metadata.DelaySeconds)
	}

	names := map[string]bool{}
	for i, u := range c.Upstream {
		f := fmt.Sprintf("upstream[%d]", i)
		if u.Name == "" {
			add("%s.name: must not be empty", f)
		} else if names[u.Name] {
			add("%s.name: duplicate upstream name %q", f, u.Name)
		}
		names[u.Name] = true
		if err := checkURL(f+".url", u.URL, false); err != nil {
			errs = append(errs, err)
		}
		if u.Mount == "" {
			add("%s.mount: must not be empty", f)
		}
		if !oneOf(u.Protocol, "put", "source") {
			add("%s.protocol: %q is not one of put, source", f, u.Protocol)
		}
		if !oneOf(u.AuthStyle, "basic", "user_in_password") {
			add("%s.auth_style: %q is not one of basic, user_in_password", f, u.AuthStyle)
		}
		if u.HoldSeconds < 0 {
			add("%s.hold_seconds: must be >= 0, got %v", f, u.HoldSeconds)
		}
		if u.BufferBytes <= 0 {
			add("%s.buffer_bytes: must be > 0, got %d", f, u.BufferBytes)
		}
		if u.SourceMount != "" && !mounts[u.SourceMount] {
			add("%s.source_mount: %q is not a configured ingest mount", f, u.SourceMount)
		}
	}

	if c.Recorder.Enabled && c.Recorder.Dir == "" {
		add("recorder.dir: must not be empty when the recorder is enabled")
	}
	if c.Recorder.MinFreeMB < 0 {
		add("recorder.min_free_mb: must be >= 0, got %d", c.Recorder.MinFreeMB)
	}
	if c.HTTP.HistoryLength < 0 {
		add("http.history_length: must be >= 0, got %d", c.HTTP.HistoryLength)
	}

	for i, w := range c.Webhook {
		f := fmt.Sprintf("webhook[%d]", i)
		if err := checkURL(f+".url", w.URL, true); err != nil {
			errs = append(errs, err)
		}
		if w.TimeoutSeconds < 0 {
			add("%s.timeout_seconds: must be >= 0, got %v", f, w.TimeoutSeconds)
		}
	}
	for i, t := range c.TextFile {
		if t.Path == "" {
			add("textfile[%d].path: must not be empty", i)
		}
	}
	return errors.Join(errs...)
}

// checkURL validates a URL. allowHTTPS is true for webhooks; upstreams only
// support plain http for now.
func checkURL(field, raw string, allowHTTPS bool) error {
	if raw == "" {
		return fmt.Errorf("%s: must not be empty", field)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%s: %q does not parse as a URL: %v", field, raw, err)
	}
	switch {
	case u.Scheme == "http":
	case u.Scheme == "https" && allowHTTPS:
	case u.Scheme == "https":
		return fmt.Errorf("%s: https is not supported yet (%q)", field, raw)
	default:
		if allowHTTPS {
			return fmt.Errorf("%s: %q must use http or https", field, raw)
		}
		return fmt.Errorf("%s: %q must use http", field, raw)
	}
	if u.Host == "" {
		return fmt.Errorf("%s: %q has no host", field, raw)
	}
	return nil
}

const redacted = "***"

func redact(s *string) {
	if *s != "" {
		*s = redacted
	}
}

// redactURL hides the password part of URL userinfo, if any.
func redactURL(s string) string {
	u, err := url.Parse(s)
	if err != nil || u.User == nil {
		return s
	}
	if _, has := u.User.Password(); has {
		u.User = url.UserPassword(u.User.Username(), redacted)
		return u.String()
	}
	return s
}

// Redacted returns a deep copy with passwords and the token replaced by
// "***" (empty secrets stay empty, so the log shows whether auth is on).
func (c Config) Redacted() Config {
	out := c
	out.Ingest.Mount = append([]Mount(nil), c.Ingest.Mount...)
	for i := range out.Ingest.Mount {
		redact(&out.Ingest.Mount[i].Password)
	}
	out.Upstream = append([]Upstream(nil), c.Upstream...)
	for i := range out.Upstream {
		redact(&out.Upstream[i].Password)
		out.Upstream[i].URL = redactURL(out.Upstream[i].URL)
	}
	out.Webhook = append([]Webhook(nil), c.Webhook...)
	for i := range out.Webhook {
		out.Webhook[i].URL = redactURL(out.Webhook[i].URL)
	}
	out.TextFile = append([]TextFile(nil), c.TextFile...)
	redact(&out.HTTP.Token)
	return out
}

// sortedKeys is a tiny helper for deterministic error output.
func sortedKeys(m map[string]struct{}) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
