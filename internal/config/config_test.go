package config

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

func writeTemp(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "proktor.toml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func mustContain(t *testing.T, err error, subs ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error containing %q, got nil", subs)
	}
	for _, s := range subs {
		if !strings.Contains(err.Error(), s) {
			t.Errorf("error %q does not contain %q", err, s)
		}
	}
}

func TestDefault(t *testing.T) {
	c := Default()
	if c.Ingest.Listen != "0.0.0.0:8000" || c.HTTP.Listen != "0.0.0.0:8080" {
		t.Errorf("listen defaults: %q %q", c.Ingest.Listen, c.HTTP.Listen)
	}
	if len(c.Ingest.Mount) != 1 || c.Ingest.Mount[0] != (Mount{Path: "/live", OnConflict: "reject"}) {
		t.Errorf("mounts: %+v", c.Ingest.Mount)
	}
	if c.Recorder.Enabled || c.Recorder.Dir != "./recordings" || c.Recorder.MinFreeMB != 2048 {
		t.Errorf("recorder: %+v", c.Recorder)
	}
	if c.HTTP.HistoryLength != 10 || c.HTTP.Token != "" {
		t.Errorf("http: %+v", c.HTTP)
	}
	if c.Metadata.ManualPin != "until_next_auto" || c.Metadata.DelaySeconds != 0 {
		t.Errorf("metadata: %+v", c.Metadata)
	}
	if c.State.Journal != "./proktor-state.jsonl" {
		t.Errorf("journal: %q", c.State.Journal)
	}
	if err := c.Validate(); err != nil {
		t.Errorf("default config invalid: %v", err)
	}
}

func TestLoadEmptyPath(t *testing.T) {
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c, Default()) {
		t.Errorf("Load(\"\") != Default()")
	}
}

func TestLoadAppliesDefaults(t *testing.T) {
	p := writeTemp(t, `
event_name = "x"
[[ingest.mount]]
path = "live2"
[[upstream]]
name = "a"
url = "http://h:1/"
[[webhook]]
url = "http://h/hook"
`)
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.EventName != "x" {
		t.Errorf("event_name = %q", c.EventName)
	}
	if got := c.Ingest.Mount[0]; got.Path != "/live2" || got.OnConflict != "reject" {
		t.Errorf("mount: %+v", got)
	}
	u := c.Upstream[0]
	want := Upstream{Name: "a", URL: "http://h:1/", Mount: "/", Username: "source", AuthStyle: "basic",
		Protocol: "put", ContentType: "application/ogg", BufferBytes: 4 << 20}
	if u != want {
		t.Errorf("upstream: %+v\nwant %+v", u, want)
	}
	if c.Webhook[0].TimeoutSeconds != 5 {
		t.Errorf("webhook timeout: %v", c.Webhook[0].TimeoutSeconds)
	}
	if err := c.Validate(); err != nil {
		t.Errorf("validate: %v", err)
	}
}

func TestLoadFileValuesWin(t *testing.T) {
	p := writeTemp(t, `
[ingest]
listen = ":9000"
[http]
history_length = 3
[recorder]
enabled = true
`)
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.Ingest.Listen != ":9000" || c.HTTP.HistoryLength != 3 || !c.Recorder.Enabled {
		t.Errorf("%+v", c)
	}
}

func TestLoadUnknownKey(t *testing.T) {
	p := writeTemp(t, "event_name = \"x\"\n[http]\ntokn = \"oops\"\n[[upstream]]\nname = \"a\"\npasword = \"x\"\n")
	_, err := Load(p)
	mustContain(t, err, "tokn", "pasword")
}

func TestLoadSyntaxErrorAndMissingFile(t *testing.T) {
	if _, err := Load(writeTemp(t, "this is not toml")); err == nil {
		t.Error("expected syntax error")
	}
	if _, err := Load(filepath.Join(t.TempDir(), "nope.toml")); err == nil {
		t.Error("expected missing-file error")
	}
}

func TestValidateAggregates(t *testing.T) {
	c := Default()
	c.Ingest.Listen = "nonsense"
	c.HTTP.Listen = "0.0.0.0:notaport"
	c.Ingest.Mount = []Mount{
		{Path: "/a", OnConflict: "reject"},
		{Path: "/a", OnConflict: "maybe"},
		{Path: "", OnConflict: "reject"},
	}
	c.Metadata.ManualPin = "forever"
	c.Metadata.DelaySeconds = -1
	c.Upstream = []Upstream{
		{Name: "", URL: "https://h/", Mount: "/", Protocol: "put", AuthStyle: "basic", BufferBytes: 1},
		{Name: "dup", URL: "ftp://h/", Mount: "/", Protocol: "x", AuthStyle: "y", HoldSeconds: -1, BufferBytes: 0},
		{Name: "dup", URL: "http://", Mount: "/", Protocol: "put", AuthStyle: "basic", BufferBytes: 1},
	}
	c.HTTP.HistoryLength = -2
	c.Webhook = []Webhook{{URL: "ftp://x"}, {URL: ""}}
	c.TextFile = []TextFile{{Path: ""}}
	err := c.Validate()
	mustContain(t, err,
		"ingest.listen", "http.listen",
		"duplicate mount", "ingest.mount[1].on_conflict", "ingest.mount[2].path",
		"metadata.manual_pin", "metadata.delay_seconds",
		"upstream[0].name", "https is not supported yet",
		"upstream[1].url", "upstream[1].protocol", "upstream[1].auth_style", "upstream[1].hold_seconds", "upstream[1].buffer_bytes",
		"duplicate upstream name", "upstream[2].url",
		"http.history_length",
		"webhook[0].url", "webhook[1].url", "textfile[0].path",
	)
	// Aggregated: one Validate call reports many separate problems.
	if n := strings.Count(err.Error(), "\n") + 1; n < 15 {
		t.Errorf("expected many aggregated errors, got %d:\n%v", n, err)
	}
}

func TestValidateOK(t *testing.T) {
	c := Default()
	c.Upstream = []Upstream{{Name: "a", URL: "http://radio.example:8005/"}}
	c.Webhook = []Webhook{{URL: "https://example.invalid/hook"}}
	c.ApplyDefaults()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateSourceMount(t *testing.T) {
	c := Default()
	c.Upstream = []Upstream{{Name: "a", URL: "http://h:1/", SourceMount: "/nope"}}
	c.ApplyDefaults()
	mustContain(t, c.Validate(), "source_mount")
}

func TestOverrideKinds(t *testing.T) {
	c := Default()
	steps := []struct{ path, val string }{
		{"event_name", "Night"},            // string
		{"http.token", "tok"},              // string
		{"recorder.enabled", "true"},       // bool
		{"http.history_length", "25"},      // int
		{"metadata.delay_seconds", "42.5"}, // float
		{"ingest.mount.0.password", "pw"},  // array element
		{"ingest.mount.0.on_conflict", "takeover"},
		{"ingest.mount.0.path", "live"}, // normalised
		{"state.journal", ""},           // blanking is allowed
	}
	for _, s := range steps {
		if err := c.Override(s.path, s.val); err != nil {
			t.Fatalf("%s: %v", s.path, err)
		}
	}
	if c.EventName != "Night" || c.HTTP.Token != "tok" || !c.Recorder.Enabled || c.HTTP.HistoryLength != 25 ||
		c.Metadata.DelaySeconds != 42.5 || c.Ingest.Mount[0].Password != "pw" ||
		c.Ingest.Mount[0].OnConflict != "takeover" || c.Ingest.Mount[0].Path != "/live" || c.State.Journal != "" {
		t.Errorf("unexpected result: %+v", c)
	}
	if err := c.Override("recorder.enabled", "0"); err != nil || c.Recorder.Enabled {
		t.Errorf("bool 0: %v %v", err, c.Recorder.Enabled)
	}
}

func TestOverrideAppend(t *testing.T) {
	c := Default()
	if err := c.Override("upstream.0.name", "devfm"); err != nil {
		t.Fatal(err)
	}
	if err := c.Override("upstream.0.url", "http://h:8005/"); err != nil {
		t.Fatal(err)
	}
	if err := c.Override("upstream.1.hold_seconds", "20"); err != nil {
		t.Fatal(err)
	}
	if err := c.Override("ingest.mount.1.path", "/b"); err != nil {
		t.Fatal(err)
	}
	if err := c.Override("webhook.0.url", "http://h/x"); err != nil {
		t.Fatal(err)
	}
	if err := c.Override("textfile.0.path", "/tmp/np.txt"); err != nil {
		t.Fatal(err)
	}
	if len(c.Upstream) != 2 || c.Upstream[0].Name != "devfm" || c.Upstream[1].HoldSeconds != 20 {
		t.Fatalf("upstreams: %+v", c.Upstream)
	}
	// Appended elements carry per-element defaults.
	if u := c.Upstream[0]; u.Username != "source" || u.Protocol != "put" || u.BufferBytes != 4<<20 || u.Mount != "/" {
		t.Errorf("appended upstream lacks defaults: %+v", u)
	}
	if m := c.Ingest.Mount[1]; m.Path != "/b" || m.OnConflict != "reject" {
		t.Errorf("appended mount: %+v", m)
	}
	if c.Webhook[0].TimeoutSeconds != 5 {
		t.Errorf("appended webhook: %+v", c.Webhook[0])
	}
	if len(c.TextFile) != 1 {
		t.Errorf("textfile: %+v", c.TextFile)
	}
}

func TestOverrideErrors(t *testing.T) {
	c := Default()
	before := c
	cases := []struct {
		path, val string
		want      string
	}{
		{"", "x", "empty path"},
		{"nope", "x", "unknown key"},
		{"http.nope", "x", "unknown key"},
		{"http.token.deeper", "x", "cannot descend"},
		{"http", "x", "section"},
		{"upstream", "x", "section"},
		{"ingest.mount.x.path", "v", "index"},
		{"ingest.mount.-1.path", "v", "index"},
		{"upstream.3.name", "v", "out of range"},
		{"recorder.enabled", "maybe", "boolean"},
		{"http.history_length", "ten", "integer"},
		{"http.history_length", "1.5", "integer"},
		{"metadata.delay_seconds", "soon", "number"},
		{"metadata.delay_seconds", "NaN", "number"},
	}
	for _, tc := range cases {
		err := c.Override(tc.path, tc.val)
		if err == nil {
			t.Errorf("Override(%q,%q): expected error", tc.path, tc.val)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("Override(%q,%q) = %v, want substring %q", tc.path, tc.val, err, tc.want)
		}
	}
	if !reflect.DeepEqual(c, before) {
		t.Errorf("failed overrides mutated config: %+v", c)
	}
}

func TestFromEnvGreedy(t *testing.T) {
	c := Default()
	env := []string{
		"HOME=/root",
		"PATH=/bin",
		// Deliberately out of order: index 1 before 0 must still work.
		"PROKTOR_UPSTREAM_1_NAME=second",
		"PROKTOR_UPSTREAM_0_NAME=first",
		"PROKTOR_UPSTREAM_0_HOLD_SECONDS=15",
		"PROKTOR_UPSTREAM_0_AUTH_STYLE=user_in_password",
		"PROKTOR_UPSTREAM_0_PASSWORD=pw",
		"PROKTOR_EVENT_NAME=Env Night",
		"PROKTOR_METADATA_DELAY_SECONDS=45",
		"PROKTOR_INGEST_MOUNT_0_ON_CONFLICT=takeover",
		"PROKTOR_INGEST_MOUNT_0_PASSWORD=mp",
		"PROKTOR_HTTP_TOKEN=tok=with=equals",
		"PROKTOR_HTTP_HISTORY_LENGTH=7",
		"PROKTOR_RECORDER_MIN_FREE_MB=100",
		"PROKTOR_RECORDER_ENABLED=true",
		"PROKTOR_STATE_JOURNAL=",
		"PROKTOR_CONFIG=/etc/proktor.toml", // CLI's, ignored
	}
	if err := c.FromEnv(env); err != nil {
		t.Fatal(err)
	}
	if c.EventName != "Env Night" || c.Metadata.DelaySeconds != 45 {
		t.Errorf("top: %+v", c)
	}
	if len(c.Upstream) != 2 || c.Upstream[0].Name != "first" || c.Upstream[1].Name != "second" ||
		c.Upstream[0].HoldSeconds != 15 || c.Upstream[0].AuthStyle != "user_in_password" || c.Upstream[0].Password != "pw" {
		t.Errorf("upstream: %+v", c.Upstream)
	}
	if m := c.Ingest.Mount[0]; m.OnConflict != "takeover" || m.Password != "mp" {
		t.Errorf("mount: %+v", m)
	}
	if c.HTTP.Token != "tok=with=equals" || c.HTTP.HistoryLength != 7 {
		t.Errorf("http: %+v", c.HTTP)
	}
	if c.Recorder.MinFreeMB != 100 || !c.Recorder.Enabled || c.State.Journal != "" {
		t.Errorf("recorder/state: %+v %+v", c.Recorder, c.State)
	}
}

func TestFromEnvUnknown(t *testing.T) {
	c := Default()
	err := c.FromEnv([]string{
		"PROKTOR_HTTP_TOKN=x",
		"PROKTOR_NOPE=1",
		"PROKTOR_HTTP=1",            // section, not a value
		"PROKTOR_UPSTREAM_X_NAME=1", // bad index
		"PROKTOR_EVENT_NAME=ok",
		"PROKTOR_CONFIG=/x",
	})
	mustContain(t, err, "PROKTOR_HTTP_TOKN", "PROKTOR_NOPE", "PROKTOR_HTTP,", "PROKTOR_UPSTREAM_X_NAME")
	if strings.Contains(err.Error(), "PROKTOR_CONFIG") || strings.Contains(err.Error(), "PROKTOR_EVENT_NAME") {
		t.Errorf("error mentions valid vars: %v", err)
	}
	if c.EventName != "ok" {
		t.Error("valid vars should still be applied")
	}
}

func TestFromEnvConfigIgnored(t *testing.T) {
	c := Default()
	if err := c.FromEnv([]string{"PROKTOR_CONFIG=/etc/x.toml"}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c, Default()) {
		t.Error("PROKTOR_CONFIG changed config")
	}
}

func TestFromEnvBadValue(t *testing.T) {
	c := Default()
	err := c.FromEnv([]string{"PROKTOR_RECORDER_ENABLED=perhaps"})
	mustContain(t, err, "PROKTOR_RECORDER_ENABLED", "boolean")
}

func TestPaths(t *testing.T) {
	c := Default()
	c.Upstream = []Upstream{{Name: "a"}, {Name: "b"}}
	c.Webhook = []Webhook{{}}
	c.TextFile = []TextFile{{}}
	paths := c.Paths()
	set := map[string]bool{}
	for _, p := range paths {
		if set[p] {
			t.Errorf("duplicate path %q", p)
		}
		set[p] = true
	}
	for _, want := range []string{
		"event_name", "ingest.listen", "ingest.mount.0.path", "ingest.mount.0.password", "ingest.mount.0.on_conflict",
		"metadata.delay_seconds", "metadata.manual_pin",
		"upstream.0.name", "upstream.1.hold_seconds", "upstream.1.buffer_bytes", "upstream.1.disabled",
		"recorder.enabled", "recorder.min_free_mb", "http.token", "http.history_length",
		"webhook.0.url", "webhook.0.timeout_seconds", "textfile.0.format", "state.journal",
	} {
		if !set[want] {
			t.Errorf("Paths() missing %q", want)
		}
	}
	if paths[0] != "event_name" {
		t.Errorf("first path = %q", paths[0])
	}
	if set["upstream.2.name"] || set["ingest.mount.1.path"] {
		t.Error("Paths() listed nonexistent elements")
	}
	// Every listed path is settable and every env name resolves back to it.
	for _, p := range paths {
		cc := c
		cc.Upstream = append([]Upstream(nil), c.Upstream...)
		val := "1"
		if err := cc.Override(p, val); err != nil {
			// bool/float/int accept "1"; strings accept anything.
			t.Errorf("Override(%q): %v", p, err)
		}
		cc2 := c
		cc2.Upstream = append([]Upstream(nil), c.Upstream...)
		if err := cc2.FromEnv([]string{EnvName(p) + "=1"}); err != nil {
			t.Errorf("FromEnv(%s): %v", EnvName(p), err)
		}
	}
}

func TestRedacted(t *testing.T) {
	c := Default()
	c.HTTP.Token = "tok"
	c.Ingest.Mount[0].Password = "mp"
	c.Upstream = []Upstream{{Name: "a", URL: "http://u:secret@h:1/", Password: "up"}, {Name: "b"}}
	c.Webhook = []Webhook{{URL: "http://h/hook"}}
	orig := c
	orig.Upstream = append([]Upstream(nil), c.Upstream...)
	orig.Ingest.Mount = append([]Mount(nil), c.Ingest.Mount...)

	r := c.Redacted()
	if r.HTTP.Token != "***" || r.Ingest.Mount[0].Password != "***" || r.Upstream[0].Password != "***" {
		t.Errorf("secrets not hidden: %+v", r)
	}
	if r.Upstream[1].Password != "" {
		t.Error("empty password should stay empty")
	}
	if strings.Contains(r.Upstream[0].URL, "secret") {
		t.Errorf("URL password leaked: %s", r.Upstream[0].URL)
	}
	if r.Webhook[0].URL != "http://h/hook" {
		t.Errorf("webhook url changed: %s", r.Webhook[0].URL)
	}
	if !reflect.DeepEqual(c, orig) {
		t.Errorf("Redacted mutated the original: %+v", c)
	}
}

// uncomment enables the optional "#key = v" / "#[table]" lines of the example.
var optionalLine = regexp.MustCompile(`(?m)^#(\[|[a-z_]+ = )`)

func TestExampleFile(t *testing.T) {
	path := filepath.Join("..", "..", "proktor.example.toml")
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("example config invalid: %v", err)
	}
	if c.EventName == "" || len(c.TextFile) != 1 || c.Ingest.Mount[0].OnConflict != "takeover" {
		t.Errorf("example not parsed as expected: %+v", c)
	}

	// With every optional (commented-out) example enabled it must still be
	// strictly valid, which also proves it documents only real keys.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	full := optionalLine.ReplaceAll(raw, []byte("$1"))
	c2, err := Load(writeTemp(t, string(full)))
	if err != nil {
		t.Fatalf("example with options enabled: %v", err)
	}
	if err := c2.Validate(); err != nil {
		t.Fatalf("example with options enabled invalid: %v", err)
	}
	if len(c2.Upstream) != 1 || c2.Upstream[0].AuthStyle != "user_in_password" || len(c2.Webhook) != 1 {
		t.Errorf("optional sections not enabled: %+v %+v", c2.Upstream, c2.Webhook)
	}
}
