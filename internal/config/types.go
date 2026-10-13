// Package config loads proktor's TOML config and applies CLI/env overrides.
package config

// Config is the whole config file.
type Config struct {
	EventName string     `toml:"event_name"`
	Ingest    Ingest     `toml:"ingest"`
	Metadata  Metadata   `toml:"metadata"`
	Upstream  []Upstream `toml:"upstream"`
	Recorder  Recorder   `toml:"recorder"`
	HTTP      HTTP       `toml:"http"`
	Webhook   []Webhook  `toml:"webhook"`
	TextFile  []TextFile `toml:"textfile"`
	State     StateConf  `toml:"state"`
}

type Ingest struct {
	Listen string  `toml:"listen"` // default "0.0.0.0:8000"
	Mount  []Mount `toml:"mount"`  // default: one mount "/live", no password
}

type Mount struct {
	Path       string `toml:"path"`
	Password   string `toml:"password"`    // empty = no auth
	OnConflict string `toml:"on_conflict"` // "reject" (default) | "takeover"
}

type Metadata struct {
	DelaySeconds float64 `toml:"delay_seconds"` // default 0
	ManualPin    string  `toml:"manual_pin"`    // "until_next_auto" (default) | "until_cleared"
}

type Upstream struct {
	Name     string `toml:"name"`
	URL      string `toml:"url"`      // http://host:port/ (scheme+host+port; path ignored if Mount set)
	Mount    string `toml:"mount"`    // default "/"
	Username string `toml:"username"` // default "source"
	Password string `toml:"password"`
	// AuthStyle is "basic" (default: Basic username:password) or
	// "user_in_password" (AzuraCast/Liquidsoap workaround: Basic
	// source:"username:password").
	AuthStyle   string  `toml:"auth_style"`
	Protocol    string  `toml:"protocol"`     // "put" (default) | "source"
	ContentType string  `toml:"content_type"` // default "application/ogg"
	HoldSeconds float64 `toml:"hold_seconds"` // default 0
	// SourceMount is the ingest mount to relay. Empty = every mount.
	SourceMount string `toml:"source_mount"`
	// BufferBytes bounds the per-upstream queue. Default 4 MiB.
	BufferBytes int  `toml:"buffer_bytes"`
	Disabled    bool `toml:"disabled"`
}

type Recorder struct {
	Enabled bool   `toml:"enabled"`
	Dir     string `toml:"dir"` // default "./recordings"
	// MinFreeMB triggers a disk-low warning below this. Default 2048.
	MinFreeMB int `toml:"min_free_mb"`
}

type HTTP struct {
	Listen        string `toml:"listen"`         // default "0.0.0.0:8080"
	Token         string `toml:"token"`          // required for POST /now; empty = writes disabled
	HistoryLength int    `toml:"history_length"` // default 10
}

type Webhook struct {
	URL            string  `toml:"url"`
	TimeoutSeconds float64 `toml:"timeout_seconds"` // default 5
}

// TextFile writes the now-playing state to a file using a Go text/template
// rendered against state.State, e.g. "{{with .Track}}{{.Artist}} - {{.Title}}{{end}}".
type TextFile struct {
	Path   string `toml:"path"`
	Format string `toml:"format"` // default: artist - title, or performer, or empty
}

type StateConf struct {
	// Journal is the append-only JSON-lines state file. Default
	// "./proktor-state.jsonl". Empty string after overrides disables it.
	Journal string `toml:"journal"`
}
