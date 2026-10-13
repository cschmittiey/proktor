// Command proktor sits between a Traktor broadcast and the outside world:
// it extracts now-playing metadata, serves it over HTTP, relays the audio
// upstream and records the night. See SPEC.md and README.md.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cschmittiey/proktor/internal/api"
	"github.com/cschmittiey/proktor/internal/clock"
	"github.com/cschmittiey/proktor/internal/config"
	"github.com/cschmittiey/proktor/internal/health"
	"github.com/cschmittiey/proktor/internal/ingest"
	"github.com/cschmittiey/proktor/internal/outputs"
	"github.com/cschmittiey/proktor/internal/recorder"
	"github.com/cschmittiey/proktor/internal/relay"
	"github.com/cschmittiey/proktor/internal/simulate"
	"github.com/cschmittiey/proktor/internal/state"
	"github.com/cschmittiey/proktor/internal/stream"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

const usage = `proktor %s — Traktor broadcast proxy

usage:
  proktor [serve] [flags]          run the proxy (default)
  proktor simulate [flags] <file>  replay an Ogg capture into a running proxy
  proktor config [flags]           print the effective config (secrets redacted)
                                   and every overridable key
  proktor version

Config precedence: defaults < config file < PROKTOR_* env < flags.
Every key can be set with -set key=value (repeatable), e.g.
  -set metadata.delay_seconds=45 -set upstream.0.password=hunter2
or with env, e.g. PROKTOR_HTTP_TOKEN=... PROKTOR_UPSTREAM_0_PASSWORD=...

Run "proktor <command> -h" for a command's flags.
`

func main() {
	args := os.Args[1:]
	cmd := "serve"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, args = args[0], args[1:]
	}
	var err error
	switch cmd {
	case "serve":
		err = runServe(args)
	case "simulate":
		err = runSimulate(args)
	case "config":
		err = runConfig(args)
	case "version":
		fmt.Println(version)
	case "help":
		fmt.Printf(usage, version)
	default:
		fmt.Fprintf(os.Stderr, usage, version)
		err = fmt.Errorf("unknown command %q", cmd)
	}
	if err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stderr, "proktor:", err)
		}
		os.Exit(2)
	}
}

// setFlags collects repeated -set key=value flags.
type setFlags []string

func (s *setFlags) String() string     { return strings.Join(*s, ",") }
func (s *setFlags) Set(v string) error { *s = append(*s, v); return nil }

// configFlags registers the flags shared by serve and config and returns a
// function that builds the effective config once flags are parsed.
func configFlags(fs *flag.FlagSet) func() (config.Config, error) {
	path := fs.String("config", os.Getenv("PROKTOR_CONFIG"), "config file (TOML); env PROKTOR_CONFIG")
	var sets setFlags
	fs.Var(&sets, "set", "override a config key, key=value (repeatable)")
	// Shortcuts for the most common on-site tweaks; each is just a -set.
	shortcuts := []struct{ name, key, help string }{
		{"event", "event_name", "event name"},
		{"ingest", "ingest.listen", "ingest listen address (Traktor connects here)"},
		{"http", "http.listen", "HTTP API listen address"},
		{"token", "http.token", "bearer token for POST /now"},
		{"delay", "metadata.delay_seconds", "seconds before a detected track goes live"},
		{"password", "ingest.mount.0.password", "source password for the first mount"},
		{"record", "recorder.enabled", "record sets to disk (true/false)"},
		{"recordings", "recorder.dir", "recordings directory"},
	}
	vals := make([]*string, len(shortcuts))
	for i, sc := range shortcuts {
		vals[i] = fs.String(sc.name, "", sc.help+" (= -set "+sc.key+"=...)")
	}
	return func() (config.Config, error) {
		cfg, err := config.Load(*path)
		if err != nil {
			return cfg, err
		}
		if err := cfg.FromEnv(os.Environ()); err != nil {
			return cfg, err
		}
		set := map[string]bool{}
		fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
		for i, sc := range shortcuts {
			if set[sc.name] {
				if err := cfg.Override(sc.key, *vals[i]); err != nil {
					return cfg, fmt.Errorf("-%s: %w", sc.name, err)
				}
			}
		}
		for _, kv := range sets {
			k, v, ok := strings.Cut(kv, "=")
			if !ok {
				return cfg, fmt.Errorf("-set %q: want key=value", kv)
			}
			if err := cfg.Override(strings.TrimSpace(k), v); err != nil {
				return cfg, fmt.Errorf("-set %s: %w", k, err)
			}
		}
		return cfg, cfg.Validate()
	}
}

func runConfig(args []string) error {
	fs := flag.NewFlagSet("config", flag.ContinueOnError)
	build := configFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := build()
	if err != nil {
		return err
	}
	out, _ := json.MarshalIndent(cfg.Redacted(), "", "  ")
	fmt.Println(string(out))
	fmt.Println("\noverridable keys (-set key=value, or PROKTOR_KEY_WITH_UNDERSCORES):")
	for _, p := range cfg.Paths() {
		fmt.Println("  " + p)
	}
	return nil
}

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	build := configFlags(fs)
	verbose := fs.Bool("v", false, "debug logging")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	cfg, err := build()
	if err != nil {
		return err
	}

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return serve(ctx, cfg, logger)
}

func serve(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	clk := clock.Real()

	machine, err := state.New(state.Options{
		Clock:         clk,
		EventName:     cfg.EventName,
		Delay:         time.Duration(cfg.Metadata.DelaySeconds * float64(time.Second)),
		ManualPin:     cfg.Metadata.ManualPin,
		HistoryLength: cfg.HTTP.HistoryLength,
		JournalPath:   cfg.State.Journal,
		Logger:        logger.With("component", "state"),
	})
	if err != nil {
		return fmt.Errorf("state: %w", err)
	}
	defer machine.Close()

	consumers := []stream.Consumer{machine.Consumer()}
	checks := map[string]health.Checker{}

	rel := relay.New(cfg.Upstream, relay.Options{Clock: clk, Logger: logger.With("component", "relay"), UserAgent: "proktor/" + version})
	defer rel.Close()
	consumers = append(consumers, rel)
	checks["relay"] = rel

	if cfg.Recorder.Enabled {
		rec, err := recorder.New(recorder.Options{
			Dir:       cfg.Recorder.Dir,
			Clock:     clk,
			Logger:    logger.With("component", "recorder"),
			MinFreeMB: cfg.Recorder.MinFreeMB,
			Performer: func(info stream.SessionInfo) string {
				if info.IceName != "" {
					return info.IceName
				}
				if p := machine.Snapshot().Performer; p != nil {
					return *p
				}
				return ""
			},
		})
		if err != nil {
			return fmt.Errorf("recorder: %w", err)
		}
		defer rec.Close()
		consumers = append(consumers, rec)
		checks["recorder"] = rec
	}

	ing := ingest.New(ingest.Options{
		Mounts:    cfg.Ingest.Mount,
		Consumers: consumers,
		Clock:     clk,
		Logger:    logger.With("component", "ingest"),
	})
	checks["ingest"] = ing

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errc := make(chan error, 8)

	if len(cfg.Webhook) > 0 {
		hooks := outputs.NewWebhooks(cfg.Webhook, &http.Client{}, logger.With("component", "webhooks"))
		checks["webhooks"] = hooks
		go func() { errc <- wrap("webhooks", hooks.Run(ctx, machine)) }()
	}
	for _, tf := range cfg.TextFile {
		go func() {
			errc <- wrap("textfile "+tf.Path, outputs.RunTextFile(ctx, machine, tf, logger.With("component", "textfile")))
		}()
	}

	ingLn, err := net.Listen("tcp", cfg.Ingest.Listen)
	if err != nil {
		return fmt.Errorf("ingest listen: %w", err)
	}
	httpLn, err := net.Listen("tcp", cfg.HTTP.Listen)
	if err != nil {
		ingLn.Close()
		return fmt.Errorf("http listen: %w", err)
	}

	srv := &http.Server{
		Handler: api.New(api.Options{
			Backend: machine,
			Token:   cfg.HTTP.Token,
			Health:  checks,
			Clock:   clk,
			Logger:  logger.With("component", "api"),
		}),
		ReadHeaderTimeout: 10 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	go func() { errc <- wrap("ingest", ing.Serve(ctx, ingLn)) }()
	go func() {
		err := srv.Serve(httpLn)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errc <- wrap("http", err)
	}()

	if cfg.HTTP.Token == "" {
		logger.Warn("http.token is empty: POST /now and the control page are read-only")
	}
	printBoothCard(os.Stderr, cfg)

	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("shutting down")
	case runErr = <-errc:
		if runErr != nil {
			logger.Error("component failed, shutting down", "err", runErr)
		}
	}
	cancel()
	shutdownCtx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	_ = srv.Shutdown(shutdownCtx)
	return runErr
}

func wrap(what string, err error) error {
	if err == nil || errors.Is(err, context.Canceled) {
		return nil
	}
	return fmt.Errorf("%s: %w", what, err)
}

// printBoothCard prints the DJ-facing Traktor settings with this machine's
// LAN addresses filled in, so whoever sets up the booth can copy it.
func printBoothCard(w io.Writer, cfg config.Config) {
	ingHost, ingPort, _ := net.SplitHostPort(cfg.Ingest.Listen)
	_, httpPort, _ := net.SplitHostPort(cfg.HTTP.Listen)
	var ips []string
	if ip := net.ParseIP(ingHost); ip != nil && !ip.IsUnspecified() {
		ips = []string{ingHost}
	} else {
		ips = lanIPs()
	}
	if len(ips) == 0 {
		ips = []string{"<proktor IP>"}
	}
	m := cfg.Ingest.Mount[0]
	pw := m.Password
	if pw == "" {
		pw = "(anything)"
	}
	fmt.Fprintf(w, `
  ┌─ Traktor → Preferences → Broadcasting ──────────────
  │ Server:   %s
  │ Port:     %s   Mount: %s   Password: %s
  │ Format:   Ogg Vorbis, 192 kbps recommended
  │ Stream name: your DJ name
  │ Then hit the broadcast (antenna) button.
  └─ control page: http://%s:%s/ ─────────────────────────
`, strings.Join(ips, " or "), ingPort, m.Path, pw, ips[0], httpPort)
}

func lanIPs() []string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	var out []string
	for _, a := range addrs {
		ipn, ok := a.(*net.IPNet)
		if !ok || ipn.IP.IsLoopback() || ipn.IP.To4() == nil || ipn.IP.IsLinkLocalUnicast() {
			continue
		}
		out = append(out, ipn.IP.String())
	}
	return out
}

func runSimulate(args []string) error {
	fs := flag.NewFlagSet("simulate", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: proktor simulate [flags] <file.ogg>\n\nReplays an Ogg capture into proktor's ingest port in real time, like Traktor would.")
		fs.PrintDefaults()
	}
	opts := simulate.Options{}
	fs.StringVar(&opts.Addr, "to", "127.0.0.1:8000", "proktor ingest address")
	fs.StringVar(&opts.Mount, "mount", "/live", "mount")
	fs.StringVar(&opts.Username, "user", "source", "username")
	fs.StringVar(&opts.Password, "password", "", "source password")
	fs.StringVar(&opts.Protocol, "protocol", "source", "source protocol: source (Traktor) or put")
	fs.StringVar(&opts.IceName, "name", "simulated dj", "stream name (ice-name), used as performer")
	fs.Float64Var(&opts.Speed, "speed", 1, "playback speed multiplier (0 = as fast as possible)")
	fs.BoolVar(&opts.Loop, "loop", false, "loop the file forever")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return errors.New("simulate needs exactly one file")
	}
	opts.File = fs.Arg(0)
	opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return wrap("simulate", simulate.Run(ctx, opts))
}
