// Package simulate replays an Ogg capture into proktor's ingest port in real
// time, the way Traktor's broadcast feature would, so the whole pipeline can
// be exercised without Traktor.
package simulate

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cschmittiey/proktor/internal/clock"
	"github.com/cschmittiey/proktor/internal/ogg"
)

// Options configures Run.
type Options struct {
	File     string
	Addr     string // host:port
	Mount    string
	Username string // default "source"
	Password string
	// Protocol is "source" (default, like Traktor) or "put".
	Protocol string
	IceName  string
	// Speed is the playback multiplier: 1 = real time, 0 = as fast as possible.
	Speed float64
	// Loop replays the file forever over a single connection.
	Loop   bool
	Logger *slog.Logger
	// Clock paces the replay; default real time.
	Clock clock.Clock
}

const (
	defaultRate    = 44100
	chunkSize      = 32 << 10
	connectTimeout = 10 * time.Second
	replyTimeout   = 10 * time.Second
	userAgent      = "proktor-simulate/1"
)

// Run connects to the ingest server and streams opts.File until EOF (or
// forever with Loop). It returns nil on EOF and when ctx is cancelled.
func Run(ctx context.Context, opts Options) error {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Clock == nil {
		opts.Clock = clock.Real()
	}
	if opts.Speed < 0 {
		return fmt.Errorf("simulate: negative speed %v", opts.Speed)
	}
	if opts.Username == "" {
		opts.Username = "source"
	}
	if opts.Mount == "" {
		opts.Mount = "/live"
	}
	if !strings.HasPrefix(opts.Mount, "/") {
		opts.Mount = "/" + opts.Mount
	}
	opts.Protocol = strings.ToLower(opts.Protocol)
	switch opts.Protocol {
	case "":
		opts.Protocol = "source"
	case "source", "put":
	default:
		return fmt.Errorf("simulate: unknown protocol %q (want source or put)", opts.Protocol)
	}
	if opts.File == "" {
		return errors.New("simulate: no file given")
	}
	f, err := os.Open(opts.File)
	if err != nil {
		return fmt.Errorf("simulate: %w", err)
	}
	defer f.Close()

	d := net.Dialer{Timeout: connectTimeout}
	conn, err := d.DialContext(ctx, "tcp", opts.Addr)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("simulate: connect: %w", err)
	}
	defer conn.Close()

	// Cancelling ctx (or the server hanging up) unblocks writes by closing
	// the connection.
	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stopClose := context.AfterFunc(runCtx, func() { conn.Close() })
	defer stopClose()

	br, err := handshake(conn, opts)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	opts.Logger.Info("connected", "addr", opts.Addr, "mount", opts.Mount, "protocol", opts.Protocol, "speed", opts.Speed)

	// Notice the server hanging up (takeover, shutdown) even while we are
	// sleeping between pages.
	finished := make(chan struct{})
	go func() {
		_, err := io.Copy(io.Discard, br)
		select {
		case <-finished:
		default:
			if err == nil {
				err = io.EOF
			}
			cancel(fmt.Errorf("server closed the connection: %w", err))
		}
	}()

	p := &player{opts: opts, conn: conn, ctx: runCtx, rates: map[uint32]uint32{}}
	err = p.play(f)
	close(finished)
	if ctx.Err() != nil {
		return nil // interrupted by the caller: not an error
	}
	return err
}

// handshake sends the source request and checks the server's reply. It
// returns the reader positioned after the reply head.
func handshake(conn net.Conn, o Options) (*bufio.Reader, error) {
	var sb strings.Builder
	auth := base64.StdEncoding.EncodeToString([]byte(o.Username + ":" + o.Password))
	if o.Protocol == "put" {
		fmt.Fprintf(&sb, "PUT %s HTTP/1.1\r\nHost: %s\r\n", o.Mount, o.Addr)
	} else {
		fmt.Fprintf(&sb, "SOURCE %s HTTP/1.0\r\n", o.Mount)
	}
	fmt.Fprintf(&sb, "Authorization: Basic %s\r\n", auth)
	fmt.Fprintf(&sb, "User-Agent: %s\r\n", userAgent)
	sb.WriteString("Content-Type: application/ogg\r\n")
	if o.IceName != "" {
		fmt.Fprintf(&sb, "ice-name: %s\r\n", oneLine(o.IceName))
	}
	sb.WriteString("ice-public: 0\r\n")
	if o.Protocol == "put" {
		sb.WriteString("Expect: 100-continue\r\n")
	}
	sb.WriteString("\r\n")

	conn.SetDeadline(time.Now().Add(replyTimeout))
	if _, err := io.WriteString(conn, sb.String()); err != nil {
		return nil, fmt.Errorf("simulate: sending request: %w", err)
	}
	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("simulate: reading server reply: %w", err)
	}
	status = strings.TrimSpace(status)
	f := strings.Fields(status)
	code := 0
	if len(f) >= 2 {
		code, _ = strconv.Atoi(f[1])
	}
	if code != 200 && code != 100 {
		return nil, fmt.Errorf("simulate: server rejected the source: %s", status)
	}
	for { // rest of the reply head
		l, err := br.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("simulate: reading server reply: %w", err)
		}
		if strings.TrimSpace(l) == "" {
			break
		}
	}
	conn.SetDeadline(time.Time{})
	return br, nil
}

func oneLine(s string) string {
	return strings.NewReplacer("\r", " ", "\n", " ").Replace(s)
}

// player streams a file page by page, pacing by audio time.
type player struct {
	opts Options
	conn net.Conn
	ctx  context.Context

	rates map[uint32]uint32 // serial -> sample rate

	start   time.Time // wall time of audio time 0 (set on first paced page)
	started bool
	base    float64 // audio seconds at which the current logical stream starts
	curRate float64
	lastEnd float64 // furthest audio time seen
	sentErr error
}

func (p *player) play(f *os.File) error {
	for iter := 0; ; iter++ {
		if iter > 0 {
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				return fmt.Errorf("simulate: rewinding %s: %w", p.opts.File, err)
			}
			p.opts.Logger.Info("looping", "file", p.opts.File)
		}
		if err := p.playOnce(f); err != nil {
			return err
		}
		if !p.opts.Loop {
			return nil
		}
	}
}

func (p *player) playOnce(f *os.File) error {
	p.rates = map[uint32]uint32{}
	sc := ogg.NewScanner(ogg.Options{
		OnSegment: p.onSegment,
		OnTrack:   p.onTrack,
		OnError:   func(err error) { p.opts.Logger.Debug("ogg parse problem", "err", err) },
	})
	buf := make([]byte, chunkSize)
	for {
		if p.ctx.Err() != nil {
			return context.Cause(p.ctx)
		}
		n, rerr := f.Read(buf)
		if n > 0 {
			sc.Write(buf[:n])
			if p.sentErr != nil {
				return p.sentErr
			}
		}
		if rerr == io.EOF {
			sc.Flush()
			return p.sentErr
		}
		if rerr != nil {
			return fmt.Errorf("simulate: reading %s: %w", p.opts.File, rerr)
		}
	}
}

func (p *player) onTrack(t ogg.Track) {
	p.rates[t.Serial] = t.SampleRate
	name := t.Title
	if t.Artist != "" {
		name = t.Artist + " - " + t.Title
	}
	p.opts.Logger.Info("now sending: "+name, "artist", t.Artist, "title", t.Title)
}

func (p *player) onSegment(seg ogg.Segment) {
	if p.sentErr != nil {
		return
	}
	if seg.Page {
		if seg.BOS {
			// Chained streams restart their granule positions at zero.
			p.base = p.lastEnd
			if r := p.rates[seg.Serial]; r > 0 {
				p.curRate = float64(r)
			} else {
				p.curRate = defaultRate
			}
		}
		if seg.Granule >= 0 && p.opts.Speed > 0 {
			rate := p.curRate
			if r := p.rates[seg.Serial]; r > 0 {
				rate = float64(r)
			}
			if rate == 0 {
				rate = defaultRate
			}
			t := p.base + float64(seg.Granule)/rate
			if t > p.lastEnd {
				p.lastEnd = t
			}
			if err := p.waitUntil(t); err != nil {
				p.sentErr = err
				return
			}
		}
	}
	p.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	if _, err := p.conn.Write(seg.Data); err != nil {
		if cause := context.Cause(p.ctx); cause != nil {
			p.sentErr = cause
		} else {
			p.sentErr = fmt.Errorf("simulate: write: %w", err)
		}
	}
}

// waitUntil sleeps until wall time has caught up with audio time t (seconds
// since the start of playback, scaled by Speed).
func (p *player) waitUntil(t float64) error {
	clk := p.opts.Clock
	if !p.started {
		p.start, p.started = clk.Now(), true
	}
	due := p.start.Add(time.Duration(t / p.opts.Speed * float64(time.Second)))
	d := due.Sub(clk.Now())
	if d <= 0 {
		return nil
	}
	select {
	case <-clk.After(d):
		return nil
	case <-p.ctx.Done():
		return context.Cause(p.ctx)
	}
}
