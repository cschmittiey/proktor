// Package recorder writes every source session to disk, byte for byte, with
// a tracklist sidecar (.tracks.jsonl) and a CUE sheet (.cue) next to it.
//
// Recordings go to <Dir>/<YYYY-MM-DD>/<HHMMSS>-<mount>-<performer>.ogg.
// Each session has its own writer goroutine fed through an in-memory queue,
// so OnSegment never blocks the ingest goroutine; if the disk cannot keep up
// the excess is dropped and reported loudly through Health and the log.
package recorder

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cschmittiey/proktor/internal/clock"
	"github.com/cschmittiey/proktor/internal/health"
	"github.com/cschmittiey/proktor/internal/ogg"
	"github.com/cschmittiey/proktor/internal/stream"
)

// ErrUnsupported is returned by the default DiskFree on platforms without a
// free-space implementation. The recorder then simply skips the disk check.
var ErrUnsupported = errors.New("recorder: free-space check unsupported on this platform")

const (
	defaultMinFreeMB     = 2048
	defaultCheckInterval = 30 * time.Second
	// queueLimitBytes is how much unwritten audio one session may buffer
	// before new segments are dropped.
	queueLimitBytes = 8 << 20
	// warnEvery rate-limits repeated warnings while a problem persists.
	warnEvery = 10 * time.Minute
)

// Options configures a Recorder.
type Options struct {
	Dir       string      // root, e.g. "./recordings"
	Clock     clock.Clock // default clock.Real()
	Logger    *slog.Logger
	MinFreeMB int // disk-low warning threshold, default 2048
	// Performer returns the name to use in the filename for a session (e.g.
	// from the state machine). nil means info.IceName.
	Performer func(info stream.SessionInfo) string
	// DiskFree returns free bytes for a path; the default uses statfs where
	// available and otherwise returns ErrUnsupported.
	DiskFree          func(path string) (uint64, error)
	DiskCheckInterval time.Duration // default 30s

	// Test seams.
	queueLimit     int
	writerGate     func() // called by a session writer after taking a batch
	afterDiskCheck func() // called after every disk check
}

// Recorder is a stream.Consumer that records sessions to disk.
type Recorder struct {
	dir       string
	clock     clock.Clock
	log       *slog.Logger
	minFree   uint64
	performer func(stream.SessionInfo) string
	diskFn    func(string) (uint64, error)
	opts      Options

	bytesWritten atomic.Uint64
	droppedBytes atomic.Uint64
	writeErrors  atomic.Uint64

	mu        sync.Mutex
	closed    bool
	sessions  map[*session]struct{}
	lastErr   string
	lastErrAt time.Time

	diskKnown   bool
	diskFree    uint64
	diskLow     bool
	diskErr     string
	lastLowWarn time.Time

	stopMon  chan struct{}
	monDone  chan struct{}
	closeOne sync.Once
}

// New creates the recording directory and starts the disk monitor.
func New(opts Options) (*Recorder, error) {
	if opts.Dir == "" {
		return nil, errors.New("recorder: Dir is required")
	}
	r := &Recorder{
		dir:       opts.Dir,
		clock:     opts.Clock,
		log:       opts.Logger,
		performer: opts.Performer,
		diskFn:    opts.DiskFree,
		opts:      opts,
		sessions:  make(map[*session]struct{}),
		stopMon:   make(chan struct{}),
		monDone:   make(chan struct{}),
	}
	if r.clock == nil {
		r.clock = clock.Real()
	}
	if r.log == nil {
		r.log = slog.Default()
	}
	if r.diskFn == nil {
		r.diskFn = statfsFree
	}
	mb := opts.MinFreeMB
	if mb <= 0 {
		mb = defaultMinFreeMB
	}
	r.minFree = uint64(mb) << 20
	interval := opts.DiskCheckInterval
	if interval <= 0 {
		interval = defaultCheckInterval
	}
	if opts.queueLimit <= 0 {
		r.opts.queueLimit = queueLimitBytes
	}
	if err := os.MkdirAll(r.dir, 0o755); err != nil {
		return nil, fmt.Errorf("recorder: create dir: %w", err)
	}

	r.checkDisk()
	ticker := r.clock.NewTicker(interval) // created here so tests can rely on it
	go func() {
		defer close(r.monDone)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C():
				r.checkDisk()
			case <-r.stopMon:
				return
			}
		}
	}()
	return r, nil
}

// StartSession implements stream.Consumer.
func (r *Recorder) StartSession(info stream.SessionInfo) stream.Handler {
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return nil
	}

	start := info.StartedAt
	if start.IsZero() {
		start = r.clock.Now()
	}
	local := start.Local()
	performer := info.IceName
	if r.performer != nil {
		performer = r.performer(info)
	}

	f, path, err := r.createUnique(local, slug(info.Mount, "mount"), slug(performer, "unknown"))
	if err != nil {
		r.noteError(fmt.Errorf("cannot create recording file: %w", err))
		r.log.Error("recorder: cannot create recording file; session will not be recorded",
			"mount", info.Mount, "session", info.ID, "err", err)
		return &discardHandler{r: r}
	}

	s := newSession(r, info, start, performer, f, path)
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		_ = f.Close()
		_ = os.Remove(path)
		return nil
	}
	r.sessions[s] = struct{}{}
	r.mu.Unlock()
	go s.run()
	r.log.Info("recorder: recording started", "path", path, "mount", info.Mount, "session", info.ID)
	return s
}

// createUnique creates <dir>/<date>/<HHMMSS>-<mount>-<performer>[-N].ogg
// exclusively and returns the open file.
func (r *Recorder) createUnique(local time.Time, mount, performer string) (*os.File, string, error) {
	dir := filepath.Join(r.dir, local.Format("2006-01-02"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, "", err
	}
	base := fmt.Sprintf("%s-%s-%s", local.Format("150405"), mount, performer)
	for n := 1; ; n++ {
		name := base
		if n > 1 {
			name += "-" + strconv.Itoa(n)
		}
		path := filepath.Join(dir, name+".ogg")
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err == nil {
			return f, path, nil
		}
		if !errors.Is(err, os.ErrExist) || n > 10000 {
			return nil, "", err
		}
	}
}

// noteError records a problem for Health.
func (r *Recorder) noteError(err error) {
	r.writeErrors.Add(1)
	r.mu.Lock()
	r.lastErr = err.Error()
	r.lastErrAt = r.clock.Now()
	r.mu.Unlock()
}

func (r *Recorder) forget(s *session) {
	r.mu.Lock()
	delete(r.sessions, s)
	r.mu.Unlock()
}

// checkDisk samples free space and logs transitions.
func (r *Recorder) checkDisk() {
	free, err := r.diskFn(r.dir)
	now := r.clock.Now()

	r.mu.Lock()
	prevErr := r.diskErr
	wasLow := r.diskLow
	switch {
	case errors.Is(err, ErrUnsupported):
		r.diskKnown, r.diskErr, r.diskLow = false, "", false
	case err != nil:
		r.diskKnown, r.diskErr, r.diskLow = false, err.Error(), false
	default:
		r.diskKnown, r.diskErr, r.diskFree = true, "", free
		r.diskLow = free < r.minFree
	}
	low := r.diskLow
	warn := low && (!wasLow || now.Sub(r.lastLowWarn) >= warnEvery)
	if warn {
		r.lastLowWarn = now
	}
	newErr := r.diskErr
	minFree := r.minFree
	r.mu.Unlock()

	switch {
	case warn:
		r.log.Warn("recorder: DISK SPACE LOW, recordings may fail",
			"free", humanBytes(free), "threshold", humanBytes(minFree), "dir", r.dir)
	case wasLow && !low && newErr == "":
		r.log.Info("recorder: disk space recovered", "free", humanBytes(free))
	}
	if newErr != "" && newErr != prevErr {
		r.log.Warn("recorder: cannot determine free disk space", "dir", r.dir, "err", newErr)
	}
	if r.opts.afterDiskCheck != nil {
		r.opts.afterDiskCheck()
	}
}

// FileStatus is the health detail for one active recording.
type FileStatus struct {
	Path         string `json:"path"`
	Mount        string `json:"mount"`
	BytesWritten uint64 `json:"bytes_written"`
	QueuedBytes  int    `json:"queued_bytes"`
	Failed       bool   `json:"failed,omitempty"`
}

// Detail is the recorder's health detail.
type Detail struct {
	Dir          string       `json:"dir"`
	Files        []FileStatus `json:"files"`
	BytesWritten uint64       `json:"bytes_written"`
	DroppedBytes uint64       `json:"dropped_bytes"`
	WriteErrors  uint64       `json:"write_errors"`
	LastError    string       `json:"last_error,omitempty"`
	FreeBytes    *uint64      `json:"free_bytes,omitempty"`
	MinFreeBytes uint64       `json:"min_free_bytes"`
	DiskError    string       `json:"disk_error,omitempty"`
}

// Health reports recorder status: not OK while disk space is low, while
// writes are failing or dropping data, or for 10 minutes after the last
// such error.
func (r *Recorder) Health() health.Status {
	r.mu.Lock()
	sessions := make([]*session, 0, len(r.sessions))
	for s := range r.sessions {
		sessions = append(sessions, s)
	}
	d := Detail{
		Dir:          r.dir,
		Files:        []FileStatus{},
		MinFreeBytes: r.minFree,
		LastError:    r.lastErr,
		DiskError:    r.diskErr,
	}
	low, diskKnown, free := r.diskLow, r.diskKnown, r.diskFree
	recentErr := !r.lastErrAt.IsZero() && r.clock.Now().Sub(r.lastErrAt) < warnEvery
	lastErr := r.lastErr
	r.mu.Unlock()

	st := health.Status{OK: true}
	failing := false
	for _, s := range sessions {
		fs := s.status()
		failing = failing || fs.Failed
		d.Files = append(d.Files, fs)
	}
	sort.Slice(d.Files, func(i, j int) bool { return d.Files[i].Path < d.Files[j].Path })
	d.BytesWritten = r.bytesWritten.Load()
	d.DroppedBytes = r.droppedBytes.Load()
	d.WriteErrors = r.writeErrors.Load()
	if diskKnown {
		d.FreeBytes = &free
	}

	if low {
		st.OK = false
		st.Warnings = append(st.Warnings, fmt.Sprintf("disk low: %s free (threshold %s)",
			humanBytes(free), humanBytes(r.minFree)))
	}
	if d.DiskError != "" {
		st.OK = false
		st.Warnings = append(st.Warnings, "cannot check free disk space: "+d.DiskError)
	}
	if failing || recentErr {
		st.OK = false
		msg := fmt.Sprintf("recording errors: %d write error(s)", d.WriteErrors)
		if d.DroppedBytes > 0 {
			msg += fmt.Sprintf(", %s of audio NOT recorded", humanBytes(d.DroppedBytes))
		}
		if lastErr != "" {
			msg += ", last: " + lastErr
		}
		st.Warnings = append(st.Warnings, msg)
	}
	st.Detail = d
	return st
}

// Close flushes and closes every recording and stops the disk monitor.
func (r *Recorder) Close() error {
	r.closeOne.Do(func() {
		r.mu.Lock()
		r.closed = true
		sessions := make([]*session, 0, len(r.sessions))
		for s := range r.sessions {
			sessions = append(sessions, s)
		}
		r.mu.Unlock()

		close(r.stopMon)
		<-r.monDone
		now := r.clock.Now()
		for _, s := range sessions {
			s.finish(now, nil)
		}
		for _, s := range sessions {
			<-s.done
		}
	})
	return nil
}

// discardHandler stands in for a session that could not be opened.
type discardHandler struct{ r *Recorder }

func (h *discardHandler) OnSegment(seg ogg.Segment)    { h.r.droppedBytes.Add(uint64(len(seg.Data))) }
func (h *discardHandler) OnTrack(ogg.Track, time.Time) {}
func (h *discardHandler) OnEnd(time.Time, error)       {}

func humanBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := uint64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

var errDropped = errors.New("writer cannot keep up; audio dropped")
