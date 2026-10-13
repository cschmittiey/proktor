package recorder

import (
	"bufio"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cschmittiey/proktor/internal/ogg"
	"github.com/cschmittiey/proktor/internal/stream"
)

type itemKind uint8

const (
	itemData itemKind = iota
	itemTrack
	itemEnd
)

type item struct {
	kind  itemKind
	data  []byte
	track trackRec
	err   error // itemEnd: why the source ended
	at    time.Time
}

// session records one source connection. The stream.Handler methods run on
// the ingest goroutine; all disk I/O happens on the writer goroutine (run).
type session struct {
	r     *Recorder
	info  stream.SessionInfo
	start time.Time // session start, used for wall-clock offset fallback

	path, jsonlPath, cuePath string
	performer                string
	cueTitle                 string

	// Ingest-goroutine state.
	tl           timeline
	lastDropWarn time.Time

	// Queue shared with the writer goroutine.
	mu          sync.Mutex
	cond        *sync.Cond
	queue       []item
	queuedBytes int
	ending      bool
	limit       int

	bytesWritten atomic.Uint64
	failed       atomic.Bool // a write failed; the file is no longer written
	degraded     atomic.Bool // data was dropped; the file has gaps
	done         chan struct{}

	// Writer-goroutine state.
	f         *os.File
	bw        *bufio.Writer
	jsonl     *os.File
	tracks    []trackRec
	errLogged map[string]bool
}

func newSession(r *Recorder, info stream.SessionInfo, start time.Time, performer string, f *os.File, path string) *session {
	stem := strings.TrimSuffix(path, ".ogg")
	s := &session{
		r:         r,
		info:      info,
		start:     start,
		path:      path,
		jsonlPath: stem + ".tracks.jsonl",
		cuePath:   stem + ".cue",
		performer: performer,
		limit:     r.opts.queueLimit,
		done:      make(chan struct{}),
		f:         f,
		bw:        bufio.NewWriterSize(f, 64<<10),
		errLogged: map[string]bool{},
	}
	if s.performer == "" {
		s.performer = "Unknown"
	}
	mount := strings.Trim(info.Mount, "/")
	if mount == "" {
		mount = "/"
	}
	s.cueTitle = mount + " " + start.Local().Format("2006-01-02")
	s.cond = sync.NewCond(&s.mu)
	return s
}

// OnSegment implements stream.Handler. It never blocks on I/O.
func (s *session) OnSegment(seg ogg.Segment) {
	s.tl.page(seg)
	if len(seg.Data) == 0 {
		return
	}
	switch s.enqueue(item{kind: itemData, data: seg.Data}) {
	case enqDropped:
		s.r.droppedBytes.Add(uint64(len(seg.Data)))
		s.degraded.Store(true)
		now := s.r.clock.Now()
		if s.lastDropWarn.IsZero() || now.Sub(s.lastDropWarn) >= 10*time.Second {
			s.lastDropWarn = now
			s.r.noteError(errDropped)
			s.r.log.Error("recorder: writer cannot keep up, audio is NOT being recorded",
				"path", s.path, "dropped_total", s.r.droppedBytes.Load())
		}
	}
}

// OnTrack implements stream.Handler.
func (s *session) OnTrack(t ogg.Track, at time.Time) {
	off, ok := s.tl.startOf(t.Serial, t.SampleRate)
	if !ok {
		off = at.Sub(s.start)
		if off < 0 {
			off = 0
		}
	}
	comments := t.Comments
	if comments == nil {
		comments = []string{}
	}
	s.enqueue(item{kind: itemTrack, track: trackRec{
		Artist:      t.Artist,
		Title:       t.Title,
		Comments:    comments,
		OffsetBytes: t.Offset,
		Offset:      off,
		DetectedAt:  at.UTC(),
	}})
}

// OnEnd implements stream.Handler.
func (s *session) OnEnd(at time.Time, err error) { s.finish(at, err) }

type enqResult int

const (
	enqOK enqResult = iota
	enqDropped
	enqClosed
)

func (s *session) enqueue(it item) enqResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ending {
		return enqClosed
	}
	if it.kind == itemData {
		if s.queuedBytes > 0 && s.queuedBytes+len(it.data) > s.limit {
			return enqDropped
		}
		s.queuedBytes += len(it.data)
	}
	s.queue = append(s.queue, it)
	s.cond.Signal()
	return enqOK
}

// finish queues the end-of-session marker (idempotent).
func (s *session) finish(at time.Time, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ending {
		return
	}
	s.ending = true
	s.queue = append(s.queue, item{kind: itemEnd, at: at, err: err})
	s.cond.Signal()
}

func (s *session) status() FileStatus {
	s.mu.Lock()
	q := s.queuedBytes
	s.mu.Unlock()
	return FileStatus{
		Path:         s.path,
		Mount:        s.info.Mount,
		BytesWritten: s.bytesWritten.Load(),
		QueuedBytes:  q,
		Failed:       s.failed.Load() || s.degraded.Load(),
	}
}

// run is the writer goroutine.
func (s *session) run() {
	defer close(s.done)
	defer s.r.forget(s)
	var endAt time.Time
	var endErr error
loop:
	for {
		s.mu.Lock()
		for len(s.queue) == 0 {
			s.cond.Wait()
		}
		batch := s.queue
		s.queue = nil
		s.mu.Unlock()

		if g := s.r.opts.writerGate; g != nil {
			g()
		}
		var dataBytes, okBytes int
		ended := false
		for _, it := range batch {
			switch it.kind {
			case itemData:
				dataBytes += len(it.data)
				if s.failed.Load() {
					s.r.droppedBytes.Add(uint64(len(it.data)))
					continue
				}
				if _, err := s.bw.Write(it.data); err != nil {
					s.writeFailed("write", err)
					s.r.droppedBytes.Add(uint64(len(it.data)))
					continue
				}
				okBytes += len(it.data)
			case itemTrack:
				s.tracks = append(s.tracks, it.track)
				s.writeJSONL(it.track)
				s.writeCue()
			case itemEnd:
				endAt, endErr = it.at, it.err
				ended = true
			}
		}
		if !s.failed.Load() {
			if err := s.bw.Flush(); err != nil {
				s.writeFailed("flush", err)
				s.r.droppedBytes.Add(uint64(okBytes))
				okBytes = 0
			}
		}
		if okBytes > 0 && !s.failed.Load() {
			s.bytesWritten.Add(uint64(okBytes))
			s.r.bytesWritten.Add(uint64(okBytes))
		}
		s.mu.Lock()
		s.queuedBytes -= dataBytes
		s.mu.Unlock()
		if ended {
			break loop
		}
	}
	s.finalize(endAt, endErr)
}

func (s *session) finalize(at time.Time, endErr error) {
	if !s.failed.Load() {
		if err := s.bw.Flush(); err != nil {
			s.writeFailed("flush", err)
		}
	}
	if err := s.f.Sync(); err != nil {
		s.writeFailed("sync", err)
	}
	if err := s.f.Close(); err != nil {
		s.writeFailed("close", err)
	}
	if s.jsonl != nil {
		_ = s.jsonl.Sync()
		_ = s.jsonl.Close()
	}
	s.writeCue()
	attrs := []any{"path", s.path, "bytes", s.bytesWritten.Load(), "tracks", len(s.tracks), "session", s.info.ID}
	if endErr != nil {
		attrs = append(attrs, "source_err", endErr)
	}
	s.r.log.Info("recorder: recording finished", attrs...)
}

// writeFailed records a write problem; the first one per kind is logged.
func (s *session) writeFailed(what string, err error) {
	if what == "write" || what == "flush" || what == "sync" || what == "close" {
		s.failed.Store(true)
	}
	s.r.noteError(err)
	if !s.errLogged[what] {
		s.errLogged[what] = true
		s.r.log.Error("recorder: write failed (recording may be incomplete)",
			"op", what, "path", s.path, "err", err)
	}
}

type trackLine struct {
	Artist        string    `json:"artist"`
	Title         string    `json:"title"`
	OffsetBytes   int64     `json:"offset_bytes"`
	OffsetSeconds float64   `json:"offset_seconds"`
	DetectedAt    time.Time `json:"detected_at"`
	Comments      []string  `json:"comments"`
}

func (s *session) writeJSONL(t trackRec) {
	if s.jsonl == nil {
		f, err := os.OpenFile(s.jsonlPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
		if err != nil {
			s.writeFailed("tracks.jsonl", err)
			return
		}
		s.jsonl = f
	}
	b, err := json.Marshal(trackLine{
		Artist:        t.Artist,
		Title:         t.Title,
		OffsetBytes:   t.OffsetBytes,
		OffsetSeconds: math.Round(t.Offset.Seconds()*1000) / 1000,
		DetectedAt:    t.DetectedAt,
		Comments:      t.Comments,
	})
	if err != nil {
		s.writeFailed("tracks.jsonl", err)
		return
	}
	if _, err := s.jsonl.Write(append(b, '\n')); err != nil {
		s.writeFailed("tracks.jsonl", err)
	}
}

// writeCue atomically replaces the .cue sheet. Sessions without tracks get
// no sheet.
func (s *session) writeCue() {
	if len(s.tracks) == 0 {
		return
	}
	sheet := cueSheet{
		Performer: s.performer,
		Title:     s.cueTitle,
		File:      filepath.Base(s.path),
		Tracks:    s.tracks,
	}
	if err := writeFileAtomic(s.cuePath, []byte(sheet.render())); err != nil {
		s.writeFailed("cue", err)
	}
}

func writeFileAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	_, err = tmp.Write(data)
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Chmod(name, 0o644)
	}
	if err == nil {
		err = os.Rename(name, path)
	}
	if err != nil {
		_ = os.Remove(name)
	}
	return err
}
