package recorder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cschmittiey/proktor/internal/clock"
	"github.com/cschmittiey/proktor/internal/ogg"
	"github.com/cschmittiey/proktor/internal/stream"
)

// ---- helpers ----

type logRec struct {
	level slog.Level
	msg   string
}

type logCap struct {
	mu   sync.Mutex
	recs []logRec
}

func (l *logCap) Enabled(context.Context, slog.Level) bool { return true }
func (l *logCap) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	l.recs = append(l.recs, logRec{r.Level, r.Message})
	l.mu.Unlock()
	return nil
}
func (l *logCap) WithAttrs([]slog.Attr) slog.Handler { return l }
func (l *logCap) WithGroup(string) slog.Handler      { return l }
func (l *logCap) count(level slog.Level, sub string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, r := range l.recs {
		if r.level == level && strings.Contains(r.msg, sub) {
			n++
		}
	}
	return n
}

var t0 = time.Date(2026, 10, 13, 22, 14, 5, 0, time.UTC)

func newRec(t *testing.T, o Options) (*Recorder, *logCap) {
	t.Helper()
	lc := &logCap{}
	if o.Dir == "" {
		o.Dir = filepath.Join(t.TempDir(), "rec")
	}
	if o.Logger == nil {
		o.Logger = slog.New(lc)
	}
	r, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	return r, lc
}

func info(mount, ice string) stream.SessionInfo {
	return stream.SessionInfo{ID: "s1", Mount: mount, IceName: ice, StartedAt: t0}
}

// pageSeg builds a fake page segment (content is not parsed by the recorder).
func pageSeg(off int64, serial uint32, bos bool, granule int64, n int) ogg.Segment {
	d := bytes.Repeat([]byte{byte(off), byte(serial)}, n/2+1)[:n]
	return ogg.Segment{Data: d, Offset: off, Page: true, BOS: bos, Serial: serial, Granule: granule}
}

func rawSeg(off int64, s string) ogg.Segment {
	return ogg.Segment{Data: []byte(s), Offset: off, Granule: -1}
}

func readFile(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func wantPath(r *Recorder, local time.Time, name string) string {
	return filepath.Join(r.dir, local.Format("2006-01-02"), local.Format("150405")+"-"+name+".ogg")
}

// ---- tests ----

func TestSlug(t *testing.T) {
	for _, c := range []struct{ in, fb, want string }{
		{"/live", "mount", "live"},
		{"DJ Example", "unknown", "DJ-Example"},
		{"", "unknown", "unknown"},
		{"///", "mount", "mount"},
		{"  a//b..c  ", "x", "a-b-c"},
		{"Björk & Sigur Rós", "x", "Björk-Sigur-Rós"},
		{"../../etc/passwd", "x", "etc-passwd"},
		{"日本語 DJ", "x", "日本語-DJ"},
		{"a\x00b\nc", "x", "a-b-c"},
		{strings.Repeat("x", 100), "x", strings.Repeat("x", 60)},
		{strings.Repeat("ab ", 40), "x", strings.TrimRight(strings.Repeat("ab-", 40)[:60], "-")},
	} {
		if got := slug(c.in, c.fb); got != c.want {
			t.Errorf("slug(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestFileNamingAndCollision(t *testing.T) {
	r, _ := newRec(t, Options{Performer: func(i stream.SessionInfo) string { return "DJ Example / " + i.IceName }})
	local := t0.Local()
	var paths []string
	for range 3 {
		h := r.StartSession(info("/live", "x"))
		if h == nil {
			t.Fatal("nil handler")
		}
		paths = append(paths, h.(*session).path)
	}
	base := "live-DJ-Example-x"
	want := []string{wantPath(r, local, base), wantPath(r, local, base+"-2"), wantPath(r, local, base+"-3")}
	for i := range want {
		if paths[i] != want[i] {
			t.Errorf("path %d = %s, want %s", i, paths[i], want[i])
		}
		if _, err := os.Stat(paths[i]); err != nil {
			t.Error(err)
		}
	}
}

func TestDefaultPerformerIsIceName(t *testing.T) {
	r, _ := newRec(t, Options{})
	h := r.StartSession(info("/live", "Ice Name"))
	if got, want := h.(*session).path, wantPath(r, t0.Local(), "live-Ice-Name"); got != want {
		t.Errorf("path = %s, want %s", got, want)
	}
	h2 := r.StartSession(info("/", ""))
	if got, want := h2.(*session).path, wantPath(r, t0.Local(), "mount-unknown"); got != want {
		t.Errorf("path = %s, want %s", got, want)
	}
}

func TestBytesInEqualBytesOut(t *testing.T) {
	r, _ := newRec(t, Options{})
	h := r.StartSession(info("/live", "dj"))
	path := h.(*session).path

	var want bytes.Buffer
	var off int64
	feed := func(s ogg.Segment) {
		s.Offset = off
		off += int64(len(s.Data))
		want.Write(s.Data)
		h.OnSegment(s)
	}
	feed(rawSeg(0, "garbage before first page"))
	feed(pageSeg(0, 1, true, 0, 58))
	for i := range 200 {
		feed(pageSeg(0, 1, false, int64(i+1)*1024, 4000+i))
		if i%50 == 0 {
			feed(rawSeg(0, "\x00\x01 junk between pages"))
		}
	}
	feed(pageSeg(0, 2, true, 0, 80))
	feed(rawSeg(0, "trailing partial OggS"))
	h.OnEnd(t0.Add(time.Minute), nil)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, path); got != want.String() {
		t.Fatalf("file differs: got %d bytes, want %d", len(got), want.Len())
	}
	st := r.Health()
	d := st.Detail.(Detail)
	if d.BytesWritten != uint64(want.Len()) || len(d.Files) != 0 {
		t.Errorf("detail = %+v", d)
	}
	if !st.OK {
		t.Errorf("health not OK: %+v", st)
	}
	// No cue/jsonl without tracks.
	for _, ext := range []string{".cue", ".tracks.jsonl"} {
		if _, err := os.Stat(strings.TrimSuffix(path, ".ogg") + ext); err == nil {
			t.Errorf("unexpected %s", ext)
		}
	}
}

func TestTracksAndCueAcrossChainedStreams(t *testing.T) {
	r, _ := newRec(t, Options{Performer: func(stream.SessionInfo) string { return "DJ Example" }})
	h := r.StartSession(info("/live", ""))
	path := h.(*session).path

	var off int64
	feed := func(s ogg.Segment) int64 {
		start := off
		s.Offset = off
		off += int64(len(s.Data))
		h.OnSegment(s)
		return start
	}
	at := func(sec int) time.Time { return t0.Add(time.Duration(sec) * 10 * time.Second) } // wall clock deliberately unrelated

	// Stream 1: 44.1 kHz, 2 s.
	o1 := feed(pageSeg(0, 1, true, 0, 60))
	feed(pageSeg(0, 1, false, 0, 100))
	h.OnTrack(ogg.Track{Artist: "New Order", Title: "Blue Monday", Serial: 1, Offset: o1, SampleRate: 44100,
		Comments: []string{"ARTIST=New Order", "TITLE=Blue Monday"}}, at(1))
	feed(pageSeg(0, 1, false, 44100, 500))
	feed(pageSeg(0, 1, false, 88200, 500))
	feed(rawSeg(0, "noise"))
	// Stream 2: 48 kHz, 3 s.
	o2 := feed(pageSeg(0, 2, true, 0, 60))
	h.OnTrack(ogg.Track{Artist: "A", Title: `Say "Hi"`, Serial: 2, Offset: o2, SampleRate: 48000}, at(2))
	feed(pageSeg(0, 2, false, 48000, 500))
	feed(pageSeg(0, 2, false, -1, 500)) // no packet ends here
	feed(pageSeg(0, 2, false, 144000, 500))
	feed(pageSeg(0, 2, false, 168000, 500)) // 3.5 s
	// Stream 3 begins at 2 + 3.5 = 5.5 s.
	o3 := feed(pageSeg(0, 3, true, 0, 60))
	h.OnTrack(ogg.Track{Artist: "B", Title: "C", Serial: 3, Offset: o3, SampleRate: 44100}, at(3))
	feed(pageSeg(0, 3, false, 44100, 500))
	h.OnEnd(at(4), nil)
	r.Close()

	base := strings.TrimSuffix(path, ".ogg")
	lines := strings.Split(strings.TrimSpace(readFile(t, base+".tracks.jsonl")), "\n")
	if len(lines) != 3 {
		t.Fatalf("jsonl lines = %d", len(lines))
	}
	type line struct {
		Artist, Title string
		OffsetBytes   int64     `json:"offset_bytes"`
		OffsetSeconds float64   `json:"offset_seconds"`
		DetectedAt    time.Time `json:"detected_at"`
		Comments      []string
	}
	wantL := []line{
		{"New Order", "Blue Monday", o1, 0, at(1).UTC(), []string{"ARTIST=New Order", "TITLE=Blue Monday"}},
		{"A", `Say "Hi"`, o2, 2, at(2).UTC(), []string{}},
		{"B", "C", o3, 5.5, at(3).UTC(), []string{}},
	}
	for i, l := range lines {
		var got line
		if err := json.Unmarshal([]byte(l), &got); err != nil {
			t.Fatal(err)
		}
		w := wantL[i]
		if got.Artist != w.Artist || got.Title != w.Title || got.OffsetBytes != w.OffsetBytes ||
			got.OffsetSeconds != w.OffsetSeconds || !got.DetectedAt.Equal(w.DetectedAt) ||
			strings.Join(got.Comments, "|") != strings.Join(w.Comments, "|") || got.Comments == nil {
			t.Errorf("line %d = %+v, want %+v", i, got, w)
		}
	}
	if !strings.Contains(lines[1], `"comments":[]`) || !strings.Contains(lines[0], `"detected_at":"`) {
		t.Errorf("unexpected json shape: %s", lines[1])
	}

	wantCue := `PERFORMER "DJ Example"
TITLE "live ` + t0.Local().Format("2006-01-02") + `"
FILE "` + filepath.Base(path) + `" OGG
  TRACK 01 AUDIO
    TITLE "Blue Monday"
    PERFORMER "New Order"
    INDEX 01 00:00:00
  TRACK 02 AUDIO
    TITLE "Say \"Hi\""
    PERFORMER "A"
    INDEX 01 00:02:00
  TRACK 03 AUDIO
    TITLE "C"
    PERFORMER "B"
    INDEX 01 00:05:37
`
	if got := readFile(t, base+".cue"); got != wantCue {
		t.Errorf("cue mismatch:\n%s\nwant:\n%s", got, wantCue)
	}
	ents, _ := os.ReadDir(filepath.Dir(path))
	if len(ents) != 3 {
		var names []string
		for _, e := range ents {
			names = append(names, e.Name())
		}
		t.Errorf("unexpected files (temp left behind?): %v", names)
	}
}

func TestOffsetFallsBackToWallClock(t *testing.T) {
	r, _ := newRec(t, Options{})
	h := r.StartSession(info("/live", "dj"))
	path := h.(*session).path
	// No pages at all: granules unusable.
	h.OnSegment(rawSeg(0, "0123456789"))
	h.OnTrack(ogg.Track{Artist: "A", Title: "T", Serial: 9, Offset: 1000}, t0.Add(90*time.Second))
	// An earlier clock than the start must clamp to zero.
	h.OnTrack(ogg.Track{Artist: "B", Title: "U", Serial: 9, Offset: 2000}, t0.Add(-time.Hour))
	h.OnEnd(t0.Add(time.Hour), errors.New("boom"))
	r.Close()
	base := strings.TrimSuffix(path, ".ogg")
	jl := readFile(t, base+".tracks.jsonl")
	if !strings.Contains(jl, `"offset_seconds":90,`) || !strings.Contains(jl, `"offset_seconds":0,`) {
		t.Errorf("jsonl: %s", jl)
	}
	if !strings.Contains(readFile(t, base+".cue"), "INDEX 01 01:30:00") {
		t.Errorf("cue: %s", readFile(t, base+".cue"))
	}
}

func TestOffsetFallsBackAfterUnusableStream(t *testing.T) {
	r, _ := newRec(t, Options{})
	h := r.StartSession(info("/live", "dj"))
	path := h.(*session).path
	// Stream 1 never reports a granule, so stream 2 cannot be placed by
	// audio time and the wall clock (30 s after session start) is used.
	h.OnSegment(pageSeg(0, 1, true, -1, 50))
	h.OnSegment(pageSeg(0, 1, false, -1, 50))
	h.OnSegment(pageSeg(0, 2, true, -1, 50))
	h.OnTrack(ogg.Track{Artist: "A", Title: "T", Serial: 2, Offset: 100, SampleRate: 44100}, t0.Add(30*time.Second))
	r.Close()
	jl := readFile(t, strings.TrimSuffix(path, ".ogg")+".tracks.jsonl")
	if !strings.Contains(jl, `"offset_seconds":30,`) {
		t.Errorf("jsonl: %s", jl)
	}
}

func TestCloseFlushesWithoutOnEnd(t *testing.T) {
	r, _ := newRec(t, Options{})
	h := r.StartSession(info("/live", "dj"))
	path := h.(*session).path
	h.OnSegment(rawSeg(0, "hello "))
	h.OnSegment(rawSeg(6, "world"))
	h.OnTrack(ogg.Track{Artist: "A", Title: "T", Serial: 1}, t0.Add(time.Second))
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, path); got != "hello world" {
		t.Errorf("file = %q", got)
	}
	if _, err := os.Stat(strings.TrimSuffix(path, ".ogg") + ".cue"); err != nil {
		t.Error(err)
	}
	// Late calls after Close are harmless.
	h.OnSegment(rawSeg(11, "late"))
	h.OnEnd(t0, nil)
	if r.StartSession(info("/live", "x")) != nil {
		t.Error("StartSession after Close should return nil")
	}
	if err := r.Close(); err != nil {
		t.Error(err)
	}
}

func TestDiskLowWarning(t *testing.T) {
	fc := clock.NewFake(t0)
	var free atomic.Uint64
	free.Store(1 << 30)
	checked := make(chan struct{}, 1000)
	r, lc := newRec(t, Options{
		Clock:          fc,
		MinFreeMB:      100,
		DiskFree:       func(string) (uint64, error) { return free.Load(), nil },
		afterDiskCheck: func() { checked <- struct{}{} },
	})
	<-checked // initial check
	tick := func() {
		t.Helper()
		fc.Advance(30 * time.Second)
		select {
		case <-checked:
		case <-time.After(10 * time.Second):
			t.Fatal("disk check did not run")
		}
	}
	st := r.Health()
	if !st.OK || st.Detail.(Detail).FreeBytes == nil || *st.Detail.(Detail).FreeBytes != 1<<30 {
		t.Fatalf("initial health = %+v", st)
	}

	free.Store(50 << 20)
	tick()
	st = r.Health()
	if st.OK || len(st.Warnings) == 0 || !strings.Contains(st.Warnings[0], "disk low") {
		t.Fatalf("health = %+v", st)
	}
	if got := lc.count(slog.LevelWarn, "DISK SPACE LOW"); got != 1 {
		t.Fatalf("warnings = %d, want 1", got)
	}
	// Rate limited: nothing more for just under 10 minutes...
	for range 18 {
		tick()
	}
	if got := lc.count(slog.LevelWarn, "DISK SPACE LOW"); got != 1 {
		t.Fatalf("warnings = %d, want 1 (rate limited)", got)
	}
	// ...then again at the 10 minute mark.
	for range 2 {
		tick()
	}
	if got := lc.count(slog.LevelWarn, "DISK SPACE LOW"); got != 2 {
		t.Fatalf("warnings = %d, want 2", got)
	}
	// Recovery.
	free.Store(500 << 20)
	tick()
	if st := r.Health(); !st.OK || len(st.Warnings) != 0 {
		t.Fatalf("health after recovery = %+v", st)
	}
	if lc.count(slog.LevelInfo, "recovered") != 1 {
		t.Error("no recovery log")
	}
	// A second dip warns again immediately.
	free.Store(1 << 20)
	tick()
	if got := lc.count(slog.LevelWarn, "DISK SPACE LOW"); got != 3 {
		t.Fatalf("warnings = %d, want 3", got)
	}
}

func TestDiskCheckErrors(t *testing.T) {
	fc := clock.NewFake(t0)
	var unsupported atomic.Bool
	unsupported.Store(true)
	checked := make(chan struct{}, 100)
	r, lc := newRec(t, Options{
		Clock: fc,
		DiskFree: func(string) (uint64, error) {
			if unsupported.Load() {
				return 0, ErrUnsupported
			}
			return 0, errors.New("statfs exploded")
		},
		afterDiskCheck: func() { checked <- struct{}{} },
	})
	<-checked
	if st := r.Health(); !st.OK || st.Detail.(Detail).FreeBytes != nil {
		t.Fatalf("unsupported should be OK and silent: %+v", st)
	}
	unsupported.Store(false)
	fc.Advance(30 * time.Second)
	<-checked
	st := r.Health()
	if st.OK || !strings.Contains(strings.Join(st.Warnings, ";"), "statfs exploded") {
		t.Fatalf("health = %+v", st)
	}
	if lc.count(slog.LevelWarn, "cannot determine") != 1 {
		t.Error("expected one warning")
	}
}

func TestDefaultDiskFreeWorks(t *testing.T) {
	free, err := statfsFree(t.TempDir())
	if errors.Is(err, ErrUnsupported) {
		t.Skip("unsupported platform")
	}
	if err != nil || free == 0 {
		t.Fatalf("statfsFree = %d, %v", free, err)
	}
}

func TestUnwritableDirDoesNotPanicOrBlock(t *testing.T) {
	r, lc := newRec(t, Options{})
	// A regular file where the date directory should be makes every
	// StartSession fail, even when running as root.
	datedir := filepath.Join(r.dir, t0.Local().Format("2006-01-02"))
	if err := os.WriteFile(datedir, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	h := r.StartSession(info("/live", "dj"))
	if h == nil {
		t.Fatal("handler must not be nil")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 1000 {
			h.OnSegment(pageSeg(int64(i), 1, i == 0, int64(i), 1000))
		}
		h.OnTrack(ogg.Track{Artist: "A", Title: "T"}, t0)
		h.OnEnd(t0, nil)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("handler blocked")
	}
	st := r.Health()
	if st.OK || len(st.Warnings) == 0 {
		t.Fatalf("health = %+v", st)
	}
	if lc.count(slog.LevelError, "cannot create") == 0 {
		t.Error("no error log")
	}
	if err := r.Close(); err != nil {
		t.Error(err)
	}
}

func TestNewRejectsBadDir(t *testing.T) {
	if _, err := New(Options{}); err == nil {
		t.Error("empty dir should fail")
	}
	f := filepath.Join(t.TempDir(), "file")
	os.WriteFile(f, nil, 0o644)
	if _, err := New(Options{Dir: filepath.Join(f, "sub")}); err == nil {
		t.Error("dir below a file should fail")
	}
}

func TestWriteErrorMidSession(t *testing.T) {
	r, lc := newRec(t, Options{})
	h := r.StartSession(info("/live", "dj"))
	s := h.(*session)
	h.OnSegment(rawSeg(0, "ok"))
	s.f.Close() // pull the file out from under the writer
	for i := range 100 {
		h.OnSegment(rawSeg(int64(i), "after the failure"))
	}
	h.OnTrack(ogg.Track{Artist: "A", Title: "T", Serial: 1}, t0)
	h.OnEnd(t0, nil)
	r.Close()
	st := r.Health()
	if st.OK {
		t.Fatalf("health should not be OK: %+v", st)
	}
	d := st.Detail.(Detail)
	if d.WriteErrors == 0 || d.LastError == "" {
		t.Errorf("detail = %+v", d)
	}
	if lc.count(slog.LevelError, "write failed") == 0 {
		t.Error("expected error log")
	}
	// Sidecars still work.
	if _, err := os.Stat(strings.TrimSuffix(s.path, ".ogg") + ".cue"); err != nil {
		t.Error(err)
	}
}

func TestQueueOverflowDropsInsteadOfBlocking(t *testing.T) {
	gate := make(chan struct{})
	r, lc := newRec(t, Options{queueLimit: 150, writerGate: func() { <-gate }})
	h := r.StartSession(info("/live", "dj"))
	path := h.(*session).path
	first := pageSeg(0, 1, true, 0, 100)
	h.OnSegment(first)
	h.OnSegment(pageSeg(100, 1, false, 10, 100)) // would exceed the limit: dropped, not blocked
	st := r.Health()
	d := st.Detail.(Detail)
	if st.OK || d.DroppedBytes != 100 || len(d.Files) != 1 || !d.Files[0].Failed || d.Files[0].QueuedBytes != 100 {
		t.Fatalf("health = %+v", st)
	}
	if lc.count(slog.LevelError, "NOT being recorded") != 1 {
		t.Error("expected loud error")
	}
	close(gate)
	h.OnEnd(t0, nil)
	r.Close()
	if got := readFile(t, path); got != string(first.Data) {
		t.Errorf("file has %d bytes", len(got))
	}
}

func TestHealthJSONAndActiveFiles(t *testing.T) {
	r, _ := newRec(t, Options{})
	h := r.StartSession(info("/live", "dj"))
	h.OnSegment(rawSeg(0, "abc"))
	st := r.Health()
	b, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	d := st.Detail.(Detail)
	if !st.OK || len(d.Files) != 1 || d.Files[0].Mount != "/live" || !strings.Contains(string(b), `"files"`) {
		t.Errorf("health = %s", b)
	}
	h.OnEnd(t0, nil)
	r.Close()
	if d := r.Health().Detail.(Detail); len(d.Files) != 0 || d.BytesWritten != 3 {
		t.Errorf("detail = %+v", d)
	}
}

func TestCueFormatting(t *testing.T) {
	for _, c := range []struct {
		d    time.Duration
		want string
	}{
		{0, "00:00:00"},
		{-time.Second, "00:00:00"},
		{14 * time.Millisecond, "00:00:01"},
		{1500 * time.Millisecond, "00:01:37"},
		{61 * time.Second, "01:01:00"},
		{100*time.Minute + 5*time.Second, "100:05:00"},
		{(2*time.Hour + 3*time.Minute + 4*time.Second), "123:04:00"},
	} {
		if got := cueTime(c.d); got != c.want {
			t.Errorf("cueTime(%v) = %s, want %s", c.d, got, c.want)
		}
	}
	got := cueSheet{Performer: "a\nb", Title: `t"`, File: "f.ogg"}.render()
	if got != "PERFORMER \"a b\"\nTITLE \"t\\\"\"\nFILE \"f.ogg\" OGG\n" {
		t.Errorf("render = %q", got)
	}
}

func TestGranuleDuration(t *testing.T) {
	if d := granuleDuration(44100*3600*5, 44100); d != 5*time.Hour {
		t.Errorf("d = %v", d)
	}
	if d := granuleDuration(22050, 0); d != 500*time.Millisecond {
		t.Errorf("fallback rate: %v", d)
	}
}
