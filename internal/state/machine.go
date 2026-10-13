package state

import (
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/cschmittiey/proktor/internal/clock"
	"github.com/cschmittiey/proktor/internal/ogg"
	"github.com/cschmittiey/proktor/internal/stream"
)

// Manual pin modes (Options.ManualPin).
const (
	PinUntilNextAuto = "until_next_auto"
	PinUntilCleared  = "until_cleared"
)

const (
	defaultHistoryLength = 10
	subscriberBuffer     = 32
)

// ErrClosed is returned by Apply after Close.
var ErrClosed = errors.New("state: machine closed")

// Options configures a Machine.
type Options struct {
	Clock         clock.Clock   // required
	EventName     string        //
	Delay         time.Duration // metadata_delay; 0 = publish immediately (synchronously)
	ManualPin     string        // "until_next_auto" (default) | "until_cleared"
	HistoryLength int           // default 10
	JournalPath   string        // append-only JSON-lines file; "" disables persistence
	Logger        *slog.Logger
}

// Machine is the now-playing state machine. All methods are safe for
// concurrent use.
//
// Pinning is not stored separately: a manual track is "pinned" exactly when
// it is the current track and ManualPin is "until_cleared". Clear, idle, live
// or a newer manual set therefore release/replace it naturally, and the pin
// survives a restart for free because the current track does.
type Machine struct {
	opts Options
	clk  clock.Clock
	log  *slog.Logger

	mu     sync.Mutex
	closed bool

	st         State // authoritative; never mutated in place, only replaced
	manualPerf string
	autoPerf   string
	tracklist  []TracklistEntry

	pendingIdx   int // tracklist index of the pending detection, -1 if none
	pendingTimer clock.Timer
	pendingGen   uint64 // bumped whenever the pending slot is replaced/cleared

	lastSuppressed string // artist\x00title last logged while pinned (dedupe)

	subs    map[uint64]chan State
	nextSub uint64

	jr *journal
}

// New creates a Machine, restoring from the journal if one exists.
func New(opts Options) (*Machine, error) {
	if opts.Clock == nil {
		return nil, errors.New("state: Options.Clock is required")
	}
	switch opts.ManualPin {
	case "":
		opts.ManualPin = PinUntilNextAuto
	case PinUntilNextAuto, PinUntilCleared:
	default:
		return nil, fmt.Errorf("state: invalid ManualPin %q", opts.ManualPin)
	}
	if opts.HistoryLength <= 0 {
		opts.HistoryLength = defaultHistoryLength
	}
	if opts.Delay < 0 {
		opts.Delay = 0
	}
	m := &Machine{
		opts:       opts,
		clk:        opts.Clock,
		log:        opts.Logger,
		pendingIdx: -1,
		subs:       make(map[uint64]chan State),
	}
	if m.log == nil {
		m.log = slog.Default()
	}
	m.st = State{
		V:         SchemaVersion,
		Mode:      ModeIdle,
		EventName: opts.EventName,
		History:   []Track{},
		UpdatedAt: m.now(),
	}

	if opts.JournalPath != "" {
		rs, err := replayJournal(opts.JournalPath, m.log)
		if err != nil {
			return nil, err
		}
		jr, err := openJournal(opts.JournalPath, m.log, rs.needsNewline)
		if err != nil {
			return nil, err
		}
		m.jr = jr
		if rs.state != nil {
			m.st = cloneState(*rs.state)
			m.manualPerf = rs.manual
			m.autoPerf = rs.auto
			m.tracklist = rs.tracklist
			// The pending timer did not survive the restart. We drop the
			// pending detection (its tracklist entry stays unpublished)
			// rather than re-arm it: after an unknown downtime it may no
			// longer be what is on air.
			ns := cloneState(m.st)
			ns.Pending = nil
			if len(ns.History) > opts.HistoryLength {
				ns.History = ns.History[:opts.HistoryLength]
			}
			m.commitLocked(ns, true)
		} else {
			m.tracklist = rs.tracklist
		}
	}
	return m, nil
}

func (m *Machine) now() time.Time { return m.clk.Now().UTC() }

// Detected reports an automatic track detection. It is debounced by
// Options.Delay.
func (m *Machine) Detected(d Detection) {
	artist, title := strings.TrimSpace(d.Artist), strings.TrimSpace(d.Title)
	if artist == "" && title == "" {
		m.log.Debug("state: ignoring empty detection", "mount", d.Mount)
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	if t := m.st.Track; t != nil && t.Artist == artist && t.Title == title {
		// Metadata flipped back to what's on air (the DJ cued the next
		// deck, then pulled it): the pending track was a false start.
		if m.st.Pending != nil {
			ns := cloneState(m.st)
			m.clearPendingLocked(&ns)
			m.commitLocked(ns, false)
		}
		return
	}
	if p := m.st.Pending; p != nil && p.Artist == artist && p.Title == title {
		return
	}
	now := m.now()
	src := d.Source
	if src == "" {
		src = SourceTraktor
	}
	at := d.At.UTC()
	if d.At.IsZero() {
		at = now
	}
	entry := TracklistEntry{
		Artist:     artist,
		Title:      title,
		Source:     src,
		Performer:  m.effectivePerformer(),
		Mount:      d.Mount,
		DetectedAt: at,
	}

	if m.pinnedLocked() {
		key := artist + "\x00" + title
		if key == m.lastSuppressed {
			return
		}
		m.lastSuppressed = key
		m.addEntryLocked(entry)
		m.log.Info("state: detection suppressed by pinned manual track", "artist", artist, "title", title)
		return
	}
	m.lastSuppressed = ""

	idx := m.addEntryLocked(entry)
	ns := cloneState(m.st)
	tr := Track{Artist: artist, Title: title, Source: src}
	if m.opts.Delay <= 0 {
		m.publishLocked(&ns, tr, idx, now)
	} else {
		m.setPendingLocked(&ns, tr, idx, at, now)
	}
	m.commitLocked(ns, false)
}

// Apply executes a manual / webhook command and returns the resulting state.
func (m *Machine) Apply(cmd Command) (State, error) {
	invalid := func(format string, args ...any) (State, error) {
		return State{}, fmt.Errorf("%w: %s", ErrInvalidCommand, fmt.Sprintf(format, args...))
	}
	artist, title := strings.TrimSpace(cmd.Artist), strings.TrimSpace(cmd.Title)
	src := cmd.Source
	perf := ""
	if cmd.Performer != nil {
		perf = strings.TrimSpace(*cmd.Performer)
	}

	// Validate before touching anything.
	switch cmd.Action {
	case ActionSet:
		if artist == "" && title == "" {
			return invalid("set requires an artist or title")
		}
		switch src {
		case "":
			src = SourceManual
		case SourceManual, SourceWebhook:
		default:
			return invalid("unknown source %q", cmd.Source)
		}
	case ActionLive:
		if perf == "" {
			return invalid("live requires a performer")
		}
	case ActionPerformer, ActionClear, ActionSkip, ActionIdle:
	default:
		return invalid("unknown action %q", cmd.Action)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return State{}, ErrClosed
	}
	m.lastSuppressed = ""
	now := m.now()
	ns := cloneState(m.st)
	internal := false

	switch cmd.Action {
	case ActionSet:
		if src == SourceWebhook && m.pinnedLocked() {
			m.addEntryLocked(TracklistEntry{
				Artist: artist, Title: title, Source: src,
				Performer: m.effectivePerformer(), DetectedAt: now,
			})
			m.log.Info("state: webhook set suppressed by pinned manual track", "artist", artist, "title", title)
			return cloneState(m.st), nil
		}
		if t := ns.Track; t != nil && t.Artist == artist && t.Title == title && t.Source == src {
			// Already showing exactly this; only drop a pending detection.
			m.clearPendingLocked(&ns)
		} else {
			pub := now
			m.addEntryLocked(TracklistEntry{
				Artist: artist, Title: title, Source: src,
				Performer: m.effectivePerformer(), DetectedAt: now, PublishedAt: &pub,
			})
			m.publishLocked(&ns, Track{Artist: artist, Title: title, Source: src}, -1, now)
		}

	case ActionPerformer:
		old := m.manualPerf
		m.manualPerf = perf
		internal = old != perf

	case ActionLive:
		old := m.manualPerf
		m.manualPerf = perf
		internal = old != perf
		m.clearPendingLocked(&ns)
		m.currentToHistory(&ns)

	case ActionClear:
		m.clearPendingLocked(&ns)
		ns.Track = nil

	case ActionSkip:
		m.clearPendingLocked(&ns)

	case ActionIdle:
		oldM, oldA := m.manualPerf, m.autoPerf
		m.manualPerf, m.autoPerf = "", ""
		internal = oldM != "" || oldA != ""
		m.clearPendingLocked(&ns)
		m.currentToHistory(&ns)
	}
	m.commitLocked(ns, internal)
	return cloneState(m.st), nil
}

// Snapshot returns a deep copy of the current state.
func (m *Machine) Snapshot() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return cloneState(m.st)
}

// Subscribe returns a channel receiving every state published after the call
// (use Snapshot for the current one). Each subscriber buffers up to 32
// states; when full, its oldest queued state is dropped so the newest always
// gets through and the machine never blocks. cancel is idempotent and closes
// the channel; Close also closes all subscriber channels.
func (m *Machine) Subscribe() (<-chan State, func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch := make(chan State, subscriberBuffer)
	if m.closed {
		close(ch)
		return ch, func() {}
	}
	id := m.nextSub
	m.nextSub++
	m.subs[id] = ch
	cancel := func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		if c, ok := m.subs[id]; ok {
			delete(m.subs, id)
			close(c)
		}
	}
	return ch, cancel
}

// Tracklist returns a copy of tonight's tracklist, oldest first.
func (m *Machine) Tracklist() []TracklistEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]TracklistEntry, len(m.tracklist))
	for i, e := range m.tracklist {
		out[i] = cloneEntry(e)
	}
	return out
}

// SetAutoPerformer sets the performer derived from the source's stream name.
// A manual performer takes precedence over it.
func (m *Machine) SetAutoPerformer(name string) {
	name = strings.TrimSpace(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.autoPerf == name {
		return
	}
	m.autoPerf = name
	m.commitLocked(cloneState(m.st), true)
}

// Consumer adapts the machine to ingest sessions.
func (m *Machine) Consumer() stream.Consumer {
	return stream.ConsumerFunc(func(info stream.SessionInfo) stream.Handler {
		if info.IceName != "" {
			m.SetAutoPerformer(info.IceName)
		}
		return &sessionHandler{m: m, info: info}
	})
}

// Close stops timers, closes subscriber channels and the journal. The
// current/pending tracks are kept in the journal for the next start.
func (m *Machine) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	if m.pendingTimer != nil {
		m.pendingTimer.Stop()
		m.pendingTimer = nil
	}
	m.pendingGen++
	for id, ch := range m.subs {
		delete(m.subs, id)
		close(ch)
	}
	if m.jr != nil {
		err := m.jr.close()
		m.jr = nil
		return err
	}
	return nil
}

type sessionHandler struct {
	m    *Machine
	info stream.SessionInfo
}

func (h *sessionHandler) OnSegment(seg ogg.Segment) {}

func (h *sessionHandler) OnTrack(t ogg.Track, at time.Time) {
	h.m.Detected(Detection{
		Artist: t.Artist,
		Title:  t.Title,
		Source: SourceTraktor,
		Mount:  h.info.Mount,
		At:     at,
	})
}

func (h *sessionHandler) OnEnd(at time.Time, err error) {
	// Keep pending and current: DJ handoffs reconnect quickly.
	h.m.log.Info("state: source ended", "mount", h.info.Mount, "session", h.info.ID, "err", err)
}

// ---- internals (all *Locked methods require m.mu) ----

func (m *Machine) effectivePerformer() string {
	if m.manualPerf != "" {
		return m.manualPerf
	}
	return m.autoPerf
}

func (m *Machine) pinnedLocked() bool {
	return m.opts.ManualPin == PinUntilCleared &&
		m.st.Track != nil && m.st.Track.Source == SourceManual
}

func (m *Machine) addEntryLocked(e TracklistEntry) int {
	e.DetectedAt = e.DetectedAt.UTC()
	m.tracklist = append(m.tracklist, e)
	idx := len(m.tracklist) - 1
	m.jr.writeEntry(idx, e)
	return idx
}

// currentToHistory moves ns.Track (if any) to the front of ns.History.
func (m *Machine) currentToHistory(ns *State) {
	if ns.Track == nil {
		return
	}
	h := make([]Track, 0, len(ns.History)+1)
	h = append(h, *ns.Track)
	h = append(h, ns.History...)
	if len(h) > m.opts.HistoryLength {
		h = h[:m.opts.HistoryLength]
	}
	ns.History = h
	ns.Track = nil
}

// clearPendingLocked drops the pending detection and invalidates its timer.
func (m *Machine) clearPendingLocked(ns *State) {
	if m.pendingTimer != nil {
		m.pendingTimer.Stop()
		m.pendingTimer = nil
	}
	m.pendingGen++
	m.pendingIdx = -1
	ns.Pending = nil
}

func (m *Machine) setPendingLocked(ns *State, tr Track, idx int, detectedAt, now time.Time) {
	m.clearPendingLocked(ns)
	publishAt := now.Add(m.opts.Delay)
	tr.StartedAt = publishAt // when it is expected to start
	ns.Pending = &Pending{Track: tr, DetectedAt: detectedAt, PublishAt: publishAt}
	m.pendingIdx = idx
	gen := m.pendingGen
	m.pendingTimer = m.clk.AfterFunc(m.opts.Delay, func() { m.firePending(gen) })
}

func (m *Machine) firePending(gen uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || gen != m.pendingGen || m.st.Pending == nil {
		return // stale timer
	}
	p := m.st.Pending
	tr := Track{Artist: p.Artist, Title: p.Title, Source: p.Source}
	idx := m.pendingIdx
	ns := cloneState(m.st)
	m.publishLocked(&ns, tr, idx, m.now())
	m.commitLocked(ns, false)
}

// publishLocked makes tr the current track at time now. The previous current
// track goes to history (unless it is the same artist/title). If idx >= 0 the
// tracklist entry is marked published. Any pending detection is dropped.
func (m *Machine) publishLocked(ns *State, tr Track, idx int, now time.Time) {
	m.clearPendingLocked(ns)
	if cur := ns.Track; cur != nil && cur.Artist == tr.Artist && cur.Title == tr.Title {
		ns.Track = nil
	}
	m.currentToHistory(ns)
	tr.StartedAt = now
	ns.Track = &tr
	if idx >= 0 && idx < len(m.tracklist) {
		pub := now
		m.tracklist[idx].PublishedAt = &pub
		m.jr.writeEntry(idx, m.tracklist[idx])
	}
}

// commitLocked derives the dependent fields of ns, and if it differs from the
// current state in any visible way, bumps Seq/UpdatedAt, journals it and
// notifies subscribers. internal forces a journal write for changes that are
// not visible (e.g. a manual performer equal to the auto performer).
func (m *Machine) commitLocked(ns State, internal bool) bool {
	ns.V = SchemaVersion
	ns.EventName = m.opts.EventName
	if ns.History == nil {
		ns.History = []Track{}
	}
	if p := m.effectivePerformer(); p != "" {
		ns.Performer = &p
	} else {
		ns.Performer = nil
	}
	switch {
	case ns.Track != nil:
		ns.Mode = ModeTrack
	case ns.Performer != nil:
		ns.Mode = ModeLive
	default:
		ns.Mode = ModeIdle
	}

	cmp := ns
	cmp.Seq, cmp.UpdatedAt = m.st.Seq, m.st.UpdatedAt
	if reflect.DeepEqual(m.st, cmp) {
		if internal {
			m.jr.writeState(m.st, m.manualPerf, m.autoPerf)
		}
		return false
	}
	ns.Seq = m.st.Seq + 1
	ns.UpdatedAt = m.now()
	m.st = ns
	m.jr.writeState(m.st, m.manualPerf, m.autoPerf)
	for _, ch := range m.subs {
		s := cloneState(m.st)
		select {
		case ch <- s:
			continue
		default:
		}
		// Full: drop the oldest queued state. Only this goroutine (under
		// m.mu) sends, so the second send always has room.
		select {
		case <-ch:
		default:
		}
		select {
		case ch <- s:
		default:
		}
	}
	return true
}

func cloneState(s State) State {
	c := s
	if s.Track != nil {
		t := *s.Track
		c.Track = &t
	}
	if s.Performer != nil {
		p := *s.Performer
		c.Performer = &p
	}
	if s.Pending != nil {
		p := *s.Pending
		c.Pending = &p
	}
	c.History = make([]Track, len(s.History))
	copy(c.History, s.History)
	return c
}

func cloneEntry(e TracklistEntry) TracklistEntry {
	if e.PublishedAt != nil {
		p := *e.PublishedAt
		e.PublishedAt = &p
	}
	return e
}
