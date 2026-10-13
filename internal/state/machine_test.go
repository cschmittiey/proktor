package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cschmittiey/proktor/internal/clock"
	"github.com/cschmittiey/proktor/internal/ogg"
	"github.com/cschmittiey/proktor/internal/stream"
)

var t0 = time.Date(2026, 10, 13, 22, 0, 0, 0, time.UTC)

type rig struct {
	t   *testing.T
	clk *clock.Fake
	m   *Machine
}

func newRig(t *testing.T, mod func(*Options)) *rig {
	t.Helper()
	clk := clock.NewFake(t0)
	o := Options{
		Clock:     clk,
		EventName: "801 something",
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if mod != nil {
		mod(&o)
	}
	m, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	return &rig{t: t, clk: clk, m: m}
}

func (r *rig) detect(artist, title string) {
	r.m.Detected(Detection{Artist: artist, Title: title, Source: SourceTraktor, Mount: "/live", At: r.clk.Now()})
}

func (r *rig) apply(c Command) State {
	r.t.Helper()
	s, err := r.m.Apply(c)
	if err != nil {
		r.t.Fatalf("Apply(%+v): %v", c, err)
	}
	return s
}

func set(a, t string) Command { return Command{Action: ActionSet, Artist: a, Title: t} }

func ptr(s string) *string { return &s }

func trackIs(t *testing.T, s State, artist, title string) {
	t.Helper()
	if s.Track == nil || s.Track.Artist != artist || s.Track.Title != title {
		t.Fatalf("track = %+v, want %s - %s", s.Track, artist, title)
	}
}

func histTitles(s State) []string {
	out := []string{}
	for _, h := range s.History {
		out = append(out, h.Title)
	}
	return out
}

func eq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestNewRequiresClock(t *testing.T) {
	if _, err := New(Options{}); err == nil {
		t.Fatal("want error")
	}
	if _, err := New(Options{Clock: clock.NewFake(t0), ManualPin: "bogus"}); err == nil {
		t.Fatal("want error for bad ManualPin")
	}
}

func TestInitialState(t *testing.T) {
	r := newRig(t, nil)
	s := r.m.Snapshot()
	if s.V != SchemaVersion || s.Mode != ModeIdle || s.Track != nil || s.Performer != nil ||
		s.Pending != nil || s.History == nil || len(s.History) != 0 || s.Seq != 0 ||
		s.EventName != "801 something" || !s.UpdatedAt.Equal(t0) {
		t.Fatalf("bad initial state: %+v", s)
	}
}

func TestDelayZeroPublishesImmediately(t *testing.T) {
	r := newRig(t, nil)
	r.detect("New Order", "Blue Monday")
	s := r.m.Snapshot()
	trackIs(t, s, "New Order", "Blue Monday")
	if s.Mode != ModeTrack || s.Pending != nil || s.Seq != 1 || s.Track.Source != SourceTraktor ||
		!s.Track.StartedAt.Equal(t0) || r.clk.Pending() != 0 {
		t.Fatalf("unexpected: %+v", s)
	}
	tl := r.m.Tracklist()
	if len(tl) != 1 || tl[0].PublishedAt == nil || !tl[0].PublishedAt.Equal(t0) ||
		tl[0].Mount != "/live" || !tl[0].DetectedAt.Equal(t0) {
		t.Fatalf("tracklist: %+v", tl)
	}
}

func TestDelayPendingThenPublish(t *testing.T) {
	r := newRig(t, func(o *Options) { o.Delay = 30 * time.Second })
	r.detect("A", "one")
	s := r.m.Snapshot()
	if s.Track != nil || s.Pending == nil || s.Seq != 1 || s.Mode != ModeIdle {
		t.Fatalf("pending state: %+v", s)
	}
	if !s.Pending.PublishAt.Equal(t0.Add(30*time.Second)) || !s.Pending.DetectedAt.Equal(t0) {
		t.Fatalf("pending times: %+v", s.Pending)
	}
	r.clk.Advance(29 * time.Second)
	if r.m.Snapshot().Track != nil {
		t.Fatal("published early")
	}
	r.clk.Advance(time.Second)
	s = r.m.Snapshot()
	trackIs(t, s, "A", "one")
	if s.Pending != nil || s.Seq != 2 || !s.Track.StartedAt.Equal(t0.Add(30*time.Second)) ||
		!s.UpdatedAt.Equal(t0.Add(30*time.Second)) {
		t.Fatalf("published state: %+v", s)
	}
	tl := r.m.Tracklist()
	if !tl[0].DetectedAt.Equal(t0) || tl[0].PublishedAt == nil || !tl[0].PublishedAt.Equal(t0.Add(30*time.Second)) {
		t.Fatalf("tracklist: %+v", tl)
	}
}

func TestDelayReplacement(t *testing.T) {
	r := newRig(t, func(o *Options) { o.Delay = 30 * time.Second })
	r.detect("A", "first")
	r.clk.Advance(20 * time.Second)
	r.detect("B", "second")
	s := r.m.Snapshot()
	if s.Pending == nil || s.Pending.Title != "second" || s.Seq != 2 ||
		!s.Pending.PublishAt.Equal(t0.Add(50*time.Second)) {
		t.Fatalf("pending: %+v", s.Pending)
	}
	// First timer would have fired at +30s; must not publish.
	r.clk.Advance(15 * time.Second)
	if s := r.m.Snapshot(); s.Track != nil || s.Pending == nil {
		t.Fatalf("stale timer published: %+v", s)
	}
	r.clk.Advance(15 * time.Second)
	s = r.m.Snapshot()
	trackIs(t, s, "B", "second")
	if len(s.History) != 0 || s.Seq != 3 {
		t.Fatalf("state: %+v", s)
	}
	tl := r.m.Tracklist()
	if len(tl) != 2 || tl[0].Title != "first" || tl[0].PublishedAt != nil ||
		tl[1].Title != "second" || tl[1].PublishedAt == nil {
		t.Fatalf("tracklist: %+v", tl)
	}
	if r.clk.Pending() != 0 {
		t.Fatal("timer leaked")
	}
}

func TestDuplicatesIgnored(t *testing.T) {
	r := newRig(t, func(o *Options) { o.Delay = 10 * time.Second })
	r.detect("A", "one")
	r.detect("A", "one") // dup of pending
	if s := r.m.Snapshot(); s.Seq != 1 || len(r.m.Tracklist()) != 1 {
		t.Fatalf("pending dup not ignored: %+v", s)
	}
	r.clk.Advance(10 * time.Second)
	seq := r.m.Snapshot().Seq
	r.detect("A", "one") // dup of current
	r.detect(" A ", "one")
	if s := r.m.Snapshot(); s.Seq != seq || len(r.m.Tracklist()) != 1 || s.Pending != nil {
		t.Fatalf("current dup not ignored: %+v", s)
	}
	r.detect("", "") // empty ignored
	if r.m.Snapshot().Seq != seq {
		t.Fatal("empty detection changed state")
	}
}

func TestFlipBackCancelsPending(t *testing.T) {
	r := newRig(t, func(o *Options) { o.Delay = 30 * time.Second })
	r.detect("A", "one")
	r.clk.Advance(30 * time.Second)
	r.detect("B", "two") // DJ cues the next deck...
	r.clk.Advance(5 * time.Second)
	r.detect("A", "one") // ...and pulls it again
	s := r.m.Snapshot()
	trackIs(t, s, "A", "one")
	if s.Pending != nil {
		t.Fatalf("pending survived flip-back: %+v", s.Pending)
	}
	r.clk.Advance(time.Minute)
	trackIs(t, r.m.Snapshot(), "A", "one")
	if tl := r.m.Tracklist(); len(tl) != 2 || tl[1].PublishedAt != nil {
		t.Fatalf("tracklist: %+v", tl)
	}
}

func TestHistoryOrderAndTrim(t *testing.T) {
	r := newRig(t, func(o *Options) { o.HistoryLength = 3 })
	for i := 1; i <= 6; i++ {
		r.detect("X", fmt.Sprintf("t%d", i))
		r.clk.Advance(time.Minute)
	}
	s := r.m.Snapshot()
	trackIs(t, s, "X", "t6")
	if !eq(histTitles(s), []string{"t5", "t4", "t3"}) {
		t.Fatalf("history: %v", histTitles(s))
	}
	if len(r.m.Tracklist()) != 6 {
		t.Fatal("tracklist should keep everything")
	}
}

func TestDefaultHistoryLength(t *testing.T) {
	r := newRig(t, nil)
	for i := 0; i < 15; i++ {
		r.detect("X", fmt.Sprintf("t%d", i))
	}
	if n := len(r.m.Snapshot().History); n != 10 {
		t.Fatalf("history len = %d", n)
	}
}

func TestInvalidCommands(t *testing.T) {
	r := newRig(t, nil)
	for _, c := range []Command{
		{Action: "bogus"},
		{Action: ""},
		{Action: ActionSet},
		{Action: ActionSet, Artist: " ", Title: "  "},
		{Action: ActionSet, Artist: "a", Source: "traktor"},
		{Action: ActionLive},
		{Action: ActionLive, Performer: ptr("  ")},
	} {
		if _, err := r.m.Apply(c); !errors.Is(err, ErrInvalidCommand) {
			t.Errorf("%+v: err = %v", c, err)
		}
	}
	if r.m.Snapshot().Seq != 0 {
		t.Fatal("invalid command changed state")
	}
}

func TestActions(t *testing.T) {
	type step struct {
		name  string
		do    func(r *rig)
		check func(t *testing.T, s State, r *rig)
	}
	steps := []step{
		{"set", func(r *rig) { r.apply(Command{Action: ActionSet, Artist: "Kraftwerk", Title: "Radioactivity"}) },
			func(t *testing.T, s State, r *rig) {
				trackIs(t, s, "Kraftwerk", "Radioactivity")
				if s.Track.Source != SourceManual || s.Mode != ModeTrack {
					t.Fatal(s)
				}
				tl := r.m.Tracklist()
				e := tl[len(tl)-1]
				if e.Source != SourceManual || e.PublishedAt == nil || !e.PublishedAt.Equal(e.DetectedAt) {
					t.Fatalf("entry %+v", e)
				}
			}},
		{"set title only", func(r *rig) { r.apply(Command{Action: ActionSet, Title: "Untitled"}) },
			func(t *testing.T, s State, r *rig) { trackIs(t, s, "", "Untitled") }},
		{"performer", func(r *rig) {
			r.detect("A", "one")
			r.apply(Command{Action: ActionPerformer, Performer: ptr("dj x")})
		}, func(t *testing.T, s State, r *rig) {
			trackIs(t, s, "A", "one")
			if s.Performer == nil || *s.Performer != "dj x" {
				t.Fatal(s.Performer)
			}
		}},
		{"performer only => live", func(r *rig) {
			r.apply(Command{Action: ActionPerformer, Performer: ptr("synth")})
		}, func(t *testing.T, s State, r *rig) {
			if s.Mode != ModeLive || s.Track != nil {
				t.Fatal(s)
			}
		}},
		{"live", func(r *rig) {
			r.detect("A", "one")
			r.apply(Command{Action: ActionLive, Performer: ptr("synth act")})
		}, func(t *testing.T, s State, r *rig) {
			if s.Mode != ModeLive || s.Track != nil || *s.Performer != "synth act" ||
				!eq(histTitles(s), []string{"one"}) {
				t.Fatal(s)
			}
		}},
		{"clear drops current without history", func(r *rig) {
			r.detect("A", "one")
			r.detect("B", "two")
			r.apply(Command{Action: ActionClear})
		}, func(t *testing.T, s State, r *rig) {
			if s.Track != nil || s.Mode != ModeIdle || !eq(histTitles(s), []string{"one"}) {
				t.Fatal(s)
			}
		}},
		{"idle moves current to history", func(r *rig) {
			r.detect("A", "one")
			r.apply(Command{Action: ActionPerformer, Performer: ptr("p")})
			r.apply(Command{Action: ActionIdle})
		}, func(t *testing.T, s State, r *rig) {
			if s.Track != nil || s.Performer != nil || s.Mode != ModeIdle || !eq(histTitles(s), []string{"one"}) {
				t.Fatal(s)
			}
		}},
	}
	for _, st := range steps {
		t.Run(st.name, func(t *testing.T) {
			r := newRig(t, nil)
			st.do(r)
			st.check(t, r.m.Snapshot(), r)
		})
	}
}

func TestSkipAndClearWithPending(t *testing.T) {
	r := newRig(t, func(o *Options) { o.Delay = 30 * time.Second })
	r.detect("A", "one")
	r.clk.Advance(30 * time.Second)
	r.detect("B", "two")
	s := r.apply(Command{Action: ActionSkip})
	trackIs(t, s, "A", "one")
	if s.Pending != nil {
		t.Fatal("pending not dropped")
	}
	r.clk.Advance(time.Minute) // must not publish
	trackIs(t, r.m.Snapshot(), "A", "one")
	if tl := r.m.Tracklist(); tl[1].PublishedAt != nil {
		t.Fatal("skipped entry published")
	}
	// clear drops pending and current
	r.detect("C", "three")
	s = r.apply(Command{Action: ActionClear})
	if s.Track != nil || s.Pending != nil || len(s.History) != 0 {
		t.Fatalf("%+v", s)
	}
	if r.clk.Pending() != 0 {
		t.Fatal("timer leaked")
	}
}

func TestNoOpDoesNotBumpSeqOrNotify(t *testing.T) {
	r := newRig(t, nil)
	ch, cancel := r.m.Subscribe()
	defer cancel()
	r.apply(Command{Action: ActionClear})
	r.apply(Command{Action: ActionSkip})
	r.apply(Command{Action: ActionIdle})
	r.apply(Command{Action: ActionPerformer})
	r.apply(Command{Action: ActionPerformer, Performer: ptr("")})
	r.m.SetAutoPerformer("")
	if s := r.m.Snapshot(); s.Seq != 0 || !s.UpdatedAt.Equal(t0) {
		t.Fatalf("%+v", s)
	}
	r.apply(set("A", "one"))
	seq := r.m.Snapshot().Seq
	r.clk.Advance(time.Second)
	r.apply(set("A", "one")) // same manual track again
	r.apply(Command{Action: ActionPerformer, Performer: ptr("p")})
	r.apply(Command{Action: ActionPerformer, Performer: ptr("p")})
	if s := r.m.Snapshot(); s.Seq != seq+1 {
		t.Fatalf("seq = %d, want %d", s.Seq, seq+1)
	}
	if n := len(ch); n != 2 {
		t.Fatalf("notifications = %d, want 2", n)
	}
	if len(r.m.Tracklist()) != 1 {
		t.Fatal("repeat set should not add a tracklist entry")
	}
}

func TestSeqAndUpdatedAt(t *testing.T) {
	r := newRig(t, func(o *Options) { o.Delay = 10 * time.Second })
	r.clk.Advance(time.Minute)
	r.detect("A", "one") // pending change
	s := r.m.Snapshot()
	if s.Seq != 1 || !s.UpdatedAt.Equal(t0.Add(time.Minute)) {
		t.Fatalf("%+v", s)
	}
	r.clk.Advance(10 * time.Second)
	s = r.m.Snapshot()
	if s.Seq != 2 || !s.UpdatedAt.Equal(t0.Add(70*time.Second)) || s.UpdatedAt.Location() != time.UTC {
		t.Fatalf("%+v", s)
	}
}

func TestPinModes(t *testing.T) {
	t.Run("until_next_auto: auto publish replaces manual", func(t *testing.T) {
		r := newRig(t, func(o *Options) { o.Delay = 10 * time.Second })
		r.apply(set("M", "manual"))
		r.detect("A", "auto")
		trackIs(t, r.m.Snapshot(), "M", "manual")
		if r.m.Snapshot().Pending == nil {
			t.Fatal("auto detection should be pending")
		}
		r.clk.Advance(10 * time.Second)
		s := r.m.Snapshot()
		trackIs(t, s, "A", "auto")
		if !eq(histTitles(s), []string{"manual"}) {
			t.Fatal(histTitles(s))
		}
	})
	t.Run("until_cleared: auto logged only", func(t *testing.T) {
		r := newRig(t, func(o *Options) { o.ManualPin = PinUntilCleared; o.Delay = 10 * time.Second })
		r.apply(set("M", "manual"))
		seq := r.m.Snapshot().Seq
		r.detect("A", "auto")
		r.detect("A", "auto") // not logged twice
		r.clk.Advance(time.Minute)
		s := r.m.Snapshot()
		trackIs(t, s, "M", "manual")
		if s.Pending != nil || s.Seq != seq {
			t.Fatalf("%+v", s)
		}
		tl := r.m.Tracklist()
		if len(tl) != 2 || tl[1].Title != "auto" || tl[1].PublishedAt != nil || tl[1].Source != SourceTraktor {
			t.Fatalf("%+v", tl)
		}
		// another manual set keeps it pinned
		r.apply(set("M2", "manual2"))
		r.detect("B", "auto2")
		trackIs(t, r.m.Snapshot(), "M2", "manual2")
		// clear releases
		r.apply(Command{Action: ActionClear})
		r.detect("C", "auto3")
		r.clk.Advance(10 * time.Second)
		trackIs(t, r.m.Snapshot(), "C", "auto3")
	})
	t.Run("until_cleared: idle releases", func(t *testing.T) {
		r := newRig(t, func(o *Options) { o.ManualPin = PinUntilCleared })
		r.apply(set("M", "manual"))
		r.apply(Command{Action: ActionIdle})
		r.detect("A", "auto")
		trackIs(t, r.m.Snapshot(), "A", "auto")
	})
	t.Run("until_cleared: pending dropped when pinning", func(t *testing.T) {
		r := newRig(t, func(o *Options) { o.ManualPin = PinUntilCleared; o.Delay = 10 * time.Second })
		r.detect("A", "auto")
		r.apply(set("M", "manual"))
		r.clk.Advance(time.Minute)
		s := r.m.Snapshot()
		trackIs(t, s, "M", "manual")
		if s.Pending != nil {
			t.Fatal("pending survived pin")
		}
	})
	t.Run("until_next_auto: manual set drops pending", func(t *testing.T) {
		r := newRig(t, func(o *Options) { o.Delay = 10 * time.Second })
		r.detect("A", "early flip")
		r.apply(set("M", "manual"))
		r.clk.Advance(time.Minute)
		trackIs(t, r.m.Snapshot(), "M", "manual")
	})
}

func TestWebhookVsManual(t *testing.T) {
	webhook := Command{Action: ActionSet, Artist: "W", Title: "hook", Source: SourceWebhook}
	t.Run("webhook publishes immediately and does not pin", func(t *testing.T) {
		r := newRig(t, func(o *Options) { o.ManualPin = PinUntilCleared; o.Delay = 10 * time.Second })
		s := r.apply(webhook)
		trackIs(t, s, "W", "hook")
		if s.Track.Source != SourceWebhook {
			t.Fatal(s.Track.Source)
		}
		r.detect("A", "auto")
		r.clk.Advance(10 * time.Second)
		trackIs(t, r.m.Snapshot(), "A", "auto")
		if e := r.m.Tracklist()[0]; e.Source != SourceWebhook || e.PublishedAt == nil {
			t.Fatalf("%+v", e)
		}
	})
	t.Run("webhook respects until_cleared pin", func(t *testing.T) {
		r := newRig(t, func(o *Options) { o.ManualPin = PinUntilCleared })
		r.apply(set("M", "manual"))
		seq := r.m.Snapshot().Seq
		s := r.apply(webhook)
		trackIs(t, s, "M", "manual")
		if s.Seq != seq {
			t.Fatal("seq bumped")
		}
		tl := r.m.Tracklist()
		if len(tl) != 2 || tl[1].Source != SourceWebhook || tl[1].PublishedAt != nil {
			t.Fatalf("%+v", tl)
		}
	})
	t.Run("webhook overrides manual under until_next_auto", func(t *testing.T) {
		r := newRig(t, nil)
		r.apply(set("M", "manual"))
		trackIs(t, r.apply(webhook), "W", "hook")
	})
	t.Run("manual overrides webhook", func(t *testing.T) {
		r := newRig(t, nil)
		r.apply(webhook)
		trackIs(t, r.apply(set("M", "manual")), "M", "manual")
	})
}

func TestPerformerPrecedence(t *testing.T) {
	r := newRig(t, nil)
	r.m.SetAutoPerformer("auto dj")
	s := r.m.Snapshot()
	if s.Performer == nil || *s.Performer != "auto dj" || s.Mode != ModeLive || s.Seq != 1 {
		t.Fatalf("%+v", s)
	}
	r.detect("A", "one")
	if tl := r.m.Tracklist(); tl[0].Performer != "auto dj" {
		t.Fatalf("%+v", tl[0])
	}
	s = r.apply(Command{Action: ActionPerformer, Performer: ptr("manual dj")})
	if *s.Performer != "manual dj" {
		t.Fatal(*s.Performer)
	}
	r.m.SetAutoPerformer("auto dj 2") // does not override manual
	s = r.m.Snapshot()
	if *s.Performer != "manual dj" {
		t.Fatal(*s.Performer)
	}
	seq := s.Seq
	// clear manual -> falls back to (latest) auto
	s = r.apply(Command{Action: ActionPerformer})
	if *s.Performer != "auto dj 2" || s.Seq != seq+1 {
		t.Fatalf("%+v", s)
	}
	// manual same as auto: invisible change, still tracked
	s = r.apply(Command{Action: ActionPerformer, Performer: ptr("auto dj 2")})
	if s.Seq != seq+1 {
		t.Fatal("invisible change bumped seq")
	}
	r.apply(Command{Action: ActionIdle}) // clears both
	s = r.m.Snapshot()
	if s.Performer != nil || s.Mode != ModeIdle {
		t.Fatalf("%+v", s)
	}
	r.apply(Command{Action: ActionPerformer}) // nothing to fall back to
	if r.m.Snapshot().Performer != nil {
		t.Fatal("auto performer survived idle")
	}
}

func TestSubscribe(t *testing.T) {
	r := newRig(t, nil)
	ch, cancel := r.m.Subscribe()
	r.detect("A", "one")
	r.apply(set("B", "two"))
	r.apply(Command{Action: ActionPerformer, Performer: ptr("p")})
	var seqs []uint64
	for i := 0; i < 3; i++ {
		select {
		case s := <-ch:
			seqs = append(seqs, s.Seq)
		default:
			t.Fatalf("missing notification %d", i)
		}
	}
	if seqs[0] != 1 || seqs[1] != 2 || seqs[2] != 3 {
		t.Fatalf("seqs %v", seqs)
	}
	select {
	case s := <-ch:
		t.Fatalf("extra state %+v", s)
	default:
	}
	cancel()
	cancel() // idempotent
	if _, ok := <-ch; ok {
		t.Fatal("channel not closed")
	}
	r.detect("C", "three") // must not panic with a cancelled subscriber
}

func TestSubscribeDoesNotAliasMachine(t *testing.T) {
	r := newRig(t, nil)
	ch, cancel := r.m.Subscribe()
	defer cancel()
	r.detect("A", "one")
	s := <-ch
	s.Track.Title = "mutated"
	s.History = append(s.History, Track{})
	if got := r.m.Snapshot(); got.Track.Title != "one" || len(got.History) != 0 {
		t.Fatalf("aliasing: %+v", got)
	}
	snap := r.m.Snapshot()
	snap.Track.Artist = "zzz"
	if r.m.Snapshot().Track.Artist != "A" {
		t.Fatal("snapshot aliases internal state")
	}
}

func TestSlowSubscriberDropsOldest(t *testing.T) {
	r := newRig(t, nil)
	ch, cancel := r.m.Subscribe()
	defer cancel()
	const n = 100
	for i := 0; i < n; i++ {
		r.detect("A", fmt.Sprintf("t%d", i)) // never blocks
	}
	if len(ch) != subscriberBuffer {
		t.Fatalf("buffered %d", len(ch))
	}
	var got []uint64
	for len(ch) > 0 {
		got = append(got, (<-ch).Seq)
	}
	if got[len(got)-1] != n {
		t.Fatalf("newest missing: %v", got)
	}
	for i := 1; i < len(got); i++ {
		if got[i] != got[i-1]+1 {
			t.Fatalf("not consecutive tail: %v", got)
		}
	}
	if got[0] != n-subscriberBuffer+1 {
		t.Fatalf("oldest kept = %d", got[0])
	}
}

func TestCloseClosesSubscribers(t *testing.T) {
	r := newRig(t, func(o *Options) { o.Delay = time.Minute })
	ch, _ := r.m.Subscribe()
	r.detect("A", "one")
	if err := r.m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.m.Close(); err != nil {
		t.Fatal(err)
	}
	<-ch // buffered state
	if _, ok := <-ch; ok {
		t.Fatal("not closed")
	}
	if _, err := r.m.Apply(set("a", "b")); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	r.clk.Advance(time.Hour) // stopped timer must not fire
	if r.m.Snapshot().Track != nil {
		t.Fatal("published after close")
	}
}

func TestConsumer(t *testing.T) {
	r := newRig(t, nil)
	c := r.m.Consumer()
	h := c.StartSession(stream.SessionInfo{ID: "1", Mount: "/live", IceName: "dj example"})
	if h == nil {
		t.Fatal("nil handler")
	}
	if p := r.m.Snapshot().Performer; p == nil || *p != "dj example" {
		t.Fatalf("performer %v", p)
	}
	h.OnSegment(ogg.Segment{})
	h.OnTrack(ogg.Track{Artist: "New Order", Title: "Blue Monday"}, t0.Add(time.Second))
	s := r.m.Snapshot()
	trackIs(t, s, "New Order", "Blue Monday")
	tl := r.m.Tracklist()
	if tl[0].Mount != "/live" || tl[0].Source != SourceTraktor || !tl[0].DetectedAt.Equal(t0.Add(time.Second)) ||
		tl[0].Performer != "dj example" {
		t.Fatalf("%+v", tl[0])
	}
	h.OnEnd(t0, nil)
	if got := r.m.Snapshot(); got.Track == nil || got.Seq != s.Seq {
		t.Fatal("OnEnd must keep state")
	}
	// session without a stream name keeps the existing performer
	c.StartSession(stream.SessionInfo{ID: "2", Mount: "/live"})
	if *r.m.Snapshot().Performer != "dj example" {
		t.Fatal("performer lost")
	}
}

func TestTracklistIsCopy(t *testing.T) {
	r := newRig(t, nil)
	r.detect("A", "one")
	tl := r.m.Tracklist()
	tl[0].Title = "x"
	*tl[0].PublishedAt = t0.Add(time.Hour)
	if got := r.m.Tracklist()[0]; got.Title != "one" || !got.PublishedAt.Equal(t0) {
		t.Fatalf("%+v", got)
	}
}

func TestStateJSONShape(t *testing.T) {
	r := newRig(t, nil)
	idle, _ := json.Marshal(r.m.Snapshot())
	wantIdle := `{"v":1,"mode":"idle","track":null,"performer":null,"event_name":"801 something","history":[],"pending":null,"seq":0,"updated_at":"2026-10-13T22:00:00Z"}`
	if string(idle) != wantIdle {
		t.Fatalf("idle:\n got %s\nwant %s", idle, wantIdle)
	}

	r.clk.Advance(14*time.Minute + 5*time.Second)
	r.m.SetAutoPerformer("dj example")
	r.detect("Kraftwerk", "Radioactivity")
	r.clk.Advance(time.Minute)
	r.detect("New Order", "Blue Monday")
	got, _ := json.Marshal(r.m.Snapshot())
	want := `{"v":1,"mode":"track","track":{"artist":"New Order","title":"Blue Monday","source":"traktor","started_at":"2026-10-13T22:15:05Z"},` +
		`"performer":"dj example","event_name":"801 something",` +
		`"history":[{"artist":"Kraftwerk","title":"Radioactivity","source":"traktor","started_at":"2026-10-13T22:14:05Z"}],` +
		`"pending":null,"seq":3,"updated_at":"2026-10-13T22:15:05Z"}`
	if string(got) != want {
		t.Fatalf("track:\n got %s\nwant %s", got, want)
	}

	rd := newRig(t, func(o *Options) { o.Delay = 30 * time.Second })
	rd.detect("A", "x")
	got, _ = json.Marshal(rd.m.Snapshot())
	wantP := `{"v":1,"mode":"idle","track":null,"performer":null,"event_name":"801 something","history":[],` +
		`"pending":{"artist":"A","title":"x","source":"traktor","started_at":"2026-10-13T22:00:30Z","detected_at":"2026-10-13T22:00:00Z","publish_at":"2026-10-13T22:00:30Z"},` +
		`"seq":1,"updated_at":"2026-10-13T22:00:00Z"}`
	if string(got) != wantP {
		t.Fatalf("pending:\n got %s\nwant %s", got, wantP)
	}
}

func TestTimesAreUTC(t *testing.T) {
	loc := time.FixedZone("X", 5*3600)
	clk := clock.NewFake(t0.In(loc))
	m, _ := New(Options{Clock: clk, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	defer m.Close()
	m.Detected(Detection{Artist: "A", Title: "b", At: t0.In(loc)})
	s := m.Snapshot()
	if s.UpdatedAt.Location() != time.UTC || s.Track.StartedAt.Location() != time.UTC ||
		m.Tracklist()[0].DetectedAt.Location() != time.UTC {
		t.Fatal("non-UTC time leaked")
	}
}

// ---- journal ----

func journalRig(t *testing.T, path string, mod func(*Options)) *rig {
	return newRig(t, func(o *Options) {
		o.JournalPath = path
		if mod != nil {
			mod(o)
		}
	})
}

func TestJournalRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.jsonl")
	mod := func(o *Options) { o.Delay = 10 * time.Second; o.ManualPin = PinUntilCleared }
	r := journalRig(t, path, mod)
	r.m.SetAutoPerformer("auto dj")
	r.detect("A", "one")
	r.clk.Advance(10 * time.Second)
	r.detect("B", "two")
	r.clk.Advance(10 * time.Second)
	r.apply(set("M", "manual"))
	r.apply(Command{Action: ActionPerformer, Performer: ptr("manual dj")})
	r.detect("C", "suppressed")
	want := r.m.Snapshot()
	wantTL := r.m.Tracklist()
	if err := r.m.Close(); err != nil {
		t.Fatal(err)
	}

	r2 := journalRig(t, path, mod)
	r2.clk.Advance(time.Hour)
	got := r2.m.Snapshot()
	got.UpdatedAt = want.UpdatedAt // identical anyway; no pending to drop
	if g, w := mustJSON(got), mustJSON(want); g != w {
		t.Fatalf("restored state differs:\n got %s\nwant %s", g, w)
	}
	if g, w := mustJSON(r2.m.Tracklist()), mustJSON(wantTL); g != w {
		t.Fatalf("tracklist differs:\n got %s\nwant %s", g, w)
	}
	// Seq continues; manual pin survived; performers survived.
	r2.detect("D", "still suppressed")
	if r2.m.Snapshot().Seq != want.Seq {
		t.Fatal("pin lost on restore")
	}
	r2.apply(Command{Action: ActionPerformer}) // manual cleared -> auto dj
	s := r2.m.Snapshot()
	if s.Seq != want.Seq+1 || *s.Performer != "auto dj" {
		t.Fatalf("%+v", s)
	}
	if n := len(r2.m.Tracklist()); n != len(wantTL)+1 {
		t.Fatalf("tracklist len %d", n)
	}
}

func TestJournalDropsPendingOnRestore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.jsonl")
	mod := func(o *Options) { o.Delay = 30 * time.Second }
	r := journalRig(t, path, mod)
	r.detect("A", "one")
	r.clk.Advance(30 * time.Second)
	r.detect("B", "pending")
	before := r.m.Snapshot()
	if before.Pending == nil {
		t.Fatal("setup")
	}
	r.m.Close()

	r2 := journalRig(t, path, mod)
	r2.clk.Advance(5 * time.Second)
	s := r2.m.Snapshot()
	trackIs(t, s, "A", "one")
	if s.Pending != nil || s.Seq != before.Seq+1 {
		t.Fatalf("%+v", s)
	}
	r2.clk.Advance(time.Hour)
	trackIs(t, r2.m.Snapshot(), "A", "one")
	tl := r2.m.Tracklist()
	if len(tl) != 2 || tl[1].PublishedAt != nil {
		t.Fatalf("%+v", tl)
	}
	// A subsequent restart sees the dropped pending (state record was written).
	r2.m.Close()
	r3 := journalRig(t, path, mod)
	if s3 := r3.m.Snapshot(); s3.Pending != nil || s3.Seq != s.Seq {
		t.Fatalf("%+v", s3)
	}
}

func TestJournalTruncatedLastLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.jsonl")
	r := journalRig(t, path, nil)
	r.detect("A", "one")
	r.detect("B", "two")
	want := r.m.Snapshot()
	r.m.Close()

	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString(`{"type":"state","state":{"v":1,"mode":"track","track":{"arti`) // crash mid-write
	f.Close()

	r2 := journalRig(t, path, nil)
	if g := r2.m.Snapshot(); g.Seq != want.Seq || g.Track.Title != "two" {
		t.Fatalf("%+v", g)
	}
	r2.detect("C", "three") // appended on a fresh line
	r2.m.Close()

	r3 := journalRig(t, path, nil)
	g := r3.m.Snapshot()
	trackIs(t, g, "C", "three")
	if g.Seq != want.Seq+1 || len(r3.m.Tracklist()) != 3 {
		t.Fatalf("%+v tl=%d", g, len(r3.m.Tracklist()))
	}
}

func TestJournalUnknownAndGarbageLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.jsonl")
	r := journalRig(t, path, nil)
	r.detect("A", "one")
	r.m.Close()
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString("not json at all\n{\"type\":\"future\",\"x\":1}\n\n")
	f.Close()
	r2 := journalRig(t, path, nil)
	trackIs(t, r2.m.Snapshot(), "A", "one")
	if len(r2.m.Tracklist()) != 1 {
		t.Fatal("tracklist")
	}
}

func TestJournalLineFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.jsonl")
	r := journalRig(t, path, nil)
	r.detect("A", "one")
	r.m.Close()
	data, _ := os.ReadFile(path)
	lines := splitLines(string(data))
	if len(lines) != 3 { // tracklist add, tracklist publish, state
		t.Fatalf("lines: %q", lines)
	}
	var types []string
	for _, l := range lines {
		var rec map[string]any
		if err := json.Unmarshal([]byte(l), &rec); err != nil {
			t.Fatal(err)
		}
		types = append(types, rec["type"].(string))
	}
	if !eq(types, []string{"tracklist", "tracklist", "state"}) {
		t.Fatal(types)
	}
}

func TestJournalEventNameAndHistoryFromOptions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.jsonl")
	r := journalRig(t, path, nil)
	for i := 0; i < 5; i++ {
		r.detect("A", fmt.Sprint("t", i))
	}
	r.m.Close()
	r2 := journalRig(t, path, func(o *Options) { o.EventName = "new name"; o.HistoryLength = 2 })
	s := r2.m.Snapshot()
	if s.EventName != "new name" || len(s.History) != 2 {
		t.Fatalf("%+v", s)
	}
}

func TestBadJournalPath(t *testing.T) {
	_, err := New(Options{Clock: clock.NewFake(t0), JournalPath: filepath.Join(t.TempDir(), "nope", "j.jsonl")})
	if err == nil {
		t.Fatal("want error")
	}
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return out
}

// ---- concurrency ----

func TestConcurrentUse(t *testing.T) {
	clk := clock.NewFake(t0)
	m, err := New(Options{Clock: clk, Delay: time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ch, cancel := m.Subscribe()
	defer cancel()
	go func() {
		for range ch {
		}
	}()
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				switch i % 5 {
				case 0:
					m.Detected(Detection{Artist: "a", Title: fmt.Sprint(g, i)})
				case 1:
					m.Apply(set("m", fmt.Sprint(g, i)))
				case 2:
					clk.Advance(500 * time.Millisecond)
				case 3:
					m.Snapshot()
					m.Tracklist()
				case 4:
					m.Apply(Command{Action: ActionSkip})
				}
			}
		}(g)
	}
	wg.Wait()
}
