package outputs

import (
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/cschmittiey/proktor/internal/state"
)

var quietLog = slog.New(slog.NewTextHandler(io.Discard, nil))

// fakeSource is a Source with an unbuffered-ish subscription.
type fakeSource struct {
	mu   sync.Mutex
	cur  state.State
	subs []chan state.State
	// subscribed receives one value per Subscribe call.
	subscribed chan struct{}
}

func newFakeSource(initial state.State) *fakeSource {
	return &fakeSource{cur: initial, subscribed: make(chan struct{}, 16)}
}

func (f *fakeSource) Snapshot() state.State {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cur
}

func (f *fakeSource) Subscribe() (<-chan state.State, func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan state.State, 64)
	f.subs = append(f.subs, ch)
	f.subscribed <- struct{}{}
	return ch, func() {}
}

// set publishes st to the snapshot and every subscriber.
func (f *fakeSource) set(st state.State) {
	f.mu.Lock()
	f.cur = st
	subs := append([]chan state.State(nil), f.subs...)
	f.mu.Unlock()
	for _, ch := range subs {
		ch <- st
	}
}

func trackState(seq uint64, artist, title string) state.State {
	return state.State{
		V:         state.SchemaVersion,
		Mode:      state.ModeTrack,
		Track:     &state.Track{Artist: artist, Title: title, Source: state.SourceTraktor, StartedAt: time.Date(2026, 10, 13, 22, 14, 5, 0, time.UTC)},
		EventName: "801 something",
		History:   []state.Track{},
		Seq:       seq,
		UpdatedAt: time.Date(2026, 10, 13, 22, 14, 5, 0, time.UTC),
	}
}

func str(s string) *string { return &s }
