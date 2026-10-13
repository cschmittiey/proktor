// Package clock abstracts time so debounce, reconnect backoff and pings can
// be tested without real time passing.
package clock

import (
	"sort"
	"sync"
	"time"
)

// Clock is the subset of the time package proktor uses.
type Clock interface {
	Now() time.Time
	// AfterFunc calls f in its own goroutine after d has elapsed.
	AfterFunc(d time.Duration, f func()) Timer
	// After returns a channel that receives the current time after d.
	After(d time.Duration) <-chan time.Time
	// NewTicker returns a ticker firing every d.
	NewTicker(d time.Duration) Ticker
}

// Timer is a cancellable pending AfterFunc call.
type Timer interface {
	// Stop prevents the timer from firing. It reports whether the call
	// stopped the timer (false if it already fired or was stopped).
	Stop() bool
}

// Ticker delivers ticks on C until stopped.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// Real returns a Clock backed by the time package.
func Real() Clock { return realClock{} }

type realClock struct{}

func (realClock) Now() time.Time                            { return time.Now() }
func (realClock) AfterFunc(d time.Duration, f func()) Timer { return time.AfterFunc(d, f) }
func (realClock) After(d time.Duration) <-chan time.Time    { return time.After(d) }
func (realClock) NewTicker(d time.Duration) Ticker          { return realTicker{time.NewTicker(d)} }

type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()               { r.t.Stop() }

// Fake is a manually advanced Clock for tests. The zero value is not usable;
// call NewFake.
type Fake struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*fakeWaiter
	nextID  int
}

type fakeWaiter struct {
	id      int
	at      time.Time
	f       func()         // AfterFunc
	ch      chan time.Time // After / Ticker
	period  time.Duration  // >0 for tickers
	stopped bool
}

// NewFake returns a Fake clock set to start.
func NewFake(start time.Time) *Fake { return &Fake{now: start} }

func (c *Fake) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *Fake) add(w *fakeWaiter) {
	c.nextID++
	w.id = c.nextID
	c.waiters = append(c.waiters, w)
}

func (c *Fake) AfterFunc(d time.Duration, f func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	w := &fakeWaiter{at: c.now.Add(d), f: f}
	c.add(w)
	return &fakeTimer{c: c, w: w}
}

func (c *Fake) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	w := &fakeWaiter{at: c.now.Add(d), ch: make(chan time.Time, 1)}
	c.add(w)
	return w.ch
}

func (c *Fake) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("clock: non-positive ticker period")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	w := &fakeWaiter{at: c.now.Add(d), ch: make(chan time.Time, 1), period: d}
	c.add(w)
	return &fakeTicker{c: c, w: w}
}

// Pending reports how many timers, Afters and tickers are waiting to fire.
func (c *Fake) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, w := range c.waiters {
		if !w.stopped {
			n++
		}
	}
	return n
}

// Advance moves the clock forward by d, firing every due waiter in time
// order. AfterFunc callbacks run synchronously on the caller's goroutine, so
// their effects are visible when Advance returns.
func (c *Fake) Advance(d time.Duration) {
	c.mu.Lock()
	target := c.now.Add(d)
	c.mu.Unlock()
	for {
		c.mu.Lock()
		var due *fakeWaiter
		live := c.waiters[:0]
		for _, w := range c.waiters {
			if !w.stopped {
				live = append(live, w)
			}
		}
		c.waiters = live
		sort.SliceStable(c.waiters, func(i, j int) bool {
			if c.waiters[i].at.Equal(c.waiters[j].at) {
				return c.waiters[i].id < c.waiters[j].id
			}
			return c.waiters[i].at.Before(c.waiters[j].at)
		})
		if len(c.waiters) > 0 && !c.waiters[0].at.After(target) {
			due = c.waiters[0]
		}
		if due == nil {
			c.now = target
			c.mu.Unlock()
			return
		}
		c.now = due.at
		now := c.now
		if due.period > 0 {
			due.at = due.at.Add(due.period)
		} else {
			due.stopped = true
		}
		c.mu.Unlock()
		if due.f != nil {
			due.f()
		} else {
			select {
			case due.ch <- now:
			default: // like time.Ticker, drop ticks nobody is reading
			}
		}
	}
}

type fakeTimer struct {
	c *Fake
	w *fakeWaiter
}

func (t *fakeTimer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	was := !t.w.stopped
	t.w.stopped = true
	return was
}

type fakeTicker struct {
	c *Fake
	w *fakeWaiter
}

func (t *fakeTicker) C() <-chan time.Time { return t.w.ch }
func (t *fakeTicker) Stop() {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	t.w.stopped = true
}
