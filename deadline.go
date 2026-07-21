package portunus

import (
	"sync"
	"time"
)

// deadline signals expiry through a channel, modeled on net.Pipe's
// pipeDeadline: wait() returns a channel that is closed once the deadline
// passes, and SetDeadline-style updates replace it.
//
// The zero value is a stream with no deadline set. Both the timer and the
// channel are created only when one is actually armed, because most streams
// never set a deadline and every stream would otherwise pay two channel
// allocations for the possibility.
type deadline struct {
	mu     sync.Mutex
	timer  *time.Timer
	cancel chan struct{} // nil until armed; closed once expired
}

// set moves the deadline. The zero time clears it.
func (d *deadline) set(t time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.timer != nil && !d.timer.Stop() {
		<-d.cancel // timer fired between Stop and lock; wait for the close
	}
	d.timer = nil

	if t.IsZero() {
		if isClosedChan(d.cancel) {
			d.cancel = make(chan struct{})
		}
		return
	}
	// Arming a real deadline: this is where the channel comes into
	// existence, and where an expired one is replaced.
	if d.cancel == nil || isClosedChan(d.cancel) {
		d.cancel = make(chan struct{})
	}
	if dur := time.Until(t); dur > 0 {
		cancel := d.cancel
		d.timer = time.AfterFunc(dur, func() { close(cancel) })
		return
	}
	close(d.cancel) // already passed
}

// wait returns the channel closed on expiry, or nil when no deadline is set.
// A nil channel never fires in a select, which is exactly what "no deadline"
// means. Callers must re-fetch it after any set() they might race with;
// stream code fetches it fresh per park.
func (d *deadline) wait() chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cancel
}

func isClosedChan(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}
