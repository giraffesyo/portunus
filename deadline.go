package portunus

import (
	"sync"
	"time"
)

// deadline signals expiry through a channel, modeled on net.Pipe's
// pipeDeadline: wait() returns a channel that is closed once the deadline
// passes, and SetDeadline-style updates replace it.
type deadline struct {
	mu     sync.Mutex
	timer  *time.Timer
	cancel chan struct{} // closed when the current deadline has expired
}

func makeDeadline() deadline {
	return deadline{cancel: make(chan struct{})}
}

// set moves the deadline. The zero time clears it.
func (d *deadline) set(t time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.timer != nil && !d.timer.Stop() {
		<-d.cancel // timer fired between Stop and lock; wait for the close
	}
	d.timer = nil

	closed := isClosedChan(d.cancel)
	if t.IsZero() {
		if closed {
			d.cancel = make(chan struct{})
		}
		return
	}
	if dur := time.Until(t); dur > 0 {
		if closed {
			d.cancel = make(chan struct{})
		}
		cancel := d.cancel
		d.timer = time.AfterFunc(dur, func() { close(cancel) })
		return
	}
	// Deadline already passed.
	if !closed {
		close(d.cancel)
	}
}

// wait returns the channel closed on expiry. Callers must re-fetch it after
// any set() they might race with; stream code fetches it fresh per park.
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
