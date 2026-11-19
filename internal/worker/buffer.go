package worker

import (
	"context"
	"slices"
	"sync"
)

// buffer is the channel's local buffer between the consumer and the pacer
// loop. Messages wait in arrival order; take returns the highest priority
// first, and the earliest arrival among equal priorities. The buffer holds at
// most the prefetch count (25), so a linear scan is enough.
type buffer struct {
	mu     sync.Mutex
	items  []Message
	closed bool
	// ready holds a token when a message may be waiting. Only the pacer loop
	// waits on it.
	ready chan struct{}
}

func newBuffer() *buffer {
	return &buffer{ready: make(chan struct{}, 1)}
}

// push adds m. It returns false, and keeps nothing, once the buffer is closed.
func (b *buffer) push(m Message) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return false
	}
	b.items = append(b.items, m)
	select {
	case b.ready <- struct{}{}:
	default:
	}
	return true
}

// wait returns nil when the buffer holds a message, without taking it, or
// ctx's error.
func (b *buffer) wait(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if b.len() > 0 {
			return nil
		}
		select {
		case <-b.ready:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// take removes and returns the highest-priority message, the earliest
// arrival among equals, or nil when the buffer is empty. Messages from an
// older connection epoch are removed first and never returned (D76).
func (b *buffer) take() Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.removeStale()
	best := -1
	for i, m := range b.items {
		if best < 0 || m.Priority() > b.items[best].Priority() {
			best = i
		}
	}
	if best < 0 {
		return nil
	}
	m := b.items[best]
	b.items = slices.Delete(b.items, best, best+1)
	return m
}

// dropStale removes the messages from an older connection epoch, without an
// answer, and returns how many it removed.
func (b *buffer) dropStale() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.removeStale()
}

// removeStale removes the stale messages; the caller holds b.mu.
func (b *buffer) removeStale() int {
	n := len(b.items)
	b.items = slices.DeleteFunc(b.items, Message.Stale)
	return n - len(b.items)
}

// close refuses further pushes and returns the messages still buffered.
func (b *buffer) close() []Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	left := b.items
	b.items = nil
	return left
}

func (b *buffer) len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.items)
}
