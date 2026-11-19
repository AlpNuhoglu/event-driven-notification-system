package queue

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

// newTestConnection returns a Connection in the state after a first
// connection on which the topology was declared: generation 1, usable. Its
// closed flag is read from closed.
func newTestConnection(closed func() bool) *Connection {
	return &Connection{
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		closed:          closed,
		lost:            make(chan struct{}),
		gaveUp:          make(chan struct{}),
		opened:          make(chan struct{}),
		consumerTimeout: time.Second,
		dials:           1,
		live:            1,
		declared:        1,
		opens:           1,
	}
}

// IsOpen combines two facts: the library's closed flag and our generation
// (the topology is declared on the current connection). A reconnection
// attempt changes both, in this order: its dial hook starts a new
// generation, then the library clears its closed flag, and only later is the
// topology declared. If the attempt runs between IsOpen's two reads, IsOpen
// must still answer false: the connection is open again, but the topology is
// not declared on it yet (D2).
//
// The test places the attempt exactly at the read of the closed flag. With
// the reads in the other order (generation first, flag second) IsOpen saw
// the old generation, still usable, and then the cleared flag, and answered
// true; that happened once on the CI runner
// (TestConnection_TopologyDeclaredAgainOnEmptyBroker: "IsOpen was true
// before the topology existed on the recovered connection").
func TestIsOpen_ReconnectionAttemptBetweenItsTwoReads(t *testing.T) {
	var c *Connection
	lost := true // the connection is lost; no reconnection attempt has run yet
	c = newTestConnection(func() bool {
		if lost {
			// The attempt, as amqp091-go's Reconnect runs it: the Dial hook
			// (a new generation), then resetState (the closed flag cleared).
			// RecoverTopology has not run yet.
			c.newGeneration()
			lost = false
		}
		return lost
	})

	if c.IsOpen() {
		t.Fatal("IsOpen is true during a reconnection attempt, before the topology is declared on the new connection")
	}
	if c.IsOpen() {
		t.Fatal("IsOpen is true on the recovered connection before the topology is declared on it")
	}
	if n, _ := c.Opens(); n != 1 {
		t.Fatalf("opens %d before the topology is declared again, want 1", n)
	}

	// The topology recovery declares the topology on generation 2.
	c.markUsable(2)
	if !c.IsOpen() {
		t.Fatal("IsOpen is false after the topology was declared on the recovered connection")
	}
	if n, _ := c.Opens(); n != 2 {
		t.Fatalf("opens %d after the reconnection, want 2", n)
	}
}

// The plain cases of IsOpen, without a reconnection in between.
func TestIsOpen(t *testing.T) {
	closed := false
	c := newTestConnection(func() bool { return closed })
	if !c.IsOpen() {
		t.Error("IsOpen is false on an open connection with its topology declared")
	}

	closed = true // the library noticed the loss
	if c.IsOpen() {
		t.Error("IsOpen is true while the library's closed flag is set")
	}

	// A failed attempt: a new generation, the flag cleared by the library,
	// then the attempt fails before the topology is declared. The flag can
	// stay cleared until the next attempt; IsOpen must not follow it.
	c.newGeneration()
	closed = false
	if c.IsOpen() {
		t.Error("IsOpen is true after a failed reconnection attempt left the closed flag cleared")
	}

	// A connection whose topology was never declared is not usable.
	never := newTestConnection(func() bool { return false })
	never.declared, never.opens = 0, 0
	if never.IsOpen() {
		t.Error("IsOpen is true on a connection whose topology was never declared")
	}
}
