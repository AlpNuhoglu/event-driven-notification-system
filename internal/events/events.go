// Package events defines how status events leave a role. Every state
// transition returns a domain.StatusChanged; the caller hands it to a
// Publisher after its transaction has committed (ADR-0005).
package events

import "github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"

// Publisher announces status changes. Publishing is best-effort: Publish
// never blocks and never fails the caller; an implementation that cannot
// deliver an event drops it.
type Publisher interface {
	Publish(e domain.StatusChanged)
}

// Nop is the Publisher used until the broker-backed publisher exists (P5).
// It discards every event.
type Nop struct{}

// Publish discards the event.
func (Nop) Publish(domain.StatusChanged) {}
