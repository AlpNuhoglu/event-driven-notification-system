package store

import (
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
)

// ProfilesFrom returns the retry back-off profile of each priority from the
// loaded configuration. The relay and the worker both build their Store with
// it.
func ProfilesFrom(c config.RetryConfig) Profiles {
	p := func(r config.Retry) domain.Profile {
		return domain.Profile{Base: r.Base, Cap: r.Cap, MinDelay: c.MinDelay}
	}
	return Profiles{
		domain.PriorityHigh:   p(c.High),
		domain.PriorityNormal: p(c.Normal),
		domain.PriorityLow:    p(c.Low),
	}
}
