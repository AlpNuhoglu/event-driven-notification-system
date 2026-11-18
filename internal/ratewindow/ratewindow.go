// Package ratewindow computes how many events fall into a sliding time
// window. The rate-limit evidence of the tests and of mockprovider's report
// rests on it (D24, D51).
package ratewindow

import (
	"slices"
	"time"
)

// MaxInWindow returns the largest number of timestamps that fall into any
// half-open window [t, t+window). The input need not be sorted and is not
// modified. A window that starts at a timestamp is enough to find the
// maximum, because moving a window's start forward to its first event never
// removes an event from it.
func MaxInWindow(timestamps []time.Time, window time.Duration) int {
	if len(timestamps) == 0 || window <= 0 {
		return 0
	}
	ts := slices.Clone(timestamps)
	slices.SortFunc(ts, time.Time.Compare)
	best, end := 0, 0
	for start := range ts {
		limit := ts[start].Add(window)
		for end < len(ts) && ts[end].Before(limit) {
			end++
		}
		best = max(best, end-start)
	}
	return best
}
