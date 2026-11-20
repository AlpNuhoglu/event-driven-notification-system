//go:build e2e

package e2e

import (
	"fmt"
	"testing"
)

// chaosReportPrefix starts the one line every chaos scenario ends with. The
// line is greppable in the output of make test-e2e:
//
//	chaos report: <scenario>: accepted=<n> sent=<n> failed=<n> provider_requests=<n> duplicate_keys=<n> max_per_key=<n>
//
// accepted is the number of notifications the scenario created and follows;
// sent and failed are their statuses in Postgres when the line is written;
// the last three come from the mock provider's report: every request it
// received since the scenario reset it, the idempotency keys it received more
// than once, and the most requests one key received. Duplicates are reported
// here, never asserted on a fixed number (ADR-0010 §3).
const chaosReportPrefix = "chaos report: "

// reportChaosAtEnd registers the chaos report of a scenario as a cleanup, so
// that the line is written also when the scenario fails. ids is read when
// the cleanup runs. Register it after leaveClean (cleanups run last in,
// first out), so that the report is read before the mock is reset.
func (s *stack) reportChaosAtEnd(t *testing.T, scenario string, ids *[]string) {
	t.Helper()
	t.Cleanup(func() { s.chaosReport(t, scenario, *ids) })
}

// chaosReport writes the chaos report line of a scenario to the test log and
// to $GITHUB_STEP_SUMMARY when that is set. A report that cannot be read is
// logged as such; it does not fail the scenario.
func (s *stack) chaosReport(t *testing.T, scenario string, ids []string) {
	t.Helper()
	sent, failed := -1, -1
	if len(ids) == 0 {
		sent, failed = 0, 0
	} else if c, err := statusCounts(ids); err == nil {
		sent, failed = c["sent"], c["failed"]
	} else {
		t.Logf("%s: chaos report: statuses not readable: %v", scenario, err)
	}
	rep, err := s.report()
	if err != nil {
		t.Logf("%s: chaos report: mock provider report not readable: %v", scenario, err)
		return
	}
	maxPerKey := 0
	for _, n := range rep.IdempotencyKeys {
		maxPerKey = max(maxPerKey, n)
	}
	line := fmt.Sprintf("%s%s: accepted=%d sent=%d failed=%d provider_requests=%d duplicate_keys=%d max_per_key=%d",
		chaosReportPrefix, scenario, len(ids), sent, failed, rep.Total, rep.DuplicateKeys, maxPerKey)
	t.Log(line)
	appendStepSummary(t, line)
}
