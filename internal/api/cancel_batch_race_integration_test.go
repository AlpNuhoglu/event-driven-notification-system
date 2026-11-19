//go:build integration

package api

import (
	"encoding/json"
	"net/http"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

const (
	// httpRaceBatchSize is the largest batch the API accepts.
	httpRaceBatchSize = 1000
	httpRaceRounds    = 5
)

// createBatchRows inserts a batch of n notifications through the store, as
// the batch endpoint does, and returns its id.
func (a *cancelAPI) createBatchRows(t *testing.T, n int) uuid.UUID {
	t.Helper()
	items := make([]store.NewNotification, n)
	for i := range items {
		items[i] = store.NewNotification{
			Channel: domain.ChannelSMS, Priority: domain.PriorityNormal, Recipient: domain.Recipient(phone),
			Content: "hello", TTL: testRetry().Normal.TTL, MaxFailures: testRetry().Normal.MaxFailures,
			CorrelationID: "corr-batch", TraceParent: traceparent,
		}
	}
	res, err := a.store.CreateBatch(ctxT(t), nil, store.NewBatch{Total: n, CorrelationID: "corr-batch", Items: items},
		func(store.BatchResult) (store.StoredResponse, error) {
			return store.StoredResponse{Status: http.StatusAccepted, Body: []byte(`{}`)}, nil
		})
	if err != nil || res.Accepted != n {
		t.Fatalf("create batch: accepted %d, %v", res.Accepted, err)
	}
	return res.BatchID
}

// Two concurrent POST /v1/batches/{id}/cancel of one 1,000-row batch: both
// answer 200 (never a 500 from a deadlock), with counts that add up, the
// later one counting every row, and one event per row in all.
func TestCancelBatch_ConcurrentHTTP(t *testing.T) {
	a := newCancelAPI(t)
	for round := range httpRaceRounds {
		batchID := a.createBatchRows(t, httpRaceBatchSize)
		start := make(chan struct{})
		var (
			wg   sync.WaitGroup
			resp [2]response
		)
		for i := range resp {
			wg.Go(func() {
				<-start
				resp[i] = a.cancelBatch(batchID)
			})
		}
		close(start)
		wg.Wait()

		maxCancelled := 0
		for i, r := range resp {
			if r.status != http.StatusOK {
				t.Fatalf("round %d, call %d: %d %s", round, i, r.status, r.body)
			}
			var c map[string]int
			if err := json.Unmarshal(r.body, &c); err != nil {
				t.Fatal(err)
			}
			if c["cancelled"]+c["not_cancellable"] != httpRaceBatchSize || c["may_have_been_sent"] != 0 {
				t.Errorf("round %d, call %d: counts %v", round, i, c)
			}
			maxCancelled = max(maxCancelled, c["cancelled"])
		}
		if maxCancelled != httpRaceBatchSize {
			t.Errorf("round %d: the later answer counts %d cancelled, want %d", round, maxCancelled, httpRaceBatchSize)
		}
	}
	seen := map[uuid.UUID]bool{}
	for _, ev := range a.events.all() {
		if seen[ev.ID] || ev.Status != domain.StatusCancelled || ev.Version != 2 {
			t.Fatalf("event %+v (seen before: %v)", ev, seen[ev.ID])
		}
		seen[ev.ID] = true
	}
	if len(seen) != httpRaceRounds*httpRaceBatchSize {
		t.Errorf("%d rows with an event, want %d", len(seen), httpRaceRounds*httpRaceBatchSize)
	}
}
