package api

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// refusedIDSpellings are the spellings of an id that the UUID library
// accepts and the API refuses: one id has one URL.
var refusedIDSpellings = []struct {
	name  string
	spell func(string) string
}{
	{"urn prefix", func(s string) string { return "urn:uuid:" + s }},
	{"braces", func(s string) string { return "{" + s + "}" }},
	{"32 digits", func(s string) string { return strings.ReplaceAll(s, "-", "") }},
	{"wrapped id", func(s string) string { return "x" + s + "y" }},
}

// idEndpoint is an operation with the id path parameter; path has %s where
// the id goes.
type idEndpoint struct {
	op, method, path string
}

var idEndpoints = []idEndpoint{
	{"getNotification", http.MethodGet, "/v1/notifications/%s"},
	{"listNotificationAttempts", http.MethodGet, "/v1/notifications/%s/attempts"},
	{"cancelNotification", http.MethodPost, "/v1/notifications/%s/cancel"},
	{"redriveNotification", http.MethodPost, "/v1/notifications/%s/redrive"},
	{"getBatch", http.MethodGet, "/v1/batches/%s"},
	{"cancelBatch", http.MethodPost, "/v1/batches/%s/cancel"},
}

func (e idEndpoint) url(id string) string {
	return strings.Replace(e.path, "%s", url.PathEscape(id), 1)
}

// idRepo is a fakeRepo that records the ids its reads, cancels and redrive
// were called with; every id is unknown (404).
type idRepo struct {
	fakeRepo
	mu  sync.Mutex
	ids []uuid.UUID
}

func (f *idRepo) record(id uuid.UUID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids = append(f.ids, id)
}

func (f *idRepo) calls() []uuid.UUID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.ids)
}

func (f *idRepo) GetNotification(ctx context.Context, id uuid.UUID) (store.Notification, error) {
	f.record(id)
	return f.fakeRepo.GetNotification(ctx, id)
}

func (f *idRepo) GetBatch(ctx context.Context, id uuid.UUID) (store.Batch, error) {
	f.record(id)
	return f.fakeRepo.GetBatch(ctx, id)
}

func (f *idRepo) ListAttempts(ctx context.Context, id uuid.UUID) ([]store.Attempt, error) {
	f.record(id)
	return f.fakeRepo.ListAttempts(ctx, id)
}

func (f *idRepo) CancelNotification(ctx context.Context, id uuid.UUID) (store.Cancelled, domain.StatusChanged, error) {
	f.record(id)
	return f.fakeRepo.CancelNotification(ctx, id)
}

func (f *idRepo) CancelBatch(ctx context.Context, id uuid.UUID) (store.CancelledBatch, []domain.StatusChanged, error) {
	f.record(id)
	return f.fakeRepo.CancelBatch(ctx, id)
}

func (f *idRepo) RedriveNotification(ctx context.Context, id uuid.UUID, at *time.Time, lead time.Duration) (store.Notification, domain.StatusChanged, error) {
	f.record(id)
	return f.fakeRepo.RedriveNotification(ctx, id, at, lead)
}

func newIDAPI(t *testing.T) (*apiClient, *idRepo) {
	t.Helper()
	repo := &idRepo{}
	h := NewHandler(Options{
		Repository:   repo,
		DB:           repo,
		Logger:       slog.New(slog.DiscardHandler),
		Retry:        testRetry(),
		MaxBodyBytes: 1024,
		DBTimeout:    1000 * time.Millisecond,
	})
	return newAPIClient(t, h), repo
}

// idEndpoints lists every operation of the specification that has the id
// path parameter, so that a new one is not left out of the tests below.
func TestIDEndpoints_AreEveryOperationWithAnIdPathParameter(t *testing.T) {
	doc := loadContract(t).doc
	var want []string
	for path, item := range doc.Paths.Map() {
		for method, op := range item.Operations() {
			for _, p := range op.Parameters {
				if p.Value != nil && p.Value.In == "path" && p.Value.Name == paramID {
					want = append(want, method+" "+strings.Replace(path, "{id}", "%s", 1)+" "+op.OperationID)
				}
			}
		}
	}
	var got []string
	for _, e := range idEndpoints {
		got = append(got, e.method+" "+e.path+" "+e.op)
	}
	slices.Sort(want)
	slices.Sort(got)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("idEndpoints\n got %v\nwant %v", got, want)
	}
}

// Every operation with an id path parameter answers an id in another
// spelling with the 400 of an id that is not a UUID, and never reaches the
// store; the responses are checked against the contract.
func TestPathID_OtherSpellingsAreRefused(t *testing.T) {
	c, repo := newIDAPI(t)
	id := uuid.Must(uuid.NewV7()).String()
	for _, e := range idEndpoints {
		for _, sp := range refusedIDSpellings {
			res := c.do(e.method, e.url(sp.spell(id)), "", nil)
			if res.status != http.StatusBadRequest {
				t.Errorf("%s with %s: status %d: %s", e.op, sp.name, res.status, res.body)
				continue
			}
			if got := res.problemCodes(t); !reflect.DeepEqual(got, []string{"id:invalid_uuid"}) {
				t.Errorf("%s with %s: errors %v", e.op, sp.name, got)
			}
		}
	}
	if calls := repo.calls(); len(calls) != 0 {
		t.Errorf("the store was called for refused ids: %v", calls)
	}
}

// The canonical form is accepted in lower and in upper case, and both name
// the same id.
func TestPathID_CanonicalFormInEitherCaseIsAccepted(t *testing.T) {
	c, repo := newIDAPI(t)
	want := uuid.Must(uuid.NewV7())
	for _, e := range idEndpoints {
		for _, s := range []string{want.String(), strings.ToUpper(want.String())} {
			res := c.do(e.method, e.url(s), "", nil)
			if res.status != http.StatusNotFound {
				t.Errorf("%s with %s: status %d, want the store's 404: %s", e.op, s, res.status, res.body)
			}
		}
	}
	calls := repo.calls()
	if len(calls) != 2*len(idEndpoints) {
		t.Fatalf("the store was called %d times, want %d", len(calls), 2*len(idEndpoints))
	}
	for _, got := range calls {
		if got != want {
			t.Errorf("the store was called with %s, want %s", got, want)
		}
	}
}

// The batch_id query parameter of the listing refuses the same spellings
// with invalid_parameter, as it refuses any other value that is not an id,
// and accepts the canonical form in upper case.
func TestListBatchID_OtherSpellingsAreRefused(t *testing.T) {
	repo := &listRepo{}
	c, _ := newListAPI(t, repo)
	id := uuid.Must(uuid.NewV7())
	for _, sp := range refusedIDSpellings {
		res := c.get("/v1/notifications?batch_id=" + url.QueryEscape(sp.spell(id.String())))
		if res.status != http.StatusBadRequest {
			t.Errorf("%s: status %d: %s", sp.name, res.status, res.body)
			continue
		}
		if got := res.problemCodes(t); !reflect.DeepEqual(got, []string{"batch_id:invalid_parameter"}) {
			t.Errorf("%s: errors %v", sp.name, got)
		}
	}
	if repo.calls != 0 {
		t.Errorf("the store was called %d times for refused ids", repo.calls)
	}

	res := c.get("/v1/notifications?batch_id=" + strings.ToUpper(id.String()))
	if res.status != http.StatusOK {
		t.Fatalf("upper case: status %d: %s", res.status, res.body)
	}
	if repo.filter.BatchID == nil || *repo.filter.BatchID != id {
		t.Errorf("filter batch_id %v, want %s", repo.filter.BatchID, id)
	}
}
