//go:build integration

package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store/db"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// listOrder returns the ids of the notifications in the order of the listing,
// read with plain SQL.
func listOrder(t *testing.T, pool *pgxpool.Pool, where string, args ...any) []uuid.UUID {
	t.Helper()
	rows, err := pool.Query(ctxT(t), `SELECT id FROM notifications `+where+` ORDER BY created_at DESC, id DESC`, args...)
	if err != nil {
		t.Fatalf("order: %v", err)
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

func idsOf(page []Notification) []uuid.UUID {
	ids := make([]uuid.UUID, len(page))
	for i, n := range page {
		ids[i] = n.ID
	}
	return ids
}

// pageSteps follows the cursors from f until there is no next page. after
// runs after each page, with the number of pages read so far (it may insert
// rows). It returns the ids in the order read and the page count.
func pageSteps(t *testing.T, s *Store, f ListFilter, after func(pages int)) ([]uuid.UUID, int) {
	t.Helper()
	var ids []uuid.UUID
	pages := 0
	for {
		page, next, err := s.ListNotifications(ctxT(t), f)
		if err != nil {
			t.Fatalf("page %d: %v", pages+1, err)
		}
		pages++
		if len(page) > f.Limit {
			t.Fatalf("page %d has %d rows, limit %d", pages, len(page), f.Limit)
		}
		ids = append(ids, idsOf(page)...)
		if after != nil {
			after(pages)
		}
		if next == nil {
			return ids, pages
		}
		if len(page) != f.Limit {
			t.Fatalf("page %d has a next cursor but %d rows, limit %d", pages, len(page), f.Limit)
		}
		if last := page[len(page)-1]; next.ID != last.ID || !next.CreatedAt.Equal(last.CreatedAt) {
			t.Fatalf("page %d: cursor %+v is not the last row %v", pages, next, last.ID)
		}
		f.After = next
		if pages > 10_000 {
			t.Fatal("the walk does not end")
		}
	}
}

// seedSameSecond creates n rows: most with batch creates (one transaction,
// so they share one created_at) and the rest one by one.
func seedSameSecond(t *testing.T, s *Store, n int) {
	t.Helper()
	single := 50
	for left := n - single; left > 0; {
		size := min(left, 150)
		items := make([]NewNotification, size)
		for i := range items {
			items[i] = item(withLane(domain.Channels()[i%3], domain.Priorities()[i%3]))
		}
		createBatch(t, s, nil, batchOf(items...))
		left -= size
	}
	for range single {
		create(t, s)
	}
}

func setStatusOf(t *testing.T, pool *pgxpool.Pool, ids []uuid.UUID, status string) {
	t.Helper()
	reason := "NULL"
	sentAt := "NULL"
	switch status {
	case "failed":
		reason = "'provider_rejected'"
	case "sent":
		sentAt = "now()"
	}
	exec(t, pool, fmt.Sprintf(`UPDATE notifications SET status = $2, failure_reason = %s, sent_at = %s, version = version + 1 WHERE id = ANY($1)`, reason, sentAt), ids, status)
}

func TestList_PaginationWalk(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	seedSameSecond(t, s, 500)

	var maxShared int
	if err := pool.QueryRow(ctxT(t), `SELECT max(c) FROM (SELECT count(*) c FROM notifications GROUP BY created_at) g`).Scan(&maxShared); err != nil {
		t.Fatal(err)
	}
	if maxShared < 100 {
		t.Fatalf("at most %d rows share a created_at; the walk needs ties", maxShared)
	}
	want := listOrder(t, pool, "")
	if len(want) != 500 {
		t.Fatalf("seeded %d rows", len(want))
	}

	t.Run("all rows", func(t *testing.T) {
		got, pages := pageSteps(t, s, ListFilter{Limit: 7}, nil)
		if pages != 72 {
			t.Errorf("%d pages, want 72", pages)
		}
		if !slices.Equal(got, want) {
			t.Errorf("the walk does not return every row exactly once in order (got %d rows, want %d)", len(got), len(want))
		}
	})

	t.Run("a last page of exactly limit rows has no next cursor", func(t *testing.T) {
		got, pages := pageSteps(t, s, ListFilter{Limit: 125}, nil)
		if pages != 4 || len(got) != 500 {
			t.Errorf("%d pages, %d rows; want 4 pages of 125", pages, len(got))
		}
	})

	t.Run("status filter", func(t *testing.T) {
		moveTo := map[string][]uuid.UUID{}
		for i, id := range want {
			switch i % 5 {
			case 1:
				moveTo["cancelled"] = append(moveTo["cancelled"], id)
			case 2:
				moveTo["sent"] = append(moveTo["sent"], id)
			case 3:
				moveTo["failed"] = append(moveTo["failed"], id)
			}
		}
		for status, ids := range moveTo {
			setStatusOf(t, pool, ids, status)
		}
		for _, status := range []domain.Status{domain.StatusPending, domain.StatusCancelled, domain.StatusSent, domain.StatusFailed} {
			wantIDs := listOrder(t, pool, `WHERE status = $1`, string(status))
			got, pages := pageSteps(t, s, ListFilter{Limit: 7, Status: &status}, nil)
			if !slices.Equal(got, wantIDs) {
				t.Errorf("status %s: got %d rows, want %d", status, len(got), len(wantIDs))
			}
			if wantPages := (len(wantIDs) + 6) / 7; pages != wantPages {
				t.Errorf("status %s: %d pages, want %d", status, pages, wantPages)
			}
		}
		// Nothing is in processing.
		processing := domain.StatusProcessing
		if got, pages := pageSteps(t, s, ListFilter{Limit: 7, Status: &processing}, nil); len(got) != 0 || pages != 1 {
			t.Errorf("empty status: %d rows in %d pages", len(got), pages)
		}
	})
}

// Rows inserted while a client pages never show up in later pages, and no
// row is skipped or repeated.
func TestList_InsertBetweenPages(t *testing.T) {
	for _, withStatus := range []bool{false, true} {
		t.Run(fmt.Sprintf("status filter %v", withStatus), func(t *testing.T) {
			s, pool := newTestStore(t, zeroDelay)
			seedSameSecond(t, s, 120)
			f := ListFilter{Limit: 7}
			if withStatus {
				st := domain.StatusPending
				f.Status = &st
			}
			want := listOrder(t, pool, `WHERE status = 'pending'`)
			inserted := map[uuid.UUID]bool{}
			got, _ := pageSteps(t, s, f, func(pages int) {
				if pages%3 == 0 {
					inserted[create(t, s).ID] = true
				}
				if pages == 5 {
					items := []NewNotification{item(), item(), item()}
					for _, r := range createBatch(t, s, nil, batchOf(items...)).Items {
						inserted[r.ID] = true
					}
				}
			})
			if len(inserted) == 0 {
				t.Fatal("nothing was inserted")
			}
			seen := map[uuid.UUID]bool{}
			for _, id := range got {
				if seen[id] {
					t.Fatalf("row %v returned twice", id)
				}
				seen[id] = true
				if inserted[id] {
					t.Errorf("row %v, inserted after the first page was read, appears in a later page", id)
				}
			}
			if !slices.Equal(got, want) {
				t.Errorf("rows skipped or reordered: got %d, want %d", len(got), len(want))
			}
		})
	}
}

// seedFixture creates 24 rows: every channel x priority x (batched or not),
// one microsecond-spaced created_at each, then moves some to other statuses.
func seedFixture(t *testing.T, s *Store, pool *pgxpool.Pool) (rows []Notification, batchID uuid.UUID) {
	t.Helper()
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	var items []NewNotification
	var singles []NewNotification
	for _, c := range domain.Channels() {
		for _, p := range domain.Priorities() {
			for range 2 {
				items = append(items, item(withLane(c, p)))
			}
			singles = append(singles, item(withLane(c, p)), item(withLane(c, p)))
		}
	}
	res := createBatch(t, s, nil, batchOf(items...))
	batchID = res.BatchID
	var ids []uuid.UUID
	for _, r := range res.Items {
		ids = append(ids, r.ID)
	}
	for _, n := range singles {
		ids = append(ids, create(t, s, func(x *NewNotification) { *x = n }).ID)
	}
	for i, id := range ids {
		exec(t, pool, `UPDATE notifications SET created_at = $2 WHERE id = $1`, id, base.Add(time.Duration(i)*time.Microsecond))
	}
	// Statuses: every third row is cancelled, every fourth sent.
	var cancelled, sent []uuid.UUID
	for i, id := range ids {
		switch {
		case i%3 == 0:
			cancelled = append(cancelled, id)
		case i%4 == 0:
			sent = append(sent, id)
		}
	}
	setStatusOf(t, pool, cancelled, "cancelled")
	setStatusOf(t, pool, sent, "sent")
	for _, id := range slices.Backward(listOrder(t, pool, "")) {
		rows = append(rows, get(t, s, id))
	}
	slices.Reverse(rows) // newest first
	return rows, batchID
}

func TestList_Filters(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	all, batchID := seedFixture(t, s, pool)
	if len(all) != 36 {
		t.Fatalf("fixture has %d rows", len(all))
	}
	sentStatus, cancelled := domain.StatusSent, domain.StatusCancelled
	push, sms := domain.ChannelPush, domain.ChannelSMS
	low, high := domain.PriorityLow, domain.PriorityHigh
	mid := all[len(all)/2].CreatedAt

	filter := func(keep func(Notification) bool) []uuid.UUID {
		var ids []uuid.UUID
		for _, n := range all {
			if keep(n) {
				ids = append(ids, n.ID)
			}
		}
		return ids
	}
	tests := []struct {
		name string
		f    ListFilter
		keep func(Notification) bool
	}{
		{"none", ListFilter{}, func(Notification) bool { return true }},
		{"status", ListFilter{Status: &sentStatus}, func(n Notification) bool { return n.Status == sentStatus }},
		{"channel", ListFilter{Channel: &push}, func(n Notification) bool { return n.Channel == push }},
		{"priority", ListFilter{Priority: &low}, func(n Notification) bool { return n.Priority == low }},
		{"batch", ListFilter{BatchID: &batchID}, func(n Notification) bool { return n.BatchID != nil && *n.BatchID == batchID }},
		{"from", ListFilter{CreatedFrom: &mid}, func(n Notification) bool { return !n.CreatedAt.Before(mid) }},
		{"to", ListFilter{CreatedTo: &mid}, func(n Notification) bool { return n.CreatedAt.Before(mid) }},
		{
			"from and to",
			ListFilter{CreatedFrom: ptr(all[20].CreatedAt), CreatedTo: ptr(all[5].CreatedAt)},
			func(n Notification) bool {
				return !n.CreatedAt.Before(all[20].CreatedAt) && n.CreatedAt.Before(all[5].CreatedAt)
			},
		},
		{"status and channel", ListFilter{Status: &cancelled, Channel: &sms}, func(n Notification) bool { return n.Status == cancelled && n.Channel == sms }},
		{
			"all together",
			ListFilter{Status: &sentStatus, Channel: &push, Priority: &high, BatchID: &batchID, CreatedFrom: ptr(all[len(all)-1].CreatedAt), CreatedTo: ptr(all[0].CreatedAt.Add(time.Microsecond))},
			func(n Notification) bool {
				return n.Status == sentStatus && n.Channel == push && n.Priority == high && n.BatchID != nil && *n.BatchID == batchID
			},
		},
		{
			"channel, priority and a cursor",
			ListFilter{Channel: &sms, Priority: &low, After: &domain.Cursor{CreatedAt: mid, ID: uuid.Max}},
			func(n Notification) bool { return n.Channel == sms && n.Priority == low && !n.CreatedAt.After(mid) },
		},
		{"from after to is empty", ListFilter{CreatedFrom: ptr(all[0].CreatedAt), CreatedTo: ptr(all[len(all)-1].CreatedAt)}, func(Notification) bool { return false }},
		{"from equal to is empty", ListFilter{CreatedFrom: &mid, CreatedTo: &mid}, func(Notification) bool { return false }},
	}
	for _, tt := range tests {
		for _, limit := range []int{200, 5} {
			f := tt.f
			f.Limit = limit
			got, _ := pageSteps(t, s, f, nil)
			want := filter(tt.keep)
			if !slices.Equal(got, want) {
				t.Errorf("%s (limit %d): got %d rows, want %d", tt.name, limit, len(got), len(want))
			}
		}
	}

	// A batch id that does not exist selects nothing.
	got, _ := pageSteps(t, s, ListFilter{Limit: 10, BatchID: ptr(uuid.New())}, nil)
	if len(got) != 0 {
		t.Errorf("unknown batch: %d rows", len(got))
	}
}

// The date range is half open, at microsecond boundaries.
func TestList_DateRangeIsHalfOpen(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	var ids []uuid.UUID
	for i := range 5 {
		id := create(t, s).ID
		exec(t, pool, `UPDATE notifications SET created_at = $2 WHERE id = $1`, id, base.Add(time.Duration(i)*time.Microsecond))
		ids = append(ids, id)
	}
	// Newest first: ids[4] ... ids[0].
	tests := []struct {
		name     string
		from, to *time.Time
		want     []uuid.UUID
	}{
		{"from is inclusive", ptr(base.Add(2 * time.Microsecond)), nil, []uuid.UUID{ids[4], ids[3], ids[2]}},
		{"to is exclusive", nil, ptr(base.Add(2 * time.Microsecond)), []uuid.UUID{ids[1], ids[0]}},
		{"one row", ptr(base.Add(2 * time.Microsecond)), ptr(base.Add(3 * time.Microsecond)), []uuid.UUID{ids[2]}},
		{"one microsecond before from", ptr(base.Add(2*time.Microsecond + time.Microsecond)), nil, []uuid.UUID{ids[4], ids[3]}},
		{"to one microsecond later", nil, ptr(base.Add(2*time.Microsecond + time.Microsecond)), []uuid.UUID{ids[2], ids[1], ids[0]}},
		{"empty range", ptr(base.Add(2 * time.Microsecond)), ptr(base.Add(2 * time.Microsecond)), nil},
	}
	for _, tt := range tests {
		page, next, err := s.ListNotifications(ctxT(t), ListFilter{CreatedFrom: tt.from, CreatedTo: tt.to, Limit: 10})
		if err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}
		if next != nil || !slices.Equal(idsOf(page), tt.want) {
			t.Errorf("%s: got %v (next %v), want %v", tt.name, idsOf(page), next, tt.want)
		}
	}
}

func TestList_LimitBounds(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	create(t, s)
	for _, limit := range []int{-1, 0, 201, 1000} {
		if _, _, err := s.ListNotifications(ctxT(t), ListFilter{Limit: limit}); err == nil {
			t.Errorf("limit %d: no error", limit)
		}
	}
	for _, limit := range []int{1, 200} {
		page, next, err := s.ListNotifications(ctxT(t), ListFilter{Limit: limit})
		if err != nil || len(page) != 1 || next != nil {
			t.Errorf("limit %d: %d rows, next %v, err %v", limit, len(page), next, err)
		}
	}
}

func TestList_EmptyTable(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	page, next, err := s.ListNotifications(ctxT(t), ListFilter{Limit: 10})
	if err != nil || len(page) != 0 || next != nil {
		t.Errorf("%d rows, next %v, err %v", len(page), next, err)
	}
}

func TestListAttempts_UnknownAndEmpty(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	if _, err := s.ListAttempts(ctxT(t), uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown id: error %v, want ErrNotFound", err)
	}
	n := create(t, s)
	got, err := s.ListAttempts(ctxT(t), n.ID)
	if err != nil || got == nil || len(got) != 0 {
		t.Errorf("no attempts: %v, %v; want an empty slice and no error", got, err)
	}
	start := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	exec(t, pool, `INSERT INTO notification_attempts (notification_id, attempt_no, channel, provider, outcome, started_at, finished_at, http_status, latency_ms)
		VALUES ($1, 2, 'sms', 'mock', 'in_flight', $2, NULL, NULL, NULL), ($1, 1, 'sms', 'mock', 'retryable', $3, $2, 503, 12)`,
		n.ID, start.Add(time.Second), start)
	got, err = s.ListAttempts(ctxT(t), n.ID)
	if err != nil || len(got) != 2 || got[0].AttemptNo != 1 || got[1].AttemptNo != 2 {
		t.Fatalf("attempts %+v, err %v", got, err)
	}
	if got[0].Outcome != domain.OutcomeRetryable || got[0].HTTPStatus == nil || *got[0].HTTPStatus != 503 || got[1].FinishedAt != nil {
		t.Errorf("attempts %+v", got)
	}
}

// planNode is one node of EXPLAIN (FORMAT JSON).
type planNode struct {
	NodeType  string     `json:"Node Type"`
	IndexName string     `json:"Index Name"`
	IndexCond string     `json:"Index Cond"`
	Plans     []planNode `json:"Plans"`
}

func (p planNode) walk(fn func(planNode)) {
	fn(p)
	for _, c := range p.Plans {
		c.walk(fn)
	}
}

func explain(t *testing.T, conn *pgxpool.Conn, sql string) (planNode, string) {
	t.Helper()
	var raw []byte
	if err := conn.QueryRow(ctxT(t), sql).Scan(&raw); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	var doc []struct {
		Plan planNode `json:"Plan"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil || len(doc) != 1 {
		t.Fatalf("plan %s: %v", raw, err)
	}
	pretty, err := json.MarshalIndent(json.RawMessage(raw), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return doc[0].Plan, string(pretty)
}

// seedForPlans inserts n rows with mixed statuses and channels, several per
// created_at, directly (faster than n creates). "retrying" is rare (1 %),
// "pending" is frequent (about 22 %).
func seedForPlans(t *testing.T, pool *pgxpool.Pool, n int) {
	t.Helper()
	exec(t, pool, `
		INSERT INTO notifications (id, channel, priority, recipient, content, status, failure_reason, sent_at,
		                           due_at, expires_at, max_failures, correlation_id, created_at, updated_at)
		SELECT uuidv7(),
		       (ARRAY['sms','email','push'])[1 + i % 3],
		       i % 3,
		       '+905551234567', 'x',
		       st, CASE WHEN st = 'failed' THEN 'provider_rejected' END, CASE WHEN st = 'sent' THEN now() END,
		       now(), now() + interval '1 hour', 5, 'corr',
		       timestamptz '2026-03-01 12:00:00+00' + (i / 3) * interval '1 millisecond',
		       now()
		FROM (SELECT i, CASE WHEN i % 100 = 0 THEN 'retrying'
		                     ELSE (ARRAY['pending','pending','queued','sent','sent','sent','failed','cancelled'])[1 + i % 8] END AS st
		      FROM generate_series(1, $1) AS i) g`, n)
	exec(t, pool, `ANALYZE notifications`)
}

// explainList prepares sql on one connection under the plan cache mode and
// returns the plan of EXECUTE with args.
func explainList(t *testing.T, pool *pgxpool.Pool, mode, sql, args string) (planNode, string) {
	t.Helper()
	conn, err := pool.Acquire(ctxT(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		// Leave no prepared statement or setting on the pooled connection.
		_, _ = conn.Exec(context.WithoutCancel(ctxT(t)), `DEALLOCATE list_q; RESET plan_cache_mode`)
		conn.Release()
	}()
	if _, err := conn.Exec(ctxT(t), `SET plan_cache_mode = `+mode); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctxT(t), `PREPARE list_q AS `+sql); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	return explain(t, conn, `EXPLAIN (FORMAT JSON) EXECUTE list_q(`+args+`)`)
}

// indexScan returns the one scan node of the plan and fails when the plan
// has a Sort, a Seq Scan or a bitmap node.
func indexScan(t *testing.T, plan planNode, text string) planNode {
	t.Helper()
	var scans []planNode
	plan.walk(func(n planNode) {
		switch {
		case strings.Contains(n.NodeType, "Sort"), n.NodeType == "Seq Scan", strings.Contains(n.NodeType, "Bitmap"):
			t.Errorf("the plan has a %s node:\n%s", n.NodeType, text)
		case strings.HasPrefix(n.NodeType, "Index") && n.IndexName != "":
			scans = append(scans, n)
		}
	})
	if len(scans) != 1 || scans[0].NodeType != "Index Scan" {
		t.Fatalf("want exactly one Index Scan:\n%s", text)
	}
	return scans[0]
}

func TestList_ExplainUsesIndex(t *testing.T) {
	_, pool := newTestStore(t, zeroDelay)
	seedForPlans(t, pool, 30_000)

	// A cursor in the middle of the data.
	var curAt time.Time
	var curID uuid.UUID
	if err := pool.QueryRow(ctxT(t), `SELECT created_at, id FROM notifications ORDER BY created_at DESC, id DESC OFFSET 15000 LIMIT 1`).Scan(&curAt, &curID); err != nil {
		t.Fatal(err)
	}
	curLit := fmt.Sprintf("'%s'::timestamptz, '%s'::uuid", curAt.UTC().Format("2006-01-02 15:04:05.999999+00"), curID)

	cases := []struct {
		name, sql string
		status    string // "" for the plain query
		// indexes accepts these listing indexes.
		indexes []string
		// statusInCond requires the status in the Index Cond.
		statusInCond bool
	}{
		{"ListNotifications", db.ListNotificationsSQL, "", []string{"notifications_created_idx"}, false},
		{"ListNotificationsByStatus frequent", db.ListNotificationsByStatusSQL, "pending", []string{"notifications_status_created_idx", "notifications_created_idx"}, false},
		{"ListNotificationsByStatus rare", db.ListNotificationsByStatusSQL, "retrying", []string{"notifications_status_created_idx"}, true},
	}
	for _, c := range cases {
		for _, page := range []string{"first page", "page after a cursor"} {
			t.Run(c.name+"/force_custom_plan/"+page, func(t *testing.T) {
				cur := "NULL, NULL"
				if page != "first page" {
					cur = curLit
				}
				args := cur + ", NULL, NULL, NULL, NULL, NULL, 51"
				if c.status != "" {
					args = "'" + c.status + "', " + args
				}
				plan, text := explainList(t, pool, "force_custom_plan", c.sql, args)
				scan := indexScan(t, plan, text)
				if !slices.Contains(c.indexes, scan.IndexName) {
					t.Errorf("index %s, want one of %v:\n%s", scan.IndexName, c.indexes, text)
				}
				cond := scan.IndexCond
				if !strings.Contains(cond, "created_at") || !strings.Contains(cond, "id") || !strings.Contains(strings.ToUpper(cond), "ROW(") {
					t.Errorf("the Index Cond %q has no row comparison on (created_at, id):\n%s", cond, text)
				}
				if c.statusInCond && !strings.Contains(cond, "status") {
					t.Errorf("the Index Cond %q has no status condition:\n%s", cond, text)
				}
				t.Logf("%s / %s: %s on %s, Index Cond: %s", c.name, page, scan.NodeType, scan.IndexName, cond)
			})
		}
	}

	// Documents the finding, asserts nothing: planned generically, the
	// status query's optional filters give an estimate of about one row, and
	// the plan sorts (a bitmap scan and a Sort). This is why the store runs
	// the listing queries with custom plans (customPlanDB).
	t.Run("ListNotificationsByStatus/force_generic_plan (not asserted)", func(t *testing.T) {
		_, text := explainList(t, pool, "force_generic_plan", db.ListNotificationsByStatusSQL,
			"'pending', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 51")
		t.Logf("generic plan of the status query:\n%s", text)
	})
}

// The listing queries never leave a prepared statement behind: on a pool of
// one connection (so every call uses the same session) pg_prepared_statements
// has none of them, while a statement run the default way (GetNotification)
// is there, so the check can see cached statements.
func TestList_RunsWithCustomPlans(t *testing.T) {
	url := testutil.NewMigratedDatabase(t)
	pool, err := OpenPool(context.Background(), url+"&pool_max_conns=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s := New(pool, zeroDelay)
	n := create(t, s)

	status := domain.StatusPending
	for range 10 {
		if _, _, err := s.ListNotifications(ctxT(t), ListFilter{Limit: 5}); err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.ListNotifications(ctxT(t), ListFilter{Limit: 5, Status: &status}); err != nil {
			t.Fatal(err)
		}
	}
	count := func(like string) int {
		var c int
		if err := pool.QueryRow(ctxT(t), `SELECT count(*) FROM pg_prepared_statements WHERE statement LIKE $1`, like).Scan(&c); err != nil {
			t.Fatal(err)
		}
		return c
	}
	const listLike = `%ORDER BY created_at DESC, id DESC%`
	if c := count(listLike); c != 0 {
		t.Errorf("%d listing statements are prepared on the connection, want 0", c)
	}

	// Control.
	for range 3 {
		get(t, s, n.ID)
	}
	if c := count(`%FROM notifications WHERE id = $1%`); c == 0 {
		t.Error("the control statement (GetNotification) is not in pg_prepared_statements: the check cannot see cached statements")
	}
}
