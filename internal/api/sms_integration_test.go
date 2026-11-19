//go:build integration

package api

import (
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// smsMember decodes the sms member of a notification representation:
// nil for null.
func smsMember(t *testing.T, n map[string]any) map[string]any {
	t.Helper()
	v, ok := n["sms"]
	if !ok {
		t.Fatalf("no sms member in %v", n)
	}
	if v == nil {
		return nil
	}
	return v.(map[string]any)
}

// storedSMS reads sms_encoding and sms_segments of a row.
func (a *cancelAPI) storedSMS(t *testing.T, id uuid.UUID) (enc *string, segments *int16) {
	t.Helper()
	if err := a.pool.QueryRow(ctxT(t), `SELECT sms_encoding, sms_segments FROM notifications WHERE id = $1`, id).
		Scan(&enc, &segments); err != nil {
		t.Fatal(err)
	}
	return enc, segments
}

// The encoding and segment count of an SMS are stored at creation and
// returned by every representation of the notification; every other channel
// has sms: null and stores neither.
func TestSMSInfo_StoredAndReturned(t *testing.T) {
	a := newCancelAPI(t)
	cases := []struct {
		name    string
		body    string
		want    map[string]any // nil: sms is null
		storedE string
		storedS int16
	}{
		{"sms GSM-7, one segment", req("sms", phone, "hello"), map[string]any{"encoding": "GSM-7", "segments": float64(1)}, "GSM-7", 1},
		{"sms GSM-7, two segments", req("sms", phone, strings.Repeat("a", 161)), map[string]any{"encoding": "GSM-7", "segments": float64(2)}, "GSM-7", 2},
		{"sms UCS-2 by one Turkish letter", req("sms", phone, "Merhaba dünya ş"), map[string]any{"encoding": "UCS-2", "segments": float64(1)}, "UCS-2", 1},
		{"sms UCS-2, two segments", req("sms", phone, strings.Repeat("ş", 71)), map[string]any{"encoding": "UCS-2", "segments": float64(2)}, "UCS-2", 2},
		{"email", req("email", email, "hello"), nil, "", 0},
		{"push", req("push", "fake-device-token-0001", "hello"), nil, "", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := a.post(tc.body)
			if res.status != http.StatusAccepted {
				t.Fatalf("create: %d %s", res.status, res.body)
			}
			created := res.object(t)
			id := uuid.MustParse(created["id"].(string))
			check := func(where string, n map[string]any) {
				t.Helper()
				if got := smsMember(t, n); !reflect.DeepEqual(got, tc.want) {
					t.Errorf("%s: sms %v, want %v", where, got, tc.want)
				}
			}
			check("create", created)
			check("get", a.get2(t, "/v1/notifications/"+id.String()))

			list := a.get2(t, "/v1/notifications?limit=1")
			items, _ := list["data"].([]any)
			if len(items) != 1 || items[0].(map[string]any)["id"] != id.String() {
				t.Fatalf("list: %v", list)
			}
			check("list", items[0].(map[string]any))

			enc, segs := a.storedSMS(t, id)
			if tc.want == nil {
				if enc != nil || segs != nil {
					t.Errorf("stored sms_encoding %v, sms_segments %v for a non-SMS channel", enc, segs)
				}
			} else if enc == nil || *enc != tc.storedE || segs == nil || *segs != tc.storedS {
				t.Errorf("stored sms_encoding %v, sms_segments %v", enc, segs)
			}

			// Cancel and redrive answers carry the member too.
			_, n := decodeCancel(t, a.cancel(id))
			check("cancel", n)
		})
	}

	t.Run("redrive", func(t *testing.T) {
		id := a.create(t, req("sms", phone, strings.Repeat("ş", 71)))
		a.toFailed(t, id)
		r := a.redrive(id, "")
		if r.status != http.StatusOK {
			t.Fatalf("redrive: %d %s", r.status, r.body)
		}
		if got := smsMember(t, r.object(t)); !reflect.DeepEqual(got, map[string]any{"encoding": "UCS-2", "segments": float64(2)}) {
			t.Errorf("redrive: sms %v", got)
		}
	})

	t.Run("batch", func(t *testing.T) {
		r := a.do(http.MethodPost, pathBatch, batchBody(
			req("sms", phone, strings.Repeat("ş", 71)),
			req("email", email, "hello"),
			req("sms", "05551234567", "hello"), // rejected: not E.164
		), nil)
		if r.status != http.StatusAccepted {
			t.Fatalf("batch: %d %s", r.status, r.body)
		}
		doc := decodeBatch(t, r)
		if got := doc.rejectedCodes(); !reflect.DeepEqual(got, []string{"2:recipient:invalid_e164"}) {
			t.Errorf("rejected %v", got)
		}
		ids := map[int]uuid.UUID{}
		for _, it := range doc.Accepted {
			ids[it.Index] = uuid.MustParse(it.ID)
		}
		if enc, segs := a.storedSMS(t, ids[0]); enc == nil || *enc != "UCS-2" || segs == nil || *segs != 2 {
			t.Errorf("sms item stored %v %v", enc, segs)
		}
		if enc, segs := a.storedSMS(t, ids[1]); enc != nil || segs != nil {
			t.Errorf("email item stored %v %v", enc, segs)
		}
		if got := smsMember(t, a.get2(t, "/v1/notifications/"+ids[0].String())); !reflect.DeepEqual(got, map[string]any{"encoding": "UCS-2", "segments": float64(2)}) {
			t.Errorf("get of the sms item: %v", got)
		}
		if got := smsMember(t, a.get2(t, "/v1/notifications/"+ids[1].String())); got != nil {
			t.Errorf("get of the email item: %v", got)
		}
	})
}

// get2 sends a GET and decodes the 200 body.
func (a *cancelAPI) get2(t *testing.T, path string) map[string]any {
	t.Helper()
	r := a.apiClient.get(path)
	if r.status != http.StatusOK {
		t.Fatalf("GET %s: %d %s", path, r.status, r.body)
	}
	var m map[string]any
	if err := json.Unmarshal(r.body, &m); err != nil {
		t.Fatal(err)
	}
	return m
}
