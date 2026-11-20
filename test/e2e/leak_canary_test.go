//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
)

// The PII canary scan (ADR-0009 §4, D42). The test sends notifications and
// failing requests that carry values occurring nowhere else (the canaries),
// makes the provider quote them back, and then searches everything the
// stack emits for them: the logs of every service, every exported span,
// every /metrics output and the series Prometheus stored, the mock
// provider's report, and every answer of the API and the gateway. Any match
// fails the test. Every source must also show that it was searched: it is
// not empty and holds a value that must be there (a control).
//
// The file name sorts after the scenarios that produce ordinary traffic and
// before loadsmoke_test.go, so the logs searched are not those of the load
// smoke test's thousands of notifications.

const (
	defaultJaegerURL = "http://localhost:16686"
	// canaryFinalTimeout bounds the wait for a notification's final status
	// (sent, or failed for the permanent error).
	canaryFinalTimeout = 60 * time.Second
	// canaryOutputTimeout bounds the wait until the output of the run has
	// arrived: the control lines in the logs, the control trace in Jaeger,
	// a scrape of every Prometheus target after the traffic.
	canaryOutputTimeout = 90 * time.Second
	// outputPollInterval is how often the arrival of the output is checked.
	outputPollInterval = time.Second
	// wsReadTimeout bounds the wait for the gateway's answer to one request.
	wsReadTimeout = 5 * time.Second
	// tooLargeBody is larger than MAX_BODY_BYTES (5 MB by default).
	tooLargeBody = 6 << 20
)

// canaryNames is the one list of the canaries of the run. Every canary is a
// random token (canaryToken) except recipient.sms, which is the reserved
// fake number (D71): only its masked form may ever appear. The value sent
// contains the token; the token is what is searched, without regard to
// case, so that a part of a value or a lower-cased copy is found too, and
// its first tokenMatch characters are enough (canary.matches), so that a
// value cut short is found as well.
var canaryNames = []string{
	// Recipients of stored notifications. A successful answer that
	// documents the notification returns them (ADR-0006 §4).
	"recipient.email",
	"recipient.push",
	// Content, one per channel, and template variable values.
	"content.sms", "content.email", "content.push",
	"template.var.code", "template.var.name",
	// Values of failed requests: recipients invalid for their channel,
	// contents over their limit, other fields.
	"invalid.recipient.sms", "invalid.recipient.email", "invalid.recipient.push", "invalid.recipient.too_long",
	"invalid.content.email_too_long", "invalid.content.push_too_long", "invalid.content.sms_segments",
	"invalid.content.nul", "invalid.channel", "invalid.priority", "invalid.scheduled_at", "invalid.expires_at",
	"invalid.template.name", "invalid.template.var.unknown", "invalid.template.var.too_long",
	"invalid.template.var.object", "invalid.template.body", "unknown_member",
	"body.malformed_json", "body.not_an_object", "body.unexpected", "body.too_large", "body.unsupported_type",
	"duplicate.content", "idempotency_reused.content", "batch.rejected.recipient",
	// Text a client puts outside the body.
	"header.user_agent", "header.custom", "header.request_id", "header.traceparent", "header.idempotency_key",
	"header.host", "header.forwarded_for", "header.origin",
	"method", "path.id", "path.unknown", "path.template_name", "path.version",
	"query.unknown_value", "query.status", "query.channel", "query.created_from", "query.batch_id",
	"query.cursor", "query.limit", "query.duplicate",
	// The gateway's requests.
	"ws.not_json", "ws.action", "ws.notification_id", "ws.batch_id", "ws.ids_type", "ws.id_object",
	"ws.unknown_member", "ws.binary",
}

const (
	// smsNeedle is the searched form of the SMS recipient: the reserved
	// fake number without its plus sign, which also finds it with the sign.
	smsNeedle = "905551234567"
	// smsCanary is the name of the SMS recipient canary.
	smsCanary = "recipient.sms"
	// tokenMatch is how many leading characters of a token make a match:
	// "cnry" and 12 hexadecimal digits, 48 random bits, which no other
	// output of the stack contains by accident.
	tokenMatch = 16
)

// canary is one value of the run that must never appear in the output.
type canary struct {
	name string
	// needle is what is searched, in lower case.
	needle string
	// min is how many leading characters of needle make a match.
	min int
	// recipient marks the recipient of a stored notification: a successful
	// answer that documents the notification may contain it.
	recipient bool
}

// matches returns the byte ranges [from, to) of lower (a text lowered by
// lowerASCII) that hold the canary: every occurrence of its first min
// characters, extended as far as the rest of the needle goes on. A range
// shorter than the needle is a part of the value, cut short.
func (k canary) matches(lower string) [][2]int {
	var out [][2]int
	head := k.needle[:k.min]
	for i := 0; ; {
		j := strings.Index(lower[i:], head)
		if j < 0 {
			return out
		}
		from := i + j
		to := from + k.min
		for to < len(lower) && to-from < len(k.needle) && lower[to] == k.needle[to-from] {
			to++
		}
		out = append(out, [2]int{from, to})
		i = to
	}
}

// canaries holds the canaries of one run.
type canaries struct {
	list   []canary
	tokens map[string]string
}

// canaryToken returns a fresh random token: "cnry" and 20 hexadecimal
// digits, which cannot occur by accident.
func canaryToken(t *testing.T) string {
	t.Helper()
	b := make([]byte, 10)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return "cnry" + hex.EncodeToString(b)
}

func newCanaries(t *testing.T) *canaries {
	t.Helper()
	c := &canaries{tokens: map[string]string{}}
	for _, name := range canaryNames {
		tok := canaryToken(t)
		c.tokens[name] = tok
		c.list = append(c.list, canary{name: name, needle: tok, min: tokenMatch, recipient: strings.HasPrefix(name, "recipient.")})
	}
	c.list = append(c.list, canary{name: smsCanary, needle: smsNeedle, min: len(smsNeedle), recipient: true})
	return c
}

// tok returns the token of a canary of canaryNames.
func (c *canaries) tok(name string) string {
	v, ok := c.tokens[name]
	if !ok {
		panic("canary " + name + " is not in canaryNames")
	}
	return v
}

// The values sent, built around the tokens.
func (c *canaries) email() string            { return c.tok("recipient.email") + "@example.com" }
func (c *canaries) device() string           { return "e2e-device-" + c.tok("recipient.push") }
func (c *canaries) content(ch string) string { return "E2E canary " + c.tok("content."+ch) }

// recipient returns the recipient canary value of a channel.
func (c *canaries) recipient(ch string) string {
	switch ch {
	case "sms":
		return fakePhone
	case "email":
		return c.email()
	default:
		return c.device()
	}
}

// exchange is one request of the run to the API and its answer.
type exchange struct {
	label  string
	status int
	header http.Header
	body   []byte
	// recipientAllowed marks a successful answer that documents a
	// notification (ADR-0006 §4: create, read, list, cancel): it returns
	// the recipient to the client, and is not searched for recipients.
	recipientAllowed bool
}

// reqOpts describes one request of the run.
type reqOpts struct {
	body        []byte
	json        any
	contentType string
	header      map[string]string
	// recipientAllowed: see exchange.
	recipientAllowed bool
}

// canaryRun is the state of one run of TestCanaryScan.
type canaryRun struct {
	t   *testing.T
	s   *stack
	c   *canaries
	ex  []exchange
	ws  []document
	ids []string
	// templateName is the template of the run (step 1).
	templateName string
	// report is the mock provider's report, read after step 2.
	report string
}

// commonHeaders are put on every request of the run to the API: text a
// client chooses outside the body, in the headers and the Host. X-Request-ID
// and traceparent are invalid (a space, a malformed context), so that the
// API must ignore them: a valid X-Request-ID is echoed and logged by design
// (ADR-0009 §5).
func (r *canaryRun) commonHeaders(req *http.Request) {
	req.Header.Set("User-Agent", "e2e-client/"+r.c.tok("header.user_agent"))
	req.Header.Set("X-Canary", r.c.tok("header.custom"))
	req.Header.Set("X-Request-ID", "not valid "+r.c.tok("header.request_id"))
	req.Header.Set("traceparent", "00-"+r.c.tok("header.traceparent")+"-0000000000000001-01")
	req.Header.Set("X-Forwarded-For", r.c.tok("header.forwarded_for"))
	req.Host = r.c.tok("header.host") + ".example.com"
}

// send makes one request, records it and checks its status.
func (r *canaryRun) send(label, method, path string, want int, o reqOpts) exchange {
	r.t.Helper()
	body := o.body
	if o.json != nil {
		b, err := json.Marshal(o.json)
		if err != nil {
			r.t.Fatal(err)
		}
		body = b
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, r.s.apiURL+path, rd) //nolint:gosec // the stack under test, see stack.do
	if err != nil {
		r.t.Fatalf("%s: %v", label, err)
	}
	ct := o.contentType
	if ct == "" && body != nil {
		ct = "application/json"
	}
	if ct != "" {
		req.Header.Set("Content-Type", ct)
	}
	r.commonHeaders(req)
	for k, v := range o.header {
		req.Header.Set(k, v)
	}
	resp, err := r.s.client.Do(req) //nolint:gosec // G704, see stack.do
	if err != nil {
		r.t.Fatalf("%s: %v", label, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		r.t.Fatalf("%s: read the answer: %v", label, err)
	}
	e := exchange{label: label, status: resp.StatusCode, header: resp.Header, body: b, recipientAllowed: o.recipientAllowed}
	r.ex = append(r.ex, e)
	if resp.StatusCode != want {
		r.t.Errorf("%s: status %d, want %d: %s", label, resp.StatusCode, want, r.c.cut(string(b)))
	}
	return e
}

// decode decodes an answer's body into out.
func (r *canaryRun) decode(e exchange, out any) {
	r.t.Helper()
	if err := json.Unmarshal(e.body, out); err != nil {
		r.t.Fatalf("%s: decode: %v", e.label, err)
	}
}

// create posts one notification and returns its id.
func (r *canaryRun) create(label string, body map[string]any) string {
	r.t.Helper()
	var n struct {
		ID string `json:"id"`
	}
	e := r.send(label, http.MethodPost, "/v1/notifications", http.StatusAccepted, reqOpts{json: body, recipientAllowed: true})
	if e.status != http.StatusAccepted {
		r.t.FailNow()
	}
	r.decode(e, &n)
	r.ids = append(r.ids, n.ID)
	return n.ID
}

// waitFinal waits until the notification is in one of the statuses want
// and returns its status.
func (r *canaryRun) waitFinal(id string, want ...string) string {
	r.t.Helper()
	var last notification
	var lastErr error
	if !poll(canaryFinalTimeout, statusPollInterval, func() bool {
		last, lastErr = r.s.get(id)
		return lastErr == nil && slices.Contains(want, last.Status)
	}) {
		r.t.Fatalf("notification %s: status %q within %s, want one of %v (error %v)", id, last.Status, canaryFinalTimeout, want, lastErr)
	}
	return last.Status
}

// TestCanaryScan is the canary scan of ADR-0009 §4 on the running stack.
func TestCanaryScan(t *testing.T) {
	s := newStack(t)
	s.resetMock(t)
	t.Cleanup(func() { s.resetMock(t) })
	start := time.Now()
	r := &canaryRun{t: t, s: s, c: newCanaries(t)}

	sent := r.successfulSends()
	r.providerEchoes()
	r.problems(sent)
	r.gatewayErrors()
	cancelled, control := r.controls()
	src := r.collect(start, cancelled, control)
	r.scanAll(src)
}

// successfulSends is step 1: single and batch sends on the three channels
// with the canaries, and one from a template with canary variables. It
// returns the id of a sent SMS.
func (r *canaryRun) successfulSends() string {
	r.t.Helper()
	c := r.c
	var ids []string
	for _, ch := range []string{"sms", "email", "push"} {
		ids = append(ids, r.create("create "+ch, map[string]any{
			"channel": ch, "recipient": c.recipient(ch), "content": c.content(ch), "priority": "high",
		}))
	}
	items := []any{}
	for _, ch := range []string{"sms", "email", "push"} {
		items = append(items, map[string]any{"channel": ch, "recipient": c.recipient(ch), "content": c.content(ch), "priority": "high"})
	}
	var batch struct {
		BatchID  string `json:"batch_id"`
		Accepted []struct {
			ID string `json:"id"`
		} `json:"accepted"`
	}
	e := r.send("create batch", http.MethodPost, "/v1/notifications/batch", http.StatusAccepted, reqOpts{json: map[string]any{"notifications": items}})
	r.decode(e, &batch)
	if len(batch.Accepted) != 3 {
		r.t.Fatalf("batch: %d accepted, want 3", len(batch.Accepted))
	}
	for _, a := range batch.Accepted {
		ids = append(ids, a.ID)
		r.ids = append(r.ids, a.ID)
	}

	name := "e2e-canary-" + strings.ToLower(canaryToken(r.t)[4:16])
	r.send("create template", http.MethodPost, "/v1/templates", http.StatusCreated, reqOpts{json: map[string]any{
		"name": name, "channel": "email", "body": "<p>Code {{ .code }} for {{ .name }}</p>", "variables": []string{"code", "name"},
	}})
	ids = append(ids, r.create("create from template", map[string]any{
		"channel": "email", "recipient": c.email(), "priority": "high",
		"template": map[string]any{"name": name, "variables": map[string]any{
			"code": c.tok("template.var.code"), "name": "Ada " + c.tok("template.var.name"),
		}},
	}))
	for _, id := range ids {
		r.waitFinal(id, "sent")
	}
	r.send("get batch", http.MethodGet, "/v1/batches/"+batch.BatchID, http.StatusOK, reqOpts{})
	r.send("get notification", http.MethodGet, "/v1/notifications/"+ids[0], http.StatusOK, reqOpts{recipientAllowed: true})
	r.send("list notifications", http.MethodGet, "/v1/notifications?limit=20", http.StatusOK, reqOpts{recipientAllowed: true})
	r.send("list attempts", http.MethodGet, "/v1/notifications/"+ids[1]+"/attempts", http.StatusOK, reqOpts{})
	r.templateName = name
	return ids[0]
}

// storedError is an attempt as GET /v1/notifications/{id}/attempts
// answers it, with the stored provider error.
type storedError struct {
	AttemptNo  int     `json:"attempt_no"`
	Outcome    string  `json:"outcome"`
	HTTPStatus *int    `json:"http_status"`
	Error      *string `json:"error"`
}

// providerEchoes is step 2: the mock provider quotes every request back
// (echo_request): on sms a retryable 500 and then a 202 whose body is the
// echo instead of the accepted answer, on email a permanent 400, on push a
// 200 with the echo. The attempts are read back; the stored errors must not
// contain the recipient or the content (the value-based redaction), and the
// e-mail's must contain the masked recipient instead.
func (r *canaryRun) providerEchoes() {
	r.t.Helper()
	c := r.c
	r.s.configureMock(r.t, map[string]any{"channels": map[string]any{
		"sms":   map[string]any{"fail_first": 1, "fail_status": 500, "echo_request": true},
		"email": map[string]any{"status": 400, "echo_request": true},
		"push":  map[string]any{"status": 200, "echo_request": true},
	}})
	want := map[string]string{"sms": "sent", "email": "failed", "push": "sent"}
	ids := map[string]string{}
	for _, ch := range []string{"sms", "email", "push"} {
		ids[ch] = r.create("create "+ch+" (provider echoes)", map[string]any{
			"channel": ch, "recipient": c.recipient(ch), "content": c.content(ch), "priority": "high",
		})
	}
	for _, ch := range []string{"sms", "email", "push"} {
		r.waitFinal(ids[ch], want[ch])
	}
	rep := r.mockReport()
	r.checkReport(rep, ids["sms"])

	for _, ch := range []string{"sms", "email", "push"} {
		e := r.send("attempts of the "+ch+" echo", http.MethodGet, "/v1/notifications/"+ids[ch]+"/attempts", http.StatusOK, reqOpts{})
		var list struct {
			Data []storedError `json:"data"`
		}
		r.decode(e, &list)
		for _, a := range list.Data {
			status, stored := 0, "<none>"
			if a.HTTPStatus != nil {
				status = *a.HTTPStatus
			}
			if a.Error != nil {
				stored = *a.Error
			}
			r.t.Logf("%s attempt %d: outcome %s, http_status %d, stored error %s", ch, a.AttemptNo, a.Outcome, status, r.c.cut(stored))
		}
		switch ch {
		case "sms":
			if len(list.Data) != 2 || list.Data[0].Error == nil || !strings.Contains(*list.Data[0].Error, "+90555***4567") {
				r.t.Errorf("sms echo: want two attempts, the first with a stored error holding the masked number +90555***4567")
			}
		case "email":
			masked := "c***@example.com"
			if len(list.Data) != 1 || list.Data[0].Error == nil || !strings.Contains(*list.Data[0].Error, masked) ||
				!strings.Contains(*list.Data[0].Error, "[content redacted]") {
				r.t.Errorf("control of the stored provider error: the e-mail's attempt must hold the masked recipient %s and [content redacted]; the scan of the attempts would prove nothing", masked)
			}
		}
	}
	r.s.resetMock(r.t)
}

// mockReport reads the mock provider's report and keeps it as a source.
func (r *canaryRun) mockReport() reportFields {
	r.t.Helper()
	code, _, body, err := r.s.do(http.MethodGet, r.s.mockURL+"/admin/report", nil)
	if err != nil || code != http.StatusOK {
		r.t.Fatalf("mockprovider report: status %d, error %v", code, err)
	}
	var rep reportFields
	if err := json.Unmarshal(body, &rep); err != nil {
		r.t.Fatalf("decode the mockprovider report: %v", err)
	}
	r.report = string(body)
	return rep
}

// reportFields are the members of the mock's report the scan checks.
type reportFields struct {
	Total           int            `json:"total"`
	IdempotencyKeys map[string]int `json:"idempotency_keys"`
	FieldNameSets   [][]string     `json:"field_name_sets"`
}

// checkReport checks that the report holds field names only, and its
// control: the id of a notification of the run among the keys.
func (r *canaryRun) checkReport(rep reportFields, id string) {
	r.t.Helper()
	if rep.Total == 0 || len(rep.FieldNameSets) == 0 {
		r.t.Errorf("source mockprovider report: empty (total %d, %d field name sets)", rep.Total, len(rep.FieldNameSets))
	}
	for _, set := range rep.FieldNameSets {
		for _, f := range set {
			if f != "to" && f != "channel" && f != "content" {
				r.t.Errorf("source mockprovider report: field name set %s holds %q, want field names of the provider request only", r.c.cut(fmt.Sprint(set)), r.c.cut(f))
			}
		}
	}
	if rep.IdempotencyKeys[id] == 0 {
		r.t.Errorf("source mockprovider report: control missing: the id %s of a notification of the run is not among its keys", id)
	}
}

// problems is step 3: a request for every problem type of docs/errors.md
// that can be reached, with canaries where a client can put them.
func (r *canaryRun) problems(sentID string) {
	r.t.Helper()
	c := r.c
	post := func(label string, want int, body any) exchange {
		return r.send(label, http.MethodPost, "/v1/notifications", want, reqOpts{json: body})
	}
	item := func(ch, recipient, content string) map[string]any {
		return map[string]any{"channel": ch, "recipient": recipient, "content": content}
	}

	// malformed-request (400)
	r.send("empty body", http.MethodPost, "/v1/notifications", http.StatusBadRequest, reqOpts{body: []byte("  "), contentType: "application/json"})
	r.send("malformed JSON", http.MethodPost, "/v1/notifications", http.StatusBadRequest, reqOpts{
		body: []byte(`{"channel":"sms","recipient":"` + c.tok("body.malformed_json") + `","content":"x"`),
	})
	r.send("not an object", http.MethodPost, "/v1/notifications", http.StatusBadRequest, reqOpts{
		body: []byte(`["` + c.tok("body.not_an_object") + `"]`),
	})
	for _, p := range []string{"", "/attempts"} {
		r.send("invalid path id"+p, http.MethodGet, "/v1/notifications/"+c.tok("path.id")+p, http.StatusBadRequest, reqOpts{})
	}
	r.send("invalid path id of cancel", http.MethodPost, "/v1/notifications/"+c.tok("path.id")+"/cancel", http.StatusBadRequest, reqOpts{})
	r.send("invalid path id of a batch", http.MethodGet, "/v1/batches/"+c.tok("path.id"), http.StatusBadRequest, reqOpts{})
	r.send("invalid template version", http.MethodGet, "/v1/templates/"+r.templateName+"/versions/"+c.tok("path.version"), http.StatusBadRequest, reqOpts{})
	r.send("unexpected body of cancel", http.MethodPost, "/v1/notifications/"+sentID+"/cancel", http.StatusBadRequest, reqOpts{
		body: []byte(`{"reason":"` + c.tok("body.unexpected") + `"}`),
	})
	for _, q := range []struct{ label, query string }{
		{"unknown query parameter", "foo=" + c.tok("query.unknown_value")},
		{"invalid status", "status=" + c.tok("query.status")},
		{"invalid channel", "channel=" + c.tok("query.channel")},
		{"invalid created_from", "created_from=" + c.tok("query.created_from")},
		{"invalid batch_id", "batch_id=" + c.tok("query.batch_id")},
		{"invalid cursor", "cursor=" + c.tok("query.cursor")},
		{"invalid limit", "limit=" + c.tok("query.limit")},
		{"duplicate parameter", "status=sent&status=" + c.tok("query.duplicate")},
	} {
		r.send(q.label, http.MethodGet, "/v1/notifications?"+q.query, http.StatusBadRequest, reqOpts{})
	}
	r.send("query parameter of an operation without any", http.MethodGet, "/v1/notifications/"+sentID+"?x="+c.tok("query.unknown_value"), http.StatusBadRequest, reqOpts{})
	r.send("invalid Idempotency-Key", http.MethodPost, "/v1/notifications", http.StatusBadRequest, reqOpts{
		json:   item("sms", fakePhone, c.content("sms")),
		header: map[string]string{"Idempotency-Key": "not valid " + c.tok("header.idempotency_key")},
	})

	// not-found (404), method-not-allowed (405)
	r.send("unknown path", http.MethodGet, "/v1/"+c.tok("path.unknown"), http.StatusNotFound, reqOpts{})
	r.send("unknown notification", http.MethodGet, "/v1/notifications/"+uuid.NewString(), http.StatusNotFound, reqOpts{})
	r.send("unknown template", http.MethodGet, "/v1/templates/"+c.tok("path.template_name"), http.StatusNotFound, reqOpts{})
	r.send("method not allowed", http.MethodPut, "/v1/notifications", http.StatusMethodNotAllowed, reqOpts{json: item("sms", fakePhone, c.content("sms"))})
	r.send("unknown method", strings.ToUpper(c.tok("method")), "/v1/notifications", http.StatusMethodNotAllowed, reqOpts{})

	// payload-too-large (413), unsupported-media-type (415)
	big := bytes.Repeat([]byte(c.tok("body.too_large")+" "), tooLargeBody/25+1)
	r.send("body too large", http.MethodPost, "/v1/notifications", http.StatusRequestEntityTooLarge, reqOpts{
		body: append(append([]byte(`{"channel":"email","recipient":"a@example.com","content":"`), big...), `"}`...),
	})
	r.send("unsupported media type", http.MethodPost, "/v1/notifications", http.StatusUnsupportedMediaType, reqOpts{
		body: []byte(c.tok("body.unsupported_type")), contentType: "text/plain",
	})

	// validation (422): every field, with the canary in the bad value.
	long := func(name string, n int) string {
		tok := c.tok(name)
		return strings.Repeat(tok+" ", n/(len(tok)+1)+1)
	}
	for _, v := range []struct {
		label string
		body  map[string]any
	}{
		{"invalid sms recipient", item("sms", c.tok("invalid.recipient.sms"), c.content("sms"))},
		{"invalid e-mail recipient", item("email", "Ada <"+c.tok("invalid.recipient.email")+"@example.com>", c.content("email"))},
		{"invalid push token", item("push", c.tok("invalid.recipient.push")+"\x01", c.content("push"))},
		{"recipient too long", item("push", long("invalid.recipient.too_long", 4100), c.content("push"))},
		{"e-mail content too long", item("email", c.email(), long("invalid.content.email_too_long", 102500))},
		{"push content too long", item("push", c.device(), long("invalid.content.push_too_long", 4200))},
		{"sms content too many segments", item("sms", fakePhone, long("invalid.content.sms_segments", 1700))},
		{"content with NUL", item("sms", fakePhone, c.tok("invalid.content.nul")+"\x00")},
		{"invalid channel", item(c.tok("invalid.channel"), c.email(), c.content("email"))},
		{"invalid priority", map[string]any{"channel": "sms", "recipient": fakePhone, "content": c.content("sms"), "priority": c.tok("invalid.priority")}},
		{"invalid scheduled_at", map[string]any{"channel": "sms", "recipient": fakePhone, "content": c.content("sms"), "scheduled_at": c.tok("invalid.scheduled_at")}},
		{"invalid expires_at", map[string]any{"channel": "sms", "recipient": fakePhone, "content": c.content("sms"), "expires_at": c.tok("invalid.expires_at")}},
		{"unknown member", map[string]any{"channel": "sms", "recipient": fakePhone, "content": c.content("sms"), "subject": c.tok("unknown_member")}},
		{"invalid template name", map[string]any{"channel": "email", "recipient": c.email(), "template": map[string]any{"name": strings.ToUpper(c.tok("invalid.template.name"))}}},
		{"unknown template variable", map[string]any{"channel": "email", "recipient": c.email(), "template": map[string]any{
			"name": r.templateName, "variables": map[string]any{"code": "1", "name": "x", "other": c.tok("invalid.template.var.unknown")},
		}}},
		{"template variable too long", map[string]any{"channel": "email", "recipient": c.email(), "template": map[string]any{
			"name": r.templateName, "variables": map[string]any{"code": long("invalid.template.var.too_long", 1100), "name": "x"},
		}}},
		{"template variable of the wrong type", map[string]any{"channel": "email", "recipient": c.email(), "template": map[string]any{
			"name": r.templateName, "variables": map[string]any{"code": map[string]any{"x": c.tok("invalid.template.var.object")}, "name": "x"},
		}}},
		{"missing template variable", map[string]any{"channel": "email", "recipient": c.email(), "template": map[string]any{
			"name": r.templateName, "variables": map[string]any{"code": c.tok("template.var.code")},
		}}},
	} {
		post(v.label, http.StatusUnprocessableEntity, v.body)
	}
	r.send("batch with every item rejected", http.MethodPost, "/v1/notifications/batch", http.StatusUnprocessableEntity, reqOpts{json: map[string]any{
		"notifications": []any{item("sms", c.tok("batch.rejected.recipient"), c.content("sms")), item("email", "x", c.content("email"))},
	}})
	partial := r.send("batch with one item rejected", http.MethodPost, "/v1/notifications/batch", http.StatusAccepted, reqOpts{json: map[string]any{
		"notifications": []any{
			item("sms", c.tok("batch.rejected.recipient"), c.content("sms")),
			map[string]any{"channel": "push", "recipient": c.device(), "content": c.content("push"), "scheduled_at": time.Now().Add(time.Hour).Format(time.RFC3339)},
		},
	}})
	var pb struct {
		BatchID string `json:"batch_id"`
	}
	r.decode(partial, &pb)
	r.send("cancel the partial batch", http.MethodPost, "/v1/batches/"+pb.BatchID+"/cancel", http.StatusOK, reqOpts{})
	r.send("template body that does not parse", http.MethodPost, "/v1/templates", http.StatusUnprocessableEntity, reqOpts{json: map[string]any{
		"name": "e2e-canary-bad-" + strings.ToLower(canaryToken(r.t)[4:12]), "channel": "sms", "body": "Hi {{ " + c.tok("invalid.template.body"),
	}})
	r.send("template preview with an unknown variable", http.MethodPost, "/v1/templates/"+r.templateName+"/preview", http.StatusUnprocessableEntity, reqOpts{json: map[string]any{
		"variables": map[string]any{"code": "1", "name": "x", "other": c.tok("invalid.template.var.unknown")},
	}})

	// template-exists (409)
	r.send("template that exists", http.MethodPost, "/v1/templates", http.StatusConflict, reqOpts{json: map[string]any{
		"name": r.templateName, "channel": "email", "body": "<p>" + c.tok("invalid.template.body") + "</p>",
	}})

	// duplicate (409): a dedupe_key held by a stored notification.
	key := "e2e-canary-dedupe-" + uuid.NewString()
	scheduled := func(content string) map[string]any {
		return map[string]any{
			"channel": "email", "recipient": c.email(), "content": content, "dedupe_key": key,
			"scheduled_at": time.Now().Add(time.Hour).Format(time.RFC3339),
		}
	}
	held := r.create("create with a dedupe_key", scheduled(c.content("email")))
	post("duplicate dedupe_key", http.StatusConflict, scheduled(c.tok("duplicate.content")))
	r.send("cancel the holder of the dedupe_key", http.MethodPost, "/v1/notifications/"+held+"/cancel", http.StatusOK, reqOpts{recipientAllowed: true})

	// idempotency-key-reused (422): the same key with another body.
	idem := "e2e-canary-" + uuid.NewString()
	first := r.send("create with an Idempotency-Key", http.MethodPost, "/v1/notifications", http.StatusAccepted, reqOpts{
		json: item("sms", fakePhone, c.content("sms")), header: map[string]string{"Idempotency-Key": idem}, recipientAllowed: true,
	})
	var n struct {
		ID string `json:"id"`
	}
	r.decode(first, &n)
	r.ids = append(r.ids, n.ID)
	r.send("replay of the Idempotency-Key", http.MethodPost, "/v1/notifications", http.StatusAccepted, reqOpts{
		json: item("sms", fakePhone, c.content("sms")), header: map[string]string{"Idempotency-Key": idem}, recipientAllowed: true,
	})
	r.send("Idempotency-Key reused", http.MethodPost, "/v1/notifications", http.StatusUnprocessableEntity, reqOpts{
		json: item("sms", fakePhone, c.tok("idempotency_reused.content")), header: map[string]string{"Idempotency-Key": idem},
	})

	// conflict (409): cancel and redrive of a sent notification.
	r.send("cancel of a sent notification", http.MethodPost, "/v1/notifications/"+sentID+"/cancel", http.StatusConflict, reqOpts{})
	r.send("redrive of a sent notification", http.MethodPost, "/v1/notifications/"+sentID+"/redrive", http.StatusConflict, reqOpts{})
	r.waitFinal(n.ID, "sent")
}

// gatewayErrors is step 4: requests to the gateway that are wrong in every
// way its protocol names (ADR-0006 §10), with canaries as the bad values.
// Every message received is kept.
func (r *canaryRun) gatewayErrors() {
	r.t.Helper()
	c := r.c
	url := envOr("E2E_GATEWAY_URL", defaultGatewayURL)
	hdr := http.Header{}
	hdr.Set("User-Agent", "e2e-client/"+c.tok("header.user_agent"))
	hdr.Set("X-Canary", c.tok("header.custom"))

	// A cross-origin connection is refused; its answer is kept too.
	cross := hdr.Clone()
	cross.Set("Origin", "http://"+c.tok("header.origin")+".example.com")
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	ws, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: cross})
	cancel()
	if err == nil {
		_ = ws.Close(websocket.StatusNormalClosure, "")
		r.t.Errorf("gateway: a connection from a foreign origin was accepted")
	}
	if resp != nil {
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		r.ws = append(r.ws, document{where: "cross-origin answer", text: headerText(resp.Header) + "\n" + string(b)})
	}

	ctx, cancel = context.WithTimeout(context.Background(), requestTimeout)
	ws, resp, err = websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: hdr})
	cancel()
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		r.t.Fatalf("dial the gateway at %s: %v", url, err)
	}
	defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()

	write := func(typ websocket.MessageType, b []byte) {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()
		if err := ws.Write(ctx, typ, b); err != nil {
			r.t.Fatalf("gateway: write: %v", err)
		}
	}
	read := func(label string) []byte {
		ctx, cancel := context.WithTimeout(context.Background(), wsReadTimeout)
		defer cancel()
		_, b, err := ws.Read(ctx)
		if err != nil {
			r.t.Fatalf("gateway: no answer to %s: %v", label, err)
		}
		r.ws = append(r.ws, document{where: "answer to " + label, text: string(b)})
		return b
	}
	expect := func(label, code string) {
		var m struct {
			Type string `json:"type"`
			Code string `json:"code"`
		}
		b := read(label)
		if json.Unmarshal(b, &m) != nil || m.Type != "error" || m.Code != code {
			r.t.Errorf("gateway: answer to %s: %s, want an error %s", label, r.c.cut(string(b)), code)
		}
	}
	ids := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = uuid.NewString()
		}
		return out
	}
	js := func(v any) []byte {
		b, err := json.Marshal(v)
		if err != nil {
			r.t.Fatal(err)
		}
		return b
	}
	for _, m := range []struct {
		label, code string
		typ         websocket.MessageType
		body        []byte
	}{
		{"a message that is not JSON", "invalid_message", websocket.MessageText, []byte(c.tok("ws.not_json") + " {")},
		{"an unknown action", "invalid_message", websocket.MessageText, js(map[string]any{"action": c.tok("ws.action"), "notification_ids": ids(1)})},
		{"an invalid notification id", "invalid_id", websocket.MessageText, js(map[string]any{"action": "subscribe", "notification_ids": []string{uuid.NewString(), c.tok("ws.notification_id")}})},
		{"an invalid batch id", "invalid_id", websocket.MessageText, js(map[string]any{"action": "subscribe", "batch_id": c.tok("ws.batch_id")})},
		{"notification_ids of the wrong type", "invalid_message", websocket.MessageText, js(map[string]any{"action": "subscribe", "notification_ids": c.tok("ws.ids_type")})},
		{"an id that is not a string", "invalid_message", websocket.MessageText, js(map[string]any{"action": "subscribe", "notification_ids": []any{map[string]string{"id": c.tok("ws.id_object")}}})},
		{"a request without ids", "invalid_message", websocket.MessageText, js(map[string]any{"action": "subscribe", "note": c.tok("ws.unknown_member")})},
		{"a binary frame", "invalid_message", websocket.MessageBinary, []byte(c.tok("ws.binary"))},
	} {
		write(m.typ, m.body)
		expect(m.label, m.code)
	}
	// too_many_subscriptions: two requests of 600 unknown ids each; the
	// first is registered (unknown ids get no snapshot), the second would
	// take the connection over 1,000.
	write(websocket.MessageText, js(map[string]any{"action": "subscribe", "notification_ids": ids(600), "note": c.tok("ws.unknown_member")}))
	write(websocket.MessageText, js(map[string]any{"action": "subscribe", "notification_ids": ids(600), "note": c.tok("ws.unknown_member")}))
	expect("a request over the subscription limit", "too_many_subscriptions")
	// A valid request with an unknown member is served: its snapshot.
	write(websocket.MessageText, js(map[string]any{"action": "subscribe", "notification_ids": []string{r.ids[0]}, "note": c.tok("ws.unknown_member")}))
	if b := read("a subscription with an unknown member"); !strings.Contains(string(b), `"status_changed"`) {
		r.t.Errorf("gateway: answer to a valid subscription: %s, want its snapshot", r.c.cut(string(b)))
	}
}

// controls creates the notifications whose output proves that the sources
// were searched, after all other traffic: a scheduled notification that
// the api cancels (the api logs the cancellation) and an SMS that is sent
// (the worker logs it, and its first attempt is one trace from the api's
// request to the worker). Each exporter exports its spans in the order they
// end, so the control trace arrives after every earlier span of the run.
func (r *canaryRun) controls() (cancelled, sent string) {
	r.t.Helper()
	c := r.c
	cancelled = r.create("create the control to cancel", map[string]any{
		"channel": "email", "recipient": c.email(), "content": c.content("email"),
		"scheduled_at": time.Now().Add(time.Hour).Format(time.RFC3339),
	})
	r.send("cancel the control", http.MethodPost, "/v1/notifications/"+cancelled+"/cancel", http.StatusOK, reqOpts{recipientAllowed: true})
	sent = r.create("create the control to send", map[string]any{
		"channel": "sms", "recipient": fakePhone, "content": c.content("sms"), "priority": "high",
	})
	r.waitFinal(sent, "sent")
	return cancelled, sent
}

// cut replaces every canary in text with its name, so that a message can
// show where a canary was without repeating it.
func (c *canaries) cut(text string) string {
	for _, k := range c.list {
		ms := k.matches(lowerASCII(text))
		if len(ms) == 0 {
			continue
		}
		var b strings.Builder
		last := 0
		for _, m := range ms {
			b.WriteString(text[last:m[0]])
			b.WriteString("<canary " + k.name + ">")
			last = m[1]
		}
		b.WriteString(text[last:])
		text = b.String()
	}
	return text
}

// lowerASCII lowers the ASCII letters of s and keeps every other byte, so
// that an offset in the result is an offset in s. The needles are lower
// case ASCII.
func lowerASCII(s string) string {
	b := []byte(s)
	for i, ch := range b {
		if ch >= 'A' && ch <= 'Z' {
			b[i] = ch + ('a' - 'A')
		}
	}
	return string(b)
}

// headerText renders headers one per line, in a stable order.
func headerText(h http.Header) string {
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	var b strings.Builder
	for _, k := range keys {
		for _, v := range h[k] {
			fmt.Fprintf(&b, "%s: %s\n", k, v)
		}
	}
	return b.String()
}
