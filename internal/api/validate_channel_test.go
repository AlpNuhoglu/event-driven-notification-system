package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// req builds a create body; the values are JSON-encoded.
func req(channel, recipient, content string) string {
	b, _ := json.Marshal(map[string]string{"channel": channel, "recipient": recipient, "content": content})
	return string(b)
}

// The rules of each channel (ADR-0006 section 5) run after the shared field
// checks, only for fields that passed them and only with a valid channel;
// each field has at most one error.
func TestValidateCreate_PerChannel(t *testing.T) {
	const email, token = "user@example.com", "fake-device-token-0001"
	gsm := func(n int) string { return strings.Repeat("a", n) }
	ucs2 := func(n int) string { return strings.Repeat("ş", n) }
	tests := []struct {
		name  string
		body  string
		phone domain.PhoneValidation
		codes []string // nil: valid
	}{
		{"sms E.164", req("sms", phone, "hello"), domain.PhonePossible, nil},
		{"sms E.164 strict", req("sms", phone, "hello"), domain.PhoneStrict, nil},
		{"sms national form", req("sms", "05551234567", "hello"), domain.PhonePossible, []string{"recipient:invalid_e164"}},
		{"sms e-mail recipient", req("sms", email, "hello"), domain.PhonePossible, []string{"recipient:invalid_e164"}},
		{"sms possible but not valid, possible level", req("sms", "+15555550123", "x"), domain.PhonePossible, nil},
		{"sms possible but not valid, strict level", req("sms", "+15555550123", "x"), domain.PhoneStrict, []string{"recipient:invalid_e164"}},
		{"sms 10 GSM-7 segments", req("sms", phone, gsm(1530)), domain.PhonePossible, nil},
		{"sms 11 GSM-7 segments", req("sms", phone, gsm(1531)), domain.PhonePossible, []string{"content:too_many_segments"}},
		{"sms 10 UCS-2 segments", req("sms", phone, ucs2(670)), domain.PhonePossible, nil},
		{"sms 11 UCS-2 segments", req("sms", phone, ucs2(671)), domain.PhonePossible, []string{"content:too_many_segments"}},
		{"sms both invalid", req("sms", "0555", gsm(2000)), domain.PhonePossible, []string{"recipient:invalid_e164", "content:too_many_segments"}},

		{"email", req("email", email, "hello"), domain.PhonePossible, nil},
		{"email display name", req("email", "Ali <ali@example.com>", "hello"), domain.PhonePossible, []string{"recipient:invalid_email"}},
		{"email phone recipient", req("email", phone, "hello"), domain.PhonePossible, []string{"recipient:invalid_email"}},
		{"email content at the limit", req("email", email, gsm(domain.MaxEmailContentBytes)), domain.PhonePossible, nil},
		{"email content over the limit", req("email", email, gsm(domain.MaxEmailContentBytes+1)), domain.PhonePossible, []string{"content:too_long"}},
		{"email content counted in bytes", req("email", email, ucs2(domain.MaxEmailContentBytes/2+1)), domain.PhonePossible, []string{"content:too_long"}},
		{"email long SMS-sized content is fine", req("email", email, gsm(5000)), domain.PhonePossible, nil},

		{"push", req("push", token, "hello"), domain.PhonePossible, nil},
		{"push token of 4096 characters", req("push", gsm(domain.MaxPushTokenChars), "x"), domain.PhonePossible, nil},
		{"push token of 4097 characters", req("push", gsm(domain.MaxPushTokenChars+1), "x"), domain.PhonePossible, []string{"recipient:too_long"}},
		{"push token with a control character", req("push", "tok\ten", "x"), domain.PhonePossible, []string{"recipient:invalid_token"}},
		{"push content at the limit", req("push", token, gsm(domain.MaxPushContentBytes)), domain.PhonePossible, nil},
		{"push content over the limit", req("push", token, gsm(domain.MaxPushContentBytes+1)), domain.PhonePossible, []string{"content:too_long"}},

		// The shared checks win, and decide alone without a valid channel.
		{"NUL in an sms recipient: one error", req("sms", "+90\x00", "x"), domain.PhonePossible, []string{"recipient:invalid_character"}},
		{"empty recipient: one error", req("email", "", "x"), domain.PhonePossible, []string{"recipient:empty"}},
		{"recipient over the shared bound: one error", req("sms", gsm(maxRecipientChars+1), "x"), domain.PhonePossible, []string{"recipient:too_long"}},
		{"unknown channel: no channel rule", req("fax", "0555", gsm(2000)), domain.PhonePossible, []string{"channel:invalid_value"}},
		{"missing channel: no channel rule", `{"recipient":"0555","content":"` + gsm(2000) + `"}`, domain.PhonePossible, []string{"channel:required"}},
		{"repeated channel: no channel rule", `{"channel":"sms","channel":"sms","recipient":"0555","content":"x"}`, domain.PhonePossible, []string{"channel:duplicate_field"}},
		{"channel not a string: no channel rule", `{"channel":1,"recipient":"0555","content":"x"}`, domain.PhonePossible, []string{"channel:invalid_type"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := checkCreate([]byte(tt.body), tt.phone)
			if got := codes(p); !slices.Equal(got, tt.codes) {
				t.Fatalf("codes %v, want %v", got, tt.codes)
			}
			// The same rules apply to an item of a batch.
			in, bp := validateBatch([]byte(batchOf(tt.body)), tt.phone)
			if bp != nil {
				t.Fatalf("batch: %v", codes(bp))
			}
			var itemCodes []string
			for _, r := range in.rejected {
				for _, e := range r.Errors {
					itemCodes = append(itemCodes, e.Field+":"+e.Code)
				}
			}
			if !slices.Equal(itemCodes, tt.codes) {
				t.Errorf("batch item codes %v, want %v", itemCodes, tt.codes)
			}
		})
	}
}

// The messages of the channel rules are fixed texts: none contains the
// submitted value.
func TestValidateCreate_PerChannelMessages(t *testing.T) {
	const secret = "4711"
	for _, body := range []string{
		req("sms", "+9"+secret, "x"),
		req("email", "a"+secret+"@", "x"),
		req("push", "t\t"+secret, "x"),
		req("sms", phone, strings.Repeat(secret, 1000)),
		req("email", "user@example.com", strings.Repeat(secret, 30000)),
		req("push", "token", strings.Repeat(secret, 2000)),
	} {
		p := checkCreate([]byte(body), domain.PhonePossible)
		if p == nil || len(p.errors) != 1 {
			t.Fatalf("errors %v", codes(p))
		}
		if m := p.errors[0].Message; strings.Contains(m, secret) {
			t.Errorf("message leaks the value: %q", m)
		}
	}
	if got := checkCreate([]byte(req("sms", "0555", "x")), domain.PhonePossible).errors[0].Message; got != "recipient must be an E.164 number such as +905551234567" {
		t.Errorf("message %q", got)
	}
	if got := checkCreate([]byte(req("sms", phone, strings.Repeat("a", 1531))), domain.PhonePossible).errors[0].Message; got != "content needs more than 10 SMS segments" {
		t.Errorf("message %q", got)
	}
}

// The phone level reaches the validator through Options: a number that is
// possible but not valid is accepted by default and refused with
// PhoneValidation strict, on the single and the batch create.
func TestPhoneValidation_FromOptions(t *testing.T) {
	const possibleOnly = "+15555550123" // 555 is no assigned US area code
	for _, tc := range []struct {
		level  domain.PhoneValidation
		single int
	}{
		{"", http.StatusAccepted},
		{domain.PhonePossible, http.StatusAccepted},
		{domain.PhoneStrict, http.StatusUnprocessableEntity},
	} {
		h := NewHandler(Options{
			Repository: &fakeRepo{}, DB: &fakeRepo{}, Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			Retry: testRetry(), MaxBodyBytes: 1024, PhoneValidation: tc.level,
		})
		c := newAPIClient(t, h)
		if r := c.post(req("sms", possibleOnly, "x")); r.status != tc.single {
			t.Errorf("level %q: single create %d, want %d: %s", tc.level, r.status, tc.single, r.body)
		}
		// The fake repository refuses batches, so only a strict refusal is a
		// clean 422 with the item's code; under possible the item is valid
		// and the batch reaches the store.
		r := c.do(http.MethodPost, "/v1/notifications/batch", batchOf(req("sms", possibleOnly, "x")), nil)
		strictRefusal := strings.Contains(string(r.body), "invalid_e164")
		if strictRefusal != (tc.level == domain.PhoneStrict) {
			t.Errorf("level %q: batch answered %d %s", tc.level, r.status, r.body)
		}
	}
}
