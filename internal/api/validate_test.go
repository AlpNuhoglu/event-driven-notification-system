package api

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

const (
	validBody = `{"channel":"sms","recipient":"+905551234567","content":"hello","priority":"high"}`
	phone     = "+905551234567"
)

func codes(p *problem) []string {
	if p == nil {
		return nil
	}
	out := make([]string, 0, len(p.errors))
	for _, e := range p.errors {
		out = append(out, e.Field+":"+e.Code)
	}
	return out
}

func TestValidateCreate(t *testing.T) {
	long := strings.Repeat("a", maxRecipientChars)
	key255 := strings.Repeat("k", maxDedupeKeyChars)
	tests := []struct {
		name   string
		body   string
		status int // 0: valid
		codes  []string
	}{
		{"valid", validBody, 0, nil},
		{"valid without priority", `{"channel":"email","recipient":"a@example.com","content":"x"}`, 0, nil},
		{"recipient of 512 characters", `{"channel":"push","recipient":"` + long + `","content":"x"}`, 0, nil},
		{"recipient of 512 multi-byte characters", `{"channel":"push","recipient":"` + strings.Repeat("ş", maxRecipientChars) + `","content":"x"}`, 0, nil},
		{"dedupe_key of 1 character", `{"channel":"sms","recipient":"+905551234567","content":"x","dedupe_key":"k"}`, 0, nil},
		{"dedupe_key of 255 characters", `{"channel":"sms","recipient":"+905551234567","content":"x","dedupe_key":"` + key255 + `"}`, 0, nil},
		{"dedupe_key of 255 multi-byte characters", `{"channel":"sms","recipient":"+905551234567","content":"x","dedupe_key":"` + strings.Repeat("ş", maxDedupeKeyChars) + `"}`, 0, nil},

		{"empty body", ``, 400, []string{":empty_body"}},
		{"whitespace body", " \n\t", 400, []string{":empty_body"}},
		{"malformed", `{"channel":`, 400, []string{":malformed_json"}},
		{"trailing data", `{} {}`, 400, []string{":malformed_json"}},
		{"invalid utf-8", "{\"channel\":\"\xff\"}", 400, []string{":malformed_json"}},
		{"array", `[]`, 400, []string{":not_an_object"}},
		{"string", `"sms"`, 400, []string{":not_an_object"}},
		{"null", `null`, 400, []string{":not_an_object"}},
		{"number", `42`, 400, []string{":not_an_object"}},

		{"missing channel", `{"recipient":"+905551234567","content":"x"}`, 422, []string{"channel:required"}},
		{"missing recipient", `{"channel":"sms","content":"x"}`, 422, []string{"recipient:required"}},
		{"missing content", `{"channel":"sms","recipient":"+905551234567"}`, 422, []string{"content:required"}},
		{"unknown channel", `{"channel":"fax","recipient":"+905551234567","content":"x"}`, 422, []string{"channel:invalid_value"}},
		{"channel in upper case", `{"channel":"SMS","recipient":"+905551234567","content":"x"}`, 422, []string{"channel:invalid_value"}},
		{"unknown priority", `{"channel":"sms","recipient":"+905551234567","content":"x","priority":"urgent"}`, 422, []string{"priority:invalid_value"}},
		{"empty recipient", `{"channel":"sms","recipient":"","content":"x"}`, 422, []string{"recipient:empty"}},
		{"empty content", `{"channel":"sms","recipient":"+905551234567","content":""}`, 422, []string{"content:empty"}},
		{"recipient too long", `{"channel":"push","recipient":"` + long + `a","content":"x"}`, 422, []string{"recipient:too_long"}},
		{"NUL in content", `{"channel":"sms","recipient":"+905551234567","content":"a\u0000b"}`, 422, []string{"content:invalid_character"}},
		{"NUL in recipient", `{"channel":"sms","recipient":"+90\u0000","content":"x"}`, 422, []string{"recipient:invalid_character"}},
		{"channel not a string", `{"channel":5,"recipient":"+905551234567","content":"x"}`, 422, []string{"channel:invalid_type"}},
		{"recipient not a string", `{"channel":"sms","recipient":true,"content":"x"}`, 422, []string{"recipient:invalid_type"}},
		{"content not a string", `{"channel":"sms","recipient":"+905551234567","content":{"a":1}}`, 422, []string{"content:invalid_type"}},
		{"priority null", `{"channel":"sms","recipient":"+905551234567","content":"x","priority":null}`, 422, []string{"priority:invalid_type"}},
		{"recipient null", `{"channel":"sms","recipient":null,"content":"x"}`, 422, []string{"recipient:invalid_type"}},
		{"dedupe_key empty", `{"channel":"sms","recipient":"+905551234567","content":"x","dedupe_key":""}`, 422, []string{"dedupe_key:empty"}},
		{"dedupe_key too long", `{"channel":"sms","recipient":"+905551234567","content":"x","dedupe_key":"` + key255 + `k"}`, 422, []string{"dedupe_key:too_long"}},
		{"dedupe_key with NUL", `{"channel":"sms","recipient":"+905551234567","content":"x","dedupe_key":"a\u0000b"}`, 422, []string{"dedupe_key:invalid_character"}},
		{"dedupe_key not a string", `{"channel":"sms","recipient":"+905551234567","content":"x","dedupe_key":7}`, 422, []string{"dedupe_key:invalid_type"}},
		{"dedupe_key null", `{"channel":"sms","recipient":"+905551234567","content":"x","dedupe_key":null}`, 422, []string{"dedupe_key:invalid_type"}},
		{"dedupe_key repeated", `{"channel":"sms","recipient":"+905551234567","content":"x","dedupe_key":"a","dedupe_key":"b"}`, 422, []string{"dedupe_key:duplicate_field"}},
		{"unknown field", `{"channel":"sms","recipient":"+905551234567","content":"x","scheduled_at":"2030-01-01T00:00:00Z"}`, 422, []string{"scheduled_at:unknown_field"}},
		{"unknown field reported once", `{"channel":"sms","recipient":"+905551234567","content":"x","a":1,"a":2}`, 422, []string{"a:unknown_field"}},
		{"duplicate field", `{"channel":"sms","channel":"email","recipient":"+905551234567","content":"x"}`, 422, []string{"channel:duplicate_field"}},
		{"empty object", `{}`, 422, []string{"channel:required", "recipient:required", "content:required"}},
		{
			"every field invalid, in field order then unknown fields",
			`{"zeta":1,"dedupe_key":"","priority":"x","content":1,"recipient":"","channel":"fax","template":null}`, 422,
			[]string{"channel:invalid_value", "recipient:empty", "content:invalid_type", "priority:invalid_value", "dedupe_key:empty", "zeta:unknown_field", "template:unknown_field"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := validateCreate([]byte(tt.body))
			if tt.status == 0 {
				if p != nil {
					t.Fatalf("got %d %v, want valid", p.kind.status, codes(p))
				}
				return
			}
			if p == nil {
				t.Fatalf("valid, want %d %v", tt.status, tt.codes)
			}
			if p.kind.status != tt.status || !slices.Equal(codes(p), tt.codes) {
				t.Errorf("got %d %v, want %d %v", p.kind.status, codes(p), tt.status, tt.codes)
			}
		})
	}
}

// No message or detail echoes a submitted value.
func TestValidateCreate_NoValueInMessages(t *testing.T) {
	const content = "secret-content-4711"
	bodies := []string{
		`{"channel":"` + phone + `","recipient":"` + phone + `","content":"` + content + `","priority":"` + content + `"}`,
		`{"channel":"sms","recipient":"` + strings.Repeat("9", 600) + `","content":"` + content + `\u0000"}`,
		`{"channel":"sms","recipient":"` + phone + `","content":"x","dedupe_key":"` + content + `\u0000"}`,
	}
	for _, b := range bodies {
		p := validateCreate([]byte(b))
		if p == nil {
			t.Fatalf("valid: %s", b)
		}
		texts := []string{p.detail}
		for _, e := range p.errors {
			texts = append(texts, e.Message)
		}
		for _, s := range texts {
			if strings.Contains(s, "905551234567") || strings.Contains(s, content) || strings.Contains(s, "999") {
				t.Errorf("message leaks a value: %q", s)
			}
		}
	}
}

func TestValidateCreate_Detail(t *testing.T) {
	p := validateCreate([]byte(`{}`))
	if p.detail != "3 field(s) are invalid." {
		t.Errorf("detail = %q", p.detail)
	}
	if p.kind.status != http.StatusUnprocessableEntity {
		t.Errorf("status = %d", p.kind.status)
	}
}

// A body with very many unknown fields lists at most maxReportedErrors, and
// an unknown field's name is echoed truncated.
func TestValidateCreate_Bounds(t *testing.T) {
	var b strings.Builder
	b.WriteString(`{"channel":"sms","recipient":"+905551234567","content":"x"`)
	for i := range 200 {
		b.WriteString(`,"f` + strings.Repeat("x", i) + `":0`)
	}
	b.WriteString(`}`)
	p := validateCreate([]byte(b.String()))
	if len(p.errors) != 200 {
		t.Fatalf("errors = %d, want 200 before the cap", len(p.errors))
	}
	doc := p.body("id")
	if doc.Errors == nil || len(*doc.Errors) != maxReportedErrors {
		t.Fatalf("listed errors = %v, want %d", doc.Errors, maxReportedErrors)
	}
	for _, e := range p.errors {
		if n := len([]rune(e.Field)); n > maxEchoedFieldName {
			t.Fatalf("echoed field name has %d characters", n)
		}
	}
	if doc.Detail != "200 field(s) are invalid." {
		t.Errorf("detail = %q", doc.Detail)
	}
}

// The validator enforces exactly the request schema of api/openapi.yaml.
func TestValidateCreate_MatchesSpec(t *testing.T) {
	s := loadContract(t).doc.Components.Schemas["CreateNotificationRequest"].Value
	var props []string
	for name := range s.Properties {
		props = append(props, name)
	}
	slices.Sort(props)
	want := slices.Clone(createFields)
	slices.Sort(want)
	if !slices.Equal(props, want) {
		t.Errorf("spec properties %v, validator fields %v", props, want)
	}
	if s.AdditionalProperties.Has == nil || *s.AdditionalProperties.Has {
		t.Error("spec must set additionalProperties: false")
	}
	req := slices.Clone(s.Required)
	slices.Sort(req)
	if !slices.Equal(req, []string{fieldChannel, fieldContent, fieldRecipient}) {
		t.Errorf("spec required = %v", req)
	}
	rcp := s.Properties[fieldRecipient].Value
	if rcp.MaxLength == nil || *rcp.MaxLength != maxRecipientChars || rcp.MinLength != 1 {
		t.Errorf("recipient length bounds in spec: min %d max %v", rcp.MinLength, rcp.MaxLength)
	}
	if s.Properties[fieldContent].Value.MinLength != 1 {
		t.Error("content must have minLength 1 in the spec")
	}
	dk := s.Properties[fieldDedupeKey].Value
	if dk.MaxLength == nil || *dk.MaxLength != maxDedupeKeyChars || dk.MinLength != 1 {
		t.Errorf("dedupe_key length bounds in spec: min %d max %v", dk.MinLength, dk.MaxLength)
	}
	if dk.Type == nil || !dk.Type.Is("string") {
		t.Errorf("dedupe_key type in spec: %v", dk.Type)
	}
	enum := func(name string) []string {
		var out []string
		for _, v := range s.Properties[name].Value.Enum {
			out = append(out, v.(string))
		}
		return out
	}
	if got := enum(fieldChannel); !slices.Equal(got, allowedChannels) {
		t.Errorf("channel enum %v, validator %v", got, allowedChannels)
	}
	if got := enum(fieldPriority); !slices.Equal(got, allowedPriorities) {
		t.Errorf("priority enum %v, validator %v", got, allowedPriorities)
	}
	var domainChannels []string
	for _, c := range domain.Channels() {
		domainChannels = append(domainChannels, string(c))
	}
	if !slices.Equal(domainChannels, allowedChannels) {
		t.Errorf("domain channels %v, validator %v", domainChannels, allowedChannels)
	}
	for _, p := range allowedPriorities {
		if _, err := domain.ParsePriority(p); err != nil {
			t.Errorf("priority %q: %v", p, err)
		}
	}
}

const itemA = `{"channel":"sms","recipient":"+905551234567","content":"a"}`

// batchOf wraps items in a batch body.
func batchOf(items ...string) string { return `{"notifications":[` + strings.Join(items, ",") + `]}` }

// itemWithKey builds a valid item with the given dedupe_key.
func itemWithKey(key string) string {
	return `{"channel":"email","recipient":"a@example.com","content":"x","dedupe_key":"` + key + `"}`
}

func rejectedCodes(in batchInput) []string {
	var out []string
	for _, r := range in.rejected {
		for _, e := range r.Errors {
			out = append(out, fmt.Sprintf("%d/%s:%s", r.Index, e.Field, e.Code))
		}
	}
	return out
}

func validIndexes(in batchInput) []int {
	var out []int
	for _, v := range in.valid {
		out = append(out, v.index)
	}
	return out
}

// The top level of a batch is checked first; any error there is one 422 and
// no item is checked.
func TestValidateBatch_TopLevel(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		status int
		codes  []string
	}{
		{"empty body", ``, 400, []string{":empty_body"}},
		{"whitespace body", " \n", 400, []string{":empty_body"}},
		{"malformed", `{"notifications":`, 400, []string{":malformed_json"}},
		{"invalid utf-8", "{\"notifications\":\"\xff\"}", 400, []string{":malformed_json"}},
		{"array", `[]`, 400, []string{":not_an_object"}},
		{"null", `null`, 400, []string{":not_an_object"}},
		{"missing notifications", `{}`, 422, []string{"notifications:required"}},
		{"duplicate notifications", `{"notifications":[],"notifications":[]}`, 422, []string{"notifications:duplicate_field"}},
		{"notifications null", `{"notifications":null}`, 422, []string{"notifications:invalid_type"}},
		{"notifications an object", `{"notifications":{}}`, 422, []string{"notifications:invalid_type"}},
		{"notifications a string", `{"notifications":"x"}`, 422, []string{"notifications:invalid_type"}},
		{"empty array", `{"notifications":[]}`, 422, []string{"notifications:empty"}},
		{"unknown field", `{"notifications":[` + itemA + `],"priority":"high"}`, 422, []string{"priority:unknown_field"}},
		{"unknown field reported once", `{"a":1,"a":2,"notifications":[` + itemA + `]}`, 422, []string{"a:unknown_field"}},
		{"missing and unknown", `{"zeta":1}`, 422, []string{"notifications:required", "zeta:unknown_field"}},
		{"invalid items are not checked after a top-level error", `{"notifications":[{}],"x":1}`, 422, []string{"x:unknown_field"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in, p := validateBatch([]byte(tt.body))
			if p == nil {
				t.Fatalf("no problem, want %d %v", tt.status, tt.codes)
			}
			if p.kind.status != tt.status || !slices.Equal(codes(p), tt.codes) {
				t.Errorf("got %d %v, want %d %v", p.kind.status, codes(p), tt.status, tt.codes)
			}
			if in.total != 0 || len(in.valid) != 0 || len(in.rejected) != 0 {
				t.Errorf("input is not empty: %+v", in)
			}
		})
	}
}

func TestValidateBatch_Items(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		total    int
		valid    []int
		rejected []string // index/field:code
	}{
		{"one valid item", batchOf(itemA), 1, []int{0}, nil},
		{"all valid", batchOf(itemA, itemA, itemA), 3, []int{0, 1, 2}, nil},
		{
			"errors carry the item's index", batchOf(itemA, `{"channel":"fax","recipient":"","content":"x"}`, itemA, `{}`),
			4,
			[]int{0, 2},
			[]string{"1/channel:invalid_value", "1/recipient:empty", "3/channel:required", "3/recipient:required", "3/content:required"},
		},
		{
			"unknown and repeated fields of an item", batchOf(`{"channel":"sms","channel":"sms","recipient":"+905551234567","content":"x","a":1}`),
			1, nil,
			[]string{"0/channel:duplicate_field", "0/a:unknown_field"},
		},
		{
			"items that are not objects",
			batchOf(`null`, `7`, `"x"`, `[]`, `true`, itemA),
			6,
			[]int{5},
			[]string{"0/:invalid_type", "1/:invalid_type", "2/:invalid_type", "3/:invalid_type", "4/:invalid_type"},
		},
		{"every item rejected", batchOf(`{}`, `null`), 2, nil, []string{"0/channel:required", "0/recipient:required", "0/content:required", "1/:invalid_type"}},
		{"dedupe_key errors of an item", batchOf(itemWithKey(""), itemA), 2, []int{1}, []string{"0/dedupe_key:empty"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in, p := validateBatch([]byte(tt.body))
			if p != nil {
				t.Fatalf("problem %d %v", p.kind.status, codes(p))
			}
			if in.total != tt.total {
				t.Errorf("total = %d, want %d", in.total, tt.total)
			}
			if got := validIndexes(in); !slices.Equal(got, tt.valid) {
				t.Errorf("valid = %v, want %v", got, tt.valid)
			}
			if got := rejectedCodes(in); !slices.Equal(got, tt.rejected) {
				t.Errorf("rejected = %v, want %v", got, tt.rejected)
			}
			if len(in.valid)+len(in.rejected) != in.total {
				t.Errorf("valid %d + rejected %d != total %d", len(in.valid), len(in.rejected), in.total)
			}
		})
	}
}

func TestValidateBatch_ParsedItems(t *testing.T) {
	in, p := validateBatch([]byte(batchOf(itemA, itemWithKey("k-1"))))
	if p != nil {
		t.Fatalf("problem %v", codes(p))
	}
	if len(in.valid) != 2 {
		t.Fatalf("valid = %d", len(in.valid))
	}
	first, second := in.valid[0], in.valid[1]
	if first.index != 0 || first.request.Recipient != phone || first.request.Content != "a" || first.request.DedupeKey != nil {
		t.Errorf("first = %+v", first)
	}
	if second.index != 1 || second.request.DedupeKey == nil || *second.request.DedupeKey != "k-1" || string(second.request.Channel) != "email" {
		t.Errorf("second = %+v", second)
	}
}

func TestValidateBatch_Size(t *testing.T) {
	items := func(n int) string { return batchOf(slices.Repeat([]string{itemA}, n)...) }
	t.Run("1000 items are accepted", func(t *testing.T) {
		in, p := validateBatch([]byte(items(maxBatchItems)))
		if p != nil || in.total != maxBatchItems || len(in.valid) != maxBatchItems || len(in.rejected) != 0 {
			t.Fatalf("problem %v, total %d, valid %d, rejected %d", codes(p), in.total, len(in.valid), len(in.rejected))
		}
	})
	t.Run("1001 items are too many and none is validated", func(t *testing.T) {
		// Every item is invalid; none of their errors may appear.
		in, p := validateBatch([]byte(batchOf(slices.Repeat([]string{`{}`}, maxBatchItems+1)...)))
		if p == nil || p.kind.status != 422 || !slices.Equal(codes(p), []string{"notifications:too_many_items"}) {
			t.Fatalf("got %v", p)
		}
		if in.total != 0 || len(in.rejected) != 0 {
			t.Errorf("an item was validated: %+v", in)
		}
	})
}

func TestValidateBatch_DuplicatesInBatch(t *testing.T) {
	tests := []struct {
		name     string
		items    []string
		valid    []int
		rejected []string
	}{
		{"the first valid item with a key wins", []string{itemWithKey("a"), itemWithKey("b"), itemWithKey("a"), itemWithKey("a")}, []int{0, 1}, []string{"2/dedupe_key:duplicate_in_batch", "3/dedupe_key:duplicate_in_batch"}},
		{"different keys and no key", []string{itemWithKey("a"), itemA, itemA, itemWithKey("b")}, []int{0, 1, 2, 3}, nil},
		{"keys are case sensitive", []string{itemWithKey("a"), itemWithKey("A")}, []int{0, 1}, nil},
		{
			"an invalid item does not claim its key",
			[]string{`{"channel":"fax","recipient":"a@example.com","content":"x","dedupe_key":"a"}`, itemWithKey("a")},
			[]int{1},
			[]string{"0/channel:invalid_value"},
		},
		{
			"a duplicate does not hide the first item",
			[]string{itemA, itemWithKey("a"), `null`, itemWithKey("a")},
			[]int{0, 1},
			[]string{"2/:invalid_type", "3/dedupe_key:duplicate_in_batch"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in, p := validateBatch([]byte(batchOf(tt.items...)))
			if p != nil {
				t.Fatalf("problem %v", codes(p))
			}
			if got := validIndexes(in); !slices.Equal(got, tt.valid) {
				t.Errorf("valid = %v, want %v", got, tt.valid)
			}
			if got := rejectedCodes(in); !slices.Equal(got, tt.rejected) {
				t.Errorf("rejected = %v, want %v", got, tt.rejected)
			}
		})
	}
}

// A batch whose items are all rejected is not a problem of validateBatch;
// the caller answers it.
func TestValidateBatch_AllRejected(t *testing.T) {
	in, p := validateBatch([]byte(batchOf(`{}`, `{}`)))
	if p != nil {
		t.Fatalf("problem %v", codes(p))
	}
	if in.total != 2 || len(in.valid) != 0 || len(in.rejected) != 2 {
		t.Errorf("input = %+v", in)
	}
}

// At most maxReportedErrors errors are kept per item.
func TestValidateBatch_ItemErrorsBounded(t *testing.T) {
	var b strings.Builder
	b.WriteString(`{"channel":"sms","recipient":"+905551234567","content":"x"`)
	for i := range 200 {
		b.WriteString(`,"f` + strings.Repeat("x", i) + `":0`)
	}
	b.WriteString(`}`)
	in, p := validateBatch([]byte(batchOf(b.String())))
	if p != nil || len(in.rejected) != 1 {
		t.Fatalf("problem %v, rejected %d", codes(p), len(in.rejected))
	}
	if n := len(in.rejected[0].Errors); n != maxReportedErrors {
		t.Errorf("item errors = %d, want %d", n, maxReportedErrors)
	}
}

// No message or detail of a batch echoes a submitted value.
func TestValidateBatch_NoValueInMessages(t *testing.T) {
	const secret = "secret-content-4711"
	body := batchOf(
		`{"channel":"`+phone+`","recipient":"`+phone+`","content":"`+secret+`","priority":"`+secret+`"}`,
		`{"channel":"sms","recipient":"`+phone+`","content":"`+secret+`\u0000","`+secret+`":1}`,
		itemWithKey(secret), itemWithKey(secret),
		`"`+secret+`"`,
	)
	in, p := validateBatch([]byte(body))
	if p != nil {
		t.Fatalf("problem %v", codes(p))
	}
	if len(in.rejected) != 4 {
		t.Fatalf("rejected = %d", len(in.rejected))
	}
	var texts []string
	for _, r := range in.rejected {
		for _, e := range r.Errors {
			texts = append(texts, e.Field+" "+e.Message)
		}
	}
	// The unknown field's name is echoed by design (like a single create);
	// every other text must hold no value.
	for _, s := range texts {
		if strings.Contains(s, "905551234567") || (strings.Contains(s, secret) && !strings.Contains(s, "this field is not defined")) {
			t.Errorf("message leaks a value: %q", s)
		}
	}
	_, p = validateBatch([]byte(`{"notifications":"` + secret + `","` + phone + `":1}`))
	for _, e := range p.errors {
		if strings.Contains(e.Message, secret) || strings.Contains(e.Message, "905551234567") {
			t.Errorf("message leaks a value: %q", e.Message)
		}
	}
}

// The batch validator enforces exactly the batch request schema of
// api/openapi.yaml.
func TestValidateBatch_MatchesSpec(t *testing.T) {
	s := loadContract(t).doc.Components.Schemas["CreateBatchRequest"].Value
	var props []string
	for name := range s.Properties {
		props = append(props, name)
	}
	if !slices.Equal(props, []string{fieldNotifications}) {
		t.Errorf("spec properties %v, validator field %q", props, fieldNotifications)
	}
	if s.AdditionalProperties.Has == nil || *s.AdditionalProperties.Has {
		t.Error("spec must set additionalProperties: false")
	}
	if !slices.Equal(s.Required, []string{fieldNotifications}) {
		t.Errorf("spec required = %v", s.Required)
	}
	n := s.Properties[fieldNotifications].Value
	if n.MaxItems == nil || *n.MaxItems != maxBatchItems || n.MinItems != 1 {
		t.Errorf("notifications bounds in spec: min %d max %v", n.MinItems, n.MaxItems)
	}
	if ref := n.Items.Ref; ref != "#/components/schemas/CreateNotificationRequest" {
		t.Errorf("notifications items = %q", ref)
	}
}
