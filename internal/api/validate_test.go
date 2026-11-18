package api

import (
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
		{"unknown field", `{"channel":"sms","recipient":"+905551234567","content":"x","scheduled_at":"2030-01-01T00:00:00Z"}`, 422, []string{"scheduled_at:unknown_field"}},
		{"unknown field reported once", `{"channel":"sms","recipient":"+905551234567","content":"x","a":1,"a":2}`, 422, []string{"a:unknown_field"}},
		{"duplicate field", `{"channel":"sms","channel":"email","recipient":"+905551234567","content":"x"}`, 422, []string{"channel:duplicate_field"}},
		{"empty object", `{}`, 422, []string{"channel:required", "recipient:required", "content:required"}},
		{
			"every field invalid, in field order then unknown fields",
			`{"zeta":1,"priority":"x","content":1,"recipient":"","channel":"fax","template":null}`, 422,
			[]string{"channel:invalid_value", "recipient:empty", "content:invalid_type", "priority:invalid_value", "zeta:unknown_field", "template:unknown_field"},
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
