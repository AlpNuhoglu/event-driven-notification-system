package api

import (
	"net/http"
	"strings"
	"testing"
)

func TestIdempotencyKey(t *testing.T) {
	tests := []struct {
		name    string
		values  []string // nil: header absent
		key     string
		present bool
		invalid bool
	}{
		{"absent", nil, "", false, false},
		{"one byte", []string{"k"}, "k", true, false},
		{"255 bytes", []string{strings.Repeat("k", 255)}, strings.Repeat("k", 255), true, false},
		{"visible ASCII bounds", []string{"!~"}, "!~", true, false},
		{"typical key", []string{"7f4e2c1a-9d3b-4e55-8a10-5b6c7d8e9f00"}, "7f4e2c1a-9d3b-4e55-8a10-5b6c7d8e9f00", true, false},
		{"empty", []string{""}, "", false, true},
		{"256 bytes", []string{strings.Repeat("k", 256)}, "", false, true},
		{"a space", []string{"a b"}, "", false, true},
		{"a control byte", []string{"a\x01b"}, "", false, true},
		{"DEL", []string{"a\x7fb"}, "", false, true},
		{"a non-ASCII byte", []string{"keç"}, "", false, true},
		{"sent twice", []string{"k", "k"}, "", false, true},
		{"sent twice, one empty", []string{"k", ""}, "", false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := http.Header{}
			for _, v := range tt.values {
				h.Add("Idempotency-Key", v)
			}
			key, present, p := idempotencyKey(h)
			if tt.invalid {
				if p == nil || p.kind != kindMalformed || p.detail != "The Idempotency-Key header is invalid." {
					t.Fatalf("problem = %v", p)
				}
				if len(p.errors) != 1 || p.errors[0].Field != "Idempotency-Key" || p.errors[0].Code != codeInvalidIdempotencyKey {
					t.Errorf("errors = %+v", p.errors)
				}
				if key != "" || present {
					t.Errorf("key %q present %v with a problem", key, present)
				}
				return
			}
			if p != nil {
				t.Fatalf("problem %v", p)
			}
			if key != tt.key || present != tt.present {
				t.Errorf("got %q %v, want %q %v", key, present, tt.key, tt.present)
			}
		})
	}
}

// The key is never written into a message.
func TestIdempotencyKey_NotInMessages(t *testing.T) {
	const bad = "secret key 4711"
	h := http.Header{}
	h.Set("Idempotency-Key", bad)
	_, _, p := idempotencyKey(h)
	if p == nil {
		t.Fatal("valid")
	}
	texts := []string{p.detail}
	for _, e := range p.errors {
		texts = append(texts, e.Message, e.Field)
	}
	for _, s := range texts {
		if strings.Contains(s, "4711") || strings.Contains(s, "secret") {
			t.Errorf("text leaks the key: %q", s)
		}
	}
}

func TestRequestHash(t *testing.T) {
	const (
		post = http.MethodPost
		path = "/v1/notifications"
		base = `{"channel":"sms","recipient":"+905551234567","content":"hello","priority":"high"}`
	)
	hash := func(method, path, body string) string {
		t.Helper()
		h, err := requestHash(method, path, []byte(body))
		if err != nil {
			t.Fatalf("requestHash: %v", err)
		}
		return h
	}
	ref := hash(post, path, base)
	if len(ref) != 64 || ref != strings.ToLower(ref) {
		t.Errorf("hash %q is not 64 lower-case hex characters", ref)
	}

	same := []struct{ name, method, path, body string }{
		{"same request", post, path, base},
		{"another key order", post, path, `{"priority":"high","content":"hello","recipient":"+905551234567","channel":"sms"}`},
		{"whitespace", post, path, " {\n \"channel\" : \"sms\",\t\"recipient\":\"+905551234567\" ,\"content\":\"hello\",\"priority\":\"high\"}\n"},
		{"an escaped character", post, path, `{"channel":"sms","recipient":"+905551234567","content":"hello","priority":"high"}`},
	}
	for _, tt := range same {
		if got := hash(tt.method, tt.path, tt.body); got != ref {
			t.Errorf("%s: hash differs", tt.name)
		}
	}

	different := []struct{ name, method, path, body string }{
		{"another method", http.MethodPut, path, base},
		{"another path", post, "/v1/notifications/batch", base},
		{"another value", post, path, `{"channel":"sms","recipient":"+905551234567","content":"hello!","priority":"high"}`},
		{"another field", post, path, `{"channel":"sms","recipient":"+905551234567","content":"hello"}`},
		{"a nested array in another order", post, path, `{"a":[1,2]}`},
	}
	for _, tt := range different {
		got := hash(tt.method, tt.path, tt.body)
		if got == ref {
			t.Errorf("%s: hash is equal", tt.name)
		}
	}
	if hash(post, path, `{"a":[1,2]}`) == hash(post, path, `{"a":[2,1]}`) {
		t.Error("array order must matter")
	}
	// Method and path are separated, so moving a character between them
	// changes the hash.
	if hash("POST", "x", base) == hash("POS", "Tx", base) {
		t.Error("method and path are not separated")
	}
	// HTML characters are not escaped, so the canonical form is the text a
	// client sent.
	literal, escaped := `{"a":"<&>"}`, `{"a":"`+`\u003c\u0026\u003e`+`"}`
	if hash(post, path, literal) != hash(post, path, escaped) {
		t.Error("escaped and literal HTML characters must hash alike")
	}
	// Numbers keep their literal text: 1 and 1.0 give different hashes. This
	// is accepted: it can only turn a retry into a conflict, never merge two
	// different requests.
	if hash(post, path, `{"a":1}`) == hash(post, path, `{"a":1.0}`) {
		t.Error("1 and 1.0 are expected to hash differently")
	}
	// Large numbers are not rounded through float64.
	if hash(post, path, `{"a":12345678901234567890}`) == hash(post, path, `{"a":12345678901234567891}`) {
		t.Error("large numbers must keep their digits")
	}
}

func TestRequestHash_InvalidBody(t *testing.T) {
	if _, err := requestHash(http.MethodPost, "/x", []byte(`{`)); err == nil {
		t.Error("an invalid body must return an error")
	}
	if _, err := requestHash(http.MethodPost, "/x", nil); err == nil {
		t.Error("an empty body must return an error")
	}
}
