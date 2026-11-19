package domain

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"
)

const (
	peRecipient = Recipient("+905551234567")
	peMasked    = "+90555***4567"
	peContent   = `Your code is 917253 <b>"now"</b>`
)

// noPart fails when out holds value, or a prefix or suffix of it longer
// than keep bytes (keep is what the masked form shows by design).
func noPart(t *testing.T, name, out, value string, keep int) {
	t.Helper()
	for k := keep + 1; k <= len(value); k++ {
		if strings.Contains(out, value[:k]) || strings.Contains(out, value[len(value)-k:]) {
			t.Errorf("%s: %d bytes of the value remain in %q", name, k, out)
			return
		}
	}
}

func TestRedactProviderError_RemovesTheMessagesOwnValues(t *testing.T) {
	escaped, err := json.Marshal(peContent)
	if err != nil {
		t.Fatal(err)
	}
	jsonContent := strings.Trim(string(escaped), `"`)
	if jsonContent == peContent {
		t.Fatal("the test content has no JSON-escaped form of its own")
	}
	for name, in := range map[string]string{
		"several times":       "invalid number " + string(peRecipient) + ", retry " + string(peRecipient) + " later; body " + peContent + peContent,
		"at the edges":        string(peRecipient) + " rejected: " + peContent,
		"only the values":     string(peRecipient) + peContent,
		"JSON body":           `{"error":"bad","to":"` + string(peRecipient) + `","content":"` + jsonContent + `"}`,
		"recipient in upper":  "TO " + strings.ToUpper(string(peRecipient)) + " BAD",
		"unrelated plus both": "upstream timeout; " + peContent + "; to=" + string(peRecipient),
	} {
		out := RedactProviderError(in, false, peRecipient, peContent)
		noPart(t, name+" (content)", out, peContent, 1)
		noPart(t, name+" (escaped content)", out, jsonContent, 1)
		noPart(t, name+" (recipient)", out, string(peRecipient), len("+90555"))
		if strings.Contains(in, string(peRecipient)) && !strings.Contains(out, peMasked) {
			t.Errorf("%s: the masked recipient is missing: %q", name, out)
		}
	}

	// Overlapping occurrences: a content that holds the recipient, and an
	// echo in which the recipient's end is the content's start.
	holding := "call " + string(peRecipient) + " now"
	out := RedactProviderError("x"+holding+"y", false, peRecipient, holding)
	if out != "x"+ContentRedacted+"y" {
		t.Errorf("content holding the recipient: %q", out)
	}
	tail := "4567 is your number"
	out = RedactProviderError("x+90555123"+tail+"y", false, peRecipient, tail)
	noPart(t, "overlap (content)", out, tail, 1)
	noPart(t, "overlap (recipient)", out, string(peRecipient), len("+90555"))

	// An e-mail recipient echoed in another case.
	email := Recipient("Alice.Smith@Example.com")
	out = RedactProviderError("unknown mailbox alice.smith@example.com", false, email, "hi there")
	if strings.Contains(strings.ToLower(out), "alice.smith@example.com") || !strings.Contains(out, email.Masked()) {
		t.Errorf("e-mail in lower case: %q", out)
	}
}

func TestRedactProviderError_LeavesAnUnrelatedBodyUntouched(t *testing.T) {
	for _, in := range []string{
		"",
		"503 Service Unavailable",
		`{"error":"rate limited","retry_after":2}`,
		strings.Repeat("a", MaxProviderErrorBytes),
		"Post \"https://provider.example.com/send\": dial tcp: connection refused",
	} {
		if out := RedactProviderError(in, false, peRecipient, peContent); out != in {
			t.Errorf("RedactProviderError(%q) = %q, want it unchanged", in, out)
		}
	}
	// Without values to look for, nothing is replaced.
	if out := RedactProviderError("abc", false, "", ""); out != "abc" {
		t.Errorf("empty values changed the text: %q", out)
	}
}

// The values are removed before the cut, so a value across the 1 KB limit
// leaves no part of itself behind, and the cut keeps valid UTF-8.
func TestRedactProviderError_TruncatesAfterRedacting(t *testing.T) {
	for _, filler := range []string{"x", "ç", "€", "🙂"} {
		for shift := range 8 {
			// Places each value so that it straddles the limit.
			pad := strings.Repeat(filler, (MaxProviderErrorBytes-len(peContent)/2)/len(filler)) + strings.Repeat("y", shift)
			for name, in := range map[string]string{
				"content across the cut":   pad + peContent + strings.Repeat(filler, 100),
				"recipient across the cut": pad[:len(pad)-len(peContent)/2+3] + string(peRecipient) + strings.Repeat(filler, 100),
				"both, long":               strings.Repeat(string(peRecipient)+peContent+filler, 200),
			} {
				out := RedactProviderError(in, false, peRecipient, peContent)
				if len(out) > MaxProviderErrorBytes {
					t.Errorf("%s/%s: %d bytes, limit %d", filler, name, len(out), MaxProviderErrorBytes)
				}
				if !utf8.ValidString(out) {
					t.Errorf("%s/%s: the cut split a character", filler, name)
				}
				if !strings.HasSuffix(out, truncatedSuffix) {
					t.Errorf("%s/%s: the cut is not marked: ...%q", filler, name, out[max(0, len(out)-30):])
				}
				noPart(t, filler+"/"+name+" (content)", out, peContent, 1)
				noPart(t, filler+"/"+name+" (recipient)", out, string(peRecipient), len("+90555"))
			}
		}
	}
}

// Postgres text takes neither NUL bytes nor invalid UTF-8; a value split
// by NUL bytes is joined again and then removed.
func TestRedactProviderError_StoresValidText(t *testing.T) {
	in := "bad\x00 to +9055512\x0034567 \xff\xfe end"
	out := RedactProviderError(in, false, peRecipient, peContent)
	if strings.ContainsRune(out, 0) || !utf8.ValidString(out) {
		t.Errorf("not storable text: %q", out)
	}
	if strings.Contains(out, string(peRecipient)) || !strings.Contains(out, peMasked) {
		t.Errorf("recipient split by a NUL byte not removed: %q", out)
	}
}

// A provider may write a phone number without its plus sign; that spelling
// is the recipient too.
func TestRedactProviderError_PhoneNumberWithoutItsPlusSign(t *testing.T) {
	digits := strings.TrimPrefix(peRecipient.Reveal(), "+")
	out := RedactProviderError("invalid msisdn "+digits+" ("+digits+")", false, peRecipient, peContent)
	if strings.Contains(out, digits) {
		t.Errorf("the number without its plus sign is in %q", out)
	}
	if want := "invalid msisdn " + peRecipient.Masked() + " (" + peRecipient.Masked() + ")"; out != want {
		t.Errorf("got %q, want %q", out, want)
	}
	// An e-mail address has no such spelling, and nothing else is touched.
	email := Recipient("alice@example.com")
	if out := RedactProviderError("code 905551234567", false, email, "hi"); out != "code 905551234567" {
		t.Errorf("an unrelated number was changed: %q", out)
	}
}

// A body that was read up to a limit can end in the middle of a value,
// where no pattern matches it. With cut set, the end is dropped far enough
// that no beginning of a value is left, whatever the place of the cut.
func TestRedactProviderError_ValueBrokenOffAtTheReadLimit(t *testing.T) {
	full := "error: could not deliver \"" + peContent + "\" to " + peRecipient.Reveal()
	for n := 1; n <= len(full); n++ {
		read := full[:n] // what a read limit of n bytes leaves
		out := RedactProviderError(read, true, peRecipient, peContent)
		// No part of a value longer than what the masked form shows.
		for _, v := range []string{peContent, peRecipient.Reveal()} {
			for l := len(v); l > 8; l-- {
				if strings.Contains(out, v[:l]) {
					t.Fatalf("cut at %d: %d bytes of a value are left in %q", n, l, out)
				}
			}
		}
	}
	// Without the flag a complete text is not shortened.
	if out := RedactProviderError("temporary failure", false, peRecipient, peContent); out != "temporary failure" {
		t.Errorf("got %q", out)
	}
}
