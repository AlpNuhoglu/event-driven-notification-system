package domain

import (
	"strings"
	"testing"
)

// TestValidateRecipient_AssessmentNumber covers the assessment's example number
// (assumption U3 of the design): it must be accepted at the default level.
func TestValidateRecipient_AssessmentNumber(t *testing.T) {
	const n = "+905551234567"
	if got := ValidateRecipient(ChannelSMS, n, PhonePossible); got != "" {
		t.Fatalf("possible: got %q, want valid", got)
	}
	// Observed with nyaruka/phonenumbers v1.8.1: the strict check also accepts
	// it (555 is a valid Turkish mobile prefix).
	if got := ValidateRecipient(ChannelSMS, n, PhoneStrict); got != "" {
		t.Fatalf("strict: got %q, want valid", got)
	}
}

func TestValidateRecipient_SMS(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		level PhoneValidation
		want  RecipientProblem
	}{
		{"national form", "05551234567", PhonePossible, RecipientNotE164},
		{"no plus", "905551234567", PhonePossible, RecipientNotE164},
		{"leading zero after plus", "+0905551234567", PhonePossible, RecipientNotE164},
		{"spaces", "+90 555 123 45 67", PhonePossible, RecipientNotE164},
		{"dashes", "+90-555-1234567", PhonePossible, RecipientNotE164},
		// The library rejects this Turkish number as too short at both levels.
		{"too short for Turkey possible", "+9055512345", PhonePossible, RecipientNotE164},
		{"too short for Turkey strict", "+9055512345", PhoneStrict, RecipientNotE164},
		{"17 digits", "+90555123456789012", PhonePossible, RecipientNotE164},
		{"plus only", "+", PhonePossible, RecipientNotE164},
		{"empty", "", PhonePossible, RecipientNotE164},
		{"letters", "+90555abc4567", PhonePossible, RecipientNotE164},
		{"trailing newline", "+905551234567\n", PhonePossible, RecipientNotE164},
		{"leading space", " +905551234567", PhonePossible, RecipientNotE164},
		{"US", "+14155550132", PhonePossible, ""},
		{"UK", "+442079460958", PhonePossible, ""},
		{"Germany", "+4915112345678", PhonePossible, ""},
		{"US strict", "+14155550132", PhoneStrict, ""},
		// 555 is not an assigned US area code: possible (length only) accepts, strict rejects.
		{"US 555 possible", "+15555550123", PhonePossible, ""},
		{"US 555 strict", "+15555550123", PhoneStrict, RecipientNotE164},
		{"unknown level behaves as possible", "+15555550123", PhoneValidation("bogus"), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ValidateRecipient(ChannelSMS, tt.in, tt.level); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestValidateRecipient_Email(t *testing.T) {
	long := strings.Repeat("a", 255-len("@example.com")) + "@example.com"
	if len(long) != 255 {
		t.Fatalf("test setup: len = %d", len(long))
	}
	tests := []struct {
		name string
		in   string
		want RecipientProblem
	}{
		{"simple", "user@example.com", ""},
		{"tag and subdomain", "first.last+tag@sub.example.com", ""},
		{"display name", "Ali <ali@example.com>", RecipientInvalidEmail},
		{"trailing space", "ali@example.com ", RecipientInvalidEmail},
		{"leading space", " ali@example.com", RecipientInvalidEmail},
		{"no domain", "ali@", RecipientInvalidEmail},
		{"no local part", "@example.com", RecipientInvalidEmail},
		{"no at", "ali", RecipientInvalidEmail},
		{"two addresses", "ali@example.com, bob@example.com", RecipientInvalidEmail},
		{"255 characters", long, RecipientInvalidEmail},
		{"empty", "", RecipientInvalidEmail},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ValidateRecipient(ChannelEmail, tt.in, PhonePossible); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestValidateRecipient_Push(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want RecipientProblem
	}{
		{"one character", "a", ""},
		{"4096 characters", strings.Repeat("a", 4096), ""},
		{"4096 multibyte characters", strings.Repeat("é", 4096), ""},
		{"empty", "", RecipientInvalidToken},
		{"4097 characters", strings.Repeat("a", 4097), RecipientInvalidToken},
		{"newline", "abc\ndef", RecipientInvalidToken},
		{"tab", "abc\tdef", RecipientInvalidToken},
		{"DEL", "abc\x7fdef", RecipientInvalidToken},
		{"invalid UTF-8", "abc\xffdef", RecipientInvalidToken},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ValidateRecipient(ChannelPush, tt.in, PhonePossible); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestValidateRecipient_UnknownChannel(t *testing.T) {
	if got := ValidateRecipient(Channel("fax"), "x", PhonePossible); got != RecipientInvalidToken {
		t.Errorf("got %q, want %q", got, RecipientInvalidToken)
	}
}
