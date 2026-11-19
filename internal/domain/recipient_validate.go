package domain

import (
	"net/mail"
	"unicode/utf8"

	"github.com/nyaruka/phonenumbers"
)

// PhoneValidation is the level of the phone number check (PHONE_VALIDATION).
type PhoneValidation string

const (
	// PhonePossible checks length and structure only (phonenumbers.IsPossibleNumber).
	PhonePossible PhoneValidation = "possible"
	// PhoneStrict checks against national numbering plans (phonenumbers.IsValidNumber).
	PhoneStrict PhoneValidation = "strict"
)

// RecipientProblem is a stable, machine-readable reason; "" means valid.
type RecipientProblem string

const (
	// RecipientNotE164 means the SMS recipient is not a valid E.164 number.
	RecipientNotE164 RecipientProblem = "invalid_e164"
	// RecipientInvalidEmail means the email recipient is not a bare, valid address.
	RecipientInvalidEmail RecipientProblem = "invalid_email"
	// RecipientInvalidToken means the push token (or the channel) is not acceptable.
	RecipientInvalidToken RecipientProblem = "invalid_token"
)

// Size limits of the content and recipients per channel, used by the API's validation.
const (
	MaxEmailContentBytes = 100 * 1024 // UTF-8 bytes
	MaxPushContentBytes  = 4 * 1024   // UTF-8 bytes
	MaxPushTokenChars    = 4096
	MaxEmailAddressChars = 254
)

// ValidateRecipient checks recipient for channel. It returns "" when valid.
// An unknown channel returns RecipientInvalidToken. Nothing is normalised and
// the recipient is never logged or returned.
func ValidateRecipient(channel Channel, recipient string, phone PhoneValidation) RecipientProblem {
	switch channel {
	case ChannelSMS:
		if !validPhone(recipient, phone) {
			return RecipientNotE164
		}
	case ChannelEmail:
		if !validEmail(recipient) {
			return RecipientInvalidEmail
		}
	case ChannelPush:
		if !validToken(recipient) {
			return RecipientInvalidToken
		}
	default:
		return RecipientInvalidToken
	}
	return ""
}

// e164Shape reports whether s is '+', a digit 1-9, then 7 to 14 more digits, nothing else.
func e164Shape(s string) bool {
	if len(s) < 9 || len(s) > 16 || s[0] != '+' || s[1] < '1' || s[1] > '9' {
		return false
	}
	for i := 2; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func validPhone(s string, level PhoneValidation) bool {
	if !e164Shape(s) {
		return false
	}
	num, err := phonenumbers.Parse(s, "")
	if err != nil {
		return false
	}
	if level == PhoneStrict {
		return phonenumbers.IsValidNumber(num)
	}
	return phonenumbers.IsPossibleNumber(num)
}

func validEmail(s string) bool {
	if len(s) > MaxEmailAddressChars {
		return false
	}
	addr, err := mail.ParseAddress(s)
	return err == nil && addr.Address == s
}

func validToken(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	n := utf8.RuneCountInString(s)
	if n < 1 || n > MaxPushTokenChars {
		return false
	}
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}
