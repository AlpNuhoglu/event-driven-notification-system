package domain

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

const testID = "0193f0d2-7c1e-7b3a-9d41-5be0a1c2e3f4"

func TestParseID_AcceptsTheCanonicalFormInEitherCase(t *testing.T) {
	want := uuid.MustParse(testID)
	for _, s := range []string{
		testID,
		strings.ToUpper(testID),
		"0193F0D2-7c1e-7B3A-9d41-5BE0a1c2e3f4",
		"00000000-0000-0000-0000-000000000000",
	} {
		got, err := ParseID(s)
		if err != nil {
			t.Errorf("ParseID(%q): %v", s, err)
			continue
		}
		if !strings.EqualFold(got.String(), s) {
			t.Errorf("ParseID(%q) = %s", s, got)
		}
		if s != "00000000-0000-0000-0000-000000000000" && got != want {
			t.Errorf("ParseID(%q) = %s, want %s", s, got, want)
		}
	}
}

func TestParseID_RefusesEveryOtherSpelling(t *testing.T) {
	for _, tt := range []struct{ name, s string }{
		{"urn prefix", "urn:uuid:" + testID},
		{"upper-case urn prefix", "URN:UUID:" + testID},
		{"braces", "{" + testID + "}"},
		{"32 digits", strings.ReplaceAll(testID, "-", "")},
		{"wrapped in two characters", "x" + testID + "y"},
		{"wrapped in quotes", `"` + testID + `"`},
		{"empty", ""},
		{"too short", testID[:35]},
		{"too long", testID + "0"},
		{"hyphen moved", "0193f0d27-c1e-7b3a-9d41-5be0a1c2e3f4"},
		{"no hyphens, padded to 36", strings.ReplaceAll(testID, "-", "") + "0000"},
		{"not hexadecimal", "0193f0d2-7c1e-7b3a-9d41-5be0a1c2e3fg"},
		{"white space", " " + testID[1:]},
	} {
		if len(tt.s) == idLen {
			// The lenient parser must not be what refuses a 36-character
			// value that is not canonical: the hyphen check is.
			if _, err := uuid.Parse(tt.s); err == nil {
				t.Errorf("%s: uuid.Parse accepts %q; ParseID's length check alone would not refuse it", tt.name, tt.s)
			}
		}
		id, err := ParseID(tt.s)
		if !errors.Is(err, ErrInvalidID) {
			t.Errorf("%s: ParseID(%q) = %s, %v; want ErrInvalidID", tt.name, tt.s, id, err)
		}
		if id != uuid.Nil {
			t.Errorf("%s: ParseID(%q) returned %s with an error", tt.name, tt.s, id)
		}
	}
}

// The spellings that ParseID refuses are the ones the library accepts: the
// test above would prove nothing if the library refused them as well.
func TestParseID_TheLibraryAcceptsTheRefusedSpellings(t *testing.T) {
	for _, s := range []string{
		"urn:uuid:" + testID,
		"{" + testID + "}",
		strings.ReplaceAll(testID, "-", ""),
		"x" + testID + "y",
	} {
		if _, err := uuid.Parse(s); err != nil {
			t.Errorf("uuid.Parse(%q): %v", s, err)
		}
	}
}

func TestParseID_ErrorDoesNotContainTheValue(t *testing.T) {
	const submitted = "not-an-id-but-something-submitted"
	_, err := ParseID(submitted)
	if err == nil || strings.Contains(err.Error(), submitted) {
		t.Errorf("error %v", err)
	}
}
