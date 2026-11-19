package gateway

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

const protocolTestID = "0193f0d2-7c1e-7b3a-9d41-5be0a1c2e3f4"

// refusedSpellings are the spellings of an id that the UUID library accepts
// and the protocol refuses.
var refusedSpellings = map[string]string{
	"urn prefix":  "urn:uuid:" + protocolTestID,
	"braces":      "{" + protocolTestID + "}",
	"32 digits":   strings.ReplaceAll(protocolTestID, "-", ""),
	"wrapped id":  "x" + protocolTestID + "y",
	"upper urn":   "URN:UUID:" + strings.ToUpper(protocolTestID),
	"quoted (38)": `'` + protocolTestID + `'`,
}

func TestParseRequest_RefusesAnIdInAnyOtherSpelling(t *testing.T) {
	for name, s := range refusedSpellings {
		for member, msg := range map[string]string{
			"notification_ids": `{"action":"subscribe","notification_ids":["` + protocolTestID + `","` + s + `"]}`,
			"batch_id":         `{"action":"subscribe","batch_id":"` + s + `"}`,
		} {
			_, perr := parseRequest([]byte(msg))
			if perr == nil || perr.code != codeInvalidID {
				t.Errorf("%s in %s: got %+v, want %s", name, member, perr, codeInvalidID)
				continue
			}
			if strings.Contains(perr.message, s) {
				t.Errorf("%s in %s: the message contains the submitted value: %q", name, member, perr.message)
			}
		}
	}
}

func TestParseRequest_AcceptsTheCanonicalFormInEitherCase(t *testing.T) {
	want := uuid.MustParse(protocolTestID)
	upper := strings.ToUpper(protocolTestID)
	r, perr := parseRequest([]byte(`{"action":"subscribe","notification_ids":["` + upper + `","` + protocolTestID + `"],"batch_id":"` + upper + `"}`))
	if perr != nil {
		t.Fatalf("refused: %+v", perr)
	}
	// Both spellings are the same id, so it is subscribed once.
	if len(r.ids) != 1 || r.ids[0] != want {
		t.Errorf("ids %v, want [%s]", r.ids, want)
	}
	if r.batch == nil || *r.batch != want {
		t.Errorf("batch %v, want %s", r.batch, want)
	}
}
