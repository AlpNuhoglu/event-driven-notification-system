package domain

import "unicode/utf8"

// SMSEncoding is the character encoding an SMS is sent in.
type SMSEncoding string

// The two encodings of ADR-0006 section 5.
const (
	SMSEncodingGSM7 SMSEncoding = "GSM-7"
	SMSEncodingUCS2 SMSEncoding = "UCS-2"
)

// MaxSMSSegments is the largest number of segments one SMS may need.
const MaxSMSSegments = 10

const (
	gsm7SingleUnits = 160 // septets in a single-segment message
	gsm7MultiUnits  = 153 // septets per segment of a concatenated message
	ucs2SingleUnits = 70  // UTF-16 code units in a single-segment message
	ucs2MultiUnits  = 67  // code units per segment of a concatenated message
)

// SMSInfo says how a content is sent as SMS.
type SMSInfo struct {
	Encoding SMSEncoding
	Segments int // 0 only for the empty content
	Units    int // septets (GSM-7) or UTF-16 code units (UCS-2)
}

// gsm7Basic is the basic character set of the GSM 03.38 default alphabet
// (3GPP TS 23.038): one septet per character. The escape code (0x1B) is not
// a character and is not listed.
const gsm7Basic = "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞÆæßÉ" +
	" !\"#¤%&'()*+,-./" +
	"0123456789:;<=>?¡" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§" +
	"¿abcdefghijklmnopqrstuvwxyzäöñüà"

// gsm7Extension is the extension table of GSM 03.38: each character is sent
// as an escape septet followed by one septet, so it costs two septets.
const gsm7Extension = "^{}\\[~]|€\f"

var gsm7BasicSet, gsm7ExtensionSet = runeSet(gsm7Basic), runeSet(gsm7Extension)

func runeSet(s string) map[rune]struct{} {
	m := make(map[rune]struct{}, len(s))
	for _, r := range s {
		m[r] = struct{}{}
	}
	return m
}

// gsm7Width returns the septets r costs (1 or 2), or 0 if r is not GSM-7.
func gsm7Width(r rune) int {
	if _, ok := gsm7BasicSet[r]; ok {
		return 1
	}
	if _, ok := gsm7ExtensionSet[r]; ok {
		return 2
	}
	return 0
}

// CountSMS returns the encoding, segment count and unit count of content.
// It never panics; invalid UTF-8 iterates as U+FFFD, which is UCS-2.
func CountSMS(content string) SMSInfo {
	if content == "" {
		return SMSInfo{Encoding: SMSEncodingGSM7}
	}
	gsm, units := true, 0
	for _, r := range content {
		w := gsm7Width(r)
		if w == 0 {
			gsm = false
			break
		}
		units += w
	}
	if gsm {
		return SMSInfo{
			Encoding: SMSEncodingGSM7,
			Segments: packSegments(content, units, gsm7SingleUnits, gsm7MultiUnits, gsm7Width),
			Units:    units,
		}
	}
	units = 0
	for _, r := range content {
		units += ucs2Width(r)
	}
	return SMSInfo{
		Encoding: SMSEncodingUCS2,
		Segments: packSegments(content, units, ucs2SingleUnits, ucs2MultiUnits, ucs2Width),
		Units:    units,
	}
}

// ucs2Width is the UTF-16 code units of r: 2 outside the BMP (a surrogate
// pair), else 1. Invalid UTF-8 arrives as U+FFFD.
func ucs2Width(r rune) int {
	if r >= 0x10000 && r <= utf8.MaxRune {
		return 2
	}
	return 1
}

// packSegments fills segments of multi units greedily; a character of width 2
// that does not fit in the rest of a segment starts the next one.
func packSegments(content string, total, single, multi int, width func(rune) int) int {
	if total <= single {
		return 1
	}
	segments, used := 1, 0
	for _, r := range content {
		w := width(r)
		if used+w > multi {
			segments++
			used = 0
		}
		used += w
	}
	return segments
}
