package domain

import (
	"strings"
	"testing"
	"unicode/utf16"
)

// refSegments is an independent reference: it packs whole characters into
// segments of the given size, one character at a time.
func refSegments(widths []int, single, multi int) int {
	total := 0
	for _, w := range widths {
		total += w
	}
	if total == 0 {
		return 0
	}
	if total <= single {
		return 1
	}
	segs, room := 1, multi
	for _, w := range widths {
		if w > room {
			segs++
			room = multi
		}
		room -= w
	}
	return segs
}

func FuzzCountSMS(f *testing.F) {
	for _, s := range []string{
		"", "a", rep("a", 160), rep("a", 161), rep("a", 306), rep("a", 307),
		rep("a", 159) + "€", rep("a", 152) + "€" + rep("a", 152),
		rep("a", 159) + "ş", rep("ş", 70), rep("ş", 71), rep("ş", 134), rep("ş", 135),
		rep("ş", 66) + "😀" + rep("ş", 66), "\xff", rep("a", 1530), rep("a", 1531),
		rep("ş", 670), rep("ş", 671), "\f^{}\\[~]|€",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		got := CountSMS(s)
		runes := []rune(s)

		allGSM, gsmUnits := true, 0
		var gsmW []int
		for _, r := range runes {
			switch {
			case strings.ContainsRune(gsm7Basic, r):
				gsmW = append(gsmW, 1)
			case strings.ContainsRune(gsm7Extension, r):
				gsmW = append(gsmW, 2)
			default:
				allGSM = false
			}
		}
		var want SMSInfo
		var single, multi int
		var widths []int
		if allGSM {
			for _, w := range gsmW {
				gsmUnits += w
			}
			want.Encoding, want.Units, widths, single, multi = SMSEncodingGSM7, gsmUnits, gsmW, 160, 153
		} else {
			for _, r := range runes {
				w := len(utf16.Encode([]rune{r}))
				widths = append(widths, w)
			}
			want.Encoding, want.Units, single, multi = SMSEncodingUCS2, len(utf16.Encode(runes)), 70, 67
		}
		if got.Encoding != want.Encoding {
			t.Fatalf("encoding %s, want %s", got.Encoding, want.Encoding)
		}
		if got.Units != want.Units {
			t.Fatalf("units %d, want %d", got.Units, want.Units)
		}
		if ref := refSegments(widths, single, multi); got.Segments != ref {
			t.Fatalf("segments %d, reference %d", got.Segments, ref)
		}
		if s == "" {
			if got.Segments != 0 {
				t.Fatalf("empty: segments %d", got.Segments)
			}
			return
		}
		if got.Segments < 1 {
			t.Fatalf("segments %d for non-empty input", got.Segments)
		}
		if got.Units > single && got.Segments < (got.Units+multi-1)/multi {
			t.Fatalf("segments %d below ceil(%d/%d)", got.Segments, got.Units, multi)
		}
		for _, add := range []string{"a", "€", "ş", "😀"} {
			if more := CountSMS(s + add); more.Segments < got.Segments {
				t.Fatalf("appending %q lowered segments %d -> %d", add, got.Segments, more.Segments)
			}
		}
	})
}
