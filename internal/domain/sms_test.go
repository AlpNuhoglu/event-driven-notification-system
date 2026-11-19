package domain

import (
	"strings"
	"testing"
)

func rep(s string, n int) string { return strings.Repeat(s, n) }

func TestCountSMS(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want SMSInfo
	}{
		{"empty", "", SMSInfo{SMSEncodingGSM7, 0, 0}},
		{"ascii 1", "a", SMSInfo{SMSEncodingGSM7, 1, 1}},
		{"ascii 160", rep("a", 160), SMSInfo{SMSEncodingGSM7, 1, 160}},
		{"ascii 161", rep("a", 161), SMSInfo{SMSEncodingGSM7, 2, 161}},
		{"ascii 306", rep("a", 306), SMSInfo{SMSEncodingGSM7, 2, 306}},
		{"ascii 307", rep("a", 307), SMSInfo{SMSEncodingGSM7, 3, 307}},
		{"extension counts 2", "€", SMSInfo{SMSEncodingGSM7, 1, 2}},
		{"extension 80 fit one segment", rep("€", 80), SMSInfo{SMSEncodingGSM7, 1, 160}},
		{"159 + extension = 161", rep("a", 159) + "€", SMSInfo{SMSEncodingGSM7, 2, 161}},
		{"escape at boundary moves on", rep("a", 152) + "€" + rep("a", 152), SMSInfo{SMSEncodingGSM7, 3, 306}},
		{"escape fits exactly", rep("a", 151) + "€" + rep("a", 152), SMSInfo{SMSEncodingGSM7, 2, 305}},
		{"turkish s-cedilla makes UCS-2", rep("a", 159) + "ş", SMSInfo{SMSEncodingUCS2, 3, 160}},
		{"ucs2 70", rep("ş", 70), SMSInfo{SMSEncodingUCS2, 1, 70}},
		{"ucs2 71", rep("ş", 71), SMSInfo{SMSEncodingUCS2, 2, 71}},
		{"ucs2 134", rep("ş", 134), SMSInfo{SMSEncodingUCS2, 2, 134}},
		{"ucs2 135", rep("ş", 135), SMSInfo{SMSEncodingUCS2, 3, 135}},
		{"emoji is 2 units", "😀", SMSInfo{SMSEncodingUCS2, 1, 2}},
		{"emoji 35 fit one segment", rep("😀", 35), SMSInfo{SMSEncodingUCS2, 1, 70}},
		{"surrogate at boundary moves on", rep("ş", 66) + "😀" + rep("ş", 66), SMSInfo{SMSEncodingUCS2, 3, 134}},
		{"surrogate fits exactly", rep("ş", 65) + "😀" + rep("ş", 67), SMSInfo{SMSEncodingUCS2, 2, 134}},
		{"gsm 10 segments", rep("a", 1530), SMSInfo{SMSEncodingGSM7, 10, 1530}},
		{"gsm 11 segments", rep("a", 1531), SMSInfo{SMSEncodingGSM7, 11, 1531}},
		{"ucs2 10 segments", rep("ş", 670), SMSInfo{SMSEncodingUCS2, 10, 670}},
		{"ucs2 11 segments", rep("ş", 671), SMSInfo{SMSEncodingUCS2, 11, 671}},
		{"invalid utf-8 is UCS-2", "\xff", SMSInfo{SMSEncodingUCS2, 1, 1}},
		{"turkish letters not GSM-7", "ğ", SMSInfo{SMSEncodingUCS2, 1, 1}},
		{"c-cedilla lower not GSM-7", "ç", SMSInfo{SMSEncodingUCS2, 1, 1}},
		{"upper C-cedilla is GSM-7", "Ç", SMSInfo{SMSEncodingGSM7, 1, 1}},
		{"form feed costs 2", "\f", SMSInfo{SMSEncodingGSM7, 1, 2}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := CountSMS(tc.in); got != tc.want {
				t.Errorf("CountSMS = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestCountSMSBoundariesExceedNaiveCeiling(t *testing.T) {
	esc := CountSMS(rep("a", 152) + "€" + rep("a", 152))
	if naive := (esc.Units + 152) / 153; esc.Segments <= naive {
		t.Errorf("escape: segments %d not above ceil(units/153) = %d", esc.Segments, naive)
	}
	sur := CountSMS(rep("ş", 66) + "😀" + rep("ş", 66))
	if naive := (sur.Units + 66) / 67; sur.Segments <= naive {
		t.Errorf("surrogate: segments %d not above ceil(units/67) = %d", sur.Segments, naive)
	}
}

func TestCountSMSTables(t *testing.T) {
	for _, r := range gsm7Basic {
		if got := CountSMS(string(r)); got.Encoding != SMSEncodingGSM7 || got.Units != 1 {
			t.Errorf("basic %U: %+v", r, got)
		}
	}
	for _, r := range gsm7Extension {
		if got := CountSMS(string(r)); got.Encoding != SMSEncodingGSM7 || got.Units != 2 {
			t.Errorf("extension %U: %+v", r, got)
		}
	}
	if n := len([]rune(gsm7Basic)); n != 127 {
		t.Errorf("basic table has %d characters, want 127", n)
	}
	if n := len([]rune(gsm7Extension)); n != 10 {
		t.Errorf("extension table has %d characters, want 10", n)
	}
}
