package domain

import "testing"

func TestParseChannel(t *testing.T) {
	for _, c := range Channels() {
		got, err := ParseChannel(string(c))
		if err != nil || got != c {
			t.Errorf("ParseChannel(%q) = %q, %v", c, got, err)
		}
	}
	if _, err := ParseChannel("fax"); err == nil {
		t.Error("expected error")
	}
}

func TestPriority(t *testing.T) {
	want := map[string]struct {
		p    Priority
		amqp uint8
	}{"high": {0, 6}, "normal": {1, 5}, "low": {2, 4}}
	for name, w := range want {
		p, err := ParsePriority(name)
		if err != nil || p != w.p {
			t.Fatalf("ParsePriority(%q) = %v, %v", name, p, err)
		}
		if p.String() != name || !p.Valid() || p.AMQP() != w.amqp || p.AMQP() < 4 {
			t.Errorf("priority %q: String=%q Valid=%v AMQP=%d", name, p.String(), p.Valid(), p.AMQP())
		}
	}
	if _, err := ParsePriority("urgent"); err == nil {
		t.Error("expected error")
	}
	if Priority(3).Valid() || Priority(-1).Valid() {
		t.Error("out-of-range priority must be invalid")
	}
	got := Priorities()
	if len(got) != 3 || got[0] != PriorityHigh || got[1] != PriorityNormal || got[2] != PriorityLow {
		t.Errorf("Priorities = %v", got)
	}
}

func TestStatuses(t *testing.T) {
	if len(Statuses()) != 7 {
		t.Errorf("want 7 statuses, got %d", len(Statuses()))
	}
}
