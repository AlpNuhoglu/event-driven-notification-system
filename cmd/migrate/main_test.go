package main

import (
	"bytes"
	"testing"
)

func noEnv(string) (string, bool) { return "", false }

func TestRunHealthcheck(t *testing.T) {
	var so, se bytes.Buffer
	if code := run([]string{"healthcheck"}, noEnv, &so, &se); code != 1 {
		t.Errorf("code = %d, want 1", code)
	}
	if se.Len() == 0 {
		t.Error("expected a message on stderr")
	}
}

func TestRunUnknownArgument(t *testing.T) {
	var so, se bytes.Buffer
	if code := run([]string{"x"}, noEnv, &so, &se); code != 2 {
		t.Errorf("code = %d, want 2", code)
	}
	if code := run([]string{"up", "extra"}, noEnv, &so, &se); code != 2 {
		t.Errorf("code = %d, want 2", code)
	}
}
