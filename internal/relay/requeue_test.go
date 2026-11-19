package relay

import (
	"strings"
	"testing"
	"time"
)

// The arguments of requeue-stale (D64): --older-than=5m by default, --all for
// every queued row; anything else is a usage error.
func TestParseRequeueStale(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		want    requeueStale
		wantErr string // a part of the error; empty for success
	}{
		{name: "default", args: nil, want: requeueStale{olderThan: 5 * time.Minute}},
		{name: "older-than", args: []string{"--older-than=1ms"}, want: requeueStale{olderThan: time.Millisecond}},
		{name: "older-than as two arguments", args: []string{"--older-than", "90s"}, want: requeueStale{olderThan: 90 * time.Second}},
		{name: "single dash", args: []string{"-older-than=2h"}, want: requeueStale{olderThan: 2 * time.Hour}},
		{name: "zero", args: []string{"--older-than=0s"}, want: requeueStale{}},
		{name: "all", args: []string{"--all"}, want: requeueStale{olderThan: 5 * time.Minute, all: true}},
		{name: "negative", args: []string{"--older-than=-1m"}, wantErr: "must not be negative"},
		{name: "unparsable", args: []string{"--older-than=five"}, wantErr: "invalid value"},
		{name: "no unit", args: []string{"--older-than=5"}, wantErr: "invalid value"},
		{name: "missing value", args: []string{"--older-than"}, wantErr: "needs an argument"},
		{name: "both flags", args: []string{"--all", "--older-than=5m"}, wantErr: "cannot be used together"},
		{name: "both flags, other order", args: []string{"--older-than=1s", "--all"}, wantErr: "cannot be used together"},
		{name: "unknown flag", args: []string{"--force"}, wantErr: "not defined"},
		{name: "positional argument", args: []string{"now"}, wantErr: `unexpected argument "now"`},
		{name: "argument after the flags", args: []string{"--all", "extra"}, wantErr: `unexpected argument "extra"`},
		{name: "all with a value", args: []string{"--all=maybe"}, wantErr: "invalid boolean value"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := parseRequeueStale(c.args)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Errorf("error %v, want one containing %q", err, c.wantErr)
				}
				return
			}
			if err != nil || got != c.want {
				t.Errorf("got %+v, %v; want %+v", got, err, c.want)
			}
		})
	}
}
