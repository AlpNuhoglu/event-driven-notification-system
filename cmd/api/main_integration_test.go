//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

const waitTimeout = 30_000 * time.Millisecond

// syncBuffer is the process's stdout, written by several goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// logField returns the value of key in the first log line with message msg.
func logField(out, msg, key string) string {
	for _, line := range strings.Split(out, "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) != nil || m["msg"] != msg {
			continue
		}
		s, _ := m[key].(string)
		return s
	}
	return ""
}

func env(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func getStatus(t *testing.T, u string) int {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, u, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", u, err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// The role serves the API and the admin port, answers the healthcheck
// subcommand and stops with exit code 0 when its context is cancelled.
func TestRun_ServeHealthcheckAndStop(t *testing.T) {
	vars := map[string]string{
		"DATABASE_URL": testutil.NewMigratedDatabase(t),
		"API_ADDR":     "127.0.0.1:0",
		"ADMIN_ADDR":   "127.0.0.1:0",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stdout, stderr := &syncBuffer{}, &syncBuffer{}
	done := make(chan int, 1)
	go func() { done <- run(ctx, nil, env(vars), stdout, stderr) }()

	var apiAddr, adminAddr string
	testutil.Eventually(t, func() bool {
		out := stdout.String()
		apiAddr = logField(out, "api listening", "api_addr")
		adminAddr = logField(out, "service started", "admin_addr")
		return apiAddr != "" && adminAddr != ""
	}, waitTimeout)

	for _, path := range []string{"/healthz", "/readyz"} {
		if code := getStatus(t, (&url.URL{Scheme: "http", Host: apiAddr, Path: path}).String()); code != http.StatusOK {
			t.Errorf("public %s: %d", path, code)
		}
	}

	vars["ADMIN_ADDR"] = adminAddr
	var so, se bytes.Buffer
	if code := run(context.Background(), []string{"healthcheck"}, env(vars), &so, &se); code != 0 {
		t.Errorf("healthcheck: exit %d, stderr %q", code, se.String())
	}

	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Errorf("exit code %d, stderr %q", code, stderr.String())
		}
	case <-time.After(waitTimeout):
		t.Fatal("did not stop")
	}
	if !strings.Contains(stdout.String(), `"msg":"api stopped"`) {
		t.Errorf("no graceful stop logged: %s", stdout.String())
	}
}

// A database that cannot be used at start ends the process with a non-zero code.
func TestRun_DatabaseUnavailableAtStart(t *testing.T) {
	u, err := url.Parse(testutil.NewMigratedDatabase(t))
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/no_such_database"
	vars := map[string]string{"DATABASE_URL": u.String(), "API_ADDR": "127.0.0.1:0", "ADMIN_ADDR": "127.0.0.1:0"}
	var so, se syncBuffer
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	if code := run(ctx, nil, env(vars), &so, &se); code != 1 {
		t.Errorf("exit code %d, want 1", code)
	}
	if !strings.Contains(se.String(), "postgres") {
		t.Errorf("stderr %q", se.String())
	}
}
