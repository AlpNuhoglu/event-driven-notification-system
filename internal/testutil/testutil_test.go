package testutil

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"
)

func TestPostgresImageMatchesCompose(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^\s*image:\s*(postgres:\S+)\s*$`).FindSubmatch(b)
	if m == nil {
		t.Fatal("no postgres image in docker-compose.yml")
	}
	if got := string(m[1]); got != PostgresImage {
		t.Errorf("docker-compose.yml uses %s, testutil.PostgresImage is %s", got, PostgresImage)
	}
}

func TestPoll(t *testing.T) {
	n := 0
	if !poll(func() bool { n++; return n >= 3 }, time.Second) {
		t.Error("poll gave up on a condition that becomes true")
	}
	if poll(func() bool { return false }, 20*time.Millisecond) {
		t.Error("poll reported a condition that never holds")
	}
}

func TestWithDatabase(t *testing.T) {
	got, err := withDatabase("postgres://localhost:5432/edns?sslmode=disable", "t_1")
	if err != nil {
		t.Fatal(err)
	}
	if want := "postgres://localhost:5432/t_1?sslmode=disable"; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}
