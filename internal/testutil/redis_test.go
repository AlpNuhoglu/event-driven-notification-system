package testutil

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

func TestRedisImageMatchesCompose(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^\s*image:\s*(redis:\S+)\s*$`).FindSubmatch(b)
	if m == nil {
		t.Fatal("no redis image in docker-compose.yml")
	}
	if got := string(m[1]); got != RedisImage {
		t.Errorf("docker-compose.yml uses %s, testutil.RedisImage is %s", got, RedisImage)
	}
}
