package relay

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// redisModule is the import path prefix of the Redis client.
const redisModule = "github.com/redis/go-redis/"

// "The relay does not use Redis" (D5): the relay binary, its requeue-stale
// subcommand included, does not depend on the Redis client at all, so no
// path of the relay can need Redis. The worker binary is the control: it
// does depend on the client (GCRA, D24), so the check can see the client.
func TestRelayBinaryDoesNotDependOnRedis(t *testing.T) {
	root := moduleRootDir(t)
	deps := func(pkg string) []string {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), "go", "list", "-deps", pkg) //nolint:gosec // a package path of this test
		cmd.Dir = root
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("go list -deps %s: %v: %s", pkg, err, stderr.String())
		}
		var redis []string
		for _, p := range strings.Fields(string(out)) {
			if strings.HasPrefix(p, redisModule) {
				redis = append(redis, p)
			}
		}
		return redis
	}
	if got := deps("./cmd/worker"); len(got) == 0 {
		t.Fatal("the worker binary does not depend on the Redis client: the check cannot see it")
	}
	if got := deps("./cmd/relay"); len(got) != 0 {
		t.Errorf("the relay binary depends on the Redis client (%s): the relay does not use Redis (D5)",
			strings.Join(got, ", "))
	}
}

func moduleRootDir(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		_, err := os.Stat(filepath.Join(dir, "go.mod"))
		if err == nil {
			return dir
		}
		if !errors.Is(err, fs.ErrNotExist) {
			t.Fatal(err)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}
