package testutil

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// localImage is the image the Compose files build from this repository.
const localImage = "event-driven-notification-system:local"

var (
	imageLine = regexp.MustCompile(`(?m)^\s*image:\s*["']?([^"'\s]+)["']?\s*$`)
	// exactTag is a version tag with at least major.minor, optionally with a
	// leading v and a suffix such as -alpine: never latest, never a major
	// version alone.
	exactTag = regexp.MustCompile(`^v?[0-9]+\.[0-9]+(\.[0-9]+)?(-[A-Za-z0-9.]+)?$`)
)

// composeFiles returns the Compose files of the repository: the two at the
// root and the e2e override.
func composeFiles(t *testing.T) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", "..", "docker-compose*.yml"))
	if err != nil {
		t.Fatal(err)
	}
	return append(files, filepath.Join("..", "..", "test", "e2e", "docker-compose.e2e.yml"))
}

// Every container image of the Compose files is pinned to an exact tag
// (D48), the versions the ledger names are the ones in use (RabbitMQ 4.4.x,
// D7 and D48; Postgres 18.x, D48), and Jaeger is the v2 image, not the v1
// all-in-one (D43).
func TestComposeImagesArePinned(t *testing.T) {
	images := map[string]string{} // repository -> tag
	for _, f := range composeFiles(t) {
		b, err := os.ReadFile(f) //nolint:gosec // a file of this repository
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range imageLine.FindAllSubmatch(b, -1) {
			ref := string(m[1])
			if ref == localImage {
				continue
			}
			repo, tag, ok := strings.Cut(ref, ":")
			if !ok || strings.Contains(tag, "/") || !exactTag.MatchString(tag) {
				t.Errorf("%s: image %s is not pinned to an exact tag (D48)", filepath.Base(f), ref)
				continue
			}
			images[repo] = tag
		}
	}
	if len(images) == 0 {
		t.Fatal("no image found in the Compose files: the scan is broken")
	}
	for repo, prefix := range map[string]string{
		"rabbitmq":             "4.4.",
		"postgres":             "18.",
		"jaegertracing/jaeger": "2.",
	} {
		tag, ok := images[repo]
		if !ok {
			t.Errorf("no %s image in the Compose files", repo)
			continue
		}
		if !strings.HasPrefix(tag, prefix) {
			t.Errorf("%s:%s, the ledger decided %sx", repo, tag, prefix)
		}
	}
	if tag, ok := images["jaegertracing/all-in-one"]; ok {
		t.Errorf("jaegertracing/all-in-one:%s is the v1 image; D43 decided the v2 image", tag)
	}
}

var (
	usesLine  = regexp.MustCompile(`(?m)^\s*(?:-\s*)?uses:\s*["']?([^"'\s#]+)`)
	pinnedSHA = regexp.MustCompile(`^[^@\s]+@[0-9a-f]{40}$`)
	topLevel  = regexp.MustCompile(`(?m)^permissions:`)
)

// The workflows pin every third-party action to a full commit SHA and
// declare their permissions at the top level (D53), and Dependabot is off
// (D53: no configuration file).
func TestWorkflowActionsArePinnedBySHA(t *testing.T) {
	dir := filepath.Join("..", "..", ".github", "workflows")
	files, err := filepath.Glob(filepath.Join(dir, "*.yml"))
	if err != nil {
		t.Fatal(err)
	}
	yaml, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	files = append(files, yaml...)
	if len(files) == 0 {
		t.Fatal("no workflow found: the scan is broken")
	}
	uses := 0
	for _, f := range files {
		b, err := os.ReadFile(f) //nolint:gosec // a file of this repository
		if err != nil {
			t.Fatal(err)
		}
		if !topLevel.Match(b) {
			t.Errorf("%s declares no top-level permissions (D53)", filepath.Base(f))
		}
		for _, m := range usesLine.FindAllSubmatch(b, -1) {
			ref := string(m[1])
			if strings.HasPrefix(ref, "./") || strings.HasPrefix(ref, "docker://") {
				continue
			}
			uses++
			if !pinnedSHA.MatchString(ref) {
				t.Errorf("%s: %s is not pinned to a full commit SHA (D53)", filepath.Base(f), ref)
			}
		}
	}
	if uses == 0 {
		t.Error("no action found in the workflows: the scan is broken")
	}
	for _, name := range []string{"dependabot.yml", "dependabot.yaml"} {
		_, err := os.Stat(filepath.Join("..", "..", ".github", name))
		if err == nil {
			t.Errorf(".github/%s exists: Dependabot is off until after the evaluation (D53)", name)
		} else if !errors.Is(err, fs.ErrNotExist) {
			t.Fatal(err)
		}
	}
}
