package testutil

import (
	"bufio"
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// The guard of the documents that name evidence: docs/decision-tests.md
// names, for every decision of the ledger, the tests that would fail without
// it, and ADR-0010 names the tests of the actor-pair matrix, of the areas and
// of the mutation table. A name that does not exist as a test function is a
// claim without evidence, so it fails here; so does a decision of the ledger
// without a row, and a row for a decision the ledger does not have.
//
// The rules of the documents:
//   - a test name is written in backticks and starts with Test or Fuzz; a
//     name ending in "…" is a prefix that at least one test must match;
//   - in the Tests column of docs/decision-tests.md every backticked token is
//     a package directory (it applies to the names after it, up to the next
//     one) or a test name, which must exist in that package;
//   - the Covered column is one of yes, partly, no and n/a.
//
// The test functions are read from every _test.go file of the module,
// whatever its build tags (integration, e2e, the experiments).

const (
	ledgerPath        = "docs/ai/decision-ledger.md"
	decisionTestsPath = "docs/decision-tests.md"
	adrTestingPath    = "docs/adr/0010-testing-and-ci.md"
	prefixMark        = "…"
)

var (
	// decisionRow is a row of the ledger's tables or of decision-tests.md.
	decisionRow = regexp.MustCompile(`^\|\s*([DG][0-9]+)\s*\|`)
	backticked  = regexp.MustCompile("`([^`]+)`")
	testName    = regexp.MustCompile(`^(Test|Fuzz)[A-Za-z0-9_]*(` + prefixMark + `)?$`)
	coveredSet  = []string{"yes", "partly", "no", "n/a"}
)

// testIndex maps a package directory (slash-separated, from the module root)
// to the names of its test functions.
type testIndex map[string][]string

// has reports whether a test named name (a prefix when it ends in "…")
// exists in dir, or anywhere when dir is empty.
func (ix testIndex) has(dir, name string) bool {
	prefix, isPrefix := strings.CutSuffix(name, prefixMark)
	match := func(names []string) bool {
		return slices.ContainsFunc(names, func(n string) bool {
			if isPrefix {
				return strings.HasPrefix(n, prefix)
			}
			return n == name
		})
	}
	if dir != "" {
		return match(ix[dir])
	}
	for _, names := range ix {
		if match(names) {
			return true
		}
	}
	return false
}

// indexTests parses every _test.go file under root and collects its
// top-level Test and Fuzz functions.
func indexTests(t *testing.T, root string) testIndex {
	t.Helper()
	ix := testIndex{}
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if p != root && (strings.HasPrefix(name, ".") || name == "vendor" || name == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, p, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, filepath.Dir(p))
		if err != nil {
			return err
		}
		dir := filepath.ToSlash(rel)
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if ok && fn.Recv == nil && testName.MatchString(fn.Name.Name) {
				ix[dir] = append(ix[dir], fn.Name.Name)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return ix
}

// tableRow is one row of decision-tests.md.
type tableRow struct {
	line    int
	id      string
	cells   []string // the cells between the outer pipes
	covered string
}

// readRows returns the rows of path whose first cell is a decision id.
func readRows(t *testing.T, path string) []tableRow {
	t.Helper()
	b, err := os.ReadFile(path) //nolint:gosec // a file of this repository
	if err != nil {
		t.Fatal(err)
	}
	var rows []tableRow
	sc := bufio.NewScanner(bytes.NewReader(b))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for n := 1; sc.Scan(); n++ {
		line := sc.Text()
		m := decisionRow.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		cells := strings.Split(strings.Trim(strings.TrimSpace(line), "|"), "|")
		for i := range cells {
			cells[i] = strings.TrimSpace(cells[i])
		}
		r := tableRow{line: n, id: m[1], cells: cells}
		if len(cells) >= 4 {
			r.covered = cells[3]
		}
		rows = append(rows, r)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return rows
}

// ledgerIDs returns the decision ids of the ledger in its order.
func ledgerIDs(t *testing.T, root string) []string {
	t.Helper()
	var ids []string
	for _, r := range readRows(t, filepath.Join(root, ledgerPath)) {
		ids = append(ids, r.id)
	}
	return ids
}

// testNamesIn returns the backticked test names of s.
func testNamesIn(s string) []string {
	var names []string
	for _, m := range backticked.FindAllStringSubmatch(s, -1) {
		if testName.MatchString(m[1]) {
			names = append(names, m[1])
		}
	}
	return names
}

// checkDecisionTests returns the problems of decision-tests.md against the
// ledger ids and the test index.
func checkDecisionTests(rows []tableRow, ledger []string, ix testIndex, isPackage func(string) bool) []string {
	var problems []string
	add := func(r tableRow, format string, args ...any) {
		problems = append(problems, fmt.Sprintf("%s:%d (%s): %s", decisionTestsPath, r.line, r.id, fmt.Sprintf(format, args...)))
	}
	seen := map[string]int{}
	pos := -1 // the ledger position of the last row, for the order
	for _, r := range rows {
		seen[r.id]++
		if seen[r.id] == 2 {
			add(r, "a second row for %s", r.id)
		}
		i := slices.Index(ledger, r.id)
		switch {
		case i < 0:
			add(r, "the ledger has no decision %s", r.id)
		case i < pos:
			add(r, "out of ledger order: %s comes after %s in the ledger", r.id, ledger[pos])
		default:
			pos = i
		}
		if len(r.cells) != 5 {
			add(r, "%d cells, want 5 (ID, decision, tests, covered, notes)", len(r.cells))
			continue
		}
		if !slices.Contains(coveredSet, r.covered) {
			add(r, "covered is %q, want one of %s", r.covered, strings.Join(coveredSet, ", "))
		}
		// The Tests column: packages and the names in them.
		pkg := ""
		for _, m := range backticked.FindAllStringSubmatch(r.cells[2], -1) {
			tok := m[1]
			switch {
			case testName.MatchString(tok):
				if pkg == "" {
					add(r, "%s is named before any package", tok)
				} else if !ix.has(pkg, tok) {
					add(r, "%s: no test function %s", pkg, tok)
				}
			case isPackage(tok):
				pkg = tok
			default:
				add(r, "%q in the tests column is neither a package with tests nor a test name", tok)
			}
		}
		if r.covered != "n/a" && r.covered != "no" && len(testNamesIn(r.cells[2])) == 0 {
			add(r, "covered %q without a test named", r.covered)
		}
		// A test named elsewhere in the row (the notes) must exist too.
		for _, c := range []string{r.cells[1], r.cells[4]} {
			for _, name := range testNamesIn(c) {
				if !ix.has("", name) {
					add(r, "no test function %s", name)
				}
			}
		}
	}
	for _, id := range ledger {
		if strings.HasPrefix(id, "D") && seen[id] == 0 {
			problems = append(problems, fmt.Sprintf("%s: no row for %s, a decision of the ledger", decisionTestsPath, id))
		}
	}
	return problems
}

// checkNamedTests returns the backticked test names of the file at path that
// no test function has.
func checkNamedTests(t *testing.T, root, path string, ix testIndex) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, path)) //nolint:gosec // a file of this repository
	if err != nil {
		t.Fatal(err)
	}
	var problems []string
	for n, line := range strings.Split(string(b), "\n") {
		for _, name := range testNamesIn(line) {
			if !ix.has("", name) {
				problems = append(problems, fmt.Sprintf("%s:%d: no test function %s", path, n+1, name))
			}
		}
	}
	return problems
}

func packageChecker(root string, ix testIndex) func(string) bool {
	return func(tok string) bool {
		if _, ok := ix[tok]; !ok {
			return false
		}
		fi, err := os.Stat(filepath.Join(root, filepath.FromSlash(tok)))
		return err == nil && fi.IsDir()
	}
}

// Every decision of the ledger has one row in docs/decision-tests.md, in
// ledger order; no row names a decision the ledger does not have; every test
// the document names exists in the package it is listed under.
func TestDecisionTests_NameRealTestsForEveryLedgerDecision(t *testing.T) {
	root := repoRoot(t)
	ix := indexTests(t, root)
	if !ix.has("internal/testutil", "TestDecisionTests_NameRealTestsForEveryLedgerDecision") {
		t.Fatal("the index does not contain this test: the scan is broken")
	}
	ledger := ledgerIDs(t, root)
	if !slices.Contains(ledger, "D1") || !slices.Contains(ledger, "G1") {
		t.Fatalf("ledger ids %v: the ledger's tables were not read", ledger)
	}
	rows := readRows(t, filepath.Join(root, decisionTestsPath))
	for _, p := range checkDecisionTests(rows, ledger, ix, packageChecker(root, ix)) {
		t.Error(p)
	}
}

// Every test ADR-0010 names (the actor-pair matrix, the tests by area and
// the mutation table) exists as a test function.
func TestADR0010_NamesOnlyExistingTests(t *testing.T) {
	root := repoRoot(t)
	ix := indexTests(t, root)
	for _, p := range checkNamedTests(t, root, adrTestingPath, ix) {
		t.Error(p)
	}
}

// The checks find what they claim to find: a document with each kind of
// problem, checked against a small index.
func TestDecisionTestsCheck_FindsEveryKindOfProblem(t *testing.T) {
	ix := testIndex{"internal/store": {"TestClaim_A", "TestClaim_B"}, "internal/api": {"TestCancel_X"}}
	isPackage := func(tok string) bool { _, ok := ix[tok]; return ok }
	ledger := []string{"G1", "D1", "D2", "D3"}
	row := func(line int, id, tests, covered, notes string) tableRow {
		return tableRow{line: line, id: id, cells: []string{id, "decision", tests, covered, notes}, covered: covered}
	}
	// Rows 1 and 2 are clean. Row 3: out of order, a test of another
	// package. Row 4: a second row, an unknown name in the notes. Row 5: an
	// unknown id, a name before any package, an unknown verdict. Row 6: a
	// token that is neither a package nor a test, and "yes" without a test.
	rows := []tableRow{
		row(1, "G1", "—", "n/a", "a file"),
		row(2, "D2", "`internal/store`: `TestClaim_A`, `TestClaim_…`", "yes", ""),
		row(3, "D1", "`internal/store`: `TestCancel_X`", "yes", ""),
		row(4, "D2", "`internal/api`: `TestCancel_X`", "partly", "`TestGone`"),
		row(5, "D9", "`TestClaim_A`", "maybe", ""),
		row(6, "D3", "`make test`", "yes", ""),
	}
	got := checkDecisionTests(rows, ledger, ix, isPackage)
	want := []string{
		"(D1): out of ledger order: D1 comes after D2 in the ledger",
		"(D1): internal/store: no test function TestCancel_X",
		"(D2): a second row for D2",
		"(D2): no test function TestGone",
		"(D9): the ledger has no decision D9",
		`(D9): covered is "maybe"`,
		"(D9): TestClaim_A is named before any package",
		`(D3): "make test" in the tests column is neither a package with tests nor a test name`,
		`(D3): covered "yes" without a test named`,
	}
	for _, w := range want {
		if !slices.ContainsFunc(got, func(g string) bool { return strings.Contains(g, w) }) {
			t.Errorf("missing problem %q in\n  %s", w, strings.Join(got, "\n  "))
		}
	}
	if len(got) != len(want) {
		t.Errorf("%d problems, want %d:\n  %s", len(got), len(want), strings.Join(got, "\n  "))
	}
	// A decision without a row: D3 removed.
	got = checkDecisionTests(rows[:5], ledger, ix, isPackage)
	if !slices.ContainsFunc(got, func(g string) bool { return strings.Contains(g, "no row for D3") }) {
		t.Errorf("a missing row was not reported:\n  %s", strings.Join(got, "\n  "))
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "go.mod")); err != nil {
		t.Fatalf("module root %s: %v", dir, err)
	}
	return dir
}
