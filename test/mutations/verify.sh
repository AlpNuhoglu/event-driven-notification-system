#!/bin/sh
# make verify-suite (ADR-0010 section 8, D54): applies each mutation patch of
# this directory to a temporary worktree of HEAD, one at a time, and runs the
# tests the patch names. Every named test must FAIL: a suite that still
# passes when a guarantee is removed does not test that guarantee. A patch
# that does not apply, a build that fails and a named test that did not run
# all count as "not caught", because none of them shows a test failing for
# the mutation.
#
# A patch file starts with a description and one line per target test:
#
#   Must-fail: <package> <TestName>
#
# followed by the diff (git apply ignores the text before it). The worktree
# is HEAD, so uncommitted changes are not part of the run.
#
# usage: verify.sh [name-part ...]   (no argument: every patch)
set -u

root=$(git rev-parse --show-toplevel) || exit 2
dir="$root/test/mutations"
work=$(mktemp -d "${TMPDIR:-/tmp}/verify-suite.XXXXXX") || exit 2
out="$root/verify-suite-logs"
rm -rf "$out"
mkdir -p "$out"

cleanup() {
	git -C "$root" worktree remove --force "$work/tree" >/dev/null 2>&1
	rm -rf "$work"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

git -C "$root" worktree add --quiet --detach "$work/tree" HEAD || exit 2

total=0
caught=0
summary=""
for patch in "$dir"/*.patch; do
	name=$(basename "$patch" .patch)
	if [ $# -gt 0 ]; then
		keep=no
		for want in "$@"; do
			case "$name" in *"$want"*) keep=yes ;; esac
		done
		[ "$keep" = yes ] || continue
	fi
	total=$((total + 1))
	log="$out/$name.log"
	echo "verify-suite: $name: $(sed -n 's/^Mutation: //p' "$patch" | head -1)"

	if ! git -C "$work/tree" apply "$patch" >"$log" 2>&1; then
		echo "verify-suite: $name: NOT CAUGHT: the patch does not apply; it is out of step with the code (see $log)"
		summary="$summary\n  NOT CAUGHT  $name (patch does not apply)"
		continue
	fi

	ok=yes
	# One go test run per package, with exactly the tests named for it.
	for pkg in $(sed -n 's/^Must-fail: \([^ ]*\) .*/\1/p' "$patch" | sort -u); do
		tests=$(sed -n "s|^Must-fail: $pkg \(.*\)|\1|p" "$patch" | tr '\n' '|' | sed 's/|$//')
		(cd "$work/tree" && go test -race -tags=integration -count=1 -run "^($tests)\$" "$pkg") >>"$log" 2>&1
		for t in $(echo "$tests" | tr '|' ' '); do
			if grep -q -- "^--- FAIL: $t " "$log"; then
				echo "verify-suite: $name: caught by $t ($pkg)"
			else
				ok=no
				if grep -q -e '\[build failed\]' -e '\[setup failed\]' "$log"; then
					echo "verify-suite: $name: NOT CAUGHT by $t ($pkg): the build failed, which does not count (see $log)"
				else
					echo "verify-suite: $name: NOT CAUGHT by $t ($pkg): the test did not fail (see $log)"
				fi
			fi
		done
	done

	git -C "$work/tree" apply -R "$patch" >>"$log" 2>&1 || git -C "$work/tree" checkout --quiet -- .
	if [ "$ok" = yes ]; then
		caught=$((caught + 1))
		summary="$summary\n  caught      $name"
	else
		summary="$summary\n  NOT CAUGHT  $name"
	fi
done

printf 'verify-suite: %d of %d mutations made every test named for them fail%b\n' "$caught" "$total" "$summary"
if [ "$total" -eq 0 ]; then
	echo "verify-suite: no mutation selected" >&2
	exit 2
fi
[ "$caught" -eq "$total" ]
