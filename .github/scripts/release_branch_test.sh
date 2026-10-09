#!/bin/sh
# Verify daily branch creation with deterministic dates and a local fake GitHub CLI.
set -eu

script=$(cd "$(dirname "$0")" && pwd)/release_branch.sh
sandbox=$(mktemp -d)
trap 'rm -r -- "$sandbox"' EXIT HUP INT TERM
mkdir "$sandbox/bin"
export GH_REPO=Waterkyuu/cacheq
export RELEASE_BRANCH_TEST_CALLS="$sandbox/calls"
export RELEASE_BRANCH_TEST_REFS='[]'
export RELEASE_BRANCH_TEST_FAILURE=''

cat > "$sandbox/bin/date" <<'DATE'
#!/bin/sh
set -eu
test "$TZ" = Asia/Shanghai
test "$1" = +%Y%m%d
printf '%s\n' 20261010
DATE
cat > "$sandbox/bin/gh" <<'CLI'
#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$RELEASE_BRANCH_TEST_CALLS"
case "$*" in
  *git/matching-refs/heads/release/20261010*)
    test "$RELEASE_BRANCH_TEST_FAILURE" != lookup
    test "$1" = api
    test "$3" = --jq
    printf '%s\n' "$RELEASE_BRANCH_TEST_REFS" | jq -r "$4"
    ;;
  *git/ref/heads/main*)
    test "$RELEASE_BRANCH_TEST_FAILURE" != main
    printf '%s\n' latest-main-sha
    ;;
  'api --method POST repos/Waterkyuu/cacheq/git/refs -f ref=refs/heads/release/20261010 -f sha=latest-main-sha')
    test "$RELEASE_BRANCH_TEST_FAILURE" != create
    ;;
  *) exit 1 ;;
esac
CLI
chmod +x "$sandbox/bin/date" "$sandbox/bin/gh"
export PATH="$sandbox/bin:$PATH"

# expect_created checks that only the exact dated ref is created at current main.
expect_created() {
  : > "$RELEASE_BRANCH_TEST_CALLS"
  sh "$script"
  test "$(wc -l < "$RELEASE_BRANCH_TEST_CALLS" | tr -d ' ')" = 3
  grep -q '^api --method POST .*ref=refs/heads/release/20261010 -f sha=latest-main-sha$' \
    "$RELEASE_BRANCH_TEST_CALLS"
}

expect_created
# GitHub's matching-ref endpoint may return longer branch names with the same prefix.
export RELEASE_BRANCH_TEST_REFS='[{"ref":"refs/heads/release/20261010-hotfix"}]'
expect_created

export RELEASE_BRANCH_TEST_REFS='[{"ref":"refs/heads/release/20261010"}]'
: > "$RELEASE_BRANCH_TEST_CALLS"
sh "$script"
test "$(wc -l < "$RELEASE_BRANCH_TEST_CALLS" | tr -d ' ')" = 1

export RELEASE_BRANCH_TEST_REFS='[]'
for failure in lookup main create; do
  export RELEASE_BRANCH_TEST_FAILURE=$failure
  : > "$RELEASE_BRANCH_TEST_CALLS"
  if sh "$script"; then
    printf 'Expected %s API failure to stop branch creation.\n' "$failure" >&2
    exit 1
  fi
  case "$failure" in
    lookup) expected_calls=1 ;;
    main) expected_calls=2 ;;
    create) expected_calls=3 ;;
  esac
  test "$(wc -l < "$RELEASE_BRANCH_TEST_CALLS" | tr -d ' ')" = "$expected_calls"
done
printf '%s\n' 'Release branch tests passed.'
