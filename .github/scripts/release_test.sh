#!/bin/sh
# Test release boundaries against disposable repositories and a fake GitHub CLI.
set -eu

script=$(cd "$(dirname "$0")" && pwd)/release.sh
sandbox=$(mktemp -d)
trap 'test -z "$sandbox" || rm -r -- "$sandbox"' EXIT HUP INT TERM
git init -q --bare "$sandbox/remote.git"
git init -q "$sandbox/work"
cd "$sandbox/work"
git config user.name 'Release Test'
git config user.email 'test@example.com'
git remote add origin "$sandbox/remote.git"
printf 'first\n' > example.txt
git add example.txt
git commit -qm 'feat: initial version'
git tag -a v0.1.0 -m v0.1.0
printf 'second\n' >> example.txt
git commit -qam 'fix: update example'
git push -q origin HEAD refs/tags/v0.1.0

# expect_failure ensures a rejected request cannot silently pass validation.
expect_failure() {
  if sh "$script" "$@" > /dev/null 2>&1; then
    printf 'Expected failure: %s\n' "$*" >&2
    exit 1
  fi
}

sh "$script" validate v0.1.1
sh "$script" validate v1.20.0
for version in 0.1.1 v2.0.0 v01.1.1 v1.01.0 v1.0.0-beta 'v1.0.0;echo x'; do
  expect_failure validate "$version"
done
expect_failure validate v0.1.0
sh "$script" context v0.1.1
grep -q 'Previous tag: v0.1.0' release-context.md
grep -q 'Range: v0.1.0..HEAD' release-context.md

for data in \
  '{"version":"v0.1.2","ready":true,"notes":"Changes"}' \
  '{"version":"v0.1.1","ready":false,"notes":"Insufficient evidence"}' \
  '{"version":"v0.1.1","ready":true,"notes":" "}' \
  '{"version":"v0.1.1","ready":true,"notes":42}' \
  '{"version":"v0.1.1","ready":true,"notes":"\u4e2d\u6587"}'; do
  printf '%s\n' "$data" > release-notes.json
  expect_failure notes v0.1.1
  test ! -e release-notes.md
done
printf '%s\n' '{"version":"v0.1.1","ready":true,"notes":"## Fixes\n\n- Fixed caching."}' > release-notes.json
sh "$script" notes v0.1.1
grep -q 'Fixed caching' release-notes.md

mkdir "$sandbox/bin"
export RELEASE_TEST_CALLS="$sandbox/gh-calls"
export RELEASE_TEST_LIST="$sandbox/releases.json"
printf '[]\n' > "$RELEASE_TEST_LIST"
cat > "$sandbox/bin/gh" <<'SH'
#!/bin/sh
set -eu
case "$1 $2" in
  'release list') cat "$RELEASE_TEST_LIST" ;;
  'release create') printf '%s\n' "$*" >> "$RELEASE_TEST_CALLS" ;;
  *) exit 1 ;;
esac
SH
chmod +x "$sandbox/bin/gh"
export PATH="$sandbox/bin:$PATH"
sh "$script" publish v0.1.1
test "$(git cat-file -t v0.1.1)" = tag
test "$(git --git-dir="$sandbox/remote.git" rev-parse 'v0.1.1^{commit}')" = "$(git rev-parse HEAD)"
grep -q 'release create v0.1.1 --verify-tag' "$RELEASE_TEST_CALLS"

# Retry after a pushed tag reuses the tag and the same previous release range.
sh "$script" context v0.1.1
grep -q 'Previous tag: v0.1.0' release-context.md
sh "$script" publish v0.1.1
test "$(wc -l < "$RELEASE_TEST_CALLS" | tr -d ' ')" = 2
printf '[{"tagName":"v0.1.1"}]\n' > "$RELEASE_TEST_LIST"
sh "$script" publish v0.1.1
test "$(wc -l < "$RELEASE_TEST_CALLS" | tr -d ' ')" = 2

# A remote tag collision introduced during checks is rejected before publishing.
git --git-dir="$sandbox/remote.git" tag v0.1.2 v0.1.0
expect_failure publish v0.1.2
test "$(wc -l < "$RELEASE_TEST_CALLS" | tr -d ' ')" = 2
printf '%s\n' 'Release automation tests passed.'
