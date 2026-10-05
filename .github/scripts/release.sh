#!/bin/sh
# Validate releases and publish only the commit inspected by the notes agent.
set -eu
export LC_ALL=C

# fail reports a release boundary violation before any further side effects.
fail() {
  printf '%s\n' "$1" >&2
  exit 1
}

# validate accepts stable versions compatible with the unsuffixed Go module.
validate() {
  printf '%s\n' "$version" | grep -Eq '^v[01]\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$' ||
    fail 'Use a stable v0.x.y or v1.x.y version; v2 needs a new module path.'
  if git show-ref --verify --quiet "refs/tags/$version"; then
    test "$(git rev-parse "$version^{commit}")" = "$(git rev-parse HEAD)" ||
      fail 'The requested tag already points to a different commit.'
  fi
}

# context records the release range and successful prerequisite jobs for the agent.
context() {
  validate
  # Exclude the requested tag so retries after tag publication retain their range.
  previous=$(git tag --merged HEAD --sort=-version:refname |
    awk -v version="$version" '$0 != version && /^v[0-9]+\.[0-9]+\.[0-9]+$/ {print; exit}')
  revision=HEAD
  base=$(git hash-object -t tree --stdin < /dev/null)
  if test -n "$previous"; then
    revision="$previous..HEAD"
    base=$previous
  fi
  {
    printf 'Version: %s\nTarget: %s\n' "$version" "$(git rev-parse HEAD)"
    printf 'Previous tag: %s\nRange: %s\n\n' "${previous:-first release}" "$revision"
    printf '%s\n' 'Passed: lint, format, module metadata, race tests on Linux/macOS/Windows'
    printf '%s\n\n' 'with Go 1.22 and stable, build, release automation tests, govulncheck.'
    printf 'Commits:\n'
    git log --format='%h %s' "$revision"
    printf '\nDiff summary:\n'
    git diff --stat "$base" HEAD
  } > release-context.md
}

# notes rejects incomplete or non-English agent output before making an artifact.
notes() {
  jq -e --arg version "$version" '
    .version == $version and .ready == true and
    (.notes | type == "string") and
    (.notes | test("\\S")) and
    (.notes | test("[\u3400-\u9fff]") | not)
  ' release-notes.json > /dev/null || fail 'The agent must return ready English notes for this version.'
  jq -r '.notes' release-notes.json > release-notes.md
}

# publish creates an annotated tag and permits retries after partial publication.
publish() {
  validate
  test -s release-notes.md || fail 'Release notes must not be empty.'
  # Tags may change while checks run; never replace a tag created for another commit.
  git fetch origin --tags
  validate
  if ! git show-ref --verify --quiet "refs/tags/$version"; then
    git tag -a "$version" -m "$version"
  fi
  git push origin "refs/tags/$version"
  published=$(gh release list --limit 1000 --json tagName)
  if printf '%s\n' "$published" | jq -e --arg version "$version" 'any(.[]; .tagName == $version)' > /dev/null; then
    printf '%s\n' 'Release already exists; keeping its published notes.'
    return
  fi
  gh release create "$version" --verify-tag --title "$version" --notes-file release-notes.md
}

test "$#" -eq 2 || fail 'Usage: release.sh validate|context|notes|publish v0.x.y'
stage=$1
version=$2
case "$stage" in
  validate|context|notes|publish) "$stage" ;;
  *) fail 'Unknown release stage.' ;;
esac
