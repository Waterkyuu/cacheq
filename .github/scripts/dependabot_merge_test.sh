#!/bin/sh
# Exercise the merge gate with a fake CLI, without network access or a real merge.
set -eu

script=$(cd "$(dirname "$0")" && pwd)/dependabot_merge.sh
sandbox=$(mktemp -d)
trap 'rm -r -- "$sandbox"' EXIT HUP INT TERM
mkdir "$sandbox/bin"
export GH_REPO=Waterkyuu/cacheq
export DEPENDABOT_HEAD_BRANCH=dependabot/npm_and_yarn/website/react-19.3.0
export DEPENDABOT_HEAD_SHA=checked-commit
export DEPENDABOT_TEST_PR="$sandbox/pr.json"
export DEPENDABOT_TEST_CHECKS="$sandbox/checks.json"
export DEPENDABOT_TEST_CALLS="$sandbox/calls"
export DEPENDABOT_TEST_CHECK_EXIT=0
cat > "$sandbox/bin/gh" <<'CLI'
#!/bin/sh
set -eu
case "$1 $2" in
  'api --method') cat "$DEPENDABOT_TEST_PR" ;;
  'pr checks')
    cat "$DEPENDABOT_TEST_CHECKS"
    exit "$DEPENDABOT_TEST_CHECK_EXIT"
    ;;
  'pr merge'|'workflow run') printf '%s\n' "$*" >> "$DEPENDABOT_TEST_CALLS" ;;
  *) exit 1 ;;
esac
CLI
chmod +x "$sandbox/bin/gh"
export PATH="$sandbox/bin:$PATH"

# set_pr creates a candidate with independently controllable trust boundaries.
set_pr() {
  jq -n --arg author "$1" --arg repo "$2" --arg sha "$3" --arg base "$4" \
    '[{number: 6, title: "chore(deps): upgrade React to the next major version",
      user: {login: $author}, head: {repo: {full_name: $repo}, sha: $sha},
      base: {ref: $base}}]' > "$DEPENDABOT_TEST_PR"
}

# expect_no_merge verifies rejected candidates cannot merge or dispatch deployment.
expect_no_merge() {
  rm -f "$DEPENDABOT_TEST_CALLS"
  sh "$script"
  test ! -e "$DEPENDABOT_TEST_CALLS"
}

printf '[{"state":"SUCCESS"}]\n' > "$DEPENDABOT_TEST_CHECKS"
for scenario in human fork stale base; do
  case "$scenario" in
    human) set_pr waterkyuu Waterkyuu/cacheq checked-commit main ;;
    fork) set_pr 'dependabot[bot]' other/cacheq checked-commit main ;;
    stale) set_pr 'dependabot[bot]' Waterkyuu/cacheq newer-commit main ;;
    base) set_pr 'dependabot[bot]' Waterkyuu/cacheq checked-commit feature ;;
  esac
  expect_no_merge
done
printf '[]\n' > "$DEPENDABOT_TEST_PR"
expect_no_merge

set_pr 'dependabot[bot]' Waterkyuu/cacheq checked-commit main
for checks in '[]' '[{"state":"FAILURE"}]' '[{"state":"PENDING"}]' \
  '[{"state":"CANCELLED"}]' '[{"state":"SKIPPED"}]' \
  '[{"state":"SUCCESS"},{"state":"FAILURE"}]'; do
  printf '%s\n' "$checks" > "$DEPENDABOT_TEST_CHECKS"
  expect_no_merge
done
printf '[{"state":"SUCCESS"}]\n' > "$DEPENDABOT_TEST_CHECKS"
for status in 1 8; do
  export DEPENDABOT_TEST_CHECK_EXIT=$status
  expect_no_merge
done
export DEPENDABOT_TEST_CHECK_EXIT=0
sh "$script"
test "$(wc -l < "$DEPENDABOT_TEST_CALLS" | tr -d ' ')" = 2
rg_check='pr merge 6 --squash --match-head-commit checked-commit'
grep -q -- "$rg_check" "$DEPENDABOT_TEST_CALLS"
grep -q -- 'workflow run docs.yml --ref main' "$DEPENDABOT_TEST_CALLS"
printf '%s\n' 'Dependabot merge tests passed.'
