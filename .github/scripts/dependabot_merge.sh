#!/bin/sh
# Merge only the Dependabot revision whose required checks have all succeeded.
set -eu

: "${GH_REPO:?Repository is required}"
: "${DEPENDABOT_HEAD_BRANCH:?Workflow branch is required}"
: "${DEPENDABOT_HEAD_SHA:?Workflow commit is required}"

# A completed run may belong to an old revision, a human PR, or a fork.
pr=$(gh api --method GET "repos/$GH_REPO/pulls" \
  -f state=open -f "head=${GH_REPO%/*}:$DEPENDABOT_HEAD_BRANCH" -f base=main |
  jq --arg repo "$GH_REPO" --arg sha "$DEPENDABOT_HEAD_SHA" '
    [.[] | select(.user.login == "dependabot[bot]" and
      .head.repo.full_name == $repo and .head.sha == $sha and .base.ref == "main")]
    | if length == 1 then .[0] else null end')
if [ "$pr" = null ]; then
  exit 0
fi
number=$(printf '%s' "$pr" | jq -r .number)
title=$(printf '%s' "$pr" | jq -r .title)

# Each workflow completion retries the gate; pending or failed checks never merge.
if ! checks=$(gh pr checks "$number" --required --json state); then
  printf 'PR #%s is waiting for successful required checks.\n' "$number"
  exit 0
fi
# Empty requirements must not accidentally turn automatic merging into an unchecked merge.
if ! printf '%s' "$checks" | jq -e 'length > 0 and all(.state == "SUCCESS")' >/dev/null; then
  printf 'PR #%s has missing or unsuccessful required checks.\n' "$number"
  exit 0
fi

gh pr merge "$number" --squash --match-head-commit "$DEPENDABOT_HEAD_SHA" \
  --subject "$title" --body '- Update dependencies after all required CI checks pass'

# GITHUB_TOKEN merges do not trigger push workflows; dispatch the existing Pages pipeline.
gh workflow run docs.yml --ref main
