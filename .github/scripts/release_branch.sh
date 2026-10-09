#!/bin/sh
# Create the Shanghai calendar day's release branch without moving existing refs.
set -eu

: "${GH_REPO:?Repository is required}"

branch="release/$(TZ=Asia/Shanghai date +%Y%m%d)"
ref="refs/heads/$branch"
existing=$(gh api "repos/$GH_REPO/git/matching-refs/heads/$branch" \
  --jq ".[] | select(.ref == \"$ref\") | .ref")
if [ -n "$existing" ]; then
  printf 'Branch %s already exists; leaving it unchanged.\n' "$branch"
  exit 0
fi

# Resolve main immediately before creation instead of using the workflow's older SHA.
sha=$(gh api "repos/$GH_REPO/git/ref/heads/main" --jq '.object.sha')
gh api --method POST "repos/$GH_REPO/git/refs" -f "ref=$ref" -f "sha=$sha" >/dev/null
printf 'Created %s at %s.\n' "$branch" "$sha"
