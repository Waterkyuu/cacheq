# Releasing cacheq

Open **Actions → Release → Run workflow**, select `main`, and enter a stable
version such as `v0.1.1`. Leave **dry_run** unchecked to publish.

The workflow runs lint, formatting, module checks, race tests across Linux,
macOS, and Windows, builds, and vulnerability checks. A Codex Agent powered by
DeepSeek then reads the commits, diffs, and source since the previous release
and writes **English-only** release notes.

After validating the notes, a separate job creates an annotated Git tag at the
checked commit and publishes the GitHub Release. The agent has a read-only
sandbox and does not have the token used to publish releases.

To preview, enable **dry_run**. It runs the same checks and agent, then saves the
notes in the workflow summary and the `release-notes` artifact without creating
a tag or release. The preview still makes paid requests to DeepSeek.

## Configuration

Store the DeepSeek API key as the repository Actions secret `DEEPSEEK_API_KEY`.
Do not put keys in repository files. The agent uses `deepseek-flash` through
DeepSeek's Responses API and the official `openai/codex-action` runner.

The release helpers are POSIX shell scripts using Git, jq, and GitHub CLI. These
tools are available on the GitHub-hosted Ubuntu runner. Local verification:

```sh
sh .github/scripts/release_test.sh
```

## Versions and retries

Use a new stable `v0.x.y` or `v1.x.y` version. A `v2` release requires updating
the Go module path first. The workflow does not publish on ordinary commits.

An existing tag for another commit is rejected. If publication stops after
pushing the tag, rerun the workflow at the same commit and version. It reuses
that tag; an existing Release and its notes are preserved. Never move a
published Go module tag to a different commit.
