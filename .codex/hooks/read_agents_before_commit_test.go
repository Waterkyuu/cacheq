package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestInvokesGitCommit distinguishes executable commits from quoted mentions.
func TestInvokesGitCommit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		command string
		want    bool
	}{
		{name: "direct", command: "git commit -m 'add hook'", want: true},
		{name: "git directory", command: "git -C /tmp/repo commit --amend", want: true},
		{name: "chained", command: "git status && /usr/bin/git commit -F /tmp/message", want: true},
		{name: "newline separated", command: "git status\ngit commit -m update", want: true},
		{name: "CRLF separated", command: "git status\r\ngit commit -m update", want: true},
		{name: "nested bash", command: "bash -lc 'git commit -m update'", want: true},
		{name: "nested sh", command: "/bin/sh -c 'git status && git commit -m update'", want: true},
		{name: "nested twice", command: `bash -c 'sh -c "git commit -m update"'`, want: true},
		{name: "nested env", command: "env MODE=test bash -lc 'git commit -m update'", want: true},
		{name: "nested shell option", command: "bash -o pipefail -c 'git commit -m update'", want: true},
		{name: "nested shopt option", command: "bash -O extglob -c 'git commit -m update'", want: true},
		{name: "nested option operand", command: "bash -o 'git commit' script.sh", want: false},
		{name: "nested noncommit", command: "bash -lc 'echo git commit'", want: false},
		{name: "shell mention", command: "echo 'bash -lc git commit'", want: false},
		{name: "shell without command flag", command: "bash --norc 'git commit'", want: false},
		{name: "quoted newline", command: "echo 'git status\ngit commit'", want: false},
		{name: "quoted mention", command: "echo 'git commit'", want: false},
		{name: "search mention", command: "rg 'git commit' AGENTS.md", want: false},
		{name: "other subcommand", command: "git status", want: false},
		{name: "commit argument", command: "git config alias.save commit", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := invokesGitCommit(test.command); got != test.want {
				t.Fatalf("invokesGitCommit(%q) = %t, want %t", test.command, got, test.want)
			}
		})
	}
}

// TestBuildOutputReadsLatestAgentsFile checks that each invocation reloads instructions.
func TestBuildOutputReadsLatestAgentsFile(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	agentsPath := filepath.Join(root, "AGENTS.md")
	if err := os.WriteFile(agentsPath, []byte("Always run tests.\n"), 0o600); err != nil {
		t.Fatalf("write AGENTS.md: %v", err)
	}

	output, err := buildOutput(root)
	if err != nil {
		t.Fatalf("buildOutput() error = %v", err)
	}
	context := output.HookSpecificOutput.AdditionalContext
	if !strings.Contains(context, agentsPath) {
		t.Errorf("context does not contain AGENTS.md path: %q", context)
	}
	if !strings.Contains(context, "Always run tests.") {
		t.Errorf("context does not contain AGENTS.md content: %q", context)
	}
}

// TestRunIgnoresNonCommitCommand avoids injecting instructions for unrelated tools.
func TestRunIgnoresNonCommitCommand(t *testing.T) {
	t.Parallel()

	event, err := json.Marshal(hookEvent{
		CWD: ".",
		ToolInput: toolInput{
			Command: "git status",
		},
	})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	var output bytes.Buffer
	var errorOutput bytes.Buffer
	if code := run(bytes.NewReader(event), &output, &errorOutput); code != 0 {
		t.Fatalf("run() = %d, want 0; stderr = %q", code, errorOutput.String())
	}
	if output.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", output.String())
	}
}
