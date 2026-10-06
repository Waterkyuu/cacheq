// Command read-agents-before-commit reloads repository instructions before Git commits.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

var assignmentPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*=`)

var allowedGitPrefixes = map[string]struct{}{
	"command": {},
	"do":      {},
	"elif":    {},
	"else":    {},
	"env":     {},
	"exec":    {},
	"if":      {},
	"then":    {},
	"time":    {},
	"until":   {},
	"while":   {},
}

var gitOptionsWithValue = map[string]struct{}{
	"-C":             {},
	"-c":             {},
	"--config-env":   {},
	"--exec-path":    {},
	"--git-dir":      {},
	"--namespace":    {},
	"--super-prefix": {},
	"--work-tree":    {},
}

// maxShellNesting limits recursive inspection of shell command arguments.
const maxShellNesting = 8

// hookEvent carries the working directory and pending tool call from Codex.
type hookEvent struct {
	CWD       string    `json:"cwd"`
	ToolInput toolInput `json:"tool_input"`
}

// toolInput contains the shell command about to be executed.
type toolInput struct {
	Command string `json:"command"`
}

// hookOutput wraps context returned to the Codex hook protocol.
type hookOutput struct {
	HookSpecificOutput hookSpecificOutput `json:"hookSpecificOutput"`
}

// hookSpecificOutput supplies instructions before the pending tool call.
type hookSpecificOutput struct {
	HookEventName     string `json:"hookEventName"`
	AdditionalContext string `json:"additionalContext"`
}

// main exits with the result of processing one hook event from stdin.
func main() {
	os.Exit(run(os.Stdin, os.Stdout, os.Stderr))
}

// run reloads repository instructions only for Git commit commands.
func run(input io.Reader, output, errorOutput io.Writer) int {
	var event hookEvent
	if err := json.NewDecoder(input).Decode(&event); err != nil {
		return 0
	}
	if !invokesGitCommit(event.ToolInput.Command) {
		return 0
	}

	root, err := repositoryRoot(event.CWD)
	if err != nil {
		reportError(errorOutput, "cannot reload AGENTS.md", err)
		return 2
	}

	result, err := buildOutput(root)
	if err != nil {
		reportError(errorOutput, "cannot reload AGENTS.md", err)
		return 2
	}
	if err := json.NewEncoder(output).Encode(result); err != nil {
		reportError(errorOutput, "cannot emit hook output", err)
		return 2
	}

	return 0
}

// repositoryRoot resolves the Git root containing the pending command.
func repositoryRoot(cwd string) (string, error) {
	command := exec.Command("git", "-C", cwd, "rev-parse", "--show-toplevel")
	value, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("resolve repository root: %w", err)
	}

	root := strings.TrimSpace(string(value))
	if root == "" {
		return "", errors.New("resolve repository root: empty path")
	}
	return root, nil
}

// buildOutput reads the latest AGENTS.md and returns it as hook context.
func buildOutput(root string) (hookOutput, error) {
	agentsPath := filepath.Join(root, "AGENTS.md")
	instructions, err := os.ReadFile(agentsPath) // #nosec G304 -- Git resolves the root and the filename is fixed.
	if err != nil {
		return hookOutput{}, fmt.Errorf("read %s: %w", agentsPath, err)
	}

	context := fmt.Sprintf(
		"Before executing the pending git commit, re-read and obey the repository instructions from %s:\n\n%s",
		agentsPath,
		instructions,
	)
	return hookOutput{
		HookSpecificOutput: hookSpecificOutput{
			HookEventName:     "PreToolUse",
			AdditionalContext: context,
		},
	}, nil
}

// reportError records a failure that prevents a safe commit.
func reportError(output io.Writer, message string, err error) {
	// The hook already fails closed, so an unavailable stderr has no recovery path.
	_, _ = fmt.Fprintf(output, "%s: %v\n", message, err)
}

// invokesGitCommit detects a Git commit in a shell command sequence.
func invokesGitCommit(command string) bool {
	return invokesGitCommitAtDepth(command, 0)
}

// invokesGitCommitAtDepth follows shell scripts passed through nested -c options.
func invokesGitCommitAtDepth(command string, depth int) bool {
	if depth > maxShellNesting {
		return false
	}
	segments, err := shellSegments(command)
	if err != nil {
		return false
	}

	for _, segment := range segments {
		if isGitCommit(segment) {
			return true
		}
		if script, ok := nestedShellScript(segment); ok && invokesGitCommitAtDepth(script, depth+1) {
			return true
		}
	}
	return false
}

// nestedShellScript extracts a command string executed by a shell's -c option.
func nestedShellScript(words []string) (string, bool) {
	for index, word := range words {
		switch filepath.Base(word) {
		case "sh", "bash", "dash", "zsh":
		default:
			continue
		}
		if !validGitPrefix(words[:index]) {
			return "", false
		}
		for option := index + 1; option < len(words); option++ {
			word := words[option]
			if !strings.HasPrefix(word, "-") || word == "--" {
				return "", false
			}
			if strings.HasPrefix(word, "--") {
				continue
			}
			if word == "-o" || word == "-O" {
				option++
				continue
			}
			if strings.Contains(strings.TrimPrefix(word, "-"), "c") {
				if option+1 < len(words) {
					return words[option+1], true
				}
				return "", false
			}
		}
		return "", false
	}
	return "", false
}

// isGitCommit recognizes a Git commit after allowed shell and Git prefixes.
func isGitCommit(words []string) bool {
	gitIndex := -1
	for index, word := range words {
		if filepath.Base(word) == "git" {
			gitIndex = index
			break
		}
	}
	if gitIndex == -1 || !validGitPrefix(words[:gitIndex]) {
		return false
	}

	index := gitIndex + 1
	for index < len(words) {
		word := words[index]
		if word == "--" {
			index++
			break
		}
		if _, ok := gitOptionsWithValue[word]; ok {
			index += 2
			continue
		}
		if strings.HasPrefix(word, "-") {
			index++
			continue
		}
		break
	}

	return index < len(words) && words[index] == "commit"
}

// validGitPrefix accepts wrappers and assignments before a Git executable.
func validGitPrefix(words []string) bool {
	for _, word := range words {
		_, allowed := allowedGitPrefixes[word]
		if !allowed && !assignmentPattern.MatchString(word) {
			return false
		}
	}
	return true
}

// shellSegments separates unquoted commands while preserving quoted arguments.
func shellSegments(command string) ([][]string, error) {
	segments := [][]string{{}}
	var word strings.Builder
	var quoted, escaped bool
	var quote rune

	flushWord := func() {
		if word.Len() == 0 {
			return
		}
		last := len(segments) - 1
		segments[last] = append(segments[last], word.String())
		word.Reset()
	}
	flushSegment := func() {
		flushWord()
		segments = append(segments, []string{})
	}

	for _, character := range command {
		if escaped {
			word.WriteRune(character)
			escaped = false
			continue
		}
		if character == '\\' && quote != '\'' {
			escaped = true
			continue
		}
		if quoted {
			if character == quote {
				quoted = false
				quote = 0
				continue
			}
			word.WriteRune(character)
			continue
		}
		if character == '\'' || character == '"' {
			quoted = true
			quote = character
			continue
		}
		if strings.ContainsRune(";&|()\n\r", character) {
			flushSegment()
			continue
		}
		if character == '\t' || character == ' ' {
			flushWord()
			continue
		}
		word.WriteRune(character)
	}

	if quoted || escaped {
		return nil, errors.New("unterminated shell token")
	}
	flushWord()
	return segments, nil
}
