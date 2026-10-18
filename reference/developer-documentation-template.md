# Developer guide template

Use this template when writing or rewriting a developer-facing guide in `docs/`.
Produce the finished guide. Replace every placeholder with verified content and
remove the writing instructions from the published document.

Read the relevant implementation, public API comments, tests, and call sites
before writing. Use the current repository's API names, defaults, supported
versions, and behavior. Keep the English and Chinese guides aligned.

## Required document structure

### 1. Title and introduction

```markdown
# <Feature name>

[Other language](<matching-language-file>)

<What this feature does, when an application needs it, and what this guide
will implement. Use one short paragraph and a concrete application scenario.>
```

Name the application data and operation. For example: two service instances
reuse a user's profile through Redis to avoid loading it twice from the database.
Do not open with internal architecture, implementation history, or a list of
abstract benefits.

### 2. Installation

```markdown
## Installation

<Required Go version and external services.>

<Exact dependency installation commands, with the working directory stated.>

<External service startup command and required connection settings.>
```

Distinguish the core module's requirements from an optional adapter's requirements.
If setup requires a repository checkout or local `replace` directives, provide
the commands and identify the paths the reader must change. Do not assume an
unverified package version is available to install.

### 3. Quick start

```markdown
## Quick start

Save the following program as `main.go` in <the configured module directory>.
This example <states the one behavior demonstrated>.

<One complete Go program.>

Run it from <the working directory>:

<Exact run command.>

Expected output:

<Verified output.>

<Explain what in the output demonstrates success.>
```

The program must include `package`, imports, data types, dependency initialization,
configuration, a public API call, result handling, error handling, and cleanup.
Use small, concrete input values. Explain which values to replace in a real
application. Include required service addresses and environment variables.

Before the first program, explain the key objects and how they connect. For each
feature-specific constructor, explain its type argument, dependencies, concrete
option values, and returned object. State whether construction performs I/O or
only saves configuration, and which later call uses it. A configuration table
later in the guide does not replace these first-use explanations.

Demonstrate the smallest useful scenario. Keep unrelated capabilities out of this
program. For Redis shared caching, demonstrate a source read followed by another
Client reusing that result; add invalidation and notifications in later sections.
Do not paste a large demonstration covering every feature into the quick start.
Explain repeat-run behavior if existing remote state affects the output.

### 4. Usage

```markdown
## Usage

### <First common task, expressed as an action>

<When to perform this operation and where the following snippet belongs.>

<Focused Go snippet using objects introduced in the quick start.>

<Returned result, observable effect, and relevant error behavior.>

### <Next common task>

<Follow the same format. Introduce one additional capability.>
```

Use concrete task headings such as "Invalidate a result after updating the
source" or "Receive invalidation notifications in another instance". Do not
collect unrelated operations under a heading such as "Advanced usage".

Identify the origin of every variable and helper used by a snippet. State which
file or function it extends. Label snippets that are not standalone programs.
Explain inputs and return values at the call site. Say which objects to reuse
across requests and who owns their cleanup. Link to prerequisites before using
APIs introduced in another guide.

### 5. Configuration

```markdown
## Configuration

| Field | Purpose | Required or default | Constraints |
| --- | --- | --- | --- |
| <Exact API field> | <Effect on application behavior> | <Verified value> | <Units, validation, or interaction> |

<Concrete example explaining settings that are easy to confuse.>
```

Explain behavior instead of translating field names. For example, distinguish
local freshness from Redis retention with a timeline showing when a result
becomes stale. Identify zero-value behavior and incompatible option combinations.
Do not reproduce an entire implementation struct when a table is clearer.

### 6. Error handling and common problems

```markdown
## Error handling and common problems

| Symptom | Cause to check | Action |
| --- | --- | --- |
| <Observable problem> | <Specific configuration or behavior> | <Concrete diagnostic or correction> |
```

Explain which failures return errors, which fall back or retain existing data,
and how the application can observe them. Include cancellation and timeouts when
relevant. Provide diagnostic calls or short error-handling snippets where needed.
Do not provide a list of error names without explaining what the caller should do.

### 7. Behavior and limitations

```markdown
## Behavior and limitations

<Caller-visible guarantees and boundaries that affect application decisions.>
```

Cover relevant freshness, concurrency, consistency, and resource ownership rules.
Explain mechanisms through their observable effects. For example, a cache write
rejected after invalidation can still return successful source data while leaving
the local result stale. Put detailed protocol explanations in a separate design
reference when needed. Do not imply unsupported guarantees.

### 8. Complete examples and related guides

```markdown
## Complete examples and related guides

- [<Maintained example>](<source-path>): <what additional scenario it demonstrates>.
- [<Related guide>](<guide-path>): <when the reader needs it>.
```

The quick start must work without following these links. Omit sections that do
not apply to the feature; do not invent content to fill the structure.

## Chinese section headings

Use the same section order and examples in both languages:

| English | Simplified Chinese |
| --- | --- |
| Installation | 安装 |
| Quick start | 快速开始 |
| Usage | 使用方式 |
| Configuration | 配置说明 |
| Error handling and common problems | 错误处理与常见问题 |
| Behavior and limitations | 行为与限制 |
| Complete examples and related guides | 完整示例与相关文档 |

## Completion requirements

- Compile and run the complete program from the documented setup; verify output.
- Verify later snippets in their stated context. Do not leave undefined helpers,
  missing imports, ellipses, or unhandled setup errors in runnable examples.
- Keep code comments in English and follow repository declaration-comment rules.
- Check both languages for matching APIs, defaults, code, outputs, and limitations.
- Remove author instructions, placeholders, research notes, and source-comparison
  commentary from the finished guide.
- Follow repository checks. For website guide changes, build both locales and
  verify links, navigation, and search. Keep unrelated documents unchanged.
