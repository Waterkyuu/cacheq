# Code Review Standard

This standard applies to reviews of cacheq implementations, APIs, concurrency
behavior, and architecture. Reviews must follow repository instructions and
existing contracts, identify concrete errors within the library's responsibility,
and avoid attributing caller-owned errors to library design or architecture.

## 1. Required reading order

Before inspecting code for specific defects, complete these steps in order:

1. Read this entire standard.
2. Read relevant API comments, feature documentation, and stated limitations to
   establish the responsibilities of the library and its callers.
3. Inspect the target commit or diff, surrounding implementation, existing tests,
   and call sites.
4. Only then investigate specific defects and verify their triggering conditions.

Do not infer a defect from a function name, personal experience, or another
library's behavior and then search for evidence to support that conclusion.

## 2. Criteria for library defects

A finding may be reported as a library defect only when all of these conditions
are satisfied:

- **An obligation exists.** The implementation violates an established API
  contract, project requirement, compatibility guarantee, or internal invariant
  owned by the library, such as type binding, lifecycle, or resource accounting.
- **The triggering usage satisfies the contract.** The caller uses supported
  arguments, lifecycles, and concurrency patterns, and follows preconditions such
  as treating shared data as read-only.
- **The cause belongs to the library.** The failure occurs even when the caller
  meets its responsibilities. It is not solely caused by business callbacks,
  application-controlled ordering, or external service behavior.
- **The impact is concrete and verifiable.** Incorrect results, corrupted state,
  leaked resources, or broken execution guarantees can be demonstrated through a
  minimal reproduction, a test, or a complete analysis of the code path.

A statement that code "could create a risk" does not establish a defect. Explain
what the library does incorrectly in a supported scenario.

Feature plans, capabilities of other libraries, and reviewer preferences do not
automatically become obligations of the current implementation. The absence of
resource serialization, server-side version checks, or other optional capabilities
is not itself a defect.

Documentation does not automatically excuse implementation errors either. If
documentation, comments, tests, or explicit requirements contradict one another,
identify the contradiction and its evidence. Do not select only the source that
supports a predetermined conclusion.

## 3. Responsibility attribution

Classify each concern before reporting it:

| Category | Basis | Reporting rule |
| --- | --- | --- |
| Library implementation defect | Supported usage violates a library contract or internal invariant | Report a finding with evidence and severity |
| Documentation or contract inconsistency | Public statements differ from supported behavior | Identify the specific promise, actual behavior, and location requiring correction |
| Caller-owned business error | Business callbacks, external state, or application-controlled operations cause the problem | Exclude it from library findings; explain caller responsibility if discussion is needed |
| Optional capability suggestion | An additional requirement needs guarantees outside the existing contract | Present separately only when design suggestions are requested; assign no defect priority |
| Unverified concern | Contract, triggering conditions, or causal evidence remain incomplete | Mark it as requiring verification; do not report it as an established defect |

Applications define business key semantics, database commit order, the relative
freshness of remote results, and business merge policies. Unless the API explicitly
assumes these responsibilities, the library has no obligation to infer or protect
the caller's cache update policy.

Do not relabel a caller error from an "implementation bug" to a "design defect,"
"architectural risk," or "required capability" to preserve the same unsupported
claim about library responsibility.

## 4. Prohibited misattribution

The following observations alone do not establish library defects:

- A caller unconditionally passes an older business result to `Set`, replacing
  a newer cached value.
- Two independent Mutation handles modify the same resource, while the application
  expects an undeclared serialization guarantee across handles.
- An application interprets a concurrency limit as a guarantee of business
  execution order, cache synchronization order, or transactional behavior.
- A caller mutates shared slices, maps, or pointed-to data and produces a data race
  after violating a read-only contract.
- A business callback ignores its context, uses the wrong business key, or violates
  lifecycle requirements.
- An application recovers from a callback panic and expects a handle to provide an
  undeclared panic recovery guarantee.

If an independent internal library error also exists in one of these scenarios,
prove that error separately. The caller's incorrect result is not a substitute
for that proof.

### Example: an older Mutation result replaces newer cache data

Assume the existing contract states that separate Mutation handles execute
independently, `OnSuccess` is supplied by the caller, and `Set` unconditionally
installs the supplied data.

```text
A writes Bob to the database, then pauses its own OnSuccess callback.
B writes Carol to the database and uses Set to update the cache to Carol.
A resumes OnSuccess and uses Set to update the cache to Bob.
```

This reproduction demonstrates incorrect ordering in the caller's cache updates.
It does not demonstrate a library contract violation. Do not report it as a
library concurrency bug, design defect, or architecture defect, or use it to
require serialization or version APIs.

A finding is justified only if the library already promises the relevant ordering
or version protection and fails to honor that promise under supported usage.

## 5. Reproduction requirements

- Use the smallest supported call path. Do not manufacture failures by bypassing
  the contract.
- Establish concurrency ordering with channels or controlled test doubles.
  Short sleeps or accidental scheduling are insufficient evidence.
- Distinguish library operations from business callback operations. Do not hide
  deliberate blocking, stale writes, or business side effects inside callbacks
  and then attribute their consequences to the library.
- Demonstrating a bad outcome proves only that the outcome occurs. Also establish
  the violated contract and the internal library cause.
- Passing `-race` does not prove correct business ordering. Business ordering that
  differs from application expectations does not prove a library data race or
  concurrency defect either.
- If execution is unavailable, identify conclusions based on static analysis and
  any evidence still missing.

## 6. Required finding content

Each formal finding must include:

1. **Location:** The reviewed commit and specific file and line numbers.
2. **Obligation:** The violated contract or invariant and a traceable source.
3. **Trigger:** Arguments, lifecycle, concurrency ordering, and necessary
   environmental conditions.
4. **Expected and actual behavior:** What the contract requires and what occurs.
5. **Responsibility:** Why the cause belongs to the library rather than caller-owned
   business logic.
6. **Impact and verification:** The affected observable behavior and reproduction
   results or complete analysis of the code path.
7. **Fix scope:** The smallest change that restores the existing guarantee without
   adding unrelated guarantees.

Do not replace missing evidence with higher severity or phrases such as
"production risk." Assign severity based on demonstrated impact and triggering
scope.

## 7. Reporting and corrections

- Report verified defects first. If no findings satisfy this standard, state:
  "No library defects meeting this standard were found."
- Disclose incomplete verification and review limitations. Do not present an
  absence of findings as proof that no problems exist.
- Keep caller errors, feature suggestions, and verified defects separate. Do not
  combine their priorities or finding counts.
- When the user challenges responsibility attribution, recheck the contract and
  causal evidence. Withdraw an unsupported finding instead of preserving the same
  conclusion under a label such as "design risk."
- A review request alone does not authorize API changes or implementation fixes.
  When the user requests a fix, follow the repository's development and commit
  workflow.
