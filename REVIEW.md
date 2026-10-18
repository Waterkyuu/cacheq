# Code and API Design Review Standard

This standard applies to reviews of soroi implementations, API designs, and refactors. Review code quality against `AGENTS.md` first, then assess API complexity, cognitive load, design correctness, and whether the implementation meets its functional goals.

Review the library's own implementation and design. Do not attribute caller-owned business policies, callback errors, or undeclared business guarantees to library defects. A behavior being documented does not, by itself, establish that the design is sound.

## 1. Reading and review order

1. Read the entire root `AGENTS.md`, then applicable nested instructions and this standard.
2. Establish the problem being solved, functional goals, scope, and compatibility requirements. Use requirements, commit descriptions, API comments, documentation, tests, and call sites; do not infer goals solely from function names.
3. Read the target diff and related implementation, and complete the code quality review first.
4. Assess complexity, cognitive load, and relationships between interfaces from the perspective of a caller using public APIs.
5. Check API combinations, state transitions, concurrency, and lifecycle behavior for contradictions or defects, and determine whether the implementation meets its goals.
6. Perform the verification needed for specific concerns before reporting conclusions.

Identify uncertainty when goals or responsibility boundaries are unclear. Do not invent requirements or treat the current implementation as proof that the goals are correct.

## 2. Review code quality first

Use `AGENTS.md` as the authority and examine:

- **Responsibilities and organization:** Does the owning package and file contain the behavior? Are function, type, and field responsibilities clear? Are there duplicate implementations, unnecessary forwarding, or directories without a distinct responsibility?
- **Readability:** Are names and control flow clear? Are functions focused? Is there excessive nesting, implicit side effects, or state changes that are difficult to trace?
- **Implementation complexity:** Can the current problem be solved more directly? Are there speculative abstractions, over-engineering, unnecessary dependencies, or unrelated refactors?
- **Go conventions:** Does the code follow project conventions and the declared Go version? Are interfaces, embedding, constants, error wrapping, and dependency injection used appropriately?
- **Comments:** Do functions, methods, structs, fields, and constants satisfy declaration comment requirements? Do comments explain responsibilities, semantics, or invariants and agree with the implementation?
- **Internal correctness:** Is ownership of locks, shared state, contexts, channels, and resources clear? Do failure paths clean up correctly? Are there data races, deadlocks, resource leaks, or corrupted state?
- **Tests and documentation:** Do tests verify observable behavior and cover relevant failures, cancellation, and boundaries? Are concurrency tests controlled? Are documentation and examples accurate and consistent with the implementation and both language versions?

A code quality concern does not need to cause a runtime bug, but it must identify a specific project requirement violation or explain the added maintenance cost. Do not treat personal style preferences as mandatory fixes. Reference check output for tool-reported issues and avoid counting the same issue more than once.

## 3. Then assess API complexity and cognitive load

Consider the knowledge and steps needed to complete a typical use case, and examine:

- **Number of concepts:** How many types, options, states, and execution phases must callers understand? Does each serve a necessary and distinct responsibility?
- **Common paths:** Can common needs be met through direct, concise calls? Do they require workarounds, repeated configuration, or knowledge of internal implementation?
- **Option combinations:** Are options redundant, mutually exclusive, or capable of overriding one another? Are combination rules consistent? Must callers memorize many special cases to predict behavior?
- **Implicit rules:** Are parameter order, defaults, zero values, nil, override rules, and automatic side effects clear and consistent with other project APIs?
- **State and errors:** Do return values, errors, and snapshots clearly describe outcomes? Are there multiple sources of state without a defined relationship?
- **Execution and lifecycle:** Are creation, execution, waiting, cancellation, and closure easy to distinguish? Are blocking scope, concurrency limits, and resource release boundaries clear?
- **Composition:** Do APIs compose naturally? Does composition depend on hidden call ordering or internal state?

Fewer public fields or methods do not necessarily reduce cognitive load. Determine whether complexity was reduced or moved into call flows, callback composition, or implicit rules. Distinguish necessary application control from mechanical constraints the library could handle consistently.

A simplification proposal must identify a concrete use case, the rules callers currently need to understand, and the benefits and costs of a simpler alternative. Claims that an API "looks complex" or that another library is simpler are insufficient.

## 4. Assess API design defects and fitness for purpose

### 4.1 Does the API meet its functional goals?

- Can the required capabilities be implemented completely through public APIs without private state or bypassing restrictions?
- Does the API express the inputs, results, failure information, and lifecycle required by the goals?
- Does behavior remain consistent with the goals on success, failure, cancellation, and boundary conditions?
- Does a refactor omit required capabilities, introduce unintended behavior changes, or break compatibility that must be preserved?
- Is the complexity introduced to meet the goals necessary? Is there a smaller, complete solution?

### 4.2 Are there semantic conflicts or structural defects?

- Do methods, options, return values, and states express contradictory semantics for the same operation?
- Can supported API combinations cause waits that cannot complete, circular dependencies, conflicting internal policies, or states with no valid exit?
- Do types and parameters express the necessary constraints? Are combinations the library cannot handle explicitly rejected?
- Are library-owned execution ordering, state publication, cancellation, and closure rules consistent, or can they violate internal invariants?
- Is responsibility for acquiring, transferring, and releasing resources clear? Do concurrency limits correspond to actual resource use?
- Does handling shared data or contexts violate type binding, call isolation, or data isolation guarantees explicitly owned by the library?

API design review must assess whether public semantics are coherent and meet functional goals, beyond checking implementation against existing documentation. A documented limitation does not automatically make that limitation reasonable; the absence of an unrequested capability does not automatically establish a design defect either.

A design defect must identify a semantic contradiction, structural obstacle, or goal gap within the library, supported by a concrete call path or complete reasoning. Violations of existing contracts are evidence, but not the sole basis for assessing design quality.

## 5. Distinguish library responsibility from business responsibility

Review mechanisms provided by the library, not whether callers' business policies are correct.

Business key semantics, database commit order, relative freshness of remote results, and business merge policies belong to the application by default. Incorrect caller data, wrong business keys, violations of shared-data read-only requirements, or expectations of undeclared cross-handle transactions and serialization do not establish library design defects.

If supported usage exposes an independent internal library error, prove it separately. Do not relabel a business error as a "design risk" to preserve the same unsupported claim.

## 6. Verification and reporting

Choose evidence appropriate to the concern:

| Concern | Required evidence |
| --- | --- |
| Code quality | Specific code location and an applicable `AGENTS.md` requirement or explainable maintenance cost |
| API complexity or cognitive load | Typical call scenario, required knowledge and steps, and a comparable simplification with its costs |
| API design defect | Semantic conflict, structural obstacle, or functional goal gap, with a concrete call path or complete analysis |
| Implementation bug | Supported triggering conditions, internal library cause, expected and actual outcomes, and reproduction or complete code-path analysis |
| Optional capability suggestion | Additional requirement and benefits, explicitly identified as beyond existing functional goals |

Establish concurrency ordering with channels or controlled test doubles, not short sleeps or accidental scheduling. Passing `-race` does not prove the absence of logical conflicts; an incorrect business outcome alone does not prove a library defect.

Each finding should concisely explain its location, problem, impact, evidence, and smallest improvement. Design concerns do not require manufacturing a runtime bug, but hypothetical risks cannot replace causal analysis. Assign severity based on actual impact and triggering scope.

Organize conclusions in this order: code quality, API complexity and cognitive load, then API design correctness and fitness for purpose. Distinguish concern categories and highlight severe defects. Present optional capability suggestions separately and exclude them from defect counts.

When no issues are found, disclose review scope and verification limits; do not present "no issues found" as proof of a defect-free design. A review request alone does not authorize implementation or API changes. Fixes must follow user authorization and the repository workflow.
