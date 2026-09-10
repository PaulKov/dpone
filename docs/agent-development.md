# Agent-assisted development

This guide turns repository standards into a repeatable multi-agent workflow.
`AGENTS.md` files contain concise invariants; this page contains the detailed
coordination protocol.

For governance, security, and repository-administration controls, use:

- [Agent governance](agent-governance.md) for owners, control lifecycle, and
  required reviews;
- [Agent task contracts](agent-task-contracts.md) for machine-checkable writer
  boundaries in parallel worktrees;
- [Agent risk register](agent-risk-register.md) for known AI-agent failure modes
  and mitigation status;
- [Agent security mapping](agent-security-mapping.md) for OWASP, NIST, SLSA, and
  SSDF control mapping;
- [GitHub branch protection](github-branch-protection.md) for the repository
  ruleset checklist.

## Workflow

### 1. Classify

Classify the task as one or more of:

- public feature/contract;
- internal implementation;
- manifest/schema;
- connector/route/strategy;
- state/identity/evidence;
- CLI/Python UX;
- Airflow/dbt/GitOps integration;
- docs only;
- CI/release/security.

### 2. Read-only fan-out

For a non-trivial task, run independent analysis in parallel:

- `dpone_explorer`: actual code paths, tests, docs, and shared files;
- `dpone_architect`: boundaries, contracts, state, and alternatives;
- `dpone_test_certifier`: risk-based test and evidence matrix;
- `dpone_docs_ux_reviewer`: first-time-user journey and docs impact.

The parent/integrator reconciles disagreement. Do not let four agents each
implement their own architecture.

### 3. Specify and approve

For material changes use `docs/agent-templates/feature-design-spec.md` and the
`design-dpone-feature` skill. Implementation starts only at `APPROVED`.

### 4. Partition writes

Create one worktree per writer. Copy
`docs/agent-templates/agent-task-contract.yml` into the task artifact directory and
fill:

- `owned_paths`;
- `read_only_paths`;
- `forbidden_paths`;
- acceptance criteria;
- required checks;
- public-contract impact;
- shared-file owner.

Writers do not expand ownership themselves. They return a conflict/request to the
integrator.

Validate a concrete task contract before handing it to a writer:

```bash
uv run python tools/agent_policy/task_contract.py test_artifacts/agent-policy/my-task-contract.yml
```

### 5. Integrate and review

The integrator owns shared schemas, registries, dependency files, workflows,
MkDocs navigation, changelog, and common fixtures. After every feature
implementation, including one implemented by a single agent, the integrator
must obtain an independent subagent review before reporting completion or
merging the change.

Choose a reviewer that did not implement the feature and start it with a fresh
context. Provide the approved contract, base and candidate commits, applicable
repository rules, and evidence paths. The reviewer inspects the final diff and
actual execution paths for correctness, compatibility, silent data loss or
duplication, failure and recovery behavior, tests, docs, and evidence.

Record the reviewed commit, scope, findings with severity and file/line
references, validation limits, and verdict in the task artifacts. Resolve
blocking findings, validate fixes, and ask the independent reviewer to review
the changed scope. Include the final verdict, evidence link, and any remaining
findings in the completion report. Author self-review does not satisfy this
requirement. If a subagent is unavailable, record `UNVERIFIED` and leave the
feature's review requirement open.

### 6. Validate

Generate the recommended plan:

```bash
uv run python tools/agent_policy/select_checks.py --base-ref origin/master
```

Run focused checks first, then the required broad gate. Record every check as
`PASS`, `FAIL`, `SKIP`, `N/A`, or `UNVERIFIED`.

## Change-to-context map

| Change | Read first | Additional proof |
|---|---|---|
| CLI | `docs/cli-reference.md`, `docs/ux.md` | exit/output/file/negative matrix |
| Manifest/schema | `docs/manifests-variant-c.md`, `docs/schema-evolution.md` | old manifests, schema/examples, migration |
| Runtime/state | `docs/architecture.md`, `docs/state.md` | retry, replay, crash boundary, evidence order |
| Nested data | `docs/nested-normalization.md`, `docs/schema-identity.md` | deterministic lineage and no orphans |
| Connector/strategy | `docs/connector-sdk.md`, `docs/source-sink-matrix.md` | capability matrix and route evidence |
| Airflow | `docs/airflow-pack-provider.md` | import/parse/serialization/runtime compatibility |
| dbt | `docs/dbt.md` | project discovery, invocation, artifacts, compatibility |
| Docs | `docs/documentation-standard.md` | strict build, examples, rendered CJM |
| Release preparation/publication/retrospective verification | `docs/release.md`, `docs/agent-release-protocol.md` | operation-scoped R1-R9 report, separate source/controller identity and publication observation; GO does not grant upload authority |

## Shared-file policy

Default integrator-owned paths:

```text
pyproject.toml
uv.lock
CHANGELOG.md
mkdocs.yml
.github/workflows/**
src/dpone/schema/**
shared registries and factories
tests/conftest.py
release and certification indexes
```

A task may override this list explicitly, but two simultaneous writers must not
own the same semantic contract.

## Agent evaluation

Repository agent changes are product changes. Evaluate them using representative
tasks and track:

- first-pass CI success;
- accepted-without-rework rate;
- unrelated changed files;
- correct test selection;
- public-contract violations;
- escaped defects;
- merge-conflict rate;
- tokens and wall time per accepted change.

Use `evals/agent/result.schema.json` for structured run results. Do not optimize
only for generated code volume or a green CI result.
