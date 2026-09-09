# Agent task contracts

Purpose: give every non-trivial agent writer a clear, machine-checkable work
boundary before it edits files.

Audience: integrators, agent writers, reviewers, and maintainers who coordinate
parallel implementation branches.

## What the contract prevents

A task contract is the small hand-off document between the integrator and a
writer. It says:

- what problem the writer is solving;
- which paths it may edit;
- which paths are read-only context;
- which paths are forbidden because they are shared, release-sensitive, or owned
  by the integrator;
- which focused, broad, and live checks prove the work is done;
- which stop conditions require the writer to return control instead of
  improvising.

This protects against the most common multi-agent failure: one writer silently
expands scope, edits a shared file, gets a green local test, and leaves the
integrator with a conflict or a weakened control.

## When to use one

Use a task contract for:

- parallel worktrees;
- feature work with a design specification;
- any agent-control-plane change;
- changes that touch schemas, generated docs, workflows, dependency locks, or
  other shared contracts.

Tiny single-owner edits can skip a separate copied contract, but they still need
the same reasoning in the PR description and completion report.

## How to create a contract

Copy the template:

```bash
cp docs/agent-templates/agent-task-contract.yml test_artifacts/agent-policy/my-task-contract.yml
```

Fill the concrete values. At minimum, replace the template placeholders and
declare:

- `owned_paths`;
- optional `integrator_owned_paths` for shared files reconciled only by the
  declared integrator/shared-file owner;
- `read_only_paths`;
- `forbidden_paths`;
- `acceptance_criteria`;
- `required_checks.focused`;
- `required_checks.broad`;
- `public_contract_impact`;
- `shared_file_owner`.

Validate the concrete contract before giving it to a writer:

```bash
uv run python tools/agent_policy/task_contract.py test_artifacts/agent-policy/my-task-contract.yml
```

The repository template itself is validated in template mode:

```bash
uv run python tools/agent_policy/task_contract.py \
  docs/agent-templates/agent-task-contract.yml \
  --template
```

## Required protected paths

Every contract must keep these paths forbidden unless the integrator explicitly
owns the task:

```text
pyproject.toml
uv.lock
CHANGELOG.md
mkdocs.yml
.github/workflows
```

These files often affect the whole repository. A writer may request a change, but
the integrator decides whether to edit them and which broad checks are required.
When the same task needs a root integration step, keep the shared path in
`forbidden_paths` and also list its exact path under `integrator_owned_paths`.
That is a narrow exception for the actor named by both `integrator` and
`shared_file_owner`; it never grants a delegated writer access to the path.

## Review checklist

Before merging agent-generated work, verify:

1. The writer only changed `owned_paths`.
2. The integrator alone changed `integrator_owned_paths`, and those paths remain
   covered by `forbidden_paths` for delegated writers.
3. No `owned_paths` or `integrator_owned_paths` item overlaps `read_only_paths`,
   and the two writable sets are disjoint.
4. `required_checks.focused` and `required_checks.broad` ran on the reviewed
   head commit.
5. Live checks are `PASS` only when the approved environment actually ran.
6. The completion report uses only `PASS`, `FAIL`, `SKIP`, `N/A`, or
   `UNVERIFIED`.
7. Shared files were integrated by the declared `shared_file_owner`.

## CI coverage

The agent policy gate validates:

- the contract template shape and protected-path defaults;
- the closed JSON schema at `evals/agent/agent-task-contract.schema.json`;
- the Python validator through `tests/agent_policy/test_task_contract.py`;
- the governance receipt entry `agent_task_contract_template`.

Related pages:

- [Agent-assisted development](agent-development.md)
- [Agent governance](agent-governance.md)
- [Agent permissions and tool registry](agent-permissions.md)
- [Agent risk register](agent-risk-register.md)
