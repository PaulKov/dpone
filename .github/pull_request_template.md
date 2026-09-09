## Summary

- Problem:
- Solution:
- User/developer impact:

## Design and scope

- Approved specification or issue: <GitHub issue/PR, approved spec, ADR, docs path, or N/A: reason>
- In scope:
- Non-goals:
- Owned/shared paths and integrator (when parallel):

## Public contracts and compatibility

- [ ] No public-contract change
- [ ] Backward-compatible change
- [ ] Deprecation/migration documented
- [ ] Breaking change explicitly approved with ADR/migration

Affected surfaces: CLI / Python API / manifest-schema / imports / connector
capabilities / state-checkpoint / identity / artifacts-evidence / Airflow-dbt.

## Algorithm and failure behavior

Describe the actual data/control flow, identity, ordering, transaction/evidence
boundary, retry/replay, rollback, and important edge cases. Use `N/A` only with a
reason.

## Documentation and user journey

- [ ] Tutorial/how-to/reference/runbook updated as applicable
- [ ] First-time-user CJM checked
- [ ] CLI/Python/YAML examples validated
- [ ] Architecture, schemas, diagrams, and cross-links updated
- [ ] Compatibility and migration docs updated

## Market research and differentiation

- Relevant comparators and official source dates/versions:
- Adopted/rejected patterns:
- Measurable advantage (scenario, metric, target, procedure, artifact), or N/A:

## Validation evidence

Use `PASS`, `FAIL`, `SKIP`, `N/A`, or `UNVERIFIED`. Skipped, stale, or mocked
live evidence is not `PASS`. For `SKIP`, `N/A`, or `UNVERIFIED`, put the reason
in `Artifact/notes`.

| Check | Status | Command/workflow | Artifact/notes |
|---|---|---|---|
| Focused tests | | | |
| `uv run ruff check .` | | | |
| `uv run ruff format --check .` | | | |
| `uv run mypy --config-file mypy.ini` | | | |
| Import/layer/module gates | | | |
| `uv run pytest -m "not integration_live"` | | | |
| Docs strict build | | | |
| Package build/smoke | | | |
| Required live certification | | | |

## Owner attestation

Use this section when no independent reviewer exists.

- [ ] Owner reviewed the final diff and accepts the change.
- [ ] Required GitHub checks are green on the reviewed head commit.
- [ ] Admin bypass was not used, or the linked break-glass issue documents why.
- [ ] Agent governance receipt is attached when agent controls changed.

Agent governance receipt: attach or link `agent_governance_gate.json` /
`agent-governance-gate` when `.agents/**`, `.codex/**`,
`tools/agent_policy/**`, `.github/workflows/**`, `AGENTS.md`, or agent
governance docs changed. `Agent PR receipt` verifies these checked attestations,
the approved specification or issue field, validation evidence statuses, the
live required GitHub checks, and the `agent-governance-gate` artifact for
agent-control PRs. It also verifies the GitHub Artifact Attestation for the
extracted `agent_governance_gate.json` subject. The receipt workflow is
triggered by PR-body edits; after CI turns green, edit this section and let the
lightweight receipt check run on the reviewed head commit.
After merge, the same workflow automatically derives an immutable
`agent_pr_merge_receipt.json` over the exact integration commit. There is no
manual backfill from the current PR body; a missing source artifact remains
release-blocking `UNVERIFIED`.

## Risks, operations, and rollback

- Silent data loss/duplication risk:
- Security/secret risk:
- Observability/runbook impact:
- Rollback plan:
- Remaining uncertainty or follow-up:

## Reviewer focus

Please prioritize correctness, compatibility, data/state/evidence ordering,
negative and recovery cases, documentation accuracy, and absence of false
certification or credentials.
