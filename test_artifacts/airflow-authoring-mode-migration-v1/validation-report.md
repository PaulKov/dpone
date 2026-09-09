# Airflow authoring-mode migration v1 validation report

- Date: 2026-07-17
- Base commit: `2a3d6d03819a24a7df42738454b18a13be11ad21`
- Branch: `codex/airflow-self-service-roadmap`
- Specification: `docs/feature-design-airflow-authoring-mode-migration-v1.md`
- Task contract: `test_artifacts/agent-policy/airflow-authoring-mode-migration-v1.yml`

## Result

`PASS` for the local, credential-free implementation contract.

The additive `dpone migrate authoring` command covers all six directed
`classic`, `flow`, and `folder` migrations. Every candidate is compiled through
the canonical compiler and must retain the same semantic fingerprint before
apply. Source reads and writes are bounded and project-confined; apply checks
the original source digest, preserves file mode, atomically replaces the
primary source, and removes its newly created folder fragment when the primary
switch fails. Recipe materialization, semantic drift, symlink escape, source
races, and user-owned fragment conflicts fail closed.

The public JSON result is `dpone.authoring-migration.v1`. Text/JSON diffs pass
through the existing recursive secret redaction boundary. Existing authoring
sources, canonical runtime IR, releases, deployments, credentials, and Airflow
parse behavior remain unchanged until a user explicitly runs `--apply`.

## Verification

| Check | Status | Evidence |
| --- | --- | --- |
| Focused migration/schema/docs/fitness tests | PASS | 100% passed, including 16 migration tests and the architecture fitness gate |
| Full non-live suite | PASS | `5091 passed, 476 skipped in 294.54s` |
| Ruff lint and formatting | PASS | `All checks passed`; 3,280 files formatted |
| Mypy | PASS | 612 source files, no issues |
| Import rules | PASS | no architectural import violations |
| Layer metrics | PASS | cross-layer ratio `0.299`, max flow `99` within baseline tolerance |
| Module size | PASS | no hard-limit violations; pre-existing warnings did not grow into failures |
| Documentation | PASS | 534 Markdown files and 1,872 local links checked |
| Generated references | PASS | CLI, manifest schema, and GitOps schema references in sync |
| MkDocs strict build | PASS | documentation site built successfully |
| Agent policy/workflow security | PASS | setup, task template, branch protection, workflow security, red-team, SLSA and Scorecard checks |
| Agent governance receipt | PASS | `agent_governance_gate.json` in this evidence directory |
| Main/native/reader/provider builds | PASS | four sdist/wheel pairs built as version `0.72.3` |
| Twine metadata | PASS | all eight temporary build artifacts passed |
| Fresh-context reviewer | UNVERIFIED | reviewer spawn failed with `agent thread limit reached`; no review result is claimed |
| Live Airflow/Vault/Kubernetes/MSSQL/ClickHouse | N/A | this build-plane source migration performs no live I/O and makes no route-certification claim |

## Public contract and compatibility

- New additive CLI: `dpone migrate authoring`.
- New additive schema: `dpone.authoring-migration.v1`.
- Stable error pages cover path, source, recipe, mode, conflict, target,
  semantic drift, source race, apply, and post-verification failures.
- Existing `dpone manifest migrate` and `dpone fix --plan|--apply` retain their
  separate legacy-manifest and connection-field responsibilities.
- Recipe-authored sources intentionally remain unsupported by this migration;
  this prevents loss of immutable recipe provenance.

## Documentation and CJM

The authoring guide, compatibility policy, generated CLI/schema references,
backlog, changelog, navigation, dedicated migration runbook, and ten error pages
are synchronized. A beginner can review and apply one explicit migration, then
return to the ordinary `dpone check` and `dpone airflow preview` journey.

## Residual risk

The write protocol provides atomicity at the primary-source boundary and exact
rollback for a fragment created by the same attempt. It cannot provide a
cross-file transaction on arbitrary filesystems; a post-replace storage failure
therefore blocks with recovery guidance to restore the source from version
control. External usability evidence and fresh-context review remain separate
from this local implementation PASS.
