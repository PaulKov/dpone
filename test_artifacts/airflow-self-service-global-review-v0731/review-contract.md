# Airflow Self-Service v0.73.1 Global Review Contract

Status: APPROVED

Approved by: explicit user request in the current Codex task to perform the
global review, fix confirmed defects, and carry the work through validation.
Approval date: 2026-07-18.

## Baseline

- Release baseline: `v0.73.1` (`07bf788f`).
- Review branch baseline: `origin/master` merge commit `ef88ae7f`, which
  preserves both `v0.73.1` and the merged Airflow pipeline-first UX P1.
- Integration branch: `codex/airflow-self-service-review-hardening-v0731`.
- Explicitly excluded parallel workspace changes:
  `.codex/config.toml` and
  `docs/feature-design-airflow-pipeline-first-ux-p2.md`.
- Review scope: the complete Industrial Self-Service Airflow implementation,
  including authoring, canonical normalization, release/deployment artifacts,
  provider parsing, runtime execution, evidence, CLI UX, documentation,
  packaging, and compatibility.

## Non-goals

- No new architecture or public DSL.
- No unrelated refactoring.
- No live certification without an explicitly available environment.
- No rewriting or dropping changes delivered by the parallel `v0.73.1` branch.

## Safety invariants

1. Authored safety policy must reach runtime execution and evidence.
2. Invalid or unsupported safety configuration must fail closed.
3. Airflow parse must perform no network, secret, metadata DB, or cache refresh
   calls.
4. One damaged DAG must not break valid DAGs under `skip_and_report`.
5. Artifact bytes used to build a DAG must match the verified digest and size.
6. Task dependency construction must never silently omit an edge.
7. CLI success and failure output must preserve actionable diagnostics.
8. Tests must be isolated from collection order and parallel sharding.

## Required end-to-end contract matrix

For every safety-bearing self-service field:

```text
authoring source
  -> schema validation
  -> canonical normalization
  -> generated release/deployment artifact
  -> provider/runtime consumer
  -> negative execution result
  -> evidence/error code
```

The review treats component-only unit tests as insufficient when the adapter
between adjacent stages is not covered.

## Validation

- Add a failing regression test before each behavioral fix.
- Run focused tests for every affected layer.
- Run the change-aware selector.
- Run Ruff, formatting, mypy, import/layer/module checks, full non-live pytest,
  docs checks, strict MkDocs build, provider packaging, and wheel smoke as
  selected by repository policy.
- Report unavailable live routes as `UNVERIFIED`, never `PASS`.
- Obtain a fresh-context reviewer result before merge; if agent capacity remains
  unavailable, the review is not represented as independently verified.
