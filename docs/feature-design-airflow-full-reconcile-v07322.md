# Feature design: full-catalog Airflow reconcile

- Status: APPROVED
- Owner: dpone maintainers
- Issue: Canonical Airflow Pack Recovery And Deployment
- Target release: v0.73.22
- Approval authority: user-approved canonical recovery plan
- Last reviewed: 2026-07-28

## Problem and outcome

`dpone gitops airflow reconcile` is impact-oriented: without changed-file
inputs it selects no workloads. A deployment fixture incorrectly treated an
empty impact selection as a full catalog build and could therefore omit
unchanged compact packs.

The command needs an explicit, auditable full-catalog mode. The resulting
artifact set must be isolated by `--output-dir`; both workload packs and DAG
specs must be written below that root.

## Public contract

```bash
dpone gitops airflow reconcile \
  --workload-set dpone_workloads/gitops.yaml \
  --env dev \
  --all-workloads \
  --output-dir .dpone/parse-fixture/gitops
```

- `--all-workloads` selects every resolved workload in deterministic catalog
  order.
- It conflicts with `--changed-files` and `--changed-files-file`.
- Existing impact-based behavior remains unchanged when the flag is absent.
- Evidence adds `selection_mode: all|affected`.
- `packs` and `dag_specs` contain actual repo-relative paths below
  `--output-dir`.

## Algorithm and failure semantics

1. Resolve and validate the workload catalog.
2. Validate selection inputs before writing artifacts.
3. For `all`, select every catalog workload; for `affected`, use the existing
   changed-file resolver.
4. Build and validate every selected compact pack in memory.
5. Build and validate all declared DAG specs in memory.
6. If any blocker exists, emit evidence without writing or pruning artifacts.
7. Persist packs under
   `<output-dir>/airflow/<workload>/airflow-pack.json`.
8. Persist DAG specs under `<output-dir>/airflow/_dags` through the confined
   artifact writer, then prune stale DAG specs only in that directory.
9. Emit one reconcile report. Any catalog, pack, DAG-spec, or selection
   blocker returns non-zero and must not be treated as deployment evidence.

The caller owns temporary-root cleanup. Output roots are repository-relative;
absolute paths, traversal and symlink escapes are rejected. The service never
deletes outside the confined DAG-spec directory. Callers must use an isolated
output root per CI job; concurrent writers to one root are not supported.
The optional evidence mirror is validated as a file before persistence and
must not overlap any planned pack or DAG-spec path.

## Architecture

- CLI parser owns syntax only.
- `GitOpsAirflowCompactPackService` owns selection policy and orchestration.
- `AirflowDagSpecBuilder` owns pure DAG-spec construction.
- `AirflowDagSpecArtifactWriter` owns confined persistence and stale-spec
  pruning and is injected by the CLI composition root. The historical
  `build_and_write` method remains a compatibility facade only; new
  orchestration does not use it.
- `dpone.gitops.airflow_reconcile_policy` owns repo-confined input/output validation and
  bounded build-failure evidence.
- No Airflow, object-storage, connector, or vendor SDK dependency is added.

## Compatibility

- Existing callers remain impact-based.
- Existing default output remains `.dpone/gitops`.
- `GitOpsAirflowReconcileReport` keeps schema version `1`; the additive
  optional `selection_mode` field is backward-compatible for JSON consumers.
  New producers always emit it; historical payloads without the field mean
  `affected`. Its Python constructor also defaults to `affected`.

## Test and documentation plan

- Unit/CLI: all workloads, deterministic order, selection conflict, custom
  output root, unchanged impact behavior.
- Regression: deployment materialization receives every referenced compact
  pack.
- CLI reference and Airflow GitOps documentation explain the two selection
  modes.
- Standard Ruff, mypy, import/layer/module-size, pytest, docs, packaging and
  release gates apply.

## Market relevance

This is a correctness patch inside the already approved exact Airflow artifact
activation design. Apache Airflow and Astronomer Cosmos motivate immutable,
locally parsed compiled artifacts; they do not define dpone workload selection.
dlt, Airbyte, Fivetran, Informatica, Pentaho, SSIS, gusty and Apache Beam are
`N/A` for this CLI selection contract.
