# Airflow self-service UX closure v0.73.17 validation

- Base commit: `a229c5a3c2f7d5d6399accf94ade85fd551e4bc8`
- Validated implementation commit: `004acd86f4cb6d3884de1e3249e22b2cebaa7309`
- Validation date: 2026-07-23
- Environment: macOS arm64, Python 3.12
- Scope: Airflow self-service identity/default/reference closure and
  cross-artifact quality row-authority hardening

## Result

`PASS` for the local non-live contract. Airflow/Kubernetes/Vault and live
MSSQL/ClickHouse route certification remain `UNVERIFIED` because no approved
live environment was supplied.

## Functional and regression checks

| Check | Result | Evidence |
|---|---|---|
| Installed-entrypoint five-command First DAG in a clean temporary project | PASS | Final wheels installed in `/tmp/dpone-first-dag-07317-final`; all five commands exited `0`; hermetic report ended with `journey: offline golden path complete` |
| Focused self-service, identity, project policy, reference parity, hermetic test, quality-authority, schema, and docs tests | PASS | Focused pytest runs completed without failures |
| Independent source-pin and certification review suite | PASS | `168 passed`; includes `pipeline_id != process`, route-receipt bypass, selectors, certification v1, and generated-schema contracts |
| Full non-live suite | PASS | `7207 passed, 557 skipped, 1 warning` in `117.88s` |
| Live integration profiles | UNVERIFIED | Excluded by `-m "not integration_live"`; no approved environment |

The full-suite skips are retained as skips; they are not represented as live
certification passes. The remaining warning is Python's macOS
`multiprocessing` fork deprecation emitted by an existing FIFO security test.

## Static and architecture gates

| Command | Result |
|---|---|
| `uv run ruff check .` | PASS |
| `uv run ruff format --check .` | PASS, 3602 files formatted |
| `uv run mypy --config-file mypy.ini` | PASS, 654 source files |
| `uv run dpone docs check-import-rules` | PASS |
| `uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json` | PASS, cross-layer ratio `0.300` |
| `uv run dpone docs check-module-size --baseline docs/module_size_baseline.json` | PASS, no hard-limit violations |
| `uv run dpone docs check-architecture-fitness` | PASS, average clustering `0.178` |

## Public contracts and documentation

| Command | Result |
|---|---|
| `uv run dpone docs check-airflow-public-contracts` | PASS, CLI `15/15`, Python `11/11`, schemas `54/54` |
| `uv run dpone docs check-generated-references` | PASS, `3/3` synchronized |
| `uv run dpone docs check-compatibility` | PASS, 19 registry entries |
| `uv run dpone docs check-docs` | PASS, 589 Markdown files and 2143 links |
| `uv run pytest tests/test_docs_language_contracts.py -q` | PASS, 25 tests |
| `uv run mkdocs build --strict` | PASS, 8.83 seconds |

## Packaging

The following version `0.73.17` distributions built successfully:

- `dpone`
- `dpone-native-accel`
- `dpone-airflow-pack`
- `apache-airflow-providers-dpone`

`uv tool run twine check /tmp/dpone-self-service-07317-final-dist/*` passed
for every generated wheel and source distribution.

## Issues found during integration

1. Optional GCP, PyArrow, and native-acceleration dependencies were absent from
   the initial local environment. `uv sync --extra full --extra accel` restored
   the repository-declared environment; the previously failing focused set then
   passed.
2. The sterile Airflow parse worker test used a local `10s` process timeout
   instead of the production benchmark's canonical `30s` timeout. Under full
   `xdist` load it could report a false import-order failure. The test now uses
   `BenchmarkConfig().worker_timeout_seconds`; cold/warm parse SLOs are
   unchanged.
3. The MkDocs warning contract used a `90s` subprocess timeout although it
   checks output content, not build performance. Under full `xdist` load the
   otherwise successful strict build could time out. Its isolation timeout is
   now `300s`; the independent strict-build gate remains required.
4. Fresh-context review found that malformed boolean row counts, cyclic quality
   wrappers, and estimate-only artifacts could become false authority. Exact
   non-negative integer validation and cycle-safe fail-closed traversal now
   protect reconciliation.
5. Bare-ID hermetic test discovery could select a test whose metadata referred
   to another pipeline. The shared canonical resolver now preserves the
   requested ID and rejects mismatches before execution.
6. Safe-sample source identity initially stopped at the CLI response, while
   materialization could happen before policy failure and the source could
   change before handoff. Source path/digest now cross plan, handoff and runtime
   evidence boundaries; integrity is rechecked before durable handoff and live
   I/O; failed policy/target validation writes no release, cache or handoff.
7. Physical temporary-target normalization could collapse distinct logical IDs
   such as `orders-daily` and `orders_daily`. Logical identity is preserved,
   while the physical projection includes a stable collision-resistant suffix.
8. Rebase onto the current master retained compact-pack v2 runtime connection
   context promotion and release-materialize CAS stability. Their focused
   regression tests and the full non-live suite pass on the integrated tree.
9. Direct runtime execution could accept a legacy plan without
   `source_snapshot`, and a route-attestation receipt could bypass the
   snapshot-to-pack check. Runtime now rejects a missing or foreign snapshot
   before fetch, target lifecycle, or evidence; route attestation and source
   pin are independent mandatory proofs.
10. Source-pin lookup used `process.name`, while release materialization indexes
    workload packs by canonical pipeline ID. Local and live paths now use
    `target.pipeline_id`; regressions cover a pipeline whose process name is
    different.
11. Live policy re-authorization rebuilt the execution plan without preserving
    `source_snapshot`. The immutable snapshot now survives authorization and is
    rechecked before runtime side effects.
12. Recovery fixes for selected safe samples dropped selector scope,
    environment, run identity, and custom bounds. Copyable fixes now preserve
    safe intent while continuing to omit rejected ordinary-run values.
13. A final full-suite run found only a module warning-threshold regression
    caused by one documentation line. The line budget was restored to 449 LOC,
    the module-size gate passed, and the complete suite was rerun successfully.

## Review evidence

- Fresh architecture review: `GO` after independent reproduction and closure of
  route-receipt bypass and `pipeline_id != process` findings.
- Fresh docs/CLI review: no behavioral CJM blocker; both pipeline-ID error pages
  are tracked in the implementation commit.
- Certification v1 schema and model are byte-for-byte unchanged from the base
  commit; the evaluator rejects zero-command spoofing and requires the frozen
  five-command v1 protocol.
- Final build artifacts:
  `/tmp/dpone-self-service-07317-final-dist/`.
- Installed-wheel journey workspace:
  `/tmp/dpone-first-dag-07317-final/project/`.

## Compatibility and CJM

- Existing explicit `dpone init pipeline ... --airflow` remains accepted.
- `--no-airflow` is an additive explicit override.
- Missing `dpone.yaml` preserves the legacy non-Airflow default.
- Ordinary `dpone run <manifest>` path behavior is unchanged.
- The credential-free First DAG journey is exactly five successful commands.
- Safe live sample remains a separate platform-gated operation.
