# Testing

`dpone` separates fast regression tests from local service integration tests and vendor live certification.

## Default regression gate

Run this gate before publishing or merging runtime changes:

```bash
uv sync --locked --all-extras
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run pytest -m "not integration_live"
uv build
```

Locked sync is itself a gate. If project metadata and `uv.lock` differ, the
command exits nonzero without rewriting the lock. Repair the lock as a reviewed
change; `--frozen` is not an equivalent freshness check.

## Targeted unit and contract tests

Repository dependency assertions in `tests/test_architecture_fitness_gate.py`
share one complete, immutable import-graph snapshot per test-module invocation.
The snapshot is rebuilt for each pytest run; it is not a persistent cache and
does not select or omit dependency edges. Keep repository sources unchanged
during the run. Tests that construct temporary repositories or exercise different
fitness thresholds retain independent analyses. All fanout, layer and clustering
limits remain mandatory.

Use targeted tests while developing a specific subsystem:

```bash
uv run pytest tests/test_runtime_schema_evolution_contracts.py -q
uv run pytest tests/test_runtime_kafka_contracts.py -q
uv run pytest tests/test_docs_language_contracts.py -q
```

## Manual integration matrix

The matrix has two credential-free layers:

- `mock_contract` validates 200 source -> sink -> strategy contracts: 100 common base cases plus 25 `snapshot_diff` cases plus 60 DB-target `partition_replace`/`scd2`/`backfill` cases plus Postgres `xmin` and Postgres/MSSQL `cdc` source-specific cases. It writes deterministic metadata/behavior artifacts using 10,000 base rows by default, 20% changed rows, 5% physical deletes, and 120 sparse wide source columns per sampled row.
- `mock_local` runs local/mock-capable cases and skips documented-only BigQuery targets until a managed `vendor_live` run is used.

The full source/sink matrix is intentionally manual because it can start local databases, Kafka, Schema Registry, and wide-table fixtures.

```bash
uv run pytest tests/integration/matrix -m integration_matrix_mock
```

See [Manual integration matrix](manual-integration-matrix.md) and [Testing runbooks](index.md).

## Live vendor tests

Vendor tests are opt-in and should not block normal pull requests unless the connector is being certified.

```bash
DPONE_RUN_INTEGRATION=1 \
DPONE_RUN_INTEGRATION_LIVE=1 \
uv run pytest tests/integration -m integration_live
```

Use live tests for connector certification, release candidates, and scheduled confidence checks.

## Generated documentation checks

Compatibility and metrics documentation may contain generated sections. Regenerate them with the dedicated tool before committing changes that alter supported APIs or compatibility contracts.

```bash
uv run dpone docs check-compatibility
uv run dpone docs update-dev-metrics --check
```

## Changed-workflow validation

Workflow changes run focused YAML/governance tests and checksum-pinned
actionlint `1.7.12`. Non-queue files must produce exit `0`, stdout exactly
`[]\n`, and empty stderr. The nightly compatibility wrapper is first run
unwaived and must produce only the one known `concurrency.queue` syntax object
with exit `1`; its exact file/message-scoped waived invocation must then produce
exit `0`, exact `[]\n` stdout, and empty stderr.

An unwaived nightly exit `0` is not ordinary success while the compatibility
branch exists: it raises `ACTIONLINT_QUEUE_WAIVER_OBSOLETE` so the waiver is
removed in the same update. Mocked workflow tests certify repository contracts
only; they never establish hosted queue, Pages deployment, or Dependency Review
PASS. See [Testing documentation](index.md#changed-workflow-gate) for commands
and stream contracts.

## Semantic workflow privilege tests

Workflow-security changes require the PR3B semantic unit, mutation,
compatibility, and current-repository checks in addition to actionlint. First
prepare the locked environment with `uv sync --locked --all-extras` as shown in
the default gate. That setup may access the network, write the environment or uv
cache, and use stderr; it is outside the scanner process contract. Then run:

```bash
uv run pytest \
  tests/agent_policy/test_workflow_privilege_*.py \
  tests/agent_policy/test_workflow_security_fail_closed.py \
  tests/agent_policy/test_workflow_security_job_scope.py \
  tests/agent_policy/test_workflow_security_privileged_cli.py \
  tests/agent_policy/test_workflow_security_pr3b_compatibility.py -q
uv run --locked --no-sync --offline --no-python-downloads python -B \
  tools/agent_policy/workflow_security_privileged.py \
  --root . \
  --format text
uv run --locked --no-sync --offline --no-python-downloads python -B \
  tools/agent_policy/workflow_security.py . --format json
```

The focused suite covers stable snapshot acquisition, strict YAML, graph and
event variants, three-valued expressions, effective permissions, exact
profiles, canonical report validation, CLI streams/exits, resource bounds,
mutations, fail-closed internal boundaries, job-scoped authority, and atomic
umbrella projection. `PASS` from these credential-free checks is local evidence
only. Hosted CodeQL, governance artifact/attestation, required-context, App,
and receipt state remain `UNVERIFIED` until checked on the exact reviewed head.
See the
[testing reference](index.md#semantic-pr-privilege-boundary-gate) and
[recovery runbook](../cicd/runbooks.md#semantic-pr-privilege-boundary).

The no-network/no-file-mutation guarantee is process-scoped. Use the testing
reference's direct `.venv/bin/python -B` invocation, not a uv wrapper, for exact
scanner exit and stdout/stderr evidence.

## Snapshot artifacts

Long-running benchmarks and certification runs should write artifacts under `test_artifacts/` with a unique date, runner, environment, command, and result summary.

The PR3B certification exception is intentionally deterministic rather than
timestamped:
`test_artifacts/agent-policy/pr3b-semantic-privilege-certification.json` is
exact canonical JSON stdout from the standalone scanner. It has no wrapper
fields or independent decision authority and must never be hand-edited. Capture
or compare it only with the direct prepared-interpreter command documented in
the testing reference.

For normal local runs, runtime reports are written under `.dpone/runs/` unless the manifest overrides the artifact path.
