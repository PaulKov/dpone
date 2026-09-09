# Route readiness

Route readiness is the self-service gate for one `source -> sink -> strategy`
path. It answers a narrow operational question: do we have enough current
evidence to promote this route, or should the release stay blocked until a
specific artifact is regenerated?

The command is matrix-driven. It uses the same source/sink inventory as the
manual integration matrix and the same strategy certification metadata as the
strategy intelligence layer. The first critical routes are `postgres -> mssql`
and `mssql -> clickhouse`, but the contract is generic for every supported
`source -> sink -> strategy` combination.

## User workflow

1. Pick the route and strategy from the [source -> sink matrix](source-sink-matrix.md).
2. Run the focused certification, benchmark, type, documentation, and runbook
   checks that produce route evidence artifacts.
3. Prefer `dpone ops route-certification-pack` when you want a normalized
   evidence directory and embedded readiness report.
4. Pass those artifacts to `dpone ops route-readiness` directly only when
   automation already produced readiness-compatible evidence files.
5. Review `route_readiness.json` in automation and `route_readiness.md` in
   release review.
6. Feed green route readiness, route certification, execution ledger, state
   promotion, and optional CDC artifacts into
   [Route release gate](route-release-gate.md) for the final go/no-go receipt.
7. Follow the `next_actions` list when the route is blocked or degraded.

The route readiness command does not execute heavy certification tests. It is a
control-plane aggregator over artifacts that already exist.

## CLI examples

Postgres to MSSQL incremental merge:

```bash
uv run dpone ops route-readiness \
  --source postgres \
  --sink mssql \
  --strategy incremental_merge \
  --output-dir test_artifacts/route_readiness/postgres_to_mssql \
  --artifact matrix_case=test_artifacts/integration_matrix/latest/postgres_to_mssql__incremental_merge.json \
  --artifact manifest_example=examples/postgres_to_mssql.yaml \
  --artifact strategy_plan=test_artifacts/strategy/latest/postgres_to_mssql_plan.json \
  --artifact quality_reconciliation=test_artifacts/reconciliation/latest/postgres_to_mssql.json \
  --artifact run_artifact=test_artifacts/runs/latest/postgres_to_mssql.json \
  --artifact route_execution_ledger=test_artifacts/route_execution/postgres_to_mssql/orders/route_execution_ledger.json \
  --artifact state_promotion=test_artifacts/route_state/postgres_to_mssql/orders/state_promotion.json \
  --artifact route_schema_evolution=test_artifacts/route_schema/postgres_to_mssql/orders/route_schema_evolution.json \
  --artifact route_reconciliation_repair=test_artifacts/route_repair/postgres_to_mssql/orders/route_reconciliation_repair.json \
  --artifact docs_runbook=docs/source-sink/postgres-to-mssql.md \
  --artifact lossless_transport_contract=test_artifacts/contracts/latest/postgres_to_mssql_transport.json \
  --artifact benchmark_slo=test_artifacts/benchmarks/latest/postgres_to_mssql_slo.json \
  --artifact resume_checkpoint=test_artifacts/resume/latest/postgres_to_mssql_checkpoint.json \
  --artifact type_matrix=test_artifacts/type_matrix/latest/postgres_to_mssql.json \
  --format md
```

MSSQL to ClickHouse incremental merge:

```bash
uv run dpone ops route-readiness \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --output-dir test_artifacts/route_readiness/mssql_to_clickhouse \
  --artifact matrix_case=test_artifacts/integration_matrix/latest/mssql_to_clickhouse__incremental_merge.json \
  --artifact manifest_example=examples/mssql_to_clickhouse.yaml \
  --artifact strategy_plan=test_artifacts/strategy/latest/mssql_to_clickhouse_plan.json \
  --artifact quality_reconciliation=test_artifacts/reconciliation/latest/mssql_to_clickhouse.json \
  --artifact run_artifact=test_artifacts/runs/latest/mssql_to_clickhouse.json \
  --artifact route_execution_ledger=test_artifacts/route_execution/mssql_to_clickhouse/orders/route_execution_ledger.json \
  --artifact state_promotion=test_artifacts/route_state/mssql_to_clickhouse/orders/state_promotion.json \
  --artifact route_schema_evolution=test_artifacts/route_schema/mssql_to_clickhouse/orders/route_schema_evolution.json \
  --artifact route_reconciliation_repair=test_artifacts/route_repair/mssql_to_clickhouse/orders/route_reconciliation_repair.json \
  --artifact docs_runbook=docs/source-sink/mssql-to-clickhouse.md \
  --artifact type_fidelity=test_artifacts/type_fidelity/latest/mssql_to_clickhouse.json \
  --artifact typed_hash=test_artifacts/typed_hash/latest/mssql_to_clickhouse.json \
  --artifact wide_type_certification=test_artifacts/type_matrix/latest/mssql_to_clickhouse_wide.json \
  --artifact benchmark_slo=test_artifacts/benchmarks/latest/mssql_to_clickhouse_slo.json \
  --artifact schema_evolution=test_artifacts/schema_evolution/latest/mssql_to_clickhouse.json \
  --format json
```

Exit code `0` means the route passed the configured policy. Exit code `1`
means the route is `blocked`, `degraded`, or `unknown`; keep the generated
artifacts and follow the report runbook.

## Evidence taxonomy

Every evidence artifact is normalized into a `RouteEvidenceItem` with:

| Field | Meaning |
| --- | --- |
| `name` | Stable evidence domain, such as `matrix_case` or `benchmark_slo`. |
| `kind` | Normalized domain: `certification`, `performance`, `schema`, `docs`, `runtime`, or `evidence`. |
| `path` | Source artifact path used for audit and review. |
| `required` | Whether the active `RouteProfile` requires the artifact. |
| `passed` | Boolean pass/fail result derived from the artifact. |
| `sha256` | File checksum for tamper-evident review. |
| `summary` | Short human-readable status line. |
| `blockers` | Machine-readable reasons that block readiness. |

The generic required domains are:

| Evidence | Why it matters |
| --- | --- |
| `matrix_case` | Proves the route exists in the supported integration matrix. |
| `manifest_example` | Gives users a copy/paste-ready route shape. |
| `strategy_plan` | Shows the selected load strategy and native/fallback path. |
| `quality_reconciliation` | Captures count/checksum/duplicate checks. |
| `run_artifact` | Captures the factual run result for the route. |
| `route_execution_ledger` | Proves idempotent execution, lease fencing, and state commit ordering for the route. |
| `state_promotion` | Proves source state advanced only after a matching commit receipt and durable ledger evidence. |
| `docs_runbook` | Keeps operator docs present and reviewable. |

Route-specific critical domains:

| Route | Critical evidence |
| --- | --- |
| `postgres -> mssql` | `route_schema_evolution`, `route_reconciliation_repair`, `lossless_transport_contract`, `benchmark_slo`, `resume_checkpoint`, `type_matrix`. |
| `mssql -> clickhouse` | `route_schema_evolution`, `route_reconciliation_repair`, `type_fidelity`, `typed_hash`, `wide_type_certification`, `benchmark_slo`, `schema_evolution`. |

## Report contract

The JSON report uses schema version `dpone.route_readiness.v1` and contains:

- `route`: canonical `source`, `sink`, `strategy`, `pair_id`, `case_id`, and
  `colon_id`.
- `profile`: route metadata from the integration matrix and strategy
  certification matrix.
- `passed`, `level`, `score`: policy decision for CI and release gates.
- `blockers`, `warnings`, `next_actions`: operator-facing diagnostics.
- `evidence`: normalized artifact records with checksums.
- `json_path`, `markdown_path`: generated report locations.

The Markdown report is intentionally concise so it can be attached to a release
review, incident note, or GitHub Actions summary.

## Runbook

When readiness is red:

1. Open `route_readiness.json` and copy the first blocker.
2. If the blocker ends with `.missing`, generate or attach the named evidence
   artifact and rerun `dpone ops route-readiness`.
3. If the blocker ends with `.not_passed`, open the referenced artifact and use
   that artifact's runbook first.
4. If the route is `unknown`, add the route to the integration matrix and source
   -> sink docs before requesting readiness.
5. If the route is `degraded`, keep the artifact bundle, decide whether the
   warning is acceptable for the release candidate, and document the exception.

For CI/CD, keep route readiness as a post-artifact gate. The default PR gate
should run the unit/docs contract tests; scheduled or manual certification
workflows should generate the heavy route evidence first and then call the
readiness command.

## Related docs

- [Developer guide: route readiness](developer-route-readiness.md)
- [Route execution ledger](route-execution-ledger.md)
- [Route schema evolution](route-schema-evolution.md)
- [Route reconciliation repair](route-reconciliation-repair.md)
- [Route state promotion](route-state-promotion.md)
- [Route certification pack](route-certification-pack.md)
- [Route release gate](route-release-gate.md)
- [Source -> sink matrix](source-sink-matrix.md)
- [Postgres -> MSSQL](source-sink/postgres-to-mssql.md)
- [MSSQL -> ClickHouse](source-sink/mssql-to-clickhouse.md)
- [`dpone ops` operational controls](ops-cli.md)
- [Developer CI/CD guide](developer-ci-cd.md)
