# Route certification pack

Route certification pack generates a readiness-compatible evidence bundle for
one `source -> sink -> strategy` route. It is the operator step before
[`dpone ops route-readiness`](route-readiness.md): the pack writes normalized
evidence files, then immediately runs route readiness over those generated
files.

The command is intentionally side-effect light. It does not start databases,
run Docker, execute benchmarks, or mutate runtime state. Heavy checks still
belong to the specialized certification, benchmark, type, reconciliation, and
runtime commands that produce source artifacts.

## User workflow

1. Run the specialized route checks that are relevant for the release candidate.
2. Pass their artifact paths to `dpone ops route-certification-pack`.
3. Let the pack generate `evidence/*.json` files for every required evidence
   domain in the active `RouteProfile`.
4. Review `route_certification_pack.json` for pack metadata and
   `readiness/route_readiness.json` for the final go/no-go decision.
5. Upload the whole output directory to CI, release review, or incident review.

Safe auto-probes cover:

| Evidence | Behavior |
| --- | --- |
| `matrix_case` | Passes when the route exists in the integration matrix. |
| `docs_runbook` | Passes when the source -> sink guide exists. |
| `manifest_example` | Passes when the source -> sink guide contains a copy/paste YAML manifest. |

All heavy route evidence must be provided through `--artifact name=/path`.

The first industrial routes covered by examples are `postgres -> mssql` and
`mssql -> clickhouse`.

## CLI examples

Postgres -> MSSQL incremental merge:

```bash
uv run dpone ops route-certification-pack \
  --source postgres \
  --sink mssql \
  --strategy incremental_merge \
  --output-dir test_artifacts/route_certification/postgres_to_mssql \
  --artifact strategy_plan=test_artifacts/strategy/latest/postgres_to_mssql_plan.json \
  --artifact quality_reconciliation=test_artifacts/reconciliation/latest/postgres_to_mssql.json \
  --artifact run_artifact=test_artifacts/runs/latest/postgres_to_mssql.json \
  --artifact lossless_transport_contract=test_artifacts/contracts/latest/postgres_to_mssql_transport.json \
  --artifact benchmark_slo=test_artifacts/benchmarks/latest/postgres_to_mssql_slo.json \
  --artifact resume_checkpoint=test_artifacts/resume/latest/postgres_to_mssql_checkpoint.json \
  --artifact type_matrix=test_artifacts/type_matrix/latest/postgres_to_mssql.json \
  --format md
```

MSSQL -> ClickHouse incremental merge:

```bash
uv run dpone ops route-certification-pack \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --output-dir test_artifacts/route_certification/mssql_to_clickhouse \
  --artifact strategy_plan=test_artifacts/strategy/latest/mssql_to_clickhouse_plan.json \
  --artifact quality_reconciliation=test_artifacts/reconciliation/latest/mssql_to_clickhouse.json \
  --artifact run_artifact=test_artifacts/runs/latest/mssql_to_clickhouse.json \
  --artifact type_fidelity=test_artifacts/type_fidelity/latest/mssql_to_clickhouse.json \
  --artifact typed_hash=test_artifacts/typed_hash/latest/mssql_to_clickhouse.json \
  --artifact wide_type_certification=test_artifacts/type_matrix/latest/mssql_to_clickhouse_wide.json \
  --artifact benchmark_slo=test_artifacts/benchmarks/latest/mssql_to_clickhouse_slo.json \
  --artifact schema_evolution=test_artifacts/schema_evolution/latest/mssql_to_clickhouse.json \
  --format json
```

Exit code `0` means the generated pack and embedded route readiness report are
green. Exit code `1` means one or more required evidence domains are missing,
failed, or unsupported.

## Generated artifacts

The output directory contains:

| File | Description |
| --- | --- |
| `route_certification_pack.json` | Pack metadata, route profile, blocker list, generated evidence paths, and embedded readiness paths. |
| `route_certification_pack.md` | Human-readable pack summary and operator runbook. |
| `evidence/<name>.json` | One normalized evidence artifact for each required or provided evidence domain. |
| `readiness/route_readiness.json` | Final machine-readable route readiness decision. |
| `readiness/route_readiness.md` | Final human-readable route readiness decision. |

Generated evidence files use the same pass/fail semantics as route readiness:

- provided artifacts are normalized with checksums and source paths;
- missing required artifacts are written as failed evidence with
  `<name>.missing` blockers;
- safe auto-probes write factual metadata only;
- optional artifacts can be included for audit without changing route policy.

## Runbook

When `dpone ops route-certification-pack` returns exit code `1`:

1. Open `route_certification_pack.json` and inspect `blockers`.
2. If a blocker ends with `.missing`, generate the specialized evidence
   artifact and rerun the command with `--artifact`.
3. If a blocker ends with `.not_passed`, open the source artifact referenced in
   `evidence/<name>.json` and follow that artifact's runbook.
4. If the embedded readiness report is `unknown`, add the route to the
   integration matrix before certifying it.
5. Keep the generated failed pack as diagnostic evidence until the replacement
   pack is green.

## CI/CD usage

Use route certification pack after heavy route checks in manual or scheduled
workflows:

```bash
uv run dpone ops route-certification-pack \
  --source "$SOURCE" \
  --sink "$SINK" \
  --strategy "$STRATEGY" \
  --output-dir "test_artifacts/route_certification/${SOURCE}_to_${SINK}" \
  --artifact benchmark_slo="$BENCHMARK_SLO_JSON" \
  --artifact run_artifact="$RUN_ARTIFACT_JSON" \
  --format json
```

Upload `test_artifacts/route_certification/` with `if: always()` so release
reviewers can see both the source evidence and the final readiness decision.

## Related docs

- [Route readiness](route-readiness.md)
- [Developer guide: route readiness](developer-route-readiness.md)
- [Postgres -> MSSQL](source-sink/postgres-to-mssql.md)
- [MSSQL -> ClickHouse](source-sink/mssql-to-clickhouse.md)
- [`dpone ops` operational controls](ops-cli.md)
