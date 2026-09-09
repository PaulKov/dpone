# Route data quality

`dpone ops route-data-quality` builds a route-level data quality scorecard for
one `source -> sink -> strategy` pair. Use it after a manifest run, CDC apply,
route repair, or release-candidate execution when operators need one artifact
that answers whether the data is trustworthy enough to promote.

The command is control-plane only. It does not connect to databases, replay
quarantine rows, repair records, mutate sinks, or advance state. It reads
existing data contract, quarantine, reconciliation, SLO, CDC, and route run
artifacts, normalizes them, applies fail-closed policy, and writes JSON and
Markdown evidence.

## CLI example

```bash
uv run dpone ops route-data-quality \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --artifact data_contract=test_artifacts/contracts/orders/data_contract_evidence.json \
  --artifact quarantine=test_artifacts/quarantine/orders/quarantine_export.json \
  --artifact reconciliation=test_artifacts/reconciliation/orders/reconciliation_report.json \
  --artifact route_run_supervisor=test_artifacts/route_runs/mssql_to_clickhouse/orders/route_run_receipt.json \
  --require route_run_supervisor \
  --min-score 95 \
  --warning-score 98 \
  --max-quarantine-rows 0 \
  --max-exception-ratio 0 \
  --max-exception-age-hours 24 \
  --output-dir test_artifacts/route_quality/mssql_to_clickhouse/orders \
  --format json
```

## Outputs

| File | Description |
| --- | --- |
| `route_data_quality.json` | Stable schema `dpone.route_data_quality.v1` with route identity, thresholds, score, dimensions, exception backlog, evidence checksums, blockers, warnings, and next actions. |
| `route_data_quality.md` | Human-readable scorecard and Runbook. |

## Required evidence

The default required evidence domains are:

| Evidence | Purpose |
| --- | --- |
| `data_contract` | Runtime schema/data contract results, including rejected or quarantined rows. |
| `quarantine` | Exception backlog, quarantine row counts, failure ratio, and oldest open exception age. |
| `reconciliation` | Source-target count/hash/diff evidence, including missing, extra, and mismatched rows. |

Use repeated `--require <name>` to make more domains mandatory. Common route
data quality domains are `cdc_observability`, `slo`, `route_run_supervisor`,
`route_reconciliation_repair`, and `route_schema_evolution`.

## Thresholds

| Option | Default | Meaning |
| --- | ---: | --- |
| `--min-score` | `95` | Minimum route score required to pass. |
| `--warning-score` | `98` | Passing score below this value emits a warning. |
| `--max-quarantine-rows` | `0` | Maximum open exception or quarantine rows. |
| `--max-exception-ratio` | `0` | Maximum failed/quarantined row ratio. |
| `--max-exception-age-hours` | `24` | Maximum age for unresolved exceptions. |

The default policy is strict because release evidence should prove clean data.
For dirty upstream feeds, use explicit thresholds and capture the reason in the
release review.

## Statuses

| Status | Meaning |
| --- | --- |
| `passed` | Required evidence exists, belongs to the route, passed, and score meets policy. |
| `warning` | The route passes but the score is below the warning threshold or optional evidence has warnings. |
| `blocked` | Required evidence is missing, malformed, failed, unsupported, or route-mismatched. |
| `waiver_required` | A critical DQ blocker exists and steward approval is required before release. |
| `quarantine_sla_breached` | Exception count, ratio, or age exceeds configured thresholds. |

Exit code `0` means status is `passed` or `warning`. Exit code `1` means the
route is blocked, needs waiver approval, or breached quarantine SLA.

## Release gate usage

For critical route release candidates, attach the scorecard to the route release
gate:

```bash
uv run dpone ops route-release-gate \
  --release v0.10.0-rc1 \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --artifact route_data_quality=test_artifacts/route_quality/mssql_to_clickhouse/orders/route_data_quality.json \
  --require route_data_quality
```

## Python API

```python
from dpone.ops.route_data_quality import RouteDataQualityService

report = RouteDataQualityService().evaluate(
    output_dir="test_artifacts/route_quality/mssql_to_clickhouse/orders",
    source="mssql",
    sink="clickhouse",
    strategy="incremental_merge",
    artifacts={
        "data_contract": "test_artifacts/contracts/orders/data_contract_evidence.json",
        "quarantine": "test_artifacts/quarantine/orders/quarantine_export.json",
        "reconciliation": "test_artifacts/reconciliation/orders/reconciliation_report.json",
    },
    min_score=95.0,
    warning_score=98.0,
)
```

## Runbook

1. Generate data contract, quarantine, and reconciliation artifacts from the
   route run.
2. Run `dpone ops route-data-quality` with every artifact attached as
   `--artifact name=/path/to/file.json`.
3. If status is `passed`, attach `route_data_quality.json` and
   `route_data_quality.md` to route release evidence.
4. If status is `warning`, review warnings and decide whether the release
   review needs explicit acknowledgement.
5. If status is `quarantine_sla_breached`, export quarantine rows, fix root
   causes, replay safe exceptions, or capture waiver approval.
6. If status is `waiver_required`, attach approved steward waiver evidence and
   rerun `dpone ops route-data-quality`.
7. If status is `blocked`, fix the first blocker in the upstream artifact
   rather than editing aggregate route DQ JSON by hand.

## Related docs

- [Schema contracts](schema-contracts.md)
- [Route readiness](route-readiness.md)
- [Route reconciliation repair](route-reconciliation-repair.md)
- [Route run supervisor](route-run-supervisor.md)
- [Route release gate](route-release-gate.md)
- [Developer route data quality](developer-route-data-quality.md)
