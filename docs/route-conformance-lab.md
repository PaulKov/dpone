# Route Conformance Lab

`dpone ops route-conformance run`, `dpone ops route-conformance live-run`,
`dpone ops route-conformance summarize`, and
`dpone ops route-conformance release-gate` provide a deterministic acceptance
suite for one `source -> sink -> strategy` route.

Use this lab after onboarding and before release promotion when you need proof
that a route preserves rows, physical type contracts, schema evolution behavior,
and chunk-level typed hashes. It is an ops control-plane gate: it generates and
verifies evidence, but it does not open production database connections or
advance source state.

## Workflow

1. Run `dpone ops connection-doctor`, `source-discover`, `route-bootstrap`, and
   `route-doctor` for self-service onboarding.
2. Run `dpone ops route-conformance run` for each route and strategy that is in
   scope for the release.
3. Review `route_conformance.json`, `source_snapshot.json`, and
   `sink_snapshot.json`.
4. Run `dpone ops route-conformance live-run` in opt-in local-live,
   Docker-live, or vendor-live jobs when a real adapter is available.
5. Run `dpone ops route-conformance summarize` when you want one operator
   summary across several route artifacts.
6. Run `dpone ops route-conformance release-gate` in CI or release automation.
7. Attach the conformance artifacts to route readiness, route certification, or
   the release evidence pack.

The industrial certification profile is **10,000 rows** and **200 columns**.
Smaller profiles are useful for local development, but release gates should use
the full profile or document an accepted risk.

## Run

```bash
uv run dpone ops route-conformance run \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --dataset-profile wide_10k_contract \
  --rows 10000 \
  --columns 200 \
  --chunk-size 1000 \
  --include-nested \
  --require-schema-evolution \
  --min-rows 10000 \
  --min-columns 200 \
  --output-dir test_artifacts/route_conformance/mssql_to_clickhouse \
  --format json
```

The command writes:

| Artifact | Purpose |
| --- | --- |
| `route_conformance.json` | Stable machine-readable conformance report. |
| `route_conformance.md` | Human-readable release review summary. |
| `source_snapshot.json` | Generated source-side rows and physical contracts. |
| `sink_snapshot.json` | Sink-side rows and physical contracts used for exact verification. |

## Live Run

`live-run` uses the same dataset and verifier, but places live execution behind
adapter ports. The default `in_memory` adapter is credential-free and suitable
for local tests. Docker-live and vendor-live adapters should be enabled only in
protected jobs, for example with `DPONE_LIVE_ROUTE_CONFORMANCE=1`.

```bash
DPONE_LIVE_ROUTE_CONFORMANCE=1 \
uv run dpone ops route-conformance live-run \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --adapter in_memory \
  --dataset-profile wide_live_contract \
  --rows 10000 \
  --columns 200 \
  --chunk-size 1000 \
  --include-nested \
  --require-schema-evolution \
  --min-rows 10000 \
  --min-columns 200 \
  --output-dir test_artifacts/route_conformance/live/mssql_to_clickhouse \
  --format json
```

The command writes:

| Artifact | Purpose |
| --- | --- |
| `route_conformance_live.json` | Stable live-run report with adapter steps and embedded exact verification. |
| `route_conformance_live.md` | Human-readable live runner review. |
| `live_source_snapshot.json` | Source snapshot read through the live adapter. |
| `live_sink_snapshot.json` | Sink snapshot read through the live adapter after route execution. |
| `route_conformance.json` | Embedded standard conformance artifact produced from the live snapshots. |

## Vendor-Live Docker Run

The built-in `vendor_live` adapter connects to disposable Postgres, MSSQL, and
ClickHouse endpoints that are usually started with
`docker/docker-compose.integration.yml`. It is opt-in and fail-closed: without
`DPONE_VENDOR_LIVE=1`, `live-run` returns a blocked JSON report instead of
opening database connections.

First supported vendor-live routes:

| Route | Adapter alias | Evidence intent |
| --- | --- | --- |
| `postgres -> mssql` | `--adapter vendor_live` or `--adapter docker` | Seed Postgres, copy to MSSQL, read both snapshots, verify exact typed hashes and schema evolution. |
| `mssql -> clickhouse` | `--adapter vendor_live` or `--adapter docker` | Seed MSSQL, copy to ClickHouse, read both snapshots, verify exact typed hashes and schema evolution. |

Start the local services:

```bash
docker compose -f docker/docker-compose.integration.yml up -d postgres mssql clickhouse
```

Run the `mssql -> clickhouse` pack:

```bash
DPONE_VENDOR_LIVE=1 \
DPONE_LIVE_ROUTE_CONFORMANCE=1 \
DPONE_RUN_INTEGRATION=1 \
uv run dpone ops route-conformance live-run \
  --source mssql \
  --sink clickhouse \
  --strategy incremental_merge \
  --adapter vendor_live \
  --dataset-profile wide_vendor_contract \
  --rows 10000 \
  --columns 200 \
  --chunk-size 1000 \
  --include-nested \
  --require-schema-evolution \
  --min-rows 10000 \
  --min-columns 200 \
  --output-dir test_artifacts/route_conformance/vendor_live/mssql_to_clickhouse \
  --format json
```

Run the `postgres -> mssql` pack:

```bash
DPONE_VENDOR_LIVE=1 \
DPONE_LIVE_ROUTE_CONFORMANCE=1 \
DPONE_RUN_INTEGRATION=1 \
uv run dpone ops route-conformance live-run \
  --source postgres \
  --sink mssql \
  --strategy incremental_merge \
  --adapter docker \
  --dataset-profile wide_vendor_contract \
  --rows 10000 \
  --columns 200 \
  --chunk-size 1000 \
  --include-nested \
  --require-schema-evolution \
  --min-rows 10000 \
  --min-columns 200 \
  --output-dir test_artifacts/route_conformance/vendor_live/postgres_to_mssql \
  --format json
```

Default endpoint variables match the compose file:

| Backend | Variables |
| --- | --- |
| Postgres | `DPONE_IT_PG_HOST`, `DPONE_IT_PG_PORT`, `DPONE_IT_PG_DATABASE`, `DPONE_IT_PG_USER`, `DPONE_IT_PG_PASSWORD` |
| MSSQL | `DPONE_IT_MSSQL_HOST`, `DPONE_IT_MSSQL_PORT` or `DPONE_IT_MSSQL_PORT_FORWARD`, `DPONE_IT_MSSQL_DATABASE`, `DPONE_IT_MSSQL_USER`, `DPONE_IT_MSSQL_PASSWORD`, `DPONE_IT_MSSQL_DRIVER` |
| ClickHouse | `DPONE_IT_CH_HOST`, `DPONE_IT_CH_PORT`, `DPONE_IT_CH_DATABASE`, `DPONE_IT_CH_USER`, `DPONE_IT_CH_PASSWORD` |

The adapter writes the same `route_conformance_live.json`,
`live_source_snapshot.json`, `live_sink_snapshot.json`, and embedded
`route_conformance.json` as the credential-free runner. Release candidates
should attach both route artifacts and then run `route-conformance summarize`
and `route-conformance release-gate`.

## Summarize

```bash
uv run dpone ops route-conformance summarize \
  --artifact mssql_to_clickhouse=test_artifacts/route_conformance/mssql_to_clickhouse/route_conformance.json \
  --artifact postgres_to_mssql=test_artifacts/route_conformance/postgres_to_mssql/route_conformance.json \
  --output-dir test_artifacts/route_conformance/summary \
  --format json
```

The summary writes `route_conformance_summary.json` and
`route_conformance_summary.md`. It is useful for human review and for release
notes, but it does not replace the fail-closed release gate.

## Release Gate

```bash
uv run dpone ops route-conformance release-gate \
  --release v0.10.0-rc1 \
  --artifact mssql_to_clickhouse=test_artifacts/route_conformance/mssql_to_clickhouse/route_conformance.json \
  --artifact postgres_to_mssql=test_artifacts/route_conformance/postgres_to_mssql/route_conformance.json \
  --output-dir test_artifacts/route_conformance/release-gate \
  --format json
```

The release gate writes `route_conformance_release_gate.json` and
`route_conformance_release_gate.md`. It fails closed when any required artifact
is missing, malformed, red, or below the dataset scale expected by the release.

## What Is Verified

| Domain | Evidence |
| --- | --- |
| Route identity | Canonical `source`, `sink`, `strategy`, `pair_id`, `case_id`, and `colon_id`. |
| Dataset scale | Row and column counts, with release-time minimums such as 10,000 rows and 200 columns. |
| Nested objects | Deterministic `id`, `parent_id`, and nested payload fields for dlt-like hierarchy checks. |
| Physical contracts | Per-column logical type and physical contract, such as `decimal(38,10)` or `datetime2(6)`. |
| Exact verification | Source row count, sink row count, whole-dataset typed hash, and per-chunk typed hashes. |
| Schema evolution | Add-column, decimal-widening, and nullable-relaxation metadata when required. |
| Mismatches | Bounded mismatch samples that identify row index, column, source value, and sink value. |

## Runbook

| Symptom | Action |
| --- | --- |
| `route.unsupported:<route>` | Add or correct the integration matrix case and route profile metadata. |
| `dataset.rows_below_minimum` | Increase `--rows`; release certification normally uses 10,000 rows. |
| `dataset.columns_below_minimum` | Increase `--columns`; wide-schema certification normally uses 200 columns. |
| `typed_hash.mismatch` | Open `mismatch_samples`, then fix serialization, type conversion, ordering, or idempotent load drift. |
| `physical_contract.<column>.mismatch` | Align the target physical type resolver with the route physical contract. |
| `schema_evolution.missing` | Rerun with schema evolution enabled or attach explicit schema evolution evidence. |
| `adapter.<name>.missing` | Install or register the requested live adapter, then rerun `live-run`. |
| `vendor_live.opt_in_missing:DPONE_VENDOR_LIVE` | Set `DPONE_VENDOR_LIVE=1` only in a protected local, Docker-live, or vendor-live job. |
| `vendor_live.binding_missing:<route>` | Add a `VendorLiveRouteBinding` for the route or use a supported first route. |
| `vendor_live.seed_source.failed:<error>` | Check disposable source credentials, ODBC/native drivers, and container health. |
| `vendor_live.execute_route.failed:<error>` | Check sink DDL, type conversion, row batch limits, and database logs. |
| `<route>.not_passed` in release gate | Open that route's `route_conformance.json` and fix the first blocker before editing aggregate artifacts. |

## CI Usage

Credential-free OSS CI should run `route-conformance run` with deterministic
fixtures and upload the generated artifact directory with `if: always()`.
Protected vendor-live Docker jobs should set `DPONE_VENDOR_LIVE=1`, run
`--adapter vendor_live` or `--adapter docker` for `postgres -> mssql` and
`mssql -> clickhouse`, then attach their live evidence alongside
`route_conformance.json`. Keep live IO outside the conformance service; use the
service as the stable acceptance evidence contract.

## Related Docs

- [Route bootstrap and doctor](route-bootstrap-doctor.md)
- [Route readiness](route-readiness.md)
- [Route live certification](route-live-certification.md)
- [Route release gate](route-release-gate.md)
- [Developer Route Conformance Lab](developer-route-conformance-lab.md)
