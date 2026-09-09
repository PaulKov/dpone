# Feature design: MySQL → BigQuery contract-ready wire v1

- Status: IMPLEMENTED
- Owner: dpone maintainers
- Issue: follow-up to mysql Batch routes (#395+); prepare mysql→bigquery without live GCP
- Target release: next patch after merge (0.73.17 or current line)
Last verified: 2026-07-22
Approval: maintainer directed phase A (hermetic/contract-ready) 2026-07-22

## Executive summary

`mysql → bigquery` is documented as Experimental and still defaults MySQL export
to `mssql-delimited` when `sink_type` is omitted from negotiation. BigQuery
staging only accepts CSV. This feature makes the pair **contract-ready**:
sink-aware CSV default, fail-closed BCP/TSV wire, dedicated type profile,
hermetic contracts, and a landing example — **without** claiming Batch ETL or
running vendor-live BigQuery evidence.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Data engineer | Land MySQL into BigQuery safely | Experimental; BCP default risk | CSV wire + type profile + example |
| Platform engineer | Plan without GCP creds in CI | No hermetic pair contracts | pytest contracts PASS |
| Operator | Know what is certified | Docs over-claim or under-explain | Matrix: Experimental, contract-ready / live pending |

Journey: matrix → `dpone[mysql,gcp]` → `examples/batch/landing_mysql_to_bigquery.batch.yaml`
→ plan (hermetic) → live BQ deferred to phase B.

## Scope

### In scope

- Sink-aware MySQL export default: `csv` when `sink_type=bigquery`
- Fail-closed: `mssql-delimited` and `clickhouse-tsv` for BigQuery sinks
  (plan/extract path)
- Type profile `mysql_to_bigquery_analytics_v1` + PairTypeMatrix registration
- Hermetic contracts in `tests/test_runtime_mysql_contracts.py`
- Landing batch example + guide/matrix/CHANGELOG/type-mapping updates
- Status remains **Experimental — contract-ready; live evidence pending**

### Non-goals

- Vendor-live BigQuery IT / Batch ETL matrix flip (phase B)
- Binlog CDC, named native fast path, Conformance Lab
- Claiming delete-correctness from watermark alone
- Requiring gzip fail-closed (BigQuery CSV load accepts gzip)

### Assumptions and constraints

- Reuse existing BigQuery staging CSV / GCS loaders; no new ADR
- Optional deps: `dpone[mysql]` + `dpone[gcp]`
- SKIP/N/A live checks must never be reported as PASS

## Approaches considered

| Approach | Pros | Cons | Decision |
|---|---|---|---|
| **A. CSV + hermetic contracts** | Safe without GCP; matches postgres/kafka wire pattern | No Batch claim yet | **Adopt** |
| B. Docs-only | Fast | Leaves BCP default risk | Reject |
| C. Batch ETL without live | Pretty matrix | Violates evidence rules | Reject |

## Public contract

### Manifest/schema

Public `export_format: csv`. Runtime rejects `mssql-delimited` and
`clickhouse-tsv` when `sink.type` / `options.sink_type` is BigQuery.

### Compatibility

Additive. mysql→mssql BCP, postgres CSV, clickhouse TSV, kafka CSV unchanged.

## Detailed algorithm

1. Validate `source.type=mysql`, `sink.type=bigquery`.
2. Resolve export: default `csv`; fail if BCP or ClickHouse TSV.
3. Map MySQL COLUMN_TYPE → BigQuery types via `mysql_to_bigquery_analytics_v1`.
4. Extract → `FileExportArtifact(format=csv)`.
5. Existing BQ staging CSV (local or GCS threshold) loads staging then strategy finalize.
6. Evidence: hermetic PASS; live BQ = N/A until phase B.

## Market research (relevant only)

| System | Relevance | Note |
|---|---|---|
| Fivetran / Airbyte | MySQL→BQ connectors | Often CDC; dpone v1 watermark batch only |
| dlt | Load to BigQuery | N/A for staging-first fail-closed wire claim |
| SSIS / Informatica / Pentaho / Beam / Cosmos / gusty | N/A | Not the CSV staging contract under comparison |

Sources: Google BigQuery load jobs docs (CSV), dpone postgres→bigquery guide
(current repo, 2026-07-22). Differentiation axis = fail-closed MySQL CSV wire +
explainable type profile before vendor-live certification.

## Measurable differentiation

```yaml
axis: fail-closed MySQL→BigQuery CSV wire with hermetic contracts
scenario: MySQL OLTP → BigQuery landing, incremental_merge planned
baseline: Experimental guide with mssql-delimited default risk
metric: extract rejects BCP/TSV; default csv; type matrix profile present
target: hermetic pytest PASS without GCP credentials
procedure: tests/test_runtime_mysql_contracts.py bigquery cases
artifact: pytest JUnit / local evidence
```

## Test and certification plan

| Layer | Expected |
|---|---|
| Unit/hermetic | default csv; reject mssql-delimited + clickhouse-tsv; type mapper + matrix |
| Live BigQuery | N/A phase A (explicit) |
| Docs | Experimental, contract-ready / live pending |

## Agent execution plan

| Role | Owned paths |
|---|---|
| Integrator | feature design, mysql_base, type profile, matrix service, tests, examples, docs, CHANGELOG |

## Rollout

1. Land PR with hermetic evidence.
2. Phase B (separate APPROVED increment): vendor-live IT + Batch ETL claim.

## Implementation evidence (phase A)

- Status flipped to IMPLEMENTED after hermetic contracts PASS
  (`tests/test_runtime_mysql_contracts.py`, 2026-07-22).
- Review follow-up: plan-time fail-closed in
  `dpone.dag.export_format_validation` for BigQuery + BCP/TSV; COLUMN_TYPE
  display-width normalization and BigQuery NUMERIC/BIGNUMERIC bounds in
  `mysql_bigquery.py`.
- Live BigQuery IT: N/A (phase B). Batch ETL matrix claim: not asserted.
