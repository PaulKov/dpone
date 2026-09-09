# Developer CDC handoff

CDC snapshot handoff is a route-level control-plane abstraction for
replication-grade readiness evidence. It sits beside route readiness and CDC
replay planning. It does not replace runtime readers or sink apply strategies.
CDC apply certification produces fixture-based evidence; handoff evaluates that
evidence and writes the final go/no-go report.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.ops.cdc.models` | Immutable value objects and stable report contracts such as `CdcStreamKey`, `CdcHandoffProfile`, `CdcHandoffDecision`, and `CdcHandoffReport`. |
| `dpone.ops.cdc.catalog` | Builds CDC profiles from route matrix presence plus small route metadata records. |
| `dpone.ops.cdc.evidence` | Normalizes evidence artifacts through the shared route evidence reader. |
| `dpone.ops.cdc.policy` | Scores normalized evidence and returns blockers, warnings, and next actions. |
| `dpone.ops.cdc.handoff` | Provides `SnapshotCdcHandoffService`, the thin facade used by CLI and CI. |

`SnapshotCdcHandoffService` composes the catalog, evidence reader, and policy.
It must stay orchestration-only: no live database clients, no heavy replay
execution, and no source/sink-specific branches.

## Generic interfaces

`CdcStreamKey` wraps the existing `RouteKey` and adds source and target dataset
identity. This keeps route readiness and CDC handoff aligned on canonical ids.

`CdcHandoffCatalog` returns one `CdcHandoffProfile` per supported CDC route. The
profile owns source backend, sink apply mode, snapshot boundary kind, offset
kind, native apply path, default SLO hints, and required evidence names.

`CdcEvidenceReader` reads artifact paths and returns normalized
`RouteEvidenceItem` values. This deliberately reuses route-readiness
pass/fail semantics.

`CdcHandoffPolicy` is pure. It receives a profile and normalized evidence. It
does not read files, load catalogs, or inspect route-specific metadata.

## CDC apply evidence

The first profile is `mssql -> clickhouse`:

- source backend: `mssql_cdc`
- sink apply mode: `clickhouse_replacing_merge_tree`
- snapshot boundary kind: `mssql_lsn`
- native apply path: `mssql_cdc_to_clickhouse_typed_staging_apply`
- required evidence: `cdc_snapshot_boundary`, `cdc_window`,
  `retention_preflight`, `cdc_apply_correctness`, `delete_semantics`,
  `typed_cdc_hash`, and `schema_drift_governance`

Do not add route-specific branches to the service. Add a new
`CdcRouteMetadata` entry in `dpone.ops.cdc.catalog`, make sure the
`source -> sink -> cdc` route exists in the integration matrix, add docs, and
extend tests.

## Extension checklist

When adding a new CDC route:

1. Confirm `DEFAULT_INTEGRATION_MATRIX` contains the route with strategy `cdc`.
2. Add `CdcRouteMetadata` for the route pair.
3. Document the source-sink guide and this evidence contract.
4. Add a route-specific test that proves the profile exposes required evidence.
5. Add docs-contract tests for user docs, developer docs, examples, and runbooks.
6. Keep runtime CDC readers under `dpone.runtime.cdc` and replay planning under
   `dpone.readiness.cdc_replay`.
7. Run the full CI quality stack before release.

The dependency direction is one way: CDC handoff may depend on route readiness
models and evidence readers, but runtime readers and sink strategies must not
depend on CDC handoff.
