# B01/B02 repair scope and DRAFT deferred contracts

Status: **coordinator-granted isolated repairs implemented; broader contracts DRAFT**.
Baseline: `46830976b214262c7772800523e832a5a6f6d78f`.
This records the concise impact/validation plan for existing-contract repairs,
not an approved new feature specification. New public interfaces or wider authority
semantics require an approved specification before implementation.

## B01: implicit nullable time fidelity

Problem: an admitted `time nullable` source is decoded with its default scale 7,
but Python target text classification misses the nullable suffix and loses the
100ns remainder. Explicit-scale forms and accelerated Native preserve it.

Granted files:

- `src/dpone/runtime/clickhouse_binary_encoding.py`: normalize the admitted
  nullable source syntax for time classification; retain the existing canonical
  time-text encoder and authored target/time policies.
- `tests/test_native_bcp_implicit_time_fidelity.py`: focused public transcoder
  regression with independent bytes, distinct before/after row sentinels, NULL,
  implicit and explicit scale, midnight and final fractional tick. Cover
  RowBinary, Python Native and optional accelerated Native with honest skips.
- `docs/source-sink/mssql-to-clickhouse.md`: a bounded clarification of implicit
  time scale/fidelity in the existing native-type reference if assigned.

No framing table, UUID/decimal prefix, contract hash, acceleration implementation
or unbounded temporal-narrowing change is proposed. Catalog-derived impact
remains bounded by canonical metadata. Behavior changes only by restoring exact
text for an already admitted source spelling. The coordinator granted this
bounded repair under the documented isolated-regression exception.

Public reproduction mechanism: `build_mssql_bcp_native_contract` →
`SourceNativeArtifact` → `NativeWireTranscoder.to_clickhouse_binary`.
`NativeAccelerationRegistry(module_loader=...)` is constructor DI; neither
provider nor encoding method is replaced.

## B02: receipted file dispatch

Problem: the documented validated single-file fast path loses its concrete
artifact identity at ClickHouse dispatch. `ClickHouseSink.stage_payload`, with
a constructor-injected recording connector and explicit Python bulk mode,
stages the raw two-row file exactly but fails its valid wrapper on missing
`ClickHouseSink.create`. Both calls execute the real staging/ingestion code.

Granted files:

- `src/dpone/runtime/etl/contract_artifacts.py`: one explicitly internal
  consumption operation on `ContractValidatedFileArtifact`, restricted to a
  concrete `FileExportArtifact` and a typed loader callback.
- `src/dpone/runtime/sinks/clickhouse_payload_ingestion.py`: an explicit branch
  for this wrapper, invoking its internal operation and existing file dispatch.
- `tests/test_clickhouse_validated_file_contract.py`: real public
  `ClickHouseSink.stage_payload` tests with constructor DI; no method patches,
  subclass overrides or private test entrypoints.
- `docs/runtime-fast-path-contracts.md`: correct the single-file receipt and
  recovery guidance; explicitly leave partition authority unresolved.

Implemented internal algorithm:

1. Clear any prior attempt's accepted-row summary. Preserve cached construction failures and the existing typed missing-receipt
   error. Require the concrete file class; do not guess capabilities or
   recursively unwrap arbitrary objects.
2. Reverify the original receipt against the frozen source schema/contract and
   current bytes/wire identity immediately before consumption.
3. Pass the same artifact to the existing loader exactly once. Do not copy,
   recreate, rename, reinterpret, retry or terminate it.
4. After successful consumption, reverify bytes/schema/wire/receipt identity;
   reject removed or differently bound evidence. Validate the loader count with
   the existing target-row-count contract and require equality with the original
   receipt's validated source count.
5. Update the existing validation summary only after successful postchecks,
   preserving `opaque_file_prevalidated` and the exact consumed count. Preserve
   extraction/source-count authority and terminal ownership.
6. Propagate callback/postcheck failure through existing staging cleanup; return
   no successful handle/summary. This adds no publication idempotency mechanism.

Acceptance: exact raw/wrapped rows and count; empty-file zero rows; missing
receipt despite boolean hint; byte/receipt mutation before load; mutation during
the injected connector callback; connector error preservation; no accepted-row
summary after failure; source retention until the existing terminal decision;
existing MSSQL wrapper/quality/opaque-file protections remain passing. No live
route certification is implied by a recording connector.

Supporting existing contracts: `docs/runtime-fast-path-contracts.md:13,24`
documents source-validated native file loading;
`docs/adr/0005-production-runtime-connectors.md:15` requires typed artifacts and
staging before promotion; current wrapper materialization and
`tests/test_streaming_contracts_and_evidence_pack.py` enforce receipts.
`AGENTS.md` and `docs/feature-design-standard.md` permit a concise plan for an
isolated regression with an existing contract. The coordinator granted this
internal repair through that exception; a new public
`load_with` API/capability, new formats, receipt schema, partition admission or
rename semantics does not automatically fit it.

## Boundaries, checks and approvals

Only the listed source/test/docs and stage artifact paths are granted. Shared
CHANGELOG, schemas, registries, factories, dependencies and release artifacts
remain coordinator-owned. No MSSQL staging, decoder, partition or rebind edits
are justified by the demonstrated B02 case. No compiler/composition/DDA or
stage 03 query/topology edits are proposed.

Failing public-boundary tests preceded implementation: 18 failed and 12 passed.
The expanded repaired native/wrapper/quality suite passed 746 tests. A separate
new-consumption summary regression failed before its correction. Validation
history distinguishes product failures from an intermediate test enum-name error.
Obtain independent final-diff review on the exact candidate commit and retain
the coordinator-owned mandatory combined full gate as open until executed.
Any widened public-contract impact returns to specification review. Live checks
remain SKIP unless a separate explicit approved environment/task scope exists.

B03/B04/B05/B06 remain DRAFT contract decisions: ordered source schema and bytes
must stay bound; authored NULL/timezone choices stay explicit; target naming is
not source receipt authority; partitions need per-child proof, never a boolean.

## Integrator-owned changelog proposal

Preserve default-scale nullable MSSQL time text at 100ns precision across Python
binary encoders. Retain receipted single-file ClickHouse dispatch with before/after
integrity checks, rejecting changed validation evidence or mismatched load counts.
The coordinator owns the shared changelog entry and final combined validation.
