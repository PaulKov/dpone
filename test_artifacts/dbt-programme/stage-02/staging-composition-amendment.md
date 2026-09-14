# B02 staging composition correction

APPROVED FOR IMPLEMENTATION by programme coordinator, 2026-09-14, under existing
approved B02 behavior. Starting commit8182b558d993baf302ed16c9ff3f2b3911083b5d;
reconciled base1202d2cf652cf41594abdd21040378204653b656. Sole writer remains the
existing Stage02 task in b02v1, current linear branch and draft PR68.

## Evidence and reason

The required full source run failed. Two deterministic B02 regressions were also
independently confirmed in CI: sink fanout20 exceeds the existing test limit14,
and direct controlled-adapter imports in runtime.sinks violate the connector-port
boundary. Earlier aggregate architecture PASS did not cover these stronger local
contracts. Preserve both failing tests unchanged; do not exempt imports or raise
limits. Other full-run failures include actual host process exhaustion and remain
separately classified, never silently counted as passing.

Architect /root/b02_structure_design inspected the actual code and forecast all
real typed dependencies:3726modules,9624edges, sinkfanout14,
avg_clustering0.18188023738263157 under unchanged0.182. Independent architect
/root/b02_deadline_contract approved the construction-only responsibility boundary
and required preservation of callback binding; its independent graph rerun was
UNVERIFIED because OS process creation failed. Root fetched the exact8182 sink,
decoder and finalizer through the GitHub connector and checked their actual
constructor signatures/callback expressions. Forecast remains non-execution
evidence. Actual gates are mandatory.

## Exact scope

Add owned production path:
src/dpone/runtime/clickhouse_staging_composition.py.

Move actual build_file_runner construction and construction of the validated-file
service, storage-bound journal, existing staging decoder and existing staging
finalizer into this module. Return an immutable typed three-field bundle:
validated_file, decoder, finalizer. The sink assigns the same existing attributes
and discards the construction result. No execution proxy, service lookup mechanism,
generic framework, callbacks invoked during assembly, or import of the sink.

Remove seven real sink dependencies: the controlled client and HTTP adapters,
validated-file ingestion, concrete journal, storage policy, staging decoder and
staging finalizer. Add one dependency on the actual composition implementation.
Keep all remaining sink annotations/imports. No dynamic import hiding.

New module's eleven actual typed dependencies are config.load_config,
ports.clickhouse_connector, runtime.clickhouse_file_stage_contract, the two
controlled adapters, sinks.clickhouse_physical_types, sinks.clickhouse_staging_decoder,
sinks.clickhouse_staging_finalizer, sinks.clickhouse_validated_file_ingestion,
sinks.clickhouse_validated_file_journal and runtime.storage_policy. Include every
real edge in measurement; do not delete annotations to force a forecast match.

## Compatibility and algorithm

Public sink constructor/staging signatures, supplied factory fallback semantics,
existing resolver instance, clock, connector and staging attributes remain intact.
Keep build_file_runner available from the old sink module as an exact alias,
preserving its signature and required compatibility metadata.

Construct service, decoder, finalizer in that order. The default B02 runner factory
must still read self.connector when invoked after preparation: pass an explicit
connector-provider callback from the sink. Decoder/finalizer retain their original
initialization-time connector. Preserve early-bound table/create/map/count/mutation
methods and the exact existing late-bound drop/plan/create lambdas. In particular,
pass map_type=self._map_type_for_config unchanged as Callable[...,str]; retain
argument order and existing decoder signature inspection. Do not call callbacks
while assembling the bundle. Preserve clone behavior.

Move no load, verification, deadline, cancellation, cleanup, evidence or finalizer
algorithm. The preceding responsibility correction and deadline fixes remain.
No new public policy/product decision, CLI, live integration, release or merge.

## Activation and validation

Copy this amendment unchanged to existing Stage02 approval artifacts, append its
hash/scope to receipt and add only the new source path to the active contract.
Validate the contract before source edits. Existing sink, compatibility tests,
ADR0064 and developer documentation are already owned. Original approved spec and
appendix stay unchanged; no new paths or budget/baseline/workflow changes.

Run both exact failing architecture tests unchanged first, plus meaningful public
construction/callback/clone/import compatibility checks in the existing owned B02
compatibility module. Preserve real child/HTTP and value/journal regressions.
Run actual architecture producer, import/port boundaries, sinkfanout assertion and
exact-commit module gate as well as the existing required scoped gates. Update
ADR0064 and developer composition map. Commit/normalpush/update draft PR68 and
freeze newSHA. Root then requires full source and installed acceptance plus fresh
independent exact-commit review; old8182 FAIL is retained.

Host limitation: the Mac currently cannot reliably create processes. Do not alter
Cisco/security agents, system limits or unrelated user processes. If contract or
test execution fails with OS35, record UNVERIFIED and wait for normal environment
recovery; never infer validation from elapsed time or the forecast. Read-only
public GitHub connector access can support inspection without local processes.
