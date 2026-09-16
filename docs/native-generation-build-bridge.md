# Managed generation build integration

This developer reference describes an unreleased composition component. It is not
an installed-route qualification or a new analyst command. The complete analyst
journey still requires the qualified bootstrap, final quality, export and delivery.

## Bound build capability

`NativeGenerationBuildPort.execute` accepts exactly three keyword-only dependencies:
`command_runner`, `profile_renderer` and `evidence_writer`. It returns the existing
`DbtExecutionOutcome`. `BoundNativeGenerationBuild` implements that seam using the
existing `DbtExecutionService`; it does not call the public bootstrap recursively
or implement another parse/build/results algorithm.

The bootstrap supplies the pack, project/output coordinates, run identity, Airflow
attempt, interval, inspector, profile store, artifact reader, schema validators,
clock and an explicit native attempt lifecycle at construction. Value inputs are
validated and copied. Construction does not create directories, render credentials
or run commands. A missing lifecycle is rejected rather than selecting the legacy
terminalizing owner.

On execution, the exact supplied runner is used by both `DbtRuntimePreflight` and
the service. The renderer and writer are passed unchanged. The service outcome,
including nonzero exit codes and raised uncertainty errors, is preserved without
retry. An ordinary successful outcome alone does not establish native completion.

## Ownership and lifetime

The outer reserved bridge owns admission and authentication before credentials.
The qualified bootstrap holds the project, output and profile roots. Its profile
store must provide a preallocated directory that remains present through evidence
publication; the ordinary temporary store's inner-directory cleanup does not meet
that lifetime contract. This is a constructor dependency, not a fourth execution
argument or a new responsibility for the analyst.

The native attempt lifecycle retains the physical owner while the service records
its local outcome. This bound capability does not publish a source freeze, settle
the attempt, release capacity or permit another dispatch. It is not a standalone
recovery entry point. On a failed or uncertain invocation, the outer owner retains
the existing recovery responsibilities.

## Reserved execution and result

Import `ReservedDbtBuildBridge` from
`dpone.services.native_generation_build_bridge` and
`TrustedDbtInvocationRecorder` from
`dpone.services.native_generation_invocation_recorder`.
`dpone.app.native_generation_bound_build` owns the concrete engine composition.
The earlier, unreleased `dpone.runtime.native_generation_execution` grouping
facade is removed; use the defining owners directly.
The bridge requires the actual trusted recorder as its command runner and an
actual `NativeGenerationBuildEvidenceWriter`. Before credential rendering it
compares the full executor descriptor against both owners, checks the reservation
coordinates and authenticates the invocation's current originals.

Each bridge instance attempts its bound execution at most once, including after
an exception. This local guard does not replace durable admission or authorize
reconstruction of an invocation after a restart. The existing qualified bootstrap
and physical owner remain responsible for dispatch authority.

A `GenerationBuildReceipt` with outcome `SUCCEEDED` requires exact integer-zero
exit codes, existing evidence status `passed`, the writer's freshly authenticated
cohort and the recorder's matching termination original. An independent bounded
read must match the canonical bytes of the returned execution evidence. A
successful-looking DTO without actual retained writer output is rejected.

An unsuccessful returned outcome raises `NativeGenerationBuildRejected` carrying
that exact outcome. Its reporting state is `UNKNOWN` for `COMMIT_UNKNOWN` or
recovery evidence, otherwise `FAILED`. Neither state authorizes cleanup or release.
Existing raised exceptions propagate unchanged; no automatic command retry occurs.
The bridge does not republish originals or manufacture a persisted receipt kind.

## Validation and limitations

`tests/test_native_generation_bound_build.py` checks exact dependency forwarding,
constructor side effects, copied identity inputs, mandatory lifecycle, actual
existing-engine success/nonzero behavior and unchanged exception propagation.
`tests/test_native_generation_build_bridge.py` covers wrong-owner rejection before
credentials, full executor checks, actual writer provenance, forged-success
rejection, failed/unknown outcomes, unavailable originals and no redispatch.
The engine tests use synthetic command and storage fixtures; they do not certify
dbt, SQL Server or the complete route. Installed synthetic end-to-end validation
must exercise the qualified bootstrap and the real supported execution stack.

Related references: [source positive completion](native-source-positive-completion.md),
[native attempt lifecycle](native-workspace-attempt-lifecycle.md) and
[trusted command execution](native-generation-execution.md).
