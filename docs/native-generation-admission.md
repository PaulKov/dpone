# Native generation admission

This initial SQL Server capability reserves a generation under an existing active
dbt workspace attempt, charges a shared physical capacity account, binds one
writer invocation and reads its retained custody. It is an internal composition
capability; no CLI command launches a dbt build through it yet.

## Provisioning and runtime roles

The platform owner first installs the existing workspace admission schema and
[native-original SQL index](native-originals-mssql.md). Prepare and activate the
workspace through `MssqlDbtWorkspaceActivationAdmission`, then admit the complete
workflow attempt through `MssqlDbtWorkspaceAttemptAdmission`. Use the returned
guard epoch, not a locally invented epoch. This first implementation supports an
attempt whose complete write footprint belongs to one physical guard.

An administrator then calls `MssqlNativeGenerationSchemaMigration` with an
authenticated control authority, existing runtime database principal, physical
guard, resource-authority reference, total capacity, trusted-profile reference
and per-generation limit. Total and per-generation capacities are positive SQL
bigints, and the per-generation limit cannot exceed total capacity.

The installer requires the existing `dbo`-owned control schema. It validates
retained table shapes, keys, checks and procedure definitions; incompatible
objects cause failure. Repeated installation preserves the charged total.
Adding an approved profile to the same physical guard shares the same account;
a new profile cannot create another capacity allowance. Changing an existing
account's resource identity or capacity is rejected.

Runtime access is limited to these fixed procedures:

- `native_generation_reserve_v1`
- `native_source_writer_bind_v1`
- `native_source_custody_read_v1`

Each procedure authenticates the registered database principal ID and SID and
the full control-authority reference. The installer denies direct ledger access
and schema alteration to that principal. Runtime connections must be fresh,
dedicated connections with finite connection and statement timeouts. Inject
their factory; runtime code never installs or enrolls itself.

## Request and admission sequence

1. Assemble the generation subject, existing complete attempt request, exact
   guard epoch, trusted profile, command reference and requested capacity.
2. Call `generation_admission_request_bytes` to produce the canonical body.
   Publish and independently verify that original through the existing original
   store and binding path. Construct `VerifiedGenerationRequest` with its returned
   `OriginalRef`. The body excludes its enclosing reference, so publication needs
   no provisional object or self-referential hash. Construction alone does not
   authenticate originals or authorize a build.
3. Call `MssqlNativeGenerationControl.reserve(request)`. SQL verifies the bound
   reservation original, recomputes the complete attempt fingerprint from the
   request bytes, checks every write subject and the current P owner/epoch, then
   serializes capacity charging. Replaying the same acknowledged request preserves
   the same generation and charge. A conflicting request cannot overwrite it.
4. Construct the complete `SourceExecutorBinding`, including the chosen invocation
   ID, reservation, profile and command references. Call
   `bind_writer_invocation(reservation, binding, expected_revision=...)`.
   A fresh CAS moves revision 1/RESERVED to revision 2/BUILDING. SQL revalidates
   the existing P ownership before the final generation update. Python validates
   the detached result and independently reads it before returning.
5. Use `read_custody(generation_id)` for inspection. Reading an existing BUILDING
   record never authorizes a second launch.

SQL consumes the retained request bytes, not independently supplied projections
of the attempt. It checks the canonical attempt fingerprint because different
write subsets can share the same physical guard. UTF-8 decoding uses an explicitly
typed UTF-8 column and exact byte roundtrip; long and non-ASCII references retain
their identity. Direct procedure calls must also satisfy required JSON field,
type, UUID and integer checks.

## Ambiguity and recovery

A writer mutation is attempted once. An execute, commit, response-validation or
readback failure produces `NativeWriterAdmissionUncertain`. Its `observed` value
contains independently read custody when available; it is evidence only. Duplicate
binding also returns this conservative failure and cannot grant another dispatch.
No retry, timeout, exception cleanup or process restart releases charged capacity.

Reservation acknowledgement failure also raises after independent inspection;
an old retained row cannot prove that physical admission remains current. An
explicit caller retry of reservation remains metadata-only and must pass current
SQL ownership checks again. No provider method retries the mutation automatically.

The initial admission layout retains the last confirmed RESERVED or BUILDING
phase. The separately installed
[positive completion and freeze extension](native-source-positive-completion.md)
adds authenticated completion recording and FROZEN. Uncertainty is surfaced by
the adapter exception. None of these phases proves terminal success, permits
restarting a build, or authorizes capacity reclamation.

`SourceCustodySnapshot` also validates the planned frozen-state representation:
an active EXPORT grant requires a quality reference and the exact retained export
plan. UNKNOWN snapshots preserve a valid unresolved read and reservation. These
value checks do not implement quality, export or terminal state transitions.

## Validation scope and remaining work

Focused tests cover identity codecs, request publication order and snapshot
invariants. Opt-in isolated SQL tests use the real workspace activation and
attempt APIs, separate administrator/runtime principals, competing reservations,
lost commit acknowledgements, stale epochs, direct malformed JSON, complete
footprint checks and incompatible retained schemas.

The SQL fixtures use synthetic original bindings. They qualify admission and
custody behavior, not source catalog qualification, original object storage,
installed-wheel behavior or a complete dbt route. Shared and corporate endpoints
are not required for these tests.

Subsequent implementation slices provide
[trusted invocation execution](native-generation-execution.md),
[writer admission closure](native-source-admission-closure.md), and
[positive completion and freeze](native-source-positive-completion.md). Their
presence does not qualify the assembled analyst workflow.

The concrete SQL ledger still has no protected quality/export read-grant
lifecycle or irreversible SEALED/SUCCEEDED transition. Those states in
`SourceCustodySnapshot` describe a contract, not an installed capability. The
current decoder's empty `active_reads` tuple is not an observed absence of
readers. Neither that tuple, CLOSED admission, FROZEN, nor a locally successful
workspace attempt authorizes releasing a retained transaction slot or generation
capacity. Release requires independently observed terminal settlement and closed
read admission under the corresponding protected lifecycle; no such positive
release may be inferred from this partial provider.

Full source-ledger consumers must remain disabled until their concrete lifecycle
and assembled producer wiring are implemented and qualified. This capability
does not change existing CLI behavior, legacy manifest versions, P release rules
or ordinary release authority.
