# Positive source completion and freeze

This reference is for platform engineers implementing native source custody.
It describes the positive completion values used between a trusted build and
restricted quality/export operations. The durable transition and complete analyst
workflow are still under implementation; these value objects alone authorize no
database operation.

Start with [source admission closure](native-source-admission-closure.md).
Closing admission does not prove build success or stop an unknown executor.

## Evidence and identity

`SourceTrustedBuildCompletion` has six required fields: `executor`, `command`,
`toolchain`, `build_evidence`, `artifact_inventory`, and `termination`.
The command must equal the admitted executor command. Every other field is an
exact original reference, not a boolean result. The consumer must independently
authenticate the qualified runtime, complete locked membership, fresh successful
build results, owned artifacts and normal joined termination. A valid JSON shape
or matching digest is not proof of those properties.

`require_trusted_build_completion(snapshot, closure, completion)` is a pure
identity predicate. It requires BUILDING with CLOSED admission and ACTIVE or
UNKNOWN outcome, exact executor equality, and an admission revision no newer than
custody. It neither changes UNKNOWN nor grants freeze. The SQL operation must
separately compare current revision, ownership and all authenticated originals.

## Actual build artifact capture

`NativeGenerationBuildEvidenceWriter` decorates the existing execution-evidence
writer. It reads only the captured BUILD target's `manifest.json` and
`run_results.json`, validates the locked graph and complete node outcomes, and
publishes the exact bytes with a canonical inventory. Failed execution evidence
is preserved without producing a positive completion. A lost publication
acknowledgement requires independent exact-original readback; it never reruns dbt.

The bootstrap must bind the writer, command recorder, execution pack and run
identity to the same invocation and keep its OUTPUT root isolated through capture.
The preflight target is not the BUILD target. Temporary profile cleanup after
normal command return does not invalidate retained original authentication;
recovery must not require a new profile or resolve credentials again.

Freshness is not inferred from filesystem modification times or wall-clock
windows. Wall timestamps provide correlation only; monotonic elapsed time is the
trusted recorder's deadline authority. Authentication binds the observed dbt
invocation ID in both artifacts and evidence to the admitted executor through the
inventory. The observed dbt ID is not the pre-dispatch trusted invocation UUID.
Those checks depend on correctly isolated producer wiring, not merely typed
constructor arguments. Synthetic unit tests cannot establish that premise for a
production deployment.

The writer and recovery consumer share the same complete-termination validator.
It checks the admitted BUILD command membership and rejects retained monotonic
elapsed time above the plan's termination allowance; the exact boundary is valid.
They also validate the workload identifier, not just its pack digest.

`runtime.native_generation_completion_auth.NativeGenerationCompletionAuthenticator`
is the independent recovery consumer. It rereads the exact completion, admission,
termination, inventory, pack, execution evidence and both raw artifacts through
generation-bound readers. It checks declared artifact sizes before and after
bounded reads, uses the injected existing evidence decoder with canonical-byte
equality, and repeats graph, node and invocation checks. A cached writer result is
not sufficient. Its successful return does not grant a database mutation: the SQL
caller must use the same completion and descriptor in the revision-checked
transition, without choosing newer or different originals between those steps.

No dispatch, credential resolution, publication or SQL capability is available to
this verifier. Official schema validators and authorized original stores belong
to the composition root; test substitutes are explicitly not certification.

## Generation descriptors

`SourceClosureReceipt` identifies the accepted positive completion snapshot:
`generation_id`, `guard_epoch`, `revision`, `reservation`, `completion`, `receipt`.
Its revision belongs to the accepted completion snapshot, not the earlier
immutable admission closure. Its completion reference must match the persisted
snapshot's positive `closure` original.

`FrozenGeneration` contains `generation_id`, `guard_epoch`, `revision`,
`reservation`, `closure`, and `frozen`. It preserves the closure's generation,
epoch and reservation, with revision exactly one greater. It grants neither a
read nor permission to launch another build or reclaim capacity.

The codecs have explicit external descriptors:

```python
closed = decode_source_closure_receipt(payload, receipt=authenticated_descriptor)
frozen = decode_frozen_generation(frozen_payload, frozen=authenticated_frozen_descriptor)
```

The source closure payload excludes its own receipt. The frozen payload excludes
its own frozen descriptor and embeds the closure as a readback envelope containing
`payload` and `receipt`. Both payloads are canonical, bounded JSON; nested digest
and identity checks still apply when the outer digest is correct. The caller must
authenticate both descriptors, their kinds and storage versions independently.

`prepare_source_closure_receipt` validates generation coordinates and the exact
completion reference before producing bytes, without constructing a fictitious
receipt. `prepare_frozen_generation` uses the published closure and its next
revision; it verifies the nested payload digest and derives generation identity
from that closure. These functions share the existing descriptor encoders' wire
format. They perform no publication or custody transition. After publication,
the caller must independently read the bound original, compare the expected bytes
and decode with its real external descriptor before attempting the ledger CAS.

The closed schema names are:

- `dpone.native-source-trusted-build-completion.v1`;
- `dpone.native-source-closure-receipt.v1`;
- `dpone.native-frozen-generation.v1`.

Speculative older tags and extra `success` or self-reference fields are rejected.

## Durable transition and recovery requirements

The source-control adapter now has a `record_trusted_build_completion` operation
with an injected `SourceBuildCompletionVerifier`. It compares the exact retained
admission descriptor and current revision, authenticates the positive originals,
and attempts at most one recording mutation. Repeat calls and lost responses use
an independent completion-current read with the whole immutable request. If
current physical ownership cannot be proved, a historical accepted row alone
does not make the call successful. Reconciliation never releases capacity.

The fixed recording/read SQL bodies preserve physical-owner-before-generation
lock ordering. The administrator migration recognizes initial, admission and
completion layouts, checking exact table constraints and procedure definitions
before alteration. It preserves retained identities, admission descriptors,
revisions and charged capacity. Only the phase is backfilled from the existing
executor; no positive completion is inferred during upgrade. Unknown or partial
layouts are rejected, and migration failure rolls back the entire extension.

The migration installs `native_source_freeze_v1`, `native_source_freeze_read_v1`
and `native_source_freeze_inspect_v1` after inspecting all ten definitions.
An existing completion layout can acquire the missing pair and inspection
procedure without rewriting its seven procedures or table contents. An existing
nine-procedure layout acquires only the missing inspection procedure. A partially installed pair or unknown
definition is rejected. The freeze body binds the exact retained admission and
completion, verifies nested closure and frozen payload digests, and requires the
current physical owner before taking the generation lock. Its separate current
read performs no persistent mutation. These installed ledger operations still
require the planned runtime orchestration and original-authentication wiring;
they are not an analyst-facing `freeze` API by themselves.

Isolated SQL Server tests cover valid and substituted 4001/4096-byte ASCII
completion locators, 4096-byte multibyte and supplementary Unicode locators, and
invalid nested receipt digests. Reference extraction uses `OPENJSON` with
`nvarchar(max)` and explicit UTF-8 byte bounds, avoiding `JSON_VALUE` truncation
and SQL NULL comparison bypasses. Rejected requests preserve custody and capacity.
The fixture supplies synthetic retained originals, not production provenance.

The completion layout returns seventeen columns, including phase and complete
payload/locator/digest triples. Its decoder verifies canonical originals while
preserving the admission descriptor's historical revision. Historical ten-column
admission responses remain readable; they cannot supply positive completion.
Old clients reject the new response, so drain old workers before migrating and
deploy compatible runtime and procedures together.

Isolated live SQL tests cover positive reconciliation, lost acknowledgements,
stale physical ownership, charged-capacity preservation, populated upgrades,
rollback and rejected partial originals. Their build-original verifier is an
explicit synthetic test capability. This validates the ledger boundary, **not
production qualification or a complete dbt-to-target route**. Enabling the
verifier dependency alone does not install the required procedures.

Recovery consumers use `SourceBuildCompletionReader.read_completion(reference)`
to resolve the retained positive record and freshly authenticate its complete
cohort. The runtime authenticator implements this capability without cached
success, publication, SQL mutation or dispatch. A valid top-level record is not
enough if any referenced artifact is missing, changed or semantically invalid.

The planned `freeze(reservation, *, expected_revision)` operation reads the exact
accepted completion; callers do not supply a replacement success receipt. It
publishes, binds and independently reads derived immutable descriptors before a
SQL compare-and-swap revalidates custody and transitions BUILDING to FROZEN.
Metadata published before a rejected CAS can remain orphaned, but grants no
authority and changes no capacity charge. Retry must reconcile the complete
original request, not redispatch the build or manufacture new identities.

The runtime implementation is currently an **unreleased candidate**. It resolves
the retained positive completion, publishes and independently reads the closure
and frozen originals, and invokes typed ledger mutation/current-read methods.
Each call performs independent exact current-owner inspection before attempting
at most one metadata CAS, followed by independent accepted-result readback. An
already accepted FROZEN result is reread without mutation. Missing originals,
transport failure or a failed current-owner read cannot be replaced by cached
success or interpreted as proof that an operation never happened.

Freeze is a **metadata-only** transition, not a dbt or BCP dispatch. An explicit
retry, including after process restart, reauthenticates the original completion
and inspects the complete immutable request under the current owner and epoch.
Only an exact eligible BUILDING result permits the metadata CAS; the SQL mutation
revalidates ownership and revision again. An exact accepted FROZEN result requires
no CAS. No automatic mutation retry loop is introduced. This rule must never be
applied to bind/spawn, dbt, or BCP operations that can dispatch work twice.

The `native_source_freeze_inspect_v1` procedure provides this read-only
precondition check separately from accepted-result readback. The runtime and
migration are implemented; qualified producer wiring remains pending.
No additional once-only freeze-claim
table, capability issuer or budget protocol is required for this metadata path.
The candidate is not yet wired as a qualified production workflow.

Freeze orchestration belongs to the runtime layer. Narrowly typed ledger
mutation and current-read capabilities separate it from the SQL adapter; fixed
SQL rendering remains adapter-owned. Descriptor authentication and publication
do not belong in the SQL adapter, which must not import runtime orchestration.

Same-owner UNKNOWN can become ACTIVE only after all positive outcomes authenticate.
An accepted FAILED result conflicts with positive recovery and requires explicit
investigation. Neither a timeout nor a missing acknowledgement releases capacity.

## Current validation boundary

Focused tests exercise exact record shapes, canonical bytes, missing or substituted
references, revision and identity mismatch, forbidden self-reference, and tampered
nested descriptors even with a recomputed outer digest. The live SQL profile
separately exercises completion and migration; it is not installed-route
certification. Freeze, actual qualified producer wiring and their end-to-end
recovery checks remain required before the analyst workflow is operational.

Continue with [trusted invocation execution](native-generation-execution.md) for
the existing owned-command producer and its explicit qualification limits.
