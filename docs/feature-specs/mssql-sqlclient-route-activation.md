# MSSQL SqlClient route activation

Status: **APPROVED**

Parent design: [Data delivery acceleration](../feature-design-data-delivery-acceleration-v1.md),
[ADR 0072](../adr/0072-bounded-tds-importer.md) and the normative route-activation
decision in [ADR 0073](../adr/0073-mssql-sqlclient-route-activation.md).

## Problem and journey

The bounded SqlClient worker reaches durable `VERIFIED`, but the ordinary
ClickHouse to MSSQL native composition still rejects the authored
`mssql_sqlclient` backend. A platform operator therefore cannot select the
qualified worker for a real native route, recover it after a restart, or retire
its exact staging object after publication or rollback.

The operator authors an explicit SqlClient transport and injects an admitted
installation, coordinator, restricted writer, durable state and parent
publication authority at the composition root. Planning fails before source I/O
when any capability is absent. A run reads one frozen ClickHouse window, writes
bounded sealed chunks, verifies each exact stage through P10f, publishes once,
retires every exact stage and only then commits the source checkpoint.
Omitting `transport` keeps the existing BCP behavior.

## Public behavior

- `mssql_sqlclient` is selectable only for the finite layouts admitted by ADR
  0071 and only when a deployment supplies the complete capability bundle.
- An authored backend is never silently replaced by BCP or another backend.
- A verified stage is represented by a credential-free receipt derived from the
  exact P10f record and durable attempt state. Private terminal fields are not a
  public recovery API.
- Recovery inspects the exact object incarnation without reopening the source
  or resending CREATE, credentials, permission grants or bulk input.
- Cleanup of verified staging requires a matching settled parent authority:
  either a publication receipt or an authoritative abort/rollback receipt.
- A known DROP result is insufficient. Retirement completes only after exact
  object absence, durable `RETIRED`, closed directory admission and observed
  capacity release.
- Unknown publication, containment, DROP or absence retains custody and blocks
  replay and checkpoint advancement.

No new fallback, arbitrary SQL, CDC, implicit type coercion, public credential
surface or change to the BCP default is introduced.

## Algorithm and ordering

1. Freeze the half-open source window and transport policy into the native plan.
2. Resolve the exact backend capability before source I/O.
3. For each bounded sealed chunk, create a fresh attempt and execute the existing
   P7 through P10f chain with the admitted installation and principals.
4. Project the P10f terminal into the single closed
   `dpone.sqlclient.native-chunk-receipt.v1` `NativeChunkReceipt`. It binds the
   attempt, canonical object incarnation, rows, typed multiset digest, bounded
   additive `typed_sum`, input file, registration and verification evidence,
   durable custody, implementation identity and lifecycle revision. Inspector,
   preparation and retirement consume this same representation without adapters
   that weaken or reconstruct evidence.
5. Persist the receipt in the parent chunk journal. Keep SqlClient sealed input
   under parent custody; recovery uses a source-free
   inspector and the durable attempt/directory records to reproduce the same
   receipt or return UNKNOWN.
6. Seal all attempt directories before parent publication.
7. Publish the verified stages through the existing atomic finalizer and persist
   the parent publication receipt before cleanup. On known prepublication
   rollback, persist a versioned abort receipt instead.
8. Rehydrate each exact attempt under a newer fence, prove the parent authority,
   advance `VERIFIED -> CONTAINMENT_REQUIRED -> CONTAINED`, authorize retirement,
   reserve the retirement operation, DROP the exact owned incarnation and prove
   absence.
9. Persist local and remote settlement, advance to `RETIRED`, close admission,
   observe released capacity and remove only source files owned by the settled
   parent attempt.
10. Persist an ordered parent retirement receipt. Commit the source checkpoint
    only after publication, retirement and parent
    evidence are durable.

### Failed-attempt settlement before retry

For a verified successful chunk, P10g first persists its exact `VERIFIED`
projection, then consumes invocation-scoped attempt custody. The settled parent
authority advances the durable lifecycle without a fabricated error, seals the
directory, and reserves the deterministic RETIRE operation under the existing
fence. The custody entry is one-shot and is closed after the durable reservation.
If the process restarts before that handoff, only observation and takeover under a
newer fence may reconstruct writer authority.

An acknowledged writer error is not retry authority. Production composition
uses one nominal failed-attempt settlement capability instead of arbitrary
eligibility and settlement callbacks, and reuses the existing attempt lifecycle,
directory and P10g exact-retirement machinery.

1. Classify the acknowledged `TdsAttemptError` with a closed policy. Only
   `connection`, `driver`, `startup_timeout`, and `operation_timeout` are retry
   candidates. Resource, decoder, constraint, permission, protocol, ownership,
   fencing and verification failures remain terminal for the invocation.
2. Retain the predecessor input and do not create the next attempt. Rehydrate
   its exact identity from the parent journal's sealed-file record and canonical
   identity derivation under the current target/window lease.
3. Continue containment without repeating CREATE, grant, credentials or bulk
   input. Eligibility exists only after the process is known reaped, credential
   channels are closed, lifecycle is `CONTAINED`, work is sealed and every
   directory slot is settled. Unknown observation retains custody.
4. Bind the settlement request to the full attempt identity and object
   incarnation, input-custody digest, error observation, lease owner/fence, and
   exact lifecycle and directory revisions and state digests.
5. Persist a deterministic retirement reservation and DROP intent before SQL.
   Under the canonical DDL lock, revalidate lease, revisions and incarnation.
   Execute DROP once or reconcile an existing intent; never blindly resend an
   unknown outcome.
6. Observe exact absence, persist remote settlement, advance
   `CONTAINED -> RETIREMENT_REQUIRED -> RETIRED`, close directory admission and
   observe capacity release.
7. Persist a request-bound failed-attempt settlement receipt. Only that receipt
   changes a candidate from `RETRY_PENDING_SETTLEMENT` to `RETRY_READY`, after
   which the scheduler may record attempt `n + 1`. Replay returns the identical
   receipt without a second destructive effect.

Unknown process, SQL, DROP, absence, authority or revision outcomes report
`MANUAL_RECONCILIATION_REQUIRED`. Deterministic non-retryable failures report
`TERMINAL_INPUT_OR_POLICY`. Output includes a stable classification, attempt id,
target/window-safe identity digest, durable phase, custody-retained flag and
recovery action; it excludes credentials and physical object names.

The production implementation constructs every fresh attempt through the
stage-bound PREPARED factory and retains input through one canonical custody
receipt. Release after retirement is serialized by a durable intent/receipt
journal. If the process loses the release acknowledgement after intent, the
route stops and does not repeat the destructive effect. Operator reconciliation
uses only a deployment-owned source-of-truth observer for an exact release
receipt bound to the existing request; callers cannot submit a receipt, reset
intent or invoke release.

Retries create a new attempt only after the predecessor is durably retired.
Concurrent attempts for one target/window remain excluded by the existing
fenced lease. Lost acknowledgements enter observation or reconciliation; they
never resend the original effect.

## Components and dependency direction

- A narrow application-layer SqlClient chunk importer implements the existing
  `NativeChunkImporter` port.
- Runtime composition selects an injected importer factory by the exact authored
  backend; runtime code does not import application implementations.
- A service-layer terminal projector exposes immutable P10f facts.
- A source-free inspection service and a retirement service use injected state,
  coordinator and SQL observation ports.
- The parent publication journal gains a versioned abort authority while
  preserving all existing readers and BCP semantics.
- Deployment packaging supplies an immutable .NET companion inventory and lock;
  base imports and CLI help never import vendor SDKs or perform I/O.

## Failure and compatibility rules

Existing manifests with omitted transport remain byte-for-byte compatible.
Existing native chunk journal versions remain readable. New parent abort state is
versioned and migrates only by decoding old states into their original meaning;
no old state is inferred to be aborted. Unsupported layouts, missing deployment
capabilities and installation drift fail before ClickHouse acquisition.

Recovery after `published + retired + checkpoint acknowledgement lost` repeats
only the checkpoint CAS and reconstructs the same result from parent evidence. It
does not restore stages or read the source. The deployment capability publishes
finite fresh, retirement and parent actor reserves; admission proves their sum
against authored parallelism before any source read.

The legacy `dpone.sqlclient.failed-eligibility.v1` record remains readable only
as `legacy_unqualified`. It cannot authorize DROP or retry and is never inferred
or upgraded into the exact settlement contract. Omitted transport and BCP state
remain byte-compatible.

## Verification and definition of done

- Unit and contract tests cover selector failure-before-source, exact terminal
  projection, source-free inspection, published and aborted retirement,
  unknown DROP/absence, takeover and replay exclusion.
- Fault tests cover each acknowledged error class, settle-before-attempt+1,
  stale fence/revision rejection, every crash prefix from containment through
  the final receipt, single-effect concurrent settlement, source-free recovery
  and byte-identical replay.
- BCP compatibility tests prove omission still selects BCP.
- Exact-source Docker tests execute narrow and 100-column P7 through P10f with
  the production helper and final source commit.
- An approved private seven-day window executes through the actual SqlClient
  route with partition pruning, bounded chunks, typed reconciliation,
  publication and retirement. Corporate identifiers, credentials and raw
  measurements remain outside the repository.
- The full non-live suite, architecture, module-size, documentation, packaging
  and independent fresh-context review pass on the immutable release commit.
- Documentation states measured scope precisely; no market-leading performance
  claim is made without a reproducible comparison artifact.

The feature is complete only when all items above are current PASS evidence.
