# Closing native source admission

This reference is for platform engineers implementing native generation control.
Closing admission means **no further launch is admitted**. It does not mean a
running command has stopped, a build succeeded, or capacity can be released.
The analyst-facing workflow remains under implementation.

Start with [generation admission](native-generation-admission.md) and the
[trusted invocation recorder](native-generation-execution.md). This component
extends their durable SQL metadata boundary; it is not a complete writer-ledger
implementation or a delivery-route certification.

## Prerequisites and migration

The administrator installs `MssqlNativeGenerationSchemaMigration` using the
existing authenticated control authority and registered runtime principal.
Runtime connections must remain dedicated, bounded, and least privileged.
Existing workspace ownership and native-original registrations are prerequisites.
Before changing the ledger, the migration verifies the registered principal's
identity and checks its effective database-owner and control-schema ALTER
permissions. Excessive permissions are rejected; the migration does not silently
remove role membership to make an unsafe runtime account pass.

The migration recognizes either the exact initial generation layout or the exact
extended layout. Initial generation rows, reservation bytes, executors, authority
coordinates and physical capacity charges are retained. Known initial rows gain
OPEN/ACTIVE admission metadata and sequence zero before binding or one afterward.
The initial revision constraint and three procedure definitions are explicitly
upgraded in the administrator's transaction. An error rolls back that transaction;
unknown definitions, partial ledgers and weakened constraints are rejected.

Constraint recognition preserves AND/OR grouping: the migration compares the
actual definition with SQL Server's rendering of the expected predicate, using
an empty session-local temporary table that is removed immediately. It does not
accept a predicate merely because its tokens match after removing parentheses.

**Coordinate runtime and schema rollout.** The extended custody response has ten
columns instead of six. An old adapter rejects it and rolls back its transaction;
it cannot silently report OPEN after a newer runtime closed admission. The new
adapter also requires the extended response. Do not run mixed old/new adapters
against this ledger during rollout. Existing Python method signatures for
reservation and writer binding remain unchanged.

## API and observable result

After the writer was bound, obtain the current authenticated snapshot and call:

```python
snapshot = control.read_custody(reservation.generation_id)
closure = control.close_writer_admission(
    reservation, expected_revision=snapshot.revision
)
```

This example starts from OPEN admission in BUILDING with ACTIVE or UNKNOWN
outcome. Keep the exact request revision for reconciliation. The SQL transition
checks the current physical owner and epoch and performs a compare-and-swap;
it never holds one SQL transaction open across a dbt command.

`SourceAdmissionClosure` contains the exact `executor`, `admission_sequence`,
new `revision`, and external `receipt`. Its canonical payload has schema
`dpone.native-source-admission-closure.v1` and excludes its own receipt reference.
The separate readback returns payload, locator and digest. The adapter verifies
canonical bytes, digest, exact generation, complete executor and expected revision
through an independent connection before returning.

Use `control.read_admission_closure(generation_id)` to read a retained descriptor.
After successful metadata closure the custody snapshot has:

- state `BUILDING` and writer admission `CLOSED`;
- the unchanged ACTIVE or UNKNOWN outcome;
- `closure=None`, because that field is reserved for positive build completion;
- unchanged reservation and charged capacity.

## Recovery boundaries

An acknowledgement can be lost after SQL commits. The adapter performs one
mutation attempt followed by independent readback. Exact retained metadata can
prove closure; no second dbt invocation or second SQL closure mutation is issued.
Repeating the exact closure request with its original expected revision reads
the same descriptor. Another revision or reservation is not the same request.

If commit did not occur or readback cannot prove the exact descriptor, the method
raises `NativeGenerationAdmissionError`. Retain ownership and capacity. Do not
interpret timeout, absent output or an exception as proof that work stopped.
Same-owner UNKNOWN may be closed as metadata, but remains UNKNOWN afterward.

The local invocation recorder must separately stop local admission and join its
owned execution. Durable closure alone cannot stop a disconnected executor.
Positive completion, freeze, quality, export, seal and cleanup are separate
protocol steps and are not implemented by this closure method.

## Validation scope

Focused tests cover canonical payloads, forbidden self-reference fields, exact
integer bounds, unchanged initial procedure identities, metadata replay, unknown
outcome retention and lost acknowledgements. Synthetic ports are not SQL evidence.

The opt-in `tests/test_native_source_admission_closure_live.py` suite exercises
real isolated SQL transitions, retained charges, stale physical ownership,
constraint tampering, populated initial-layout upgrade, rollback after procedure
replacement, and rejection of excessive runtime permissions before upgrade.
Run it only in an approved disposable SQL environment with
`DPONE_RUN_NATIVE_ORIGINAL_MSSQL_LIVE=1` and the existing test connection inputs.
The enclosing runner must remove its owned SQL container after any failure.
Until a current live report exists, these cases remain unverified; their presence
in source is not a passing certification.

Continue with [trusted invocation execution](native-generation-execution.md) for
the normal-return evidence that must precede future positive completion and freeze.
