# Feature design: native-transfer target commit guard

- Status: APPROVED
- Owner: dpone maintainers
- Issue: global Airflow self-service review for v0.73.1
- Target release: next patch after 0.73.1
- Last verified: 2026-07-18

## Executive summary

The current native-transfer checkpoint store writes a batch sequentially. If a
target mutation succeeds and a later checkpoint write fails, some partitions
can become `COMMITTED` while the remaining partitions become ordinary
`FAILED`. The resume planner retries every non-committed partition, so an
`incremental_append` route can repeat a target mutation and duplicate data.

This approved bug fix adds a bounded, fail-closed target-commit guard to the
existing open checkpoint `diagnostics` map. It does not implement the durable
journal, compare-and-set transitions, stable operation identity, target fence,
or certified automatic reconciliation proposed by ADR-0022. Its measurable
goal is narrower:

> After dpone has armed a target mutation, no later automatic attempt may call
> the sink while any route checkpoint has an unresolved or malformed guard.

## Personas and journey

| Persona | Goal | Current failure | Required behavior |
|---|---|---|---|
| Data engineer | Resume without duplicate append | Partial checkpoint persistence makes one partition retryable | Retry stops before target I/O |
| Airflow operator | Preserve the real failure | Cleanup/reconciliation can replace the primary exception | Original exception and traceback remain authoritative |
| Platform engineer | Reconcile deliberately | Query/schema changes can hide the older unsafe attempt | Route-level guard scan still blocks |
| Connector author | Keep current APIs stable | A new checkpoint enum/store migration would expand blast radius | Existing status/store contracts remain unchanged |

The operator sees a stable manual-reconciliation error, inspects the target and
checkpoint state through approved operational procedures, and repairs the
checkpoint before execution resumes. Automatic repair is explicitly out of
scope.

## Scope

### In scope

- durable pre-target guard records using existing checkpoint diagnostics;
- one batch correlation digest for all active partitions;
- route-level unresolved-guard validation before target I/O;
- exact reconciliation of partially persisted committed candidates;
- best-effort incomplete/unknown markers without masking the primary error;
- one injected transition timestamp per terminal checkpoint batch;
- staged, legacy, file-artifact, and lazy-plan native-transfer paths;
- compatibility, recovery, event-order, and failure-injection tests.

### Non-goals

- automatic reconciliation;
- target-side idempotency or fencing;
- concurrent-worker exclusion;
- a stable retry operation identity;
- CAS or atomic multi-row checkpoint storage;
- claiming duplicate-safe retry certification;
- changing checkpoint schemas, statuses, serializers, or store ports.

## Public contract

The existing `PartitionCheckpoint.diagnostics` map may contain:

```json
{
  "artifact_sha256": "sha256:...",
  "target_commit_guard": {
    "schema_version": "dpone.native_transfer.target_commit_guard.v1",
    "checkpoint_batch_id": "sha256:...",
    "phase": "armed",
    "recovery": "manual_reconciliation_required"
  }
}
```

Allowed phases:

| Checkpoint status | Guard phase | Retry meaning |
|---|---|---|
| `EXPORTED` | `armed` | Target outcome cannot safely be inferred; block |
| `FAILED` | `failed_before_target` | Target did not start; normal retry is allowed |
| `COMMITTED` | `checkpoint_committed` | Exact terminal candidate is durable |
| `FAILED` | `checkpoint_incomplete` | Target succeeded but checkpoint is not proven durable; block |
| `FAILED` | `target_outcome_unknown` | Target call began without conclusive success; block |

Unknown schema versions, malformed values, status/phase mismatches, and
unsupported phases fail closed with
`native_transfer_target_commit_guard_invalid`.

An unresolved guard raises
`native_transfer_manual_reconciliation_required` before target I/O. Safe
diagnostics may include route identities, phase, and checkpoint batch ID. They
must never include SQL, row values, credentials, connector responses, exception
messages, or Vault paths.

`checkpoint_batch_id` is SHA-256 over the current run ID and sorted active
partition IDs. It is attempt correlation only. It is not ADR-0022's stable
operation ID, journal identity, fence, or idempotency key.

Existing checkpoints without a guard retain legacy resume behavior. Protection
starts with attempts created by this version.

## Algorithm

1. Load all latest route checkpoints for the same source table, target table,
   and strategy before deriving per-partition resume decisions.
2. Reject any unresolved or malformed target-commit guard. This route scan is
   independent of the current query, schema, and partition IDs.
3. Complete extraction, staging, and pre-target governance.
4. Immediately before `sink.load()` or `finalize_staged_load()`:
   - derive one checkpoint batch ID;
   - capture one injected UTC transition timestamp;
   - persist `armed` checkpoints for every active partition;
   - do not invoke the target unless the complete guard batch write succeeds.
5. Mark process state when target invocation begins.
6. If target invocation raises, best-effort persist
   `target_outcome_unknown`, then re-raise the original exception object with
   its traceback unchanged.
7. When target returns success, mark that fact in process state before any
   checkpoint promotion.
8. Build all `COMMITTED/checkpoint_committed` candidates with the same
   transition timestamp. Their `started_at` and `completed_at` are equal.
9. If the commit batch succeeds, continue with evidence and source-state
   advancement.
10. If the commit batch partially fails:
    - reconcile only exact current candidates;
    - never downgrade an exact durable committed candidate;
    - best-effort mark unresolved candidates
      `FAILED/checkpoint_incomplete`;
    - if reconciliation or the secondary write fails, leave the durable
      `armed` guard in place;
    - re-raise the original checkpoint exception unchanged.
11. A later attempt stops before sink mutation until an operator reconciles and
    repairs the route.

Ordering is:

```text
resume guard validation
  -> pre-target work
  -> armed checkpoint batch
  -> target mutation
  -> committed checkpoint promotion
  -> evidence
  -> source state
```

## State, retries, and compatibility

An Airflow task may still retry. Every retry must encounter the durable guard
and stop before target I/O. The public error must not advertise
`retryable: false`; the correctness boundary is the runtime guard.

`failed_before_target` and legacy checkpoints without a guard remain retryable.
Current and old checkpoint payloads round-trip unchanged because the diagnostic
map is already part of the public schema.

Mixed-version workers and rollback are unsafe: old planners ignore the guard.
During rollout, platform operators must drain older workers or disable
automatic execution. This migration warning is required in the changelog and
runtime recovery documentation.

## Architecture

Policy remains in the native-transfer checkpoint lifecycle and resume planner.
ETL/governance coordinators receive thin callbacks at the target-mutation
boundary. No vendor SDK or orchestrator dependency enters the runtime domain.
No new generic transaction abstraction is introduced.

The design amends ADR-0022 as an interim bounded mitigation. ADR-0022 remains
`Proposed`; it is still required for concurrency exclusion, CAS, process-death
recovery, target fencing, automatic reconciliation, and duplicate-safe route
certification.

## Test and evidence plan

Required focused cases:

1. partial commit plus reconciliation-read failure preserves the exact original
   exception;
2. partial commit plus incomplete-marker write failure preserves the original;
3. `incremental_append` retry after partial commit stops before sink invocation;
4. a surviving `armed` record blocks retry;
5. route guard still blocks after query/schema/partition identity changes;
6. partial guard persistence prevents the first target invocation;
7. `failed_before_target` and legacy checkpoints remain retryable;
8. malformed/unknown guards fail closed;
9. all terminal candidates use one injected timestamp;
10. staged and legacy events prove guard-before-target ordering;
11. existing checkpoint payloads round-trip without migration.

Focused tests and the change-aware broad validation plan are implementation
evidence. Live route certification is `UNVERIFIED` unless an approved
MSSQL/ClickHouse environment is actually available.

## Documentation and rollout

Update checkpoint/recovery documentation, ADR-0022, compatibility guidance, and
the changelog. Do not describe this guard as a durable journal, target fence,
or complete exactly-once mechanism.

Market comparison is `N/A`: this is a fail-closed correction of an internal
contract, not a new competitive capability.
