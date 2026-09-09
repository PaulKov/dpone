# Feature design: durable target commit recovery

- Status: RESEARCHED
- Owner: dpone maintainers
- Issue: global Airflow self-service review for v0.73.1
- Target release: unplanned; blocked on maintainer approval and connector certification
- Last verified: 2026-07-18

## Executive summary

A staged sink can commit target data and then lose its process before evidence,
checkpoint, source state, or Airflow XCom becomes durable. A normal retry can
then execute the same target write again. Process-local state, a typed exception,
and `AirflowFailException` reduce some retry windows but do not close the failure
window between target mutation and durable orchestration evidence.

The required invariant is:

> Before a target mutation can begin, dpone must durably record a stable
> operation identity and establish a target-side idempotency fence or an
> equivalent connector-certified reconciliation contract.

This document records the researched target architecture. It does not approve a
production implementation. No `retryable: false` public contract is shipped by
the current self-service hardening change.

## Personas and journey

| Persona | Goal | Failure without this design | Required success signal |
|---|---|---|---|
| Data engineer | Retry without duplicate writes | A pod dies after commit and Airflow repeats the write | Retry resolves the same durable operation before any mutation |
| Airflow operator | Recover a failed task safely | XCom never appears, so task state cannot identify the commit | Recovery command reports durable journal and target-fence state |
| Platform engineer | Reconcile target and state | Target, evidence, and checkpoints disagree | One operation ID links journal, fence, pack, target, and evidence |
| Connector author | Certify commit semantics | Driver errors do not prove server rollback | Connector supplies a tested fence/reconciliation capability |

Normal authoring and preview remain unchanged. On an unresolved commit state,
automatic execution stops. An operator runs a read-only reconciliation plan,
reviews the evidence, and explicitly applies an approved recovery action.

## Scope

### In scope

- stable operation identity;
- durable commit journal with compare-and-set transitions;
- target-side fence or connector-specific idempotency key;
- process-death recovery before an Airflow retry can mutate the target;
- explicit reconciliation and recovery CLI;
- checkpoint, evidence, and Airflow projection of durable state;
- connector capability and route certification requirements.

### Non-goals

- a distributed transaction across target, journal, source state, and Airflow;
- inferring rollback from a client-side exception;
- treating an Airflow task ID, try number, timestamp, or random UUID as stable
  operation identity;
- automatic recovery when the target cannot prove whether the operation
  committed;
- enabling this contract for an uncertified connector.

## Current-state finding

The reviewed process-local proposal classified exceptions as
`commit_indeterminate` or `committed_incomplete` and attempted to propagate
`retryable: false` through CLI, XCom, and `AirflowFailException`. It was rejected
for production because:

1. the runtime state disappeared with the pod;
2. default and visible-task Airflow paths could retry before an outcome
   evaluator observed XCom;
3. KPO cleanup, XCom extraction, worker loss, and provider failure remained
   post-mutation retry windows;
4. no stable operation identity joined the first attempt to the retry;
5. no durable journal or target fence prevented a second mutation.

The current safe behavior is to avoid claiming duplicate-write prevention until
the durable contract below is implemented and certified.

## Public contract

The names and schemas in this section are proposed, not released.

### Stable operation identity

```text
operation_id = sha256(
  release_id
  + deployment_id
  + dag_id
  + dag_run_logical_identity
  + workload_id
  + mapped_partition_identity
  + canonical_interval
  + canonical_target_identity
)
```

The identity must not contain Airflow try number, runtime pod name, correlation
ID, current time, or a random value. Retries of the same logical workload use
the same `operation_id`.

`intent_digest` identifies the exact target mutation intent:

```text
intent_digest = sha256(
  pack_fingerprint
  + normalized_load_strategy
  + source_snapshot_or_partition_identity
  + target_contract
  + schema_fingerprint
)
```

Reusing an `operation_id` with a different `intent_digest` is a hard conflict.

### Journal port

The domain owns a capability-oriented port:

```python
class TargetCommitJournalPort(Protocol):
    def create_intent(
        self,
        *,
        operation_id: str,
        intent_digest: str,
        metadata: CommitIntentMetadata,
    ) -> CommitJournalEntry: ...

    def compare_and_set(
        self,
        *,
        operation_id: str,
        expected_version: int,
        expected_states: frozenset[CommitState],
        next_state: CommitState,
        evidence: CommitTransitionEvidence,
    ) -> CommitJournalEntry: ...

    def get(self, operation_id: str) -> CommitJournalEntry | None: ...
```

Contracts belong under `dpone.contracts`, the port under `dpone.ports`, and
storage implementations under `dpone.adapters`. Runtime policy imports the port,
not a storage SDK.

### State machine

```text
PREPARING
  -> PREPARED
  -> COMMITTING
  -> TARGET_COMMITTED
  -> COMMITTED_INCOMPLETE
  -> COMPLETE

COMMITTING
  -> COMMIT_UNKNOWN

PREPARING | PREPARED
  -> FAILED_PRE_COMMIT
```

- `PREPARING`: durable intent exists; target fence is not yet confirmed.
- `PREPARED`: target fence/idempotency key is installed.
- `COMMITTING`: mutation may begin; retries must reconcile before writing.
- `COMMIT_UNKNOWN`: the client cannot prove commit or rollback.
- `TARGET_COMMITTED`: target fence proves the mutation committed.
- `COMMITTED_INCOMPLETE`: target committed but state/evidence is incomplete.
- `COMPLETE`: target, source state, checkpoints, and required evidence agree.
- `FAILED_PRE_COMMIT`: target mutation never began and ordinary retry is safe.

### CLI recovery

Proposed commands:

```bash
dpone recovery target-commit inspect --operation-id sha256:...
dpone recovery target-commit plan --operation-id sha256:...
dpone recovery target-commit apply --operation-id sha256:... --action complete-state
```

`inspect` and `plan` are read-only. `apply` requires platform authorization,
optimistic concurrency, an explicit action, and an audit record. No recovery
command accepts secret values or row data.

### Error families

```text
DPONE_TARGET_COMMIT_INTENT_CONFLICT
DPONE_TARGET_COMMIT_JOURNAL_UNAVAILABLE
DPONE_TARGET_COMMIT_FENCE_UNAVAILABLE
DPONE_TARGET_COMMIT_UNKNOWN
DPONE_TARGET_COMMITTED_INCOMPLETE
DPONE_TARGET_COMMIT_RECOVERY_CONFLICT
```

Errors expose safe identities, state, journal version, and recovery reference.
They never expose SQL, credentials, rows, Vault paths, or signed URLs.

## Detailed algorithm

### First attempt

1. Derive stable `operation_id` and `intent_digest`.
2. Create the journal intent using create-if-absent semantics.
3. If the entry exists with another intent digest, fail closed.
4. Ask the connector to prepare a target fence for `operation_id`.
5. CAS `PREPARING -> PREPARED` with safe fence evidence.
6. CAS `PREPARED -> COMMITTING` before the first target mutation.
7. Execute the connector mutation with the same idempotency/fence identity.
8. Query the target fence after the driver returns.
9. CAS to `TARGET_COMMITTED` only when the target proves commit.
10. Persist checkpoints, source state, and required evidence.
11. CAS `TARGET_COMMITTED -> COMPLETE`.
12. Cleanup staging only after the retention/recovery policy permits it.

### Retry or resumed attempt

1. Derive the same `operation_id` and `intent_digest`.
2. Read the journal before source extraction or target mutation.
3. Reject an intent mismatch.
4. For `COMPLETE`, return the recorded result without another mutation.
5. For `FAILED_PRE_COMMIT`, acquire a new CAS lease and retry normally.
6. For `PREPARING` or `PREPARED`, resume fence preparation under CAS.
7. For `COMMITTING`, query the target fence:
   - proven committed: CAS to `TARGET_COMMITTED`;
   - proven not committed: CAS back to a connector-approved retry state;
   - unknown: CAS to `COMMIT_UNKNOWN` and stop.
8. For `TARGET_COMMITTED` or `COMMITTED_INCOMPLETE`, complete state/evidence
   without rerunning the target mutation.
9. For `COMMIT_UNKNOWN`, require explicit reconciliation.

### Pseudocode

```text
entry = journal.create_or_get(operation_id, intent_digest)
assert entry.intent_digest == intent_digest

match entry.state:
    COMPLETE:
        return recorded_result
    COMMIT_UNKNOWN:
        stop_for_reconciliation()
    TARGET_COMMITTED | COMMITTED_INCOMPLETE:
        complete_state_and_evidence()
    COMMITTING:
        reconcile_target_fence()
    PREPARING | PREPARED | FAILED_PRE_COMMIT:
        acquire_cas_lease()
        prepare_fence()
        journal.cas(PREPARED, COMMITTING)
        mutate_target(operation_id)
        reconcile_target_fence()
```

## Connector capability contract

A connector can enable automatic recovery only when it certifies one of:

- target-native idempotency key with queryable final state;
- transactional commit marker written atomically with target data;
- deterministic staging/swap protocol with queryable generation;
- another route-certified fence with equivalent behavior.

Without such a capability, `COMMITTING` after process loss becomes
`COMMIT_UNKNOWN`; automatic replay is forbidden.

## Concurrency and ordering

- journal transitions use compare-and-set and monotonically increasing versions;
- only one active executor lease may mutate an operation;
- stale lease takeover never bypasses target reconciliation;
- journal storage must provide read-after-write consistency for one operation;
- target fence installation happens before `COMMITTING`;
- evidence publication cannot move the journal backward;
- recovery commands also use CAS and fail on concurrent modification.

## Failure semantics

| Failure point | Durable state | Automatic action |
|---|---|---|
| Before journal intent | No operation | Ordinary retry |
| Journal unavailable | No mutation allowed | Retry journal access |
| Fence preparation fails | `PREPARING` | Connector-approved retry |
| Process dies in `COMMITTING` | `COMMITTING` | Reconcile target before write |
| Target proves commit | `TARGET_COMMITTED` | Complete state/evidence only |
| Target proves rollback/no mutation | Connector-certified state | Retry mutation |
| Target cannot prove outcome | `COMMIT_UNKNOWN` | Manual reconciliation |
| State/evidence fails after commit | `COMMITTED_INCOMPLETE` | Resume completion only |

Secondary evidence, cleanup, or logging failures must not replace the primary
journal/fence outcome.

## Airflow integration

Airflow remains an adapter:

- KPO receives stable `operation_id`;
- retries invoke the same recovery-aware runtime entry point;
- provider exceptions improve task UX but are not the correctness boundary;
- `AirflowFailException` may be used for `COMMIT_UNKNOWN`, but the durable
  journal and target fence prevent duplicate mutation even if XCom is lost;
- DAG parse performs no journal, target, secret, or network access.

## Migration and rollout

1. Add schemas, port, and an in-memory test adapter.
2. Implement one durable journal adapter with CAS.
3. Add connector fence capability metadata.
4. Certify one route end to end.
5. Introduce read-only recovery CLI.
6. Shadow-record operation identities without changing retry behavior.
7. Enable enforcement for the certified route behind a deployment policy.
8. Add recovery apply actions after operational review.
9. Expand only through route certification.

Existing workloads remain on legacy retry semantics until explicitly opted into
a certified deployment policy. No compatibility shim may advertise durable
protection when it only stores process-local state.

## Test and certification plan

| Layer | Required scenario | Evidence |
|---|---|---|
| Unit | identity determinism and intent conflict | Focused pytest |
| Contract | CAS transitions and illegal-state rejection | Journal conformance suite |
| Fault injection | hard process kill before/during/after target mutation | Durable journal + target snapshot |
| Concurrency | two executors race for one operation | Exactly one target mutation |
| Airflow | `retries=3`, worker/pod loss, XCom loss | One operation and one target mutation |
| Recovery | inspect/plan/apply with concurrent modification | Audit artifact |
| Live route | approved MSSQL/ClickHouse route | Row parity and fence evidence |

Mandatory acceptance:

- killing the runtime process with `SIGKILL` at every transition never produces
  a second target mutation for a certified route;
- two independent workers using the same operation ID cannot both mutate;
- an intent digest conflict fails before source or target I/O;
- unknown target outcome never becomes success or automatic replay;
- live checks are reported `UNVERIFIED` when the environment is unavailable.

## Security and privacy

The journal stores content identities, state, safe target identity, versions,
timestamps, and audit references. It does not store credentials, secret values,
Vault paths, row values, SQL, signed URLs, or raw connector responses. Journal
and recovery writes require workload or platform identity and are auditable.

## Market comparison

N/A for product differentiation. This is a correctness requirement. Airflow
task exceptions are orchestration behavior, not a transaction or idempotency
mechanism.

## Documentation plan

Before implementation, add:

- operation identity reference;
- connector fence authoring guide;
- target-commit recovery runbook;
- journal adapter conformance guide;
- Airflow retry behavior and limits;
- route certification evidence template.

## Approval checklist

- [x] Problem and failure window are documented.
- [x] Stable identity, state, ordering, concurrency, and recovery are defined.
- [x] Dependency direction and connector capability boundary are defined.
- [x] Hard-process-kill acceptance is explicit.
- [ ] Maintainer approves public schemas and recovery CLI.
- [ ] Durable journal adapter is selected.
- [ ] First connector fence is designed and route-certified.
- [ ] Live failure-injection environment is approved.
