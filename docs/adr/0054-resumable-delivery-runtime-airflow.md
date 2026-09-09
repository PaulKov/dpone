# ADR 0054: REST delivery uses a resumable runtime and read-only Airflow triggers

## Status

Proposed, 2026-08-30. Runtime implementation is blocked until this ADR and the
governed REST bulk-delivery specification are approved.

## Context

`ETLProcessor.run()` currently owns an extracted payload, calls synchronous
`AbstractSink.load()`, marks the payload successful, and then persists source
state. `LoadResult` has no durable pending continuation. Teaching that path to
return before a remote job finishes would risk payload cleanup, checkpoint
advancement, and compatibility for relational sinks.

Airflow deferrable operators release workers while triggers wait. Trigger
instances can be duplicated or restarted, local operator state is not durable,
and deferral does not by itself limit active jobs at the remote service.
`dpone-airflow-pack` is deliberately scheduler-light and depends only on
PyYAML; the full provider already supports Airflow `>=2.10,<3.4`.

## Decision

Introduce a connector-neutral `ResumableDeliveryProcessor` and typed
`DeliverySink` protocol. The compiler and composition root select them for REST
bulk delivery. Preserve `ETLProcessor`, `AbstractSink.load()`, `LoadResult`, and
all relational behavior unchanged. Reuse shared extraction, quality, artifact,
logging, and source-state services through dependency injection rather than
copying policy.

The runtime result is:

```python
RouteExecutionResult = (
    DeliveryCompleted
    | DeliveryPending
    | DeliveryBlocked
)
```

`DeliveryPending` contains only operation id, typed continuation kind, and next
check time. The only automatic V1 continuation is `poll_receipt`, including
observation of an externally cancelled state. Missing-receipt reconciliation or
active cancellation is blocked/operator work, not a generic automatic
continuation. Durable `RETRY_WAIT` is an internal mutation-supervisor state: a
bounded live worker or a restarted task rereads it and performs the guarded due
transition; it never creates another public continuation kind. `DeliveryPending`
can be returned only after payload authority, mutation attempt,
accepted receipt, and continuation bounds are durable. A pending route neither
finalizes its payload nor promotes source state. Active remote cancellation is
outside V1 because a cancel call is another mutation.

V1 `async_bulk_job` is serial across admitted children: at most one child may
be accepted/running/pending for a parent. The parent resumes the next child only
after the prior child is terminal. Bounded-parallel async parent supervision is
deferred to V1.1; synchronous batch profiles may retain certified bounded
parallelism.

Implement the deferrable operator and trigger in
`apache-airflow-providers-dpone`, not `dpone-airflow-pack`.

- `execute` submits or resumes through the runtime and defers on
  `DeliveryPending`.
- The trigger receives only operation id and a non-secret journal locator.
- Trigger code uses a minimal async journal/observation client and lazy imports;
  it cannot import the full dpone runtime, call blocking `requests`, or use a
  synchronous PostgreSQL client in the shared asyncio triggerer process.
- The trigger performs receiver GET/HEAD observation only, under an observation
  lease and bounded distributed poll limits.
- Triggers are duplicate-safe and can emit the same bounded event more than
  once. An event is a wake-up hint, not state authority.
- `execute_complete` rereads the journal, verifies terminal evidence, publishes
  result artifacts, and promotes only an eligible contiguous checkpoint.
- Tokens, certificates, payloads, response bodies, and manifests never enter
  trigger kwargs or events. Each process resolves fresh credentials through the
  immutable connection binding.
- Remote-job reservations live in the same PostgreSQL control plane as the
  journal. Capacity reservation, attempt-scoped marker binding, attempt append,
  and CAS to `SUBMITTING` share one critical transaction. Capacity remains held
  for accepted/running/unknown work even while Airflow worker and ordinary pool
  capacity are released. Unknown work releases capacity only through certified
  expiry plus reconciliation or an audited transfer to incident capacity.

Airflow 3.2.x is the primary acceptance target. Because the provider declares
`apache-airflow>=2.10,<3.4`, the latest supported patch of 2.10, 2.11, 3.0,
3.1, 3.2, and 3.3 is a compatibility target unless the feature range is
explicitly narrowed before implementation. Airflow task retry is orchestration
retry, not authority to repeat an HTTP mutation.

## Consequences

- Relational lifecycle behavior is isolated from asynchronous delivery risk.
- Long receiver jobs release worker capacity while remaining durably observable.
- The full provider gains runtime behavior and tests; the scheduler-side pack
  retains its lightweight dependency boundary.
- Runtime, CLI, and other orchestrators share one continuation contract.
- A second processor introduces a public abstraction and integration work, but
  avoids conditional branches and lifecycle ambiguity in `ETLProcessor`.
- Active remote jobs require PostgreSQL journal capacity distinct from Airflow
  pools and transport request concurrency; no second semaphore authority is
  introduced in V1.

## Validation

- Verify no existing relational tests or public imports change.
- Kill worker and triggerer before/after submit, defer, event, verification, and
  checkpoint boundaries.
- Run duplicate triggers and duplicate events and prove no remote mutation and
  idempotent observations.
- Measure a 30-minute fake remote job: no more than five seconds of worker time
  after initial submission.
- Prove trigger imports and event loops remain bounded with no blocking I/O or
  heavyweight runtime dependency.
- Run the acceptance matrix on the latest patch of every declared Airflow minor
  from 2.10 through 3.3, with 3.2 as primary.
- Prove one in-flight async child per parent, externally cancelled observation
  through `poll_receipt`, and `RETRY_WAIT` recovery without a new continuation
  kind or unproved resend.

## Related decisions

- [Feature design](../feature-design-rest-api-sink-v1.md)
- [ADR 0008](0008-airflow-parse-safe-provider.md)
- [ADR 0039](0039-airflow-activation-identity-separation.md)
- [ADR 0053](0053-rest-delivery-identity-effect-journal.md)
- [ADR 0055](0055-rest-operation-profile-authority.md)
- [Threat model](../rest-bulk-delivery-threat-model.md)
