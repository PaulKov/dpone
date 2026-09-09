# ADR 0053: REST delivery separates intent, remote effect, and durable authority

## Status

Proposed, 2026-08-30. Runtime implementation is blocked until this ADR and the
governed REST bulk-delivery specification are approved.

## Context

An HTTP client can lose its response after a receiver has already mutated data.
A local task retry, expired worker lease, or changed payload must not create a
second logical mutation. One overloaded operation id or terminal status cannot
express logical scope, changed intent, acknowledgement, partial application,
verification, and checkpoint eligibility.

Adaptive batching adds another hazard: replanning by rerunning a mutable source
query can produce different child payloads. Bounded parallel delivery can also
complete later windows before an earlier gap. Neither condition may advance the
source checkpoint incorrectly.

Replacement adds a generation-order hazard: a delayed older operation can
overwrite a newer overlapping scope, and different planner versions can create
different but intersecting child windows. Journal failover/PITR adds another
authority hazard: a receiver mutation is not rolled back when PostgreSQL state
is rewound.

The current route execution stores include local JSON/SQLite implementations,
and immutable object-storage artifacts already have a repository port. V1 needs
one explicitly selected production control-plane authority with compare-and-swap
semantics, database time, recovery, and retention.

## Decision

### Identity

Use versioned canonical SHA-256 authorities:

```text
operation_namespace = H(
  platform_instance_id,
  tenant_id,
  environment_id,
  namespace_revision
)

delivery_key = H(
  logical_route_id,
  target_effect_scope,
  source_boundary,
  mutation_kind,
  explicit_generation
)

receiver_mutation_key = H(
  operation_namespace,
  delivery_key
)

receiver_binding_semantics_digest = H(
  authority_origin,
  remote_principal_id,
  remote_account_id,
  remote_resource_namespace,
  authentication_semantics,
  tls_policy_identity,
  network_policy_identity
)

operation_semantics_digest = H(
  method_and_request_semantics,
  acknowledgement_contract,
  mutation_fence_contract,
  verification_contract
)

rendered_request_semantics_digest = H(
  normalized_path_query_headers,
  complete_canonical_body_envelope,
  scalar_multipart_parts,
  deterministic_filename_boundary_and_metadata,
  sealed_payload_digest
)

mutation_intent_digest = H(
  receiver_binding_semantics_digest,
  operation_semantics_digest,
  rendered_request_semantics_digest
)

execution_policy_digest = H(
  timeout_policy,
  retry_policy,
  distributed_rate_policy,
  retention_policy,
  observability_policy
)

profile_contract_digest = H(
  operation_semantics_digest,
  execution_policy_digest,
  profile_schema_id,
  profile_version
)
```

Unique admission is `(operation_namespace, delivery_key)`. The same key and
intent resume one durable operation. The same key with a different intent fails before transport with
`REST_DELIVERY_INTENT_CONFLICT`. A new target replacement requires a new
explicit generation. Airflow try number, runtime retry, policy changes, and
payload digest do not implicitly create a generation. A receiver idempotency
key is `H(operation_namespace, delivery_key)` and never includes payload digest.
Policy-only changes do not alter
`mutation_intent_digest`; the complete `profile_contract_digest` is still pinned
in the immutable parent plan. Stable remote principal/account/resource ids come
from a certified connection handshake; a principal swap changes binding
identity even when the URL is unchanged.

For replacement, generation is the ordered tuple
`(scheduled_interval_end_utc, explicit_correction_revision)`. The effect
conflict domain binds remote principal/account/resource namespace, receiver
resource, and the platform-owned `effect_conflict_group`. Operation namespace
and mutation kind cannot split physically conflicting receiver effects.
`partition_replace` conflicts by canonical scope overlap, not scope equality. A lower overlapping generation
cannot enter `SUBMITTING` after a higher admission; a higher generation cannot
submit while a lower overlap is submitting, undetermined/running, unknown, or
partial. A fully applied generation permanently stales every lower overlap.
Stable failures are `REST_DELIVERY_STALE_GENERATION` and
`REST_DELIVERY_OVERLAPPING_EFFECT_IN_FLIGHT`.

`incremental_append` does not permit correction revision by default. Repeating
an already applied exact append scope fails with
`REST_DELIVERY_APPEND_SCOPE_ALREADY_APPLIED`; opt-in requires a separately
certified receiver dedup/upsert, natural-idempotency, replacement, or
compensating-correction contract.

The PostgreSQL adapter is the selected linearization mechanism. It canonicalizes
scopes as half-open range values, creates/locks one effect-domain row using
`SELECT FOR UPDATE`, queries overlaps with a GiST-backed `&&` predicate, checks
unresolved operations plus interval watermarks, and inserts admission in that
same transaction. Locks are acquired in canonical domain order. Fully applied
replacement effects split/merge a normalized non-overlapping interval map of
`(conflict_scope_range, highest_applied_generation)` under the same lock, so a
non-overlapping lower generation is not made stale.

### Outcome axes

Persist independent axes:

- execution: `PLANNED`, `SEALED`, `SUBMITTING`, `RETRY_WAIT`, `ACCEPTED`, `WAITING`,
  `VERIFYING`, `FINALIZED`;
- remote effect: `NONE_PROVEN`, `UNDETERMINED`, `FULLY_APPLIED`,
  `PARTIALLY_APPLIED`, `UNKNOWN`;
- acknowledgement: `NOT_ACCEPTED`, `ACCEPTED`, `RUNNING`, `SUCCEEDED`,
  `FAILED`, `CANCELLED`, `EXPIRED`, `PROTOCOL_UNKNOWN`;
- verification: `NOT_RUN`, `PASSED`, `FAILED`, `NOT_PROVABLE`;
- checkpoint: `BLOCKED`, `ELIGIBLE`, `PROMOTED`.

A checkpoint becomes eligible only when the remote effect is fully applied and
verification passed. V1 partial application always blocks promotion.
`UNDETERMINED` is normal accepted/running work; `UNKNOWN` is lost or
contradictory proof. The initial tuple, exhaustive legal tuples/transitions,
terminality, monotonic fields, operator-only reconciliation, and CAS conditions
are normative in `docs/rest-bulk-delivery-design-contract-v1.yaml`. Unlisted
axis combinations are invalid.

Operator reconciliation preserves the observed acknowledgement while new
immutable read-only receiver evidence resolves the effect. Every unknown
acknowledgement variant has legal no-effect, full, partial, and still-unknown
outcomes. A proven `FULLY_APPLIED + PASSED` tuple is checkpoint eligible even
when the original acknowledgement was lost, failed, cancelled, expired, or
only accepted.

Positive `REQUEST_NOT_STARTED` evidence with remaining budget transitions to
`RETRY_WAIT`; a due retry preserves the delivery key and increments the attempt
ordinal before a new attempt marker. The terminal not-started tuple is reserved
for exhausted, permanently blocked, or abandoned retry. Legal tuples preserve
the latest acknowledgement independently: `SUCCEEDED`, `FAILED`, `CANCELLED`,
`EXPIRED`, `ACCEPTED`, and `NOT_ACCEPTED` retain their value while effect is
classified partial or unknown. Reconciliation preserves it until new immutable
receiver evidence proves another acknowledgement.

### Production journal and artifacts

PostgreSQL 15+ in a dedicated control-plane database/schema is the only V1
production `DeliveryJournalPort` adapter. It provides unique admission by
operation namespace/delivery key, immutable intent binding, revision CAS,
database-clock leases, transactional attempt/observation append, tenancy,
migrations, backup, PITR, restore testing, monitoring, and capacity policy.
SQLite remains local-only.

Critical parent/generation admission, `SUBMITTING`, receipt, remote-effect, and
checkpoint transactions have `mutation_admission_rpo: zero` and
`submit_attempt_rpo: zero`. They require a certified synchronous replication
quorum with `synchronous_commit=remote_apply`; submission is unavailable when
the quorum or its proof is unavailable.

Before `SUBMITTING`, create an immutable independently retained `MAY_ATTEMPT`
marker keyed by `(operation_namespace, operation_id, attempt_ordinal)` and bind
its attempt id, expected journal revision, epoch, generation, and intent. In one
PostgreSQL critical transaction, reserve remote-job capacity, append the
attempt/marker binding, and CAS to `SUBMITTING`; only then execute HTTP. A marker
without its exact journal attempt proves possible attempt, not safe absence.
Remote capacity is journal authority, not an external semaphore backend, and
unknown/partial work retains it until reconciliation or audited transfer.
Journal failover/restore uses an epoch. If zero-RPO continuity is
not proved, the environment mutation gate is `BLOCKED` until markers, journal
rows, receipts, and receiver effects are reconciled and a new epoch is
approved. PITR never implies receiver rollback.

Object storage remains authority for encrypted immutable parent, payload,
result, and evidence objects. PostgreSQL stores pinned artifact identities, not
payload bodies.

Use separate mutation, observation, and checkpoint leases. Once `SUBMITTING`
is durable, mutation-lease expiry never authorizes a new POST/PATCH. A new
worker can observe or reconcile only unless exact remote absence is proven or a
certified receiver fence authorizes same-key replay. A local fence cannot stop
an already running remote request.

Persist compact effect authority beyond payload GC and receiver TTL.
Successful append keys remain until route decommission or an audited namespace
reset. Replacement retains a permanent highest-applied generation across
canonical conflict-scope coverage. Unknown/partial authority remains until
explicit reconciliation and its governed post-reconciliation horizon. Payloads
and verbose observations can expire independently. Production certification
records receiver idempotency, receipt, task, and result TTLs, payload-binding
and concurrent-duplicate behavior.

### Frozen source and frontier

Production V1 freezes/materializes one parent, verifies its completeness and
digest, and closes the source snapshot. It then plans every child, seals every
payload from the immutable parent artifact, and admits the complete immutable
parent topology before the first remote mutation. A database snapshot is only
a consistent read mechanism and is not held during potentially long
planning/sealing. Snapshot-only recovery requires a
separately certified durable cross-process reopen capability.

`parent_plan_digest` binds frozen-parent and compiled-plan digests, exact
profile contract, planner algorithm version, ordered child descriptors, payload
refs/digests, and complete gap-free coverage proof. Parent plus all children are
admitted in one PostgreSQL transaction. Existing operations resume that pinned
topology and never invoke the planner after a crash or code upgrade.

Checkpoint promotion uses the highest contiguous eligible child frontier.
Later successful children cannot jump an earlier failed or unresolved child.
An empty boundary advances only with proof that the frozen read was complete.

### Design promotion authority

`dpone.rest-bulk-delivery-design.v1` is immutable evidence with status
`RESEARCHED`; approval never rewrites that artifact or its schema identity. A
separate `dpone.rest-bulk-delivery-approval.v1` receipt binds the exact design
digest, ADR 0053–0055 digests, named owner approvals, and implementation
evidence requirements. Production certification and cutover use a third,
environment-bound receipt. Missing or mismatched receipts fail closed.

## Consequences

- Correctness no longer depends on Airflow task identity or process lifetime.
- Changed source bytes for an unresolved logical scope fail closed instead of
  appearing as a new operation.
- Older overlapping replacement generations cannot overwrite a newer admitted
  or applied effect.
- Recovery is independent of source-session lifetime and planner version.
- Journal availability becomes a submission prerequisite and introduces
  synchronous-quorum operating cost, immutable marker storage, schema
  migrations, restore reconciliation, and GC.
- Long-lived tombstones consume bounded control-plane storage but preserve
  duplicate protection after large artifacts expire.
- Synchronous profiles may use certified bounded parallelism without weakening
  ordered checkpoint semantics. V1 asynchronous bulk children are serial;
  parallel async parent supervision is deferred to V1.1.
- Other production journal technologies require a new accepted ADR and the same
  conformance suite; a nominal CAS interface alone is insufficient.

## Validation

- Model/property-test namespace/principal identity, intent conflicts,
  semantic-policy independence, rendered envelopes, and overlapping generation
  order under concurrent admission.
- Kill processes before and after every mutation/journal boundary and restart
  on another worker without an unproved resend.
- Exercise stale leases, synchronous-quorum loss, failover, lossy PITR,
  journal-epoch gate, immutable-marker mismatch, schema migration, and effect
  authority retention/reset.
- Prove seal-all and complete-plan admission precede mutation; resume under a
  different planner version without replanning; prove coverage and contiguous
  checkpoint behavior under every completion order.
- Validate every state transition against the strict Draft 2020-12 schema and
  `docs/rest-bulk-delivery-design-contract-v1.yaml`; reject duplicate YAML keys,
  unreachable/dead-end automatic paths, terminal outgoing transitions,
  unpreserved acknowledgement, and failure scenarios without a legal edge.

## Related decisions

- [Feature design](../feature-design-rest-api-sink-v1.md)
- [ADR 0022](0022-target-commit-terminal-failures.md)
- [ADR 0045](0045-durable-semantic-refresh-runtime.md)
- [ADR 0054](0054-resumable-delivery-runtime-airflow.md)
- [ADR 0055](0055-rest-operation-profile-authority.md)
- [Threat model](../rest-bulk-delivery-threat-model.md)
