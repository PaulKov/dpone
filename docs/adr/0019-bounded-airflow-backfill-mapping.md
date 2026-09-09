# ADR 0019: Bounded Airflow mapping projects the dpone backfill ledger

## Status

Accepted.

## Context

dpone's backfill runtime already owns deterministic chunk planning, campaign
identity, durable chunk state, leases, retry, resume, cancellation and evidence.
Airflow Dynamic Task Mapping can improve chunk-level operator visibility, but a
direct one-task-per-chunk implementation would create a second state machine and
could expand beyond scheduler, metadata database and UI budgets.

The current internal orchestrator also holds a campaign lock while all chunks
execute. Reusing that lock unchanged in multiple mapped task instances would
serialize the campaign or fail most mapped tasks. Conversely, dropping dpone
state in favor of Airflow task state would lose connector-neutral commit and
replay guarantees.

ADR 0008 requires provider parsing to consume bounded static artifacts without
state, network, Variables, Connections or secrets.

## Decision

- `internal` remains the default and preserves the current one-task campaign.
- `visible` and `summary` are explicit opt-in projections over the same dpone
  backfill plan and ledger.
- Build/reconcile compiles a static, fingerprinted mapping plan into each
  applicable process plan. Airflow does not discover chunks from runtime data.
- `visible` maps one task instance per chunk and fails above the configured hard
  task limit. `summary` maps deterministic contiguous chunk ranges and never
  exceeds that limit.
- v1 hard-caps `max_items` at 200, requires a static pool and applies
  `max_active_tis_per_dag` from the verified pack.
- Mapped execution uses a short campaign lock only to initialize and verify the
  ledger. Data movement occurs under independent per-chunk leases.
- Chunk completion uses owner compare-and-set. A process that outlives its lease
  cannot overwrite a newer attempt.
- The shared SQL audit state loads the latest record for every chunk and merges
  it over the campaign snapshot. A pod-local cache cannot be authoritative in
  mapped mode.
- Postgres advisory locks and MSSQL application locks are the first certified
  distributed lock adapters. Local-file and ClickHouse audit state fail closed
  for mapped execution until they provide equivalent lock/CAS evidence.
- The runtime receives only a bounded range plus plan fingerprints. It re-plans
  canonical chunks and never accepts predicate SQL from Airflow.
- Mapping execution scope stays separate from release/deployment/pack run
  identity. Both are correlated in evidence.
- Every mapped KPO validates its own outcome. A shared downstream outcome gate
  is not the authority for per-map-index success.

## Consequences

- Operators get bounded Grid-view visibility and independent task retries while
  dpone retains data-commit authority.
- Switching between mapping modes does not change chunk or campaign identity.
- Summary mode limits Airflow metadata growth without hiding ledger detail.
- Mapped execution requires stronger state capabilities than internal mode and
  is intentionally unavailable for some sinks in v1.
- SQL state adapters must support latest-per-chunk merge and owner-CAS semantics.
- Effective concurrency is predictable because `parallel_workers` is one for
  mapped items and Airflow owns the external task limit.
- Provider parsing remains static and reproducible.

## Alternatives rejected

- Make mapping the default: changes existing behavior and can overload Airflow.
- Generate mapping items from an upstream task: adds runtime topology discovery
  and another failure/reproducibility boundary.
- Use Airflow task state as the ledger: cannot prove data commit or portable
  resume semantics.
- Hold the campaign lock during each mapped task: prevents useful concurrency.
- Pass predicates in the mapping item: duplicates planner policy and creates an
  injection surface.
- Permit local-file state on separate pods: lacks distributed locking and a
  shared authoritative cache.
- Certify ClickHouse audit state based only on latest-wins rows: it lacks the
  required lock/CAS primitive.

## References

- [Feature design: bounded Airflow backfill mapping v1](../feature-design-airflow-bounded-dynamic-mapping-v1.md)
- [ADR 0008: Airflow parse-safe provider](0008-airflow-parse-safe-provider.md)
- [ADR 0011: Canonical fingerprints](0011-canonical-fingerprints.md)
- [ADR 0013: Reproducible rerun and retention](0013-reproducible-rerun-and-retention.md)
- [Airflow Dynamic Task Mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html)
