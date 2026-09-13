# ADR 0063: independent native stage limits

- Status: Accepted
- Date: 2026-09-13
- Scope: ClickHouse-to-MSSQL bounded native DDA
- Contract: [Stage 2 specification](../feature-specs/dda-independent-stage-limits.md)

## Context

The encoding process pool and BCP import thread pool have different bottlenecks.
Both currently inherit one count. Merely adding optional dataclass fields would
also change persisted resource-limit records and closed v1 benchmark admission.

## Decision

Add independent keyword-only optional encoding/import counts, each falling back
to the existing parallelism. Keep one retained-work map, with capacity
`max(effective_encoding_parallelism, effective_import_parallelism) + max_pending`.
The payload bound remains one additional frame beyond that capacity, multiplied
by max_bytes. Preserve retry slots, validation and publication authority from
[ADR 0057](0057-bounded-window-atomic-publication.md). Import concurrency bounds
file import/verification tasks, not every independently opened target connection.

The limits model owns a finite canonical serializer. Legacy-effective policies
serialize the exact eight historical fields. Other policies include both resolved
counts. The fallback parallelism remains significant. Journal equality stays
strict; reads do not rewrite old records. New run envelopes use v2 for extended
policies; v1 limits are frozen explicitly. Existing correctness receipts,
observations, campaigns and comparisons retain their schemas and authority.

Comparisons still require identical full configurations. Independent-policy
tuning is a separate diagnostic experiment with separate hashes, not permission
to produce a cross-policy comparison PASS. Never normalize historical evidence
to manufacture equivalence. Old readers reject new run versions.

## Consequences and alternatives

Users can tune CPU and target work separately without changing extraction or
creating unbounded queues. Max-based capacity does not guarantee simultaneous
saturation of both pools. Sum-based capacity and independent queues were rejected
because they enlarge retention and break legacy resource expectations. Automatic
tuning, distributed scheduler abstractions and cross-policy benchmark semantics
are deferred. Upgrade readers before adopting extended reports; finish or settle
extended-policy invocations before downgrading. Keep their original policy for
recovery. Unit, spawned-worker and approved Docker evidence must prove bounds,
legacy journal/report compatibility and typed delivery before separate release.
