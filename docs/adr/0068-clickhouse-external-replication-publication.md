# ADR 0068: External ClickHouse replication uses fenced per-member generations

Status: Accepted

Date: 2026-09-18

## Context

ADR 0067 covers one-shard `Replicated*MergeTree` publication when ClickHouse
reports `internal_replication=true`. In that topology one staged insert is
replicated by ClickHouse and all members share one replicated generation.

When `internal_replication=false`, ClickHouse Distributed writes send data to
every replica and do not check their long-term consistency. Routing that topology
through the internal protocol can leave members unstaged or duplicate inserts.
Independent non-replicated tables also have different physical UUIDs, so the V1
shared-generation contract cannot represent their truth.

## Decision

External replication is an explicit opt-in mode. Dpone acquires the existing
Keeper target fence before candidate mutation, seals one immutable replayable
artifact, and stages it directly through a member-local connection to every
required non-replicated `*MergeTree` table. The logical generation binds the
artifact and schema digests to an ordered map of opaque member identities and
physical UUIDs.

An ambiguous member load is never appended again. Before publication, dpone may
drop and rebuild only the exact owned candidate, then replay the identical
artifact. Publication reuses the one-shot, Keeper-permitted, correlated
`ON CLUSTER` DDL protocol from ADR 0067. Completion requires the desired member
generation on every required node. Cleanup is separately fenced and removes
only exact predecessor identities.

The manifest defaults to `replication_mode: internal`; external mode must match
a uniform false runtime inventory and a non-replicated engine. Mixed flags,
replicated engines in external mode, incomplete inventory, unsupported
artifacts, or divergent generations fail closed. The local publisher is never a
fallback.

External authority and receipts are versioned separately, while V1 internal
records remain readable and recoverable in the same target-key namespace.

## Consequences

The protocol adds direct member connections, transient per-member storage,
canonical content verification, and more state/evidence. It avoids depending on
hidden Distributed-table delivery and makes same-operation replay observable.
Cluster-wide cutover is still not instantaneously atomic to concurrent readers;
dpone reports success only after complete convergence and retains partial
generations for recovery.

Local synthetic evidence cannot certify a production topology. Live external
certification remains `UNVERIFIED` until an explicitly approved environment is
available.

See the approved external-replication feature design for the complete algorithm,
state machine, compatibility policy, and evidence plan. ADR 0067 remains the
authority for internal replicated publication.
