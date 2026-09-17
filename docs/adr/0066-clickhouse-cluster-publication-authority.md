# ADR 0066: Keeper is the authority for replicated ClickHouse publication

Status: Accepted

Date: 2026-09-17

## Context

`EXCHANGE TABLES ... ON CLUSTER` is atomic on each Atomic-database member, but
not across the cluster. A client timeout or a terminal error can leave only some
replicas committed. Repeating the exchange would swap committed replicas back.
A process-local table marker cannot fence concurrent workers across replicas,
and many installations do not operate a separate transactional state service.

## Decision

For bounded `full_refresh` into a one-shard, two-or-more-replica
`Replicated*MergeTree` target, use one fixed KeeperMap row per qualified target
as publication authority. The row binds scheduler-stable operation identity,
inventory and generation digests, exact table UUID/engine/schema/Keeper paths,
phase, epoch, and opaque distributed-DDL correlation tokens.

Only an acknowledged KeeperMap compare-and-swap followed by an exact
version-plus-one read may produce an in-memory dispatch permit. Authority
mutations bypass generic retry and execute once with strict KeeperMap mode and
zero Keeper insertion retries. A lost response produces an unknown outcome and
no permit.

Distributed DDL is also sent once. Its identity is the exact queue entry whose
`settings['log_comment']` equals the fenced random token. Completion requires
one well-formed row for every expected host plus exact generation identity on
every replica. Active partial work waits for that entry; terminal mixed or
unknown evidence fails closed and retains both generations. Cleanup is separately
fenced and may remove only the exact predecessor.

## Consequences

This removes blind `EXCHANGE` replay and requires no PostgreSQL backend. It adds
KeeperMap/catalog privileges, complete replica reachability during admission,
and queue-retention requirements. V1 deliberately excludes multiple shards,
Distributed targets, non-Atomic databases, automatic terminal-partial repair,
and writers that bypass the authority.

The existing local Atomic/Shared protocol and unbounded compatibility behavior
remain unchanged. Docker evidence verifies the protocol against a pinned server;
it does not certify an external production topology.

See [ClickHouse cluster publication](../clickhouse.md#bounded-cluster-full-refresh-publication)
and the approved feature design for the complete state machine and evidence plan.
