# ADR 0059: Separate complete parent admission from native constituent authority

- Status: Accepted
- Date: 2026-09-10

## Context

ADR 0058 admits an immutable v3 composition for source verification and delivery.
Native-only physical admission cannot authorize its ordinary or ClickHouse
writers. Different logical aliases can resolve to the same target, and a mutable
catalog observation cannot serve as an immutable lifecycle request.

## Decision

Add distinct parent source, occurrence, physical-domain and attempt contracts,
with a separate injected coordinator in the deployment-cache saga. Preserve the
native child, v2 receipts, wire checks and source producers. Require complete
workload/write membership and all installed backend capabilities before any
reservation or predecessor drain.

Group aliases after verified physical binding resolution, then compare all writes
within one catalog observation per physical domain. Preserve original admission
observations; later phases verify stable binding continuity and the same request.
Typed PREPARED and ACTIVE readbacks must retain exact request and guard epochs.

Protected backends must provide non-expiring ownership, one-time writer authority,
closure/quiescence proof and explicit unknown-outcome recovery. RUNNING replay
cannot issue a second executor. Native and ordinary attempts sharing a physical
domain conflict regardless of constituent boundaries.

## Consequences

This first implementation provides the base contracts/coordinator and preserves
fail-closed public activation. It does not install protected SQL Server/ClickHouse
adapters or fence actual worker sessions. The required downstream includes
native-generated MSSQL-to-ClickHouse full_refresh and ordinary
PostgreSQL-to-MSSQL full_refresh; an MSSQL-only pass is insufficient.

Actual SQL/session gates, atomic ClickHouse snapshot publication, genuine route
evidence and the current/provider/execution campaign remain dependent work. No
readiness or publication permission is implied. See the
[contract and acceptance gaps](../composition-activation-contract.md) and
[approved full feature specification](../feature-specs/composition-activation-execution.md).
