# 0062: SQL Server SWITCH has an explicit activation boundary

Status: Accepted for the isolated component; public activation is not approved.

Audience: runtime maintainers, platform engineers and certification authors.

## Context

Bounded native delivery already separates source acquisition, verified staging,
transactional publication and receipt-first recovery. A physical partition
replacement must preserve that separation, including an empty authored interval.
The existing legacy SWITCH mixin derives scope from prepared values and is not
an implementation of this contract.

The [approved delivery design](../feature-design-data-delivery-acceleration-v1.md)
permits an isolated catalog/planner/executor component. It requires the existing
public native SWITCH rejection to remain before I/O.

## Decision

Keep immutable feature-local models and pure finite-partition admission in
`dpone.contracts.native_mssql_switch`, narrow SQL/transaction ports in
`dpone.ports.native_mssql_switch`, and the catalog and executor in
`dpone.runtime.sinks.mssql_native_switch`. The existing runtime planner delegates
to the canonical admission function and preserves its public signature and import
identity. This follows the pure-admission ownership principle in
[ADR 0058](0058-verified-release-composition.md): the contract validates frozen
observations; adapters retain catalog acquisition and locked revalidation.
The catalog adapter is the deployment snapshot producer. A caller dictionary, table name or
copied ownership marker is not deployment authority.

The initial adapter profile is SQL Server 2022, major 16, with one finite temporal
RANGE RIGHT partition and matching same-database rowstore layouts. It requires
existing invocation-owned prepared and empty switch-out tables. Unknown metadata,
unsupported features, mismatched storage/index shape or out-of-window prepared
rows reject admission. The [component reference](../delivery-acceleration/partition-switch.md)
defines the exact conservative profile.

The caller owns one active, committable SERIALIZABLE transaction with XACT_ABORT,
target/operation fencing and authoritative transaction-clock metadata. Under
deterministically ordered whole-table locks, the executor rereads catalog,
ownership and content, replans, and invokes the caller's complete prepared
integrity verification. Catalog/count agreement cannot replace typed integrity.
It counts replaced rows, switches old target rows out, then prepared rows in.

The executor returns uncommitted row counts. It never begins, commits or rolls
back a transaction, creates a receipt, retries, falls back, provisions a table or
cleans resources. The caller inserts the exact receipt in the same transaction.
Any failure after the first SWITCH requires complete caller rollback.

The component remains unregistered and is absent from normal runtime admission.
No policy, schema, factory or registry bypass activates it. Existing native
full-refresh and predicate partition-replacement publication retain their current
finalizer and recovery formats.

## Recovery and consequences

Successful SWITCH empties the prepared partition. After lost acknowledgement,
resolve the exact target receipt before inspecting prepared content or attempting
another mutation. Unknown outcomes retain resources and block replay, including
when both the old target and switch-out happen to be empty. Confirmed publication
continues through durable evidence and fenced checkpoint completion.

Whole-table TABLOCKX/HOLDLOCK protection serializes other partitions as well.
Catalog checks, counts, digest verification and locks have measurable costs; no
speed claim follows from fewer publication statements. The environment must also
exclude privileged database/server DDL-trigger changes during the transaction.
Table locks do not provide that administrative exclusion.

Synthetic tests exercise the finite planner, SQL projection, transaction control
flow and receipt-first recovery. Live SQL/BCP, physical transfer, locking,
rollback and performance remain UNVERIFIED without an approved disposable
environment. Existing manifests and recovery artifacts require no migration.

## Future activation

A further approved contract must provide aligned-stage provisioning, durable
resource ownership and retention, a same-session transaction authority bridge,
prepared-content protection, public admission rules and exact-environment live
evidence. Until then, use the component only through synthetic tests or explicitly
approved disposable fixtures. See the [operations guide](../delivery-acceleration/operations.md)
and [bounded-window authority decision](0057-bounded-window-atomic-publication.md).
