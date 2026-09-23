# ADR 0071: opt-in native raw single-query reads

- Status: Accepted
- Date: 2026-09-14
- Scope: ClickHouse-to-MSSQL native source admission and recovery identity
- Contract: [Raw single-query specification](../feature-specs/dda-raw-single-query.md)

## Context

[ADR 0057](0057-bounded-window-atomic-publication.md) restricts native extraction
to one plain MergeTree relation in an Atomic database. Admitting replacing or
replicated engines without an explicit policy would change source semantics.
Adding an optional field to a dataclass would also change existing stage names
and durable hashes if generic dataclass serialization remained in use.

## Decision

Keep the legacy path unchanged. Explicit `native_transfer.source_read.mode:
raw_single_query` additionally admits local ReplicatedMergeTree, ReplacingMergeTree
and ReplicatedReplacingMergeTree in Atomic. The selected physical session issues
one query; query-level `final=0` overrides inherited FINAL behavior. A denied
override fails the load. Preserve every returned duplicate and reject unsupported
types without lossy conversion. This does not promise replica freshness or an
identical result from a later query. Existing operational DDL exclusion remains
required throughout source acquisition and cleanup.

Bind the policy to the immutable chunk plan. Its canonical serializer omits the
new field when absent and retains historical field order. Legacy journal, stage,
owner and consumed-part identities remain unchanged. Explicit raw journals use
envelope version 2; legacy journals remain version 1. Both use the existing lookup
namespace, so changing a policy finds and rejects the prior invocation rather
than silently starting a new extraction. Never upgrade persisted records on read.
The transaction route fingerprint already binds the authored native options;
transaction receipt and completed-payload schemas do not change.

Before source-free recovery the runtime compares the requested policy with the
bound plan. Existing stage_complete still requires EOF plus contiguous imported
and verified chunks. Partial extraction requires a new invocation and full
re-extraction after writer settlement. No independent source-capture authority is
introduced. Keep transactional target mutation and receipt together, followed by
durable evidence and checkpoint advancement.

## Consequences and alternatives

Operators explicitly choose raw query-result semantics and retain the same policy
for recovery. Settle version-2 invocations before downgrading to a version-1-only
reader. Existing plain-source users need no migration. Plan output still reports
composition_required and unverified live preflight; the
[composition guide](../delivery-acceleration/raw-window-composition.md) makes
authority adapters explicit.

Implicit engine widening, FINAL-based deduplication, new journal namespaces,
automatic replica failover, and reinterpretation of old records are rejected.
New bulk backends, SWITCH and weighted day scheduling require separate contracts
and proof. This ADR makes no throughput or production-certification claim.
