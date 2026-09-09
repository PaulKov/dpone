# 0056: Bounded-window publication and recovery authority

Status: Accepted

Audience: runtime maintainers and connector authors.

## Context

Chunk staging can be parallel without making target publication incremental.
A journal lease does not exclude external target writers. Recreated source
snapshots cannot safely reuse receipts from a previous snapshot. Repeating an
atomic exchange after an acknowledgement loss can restore the old generation.

## Decision

Keep transport, source consistency, staging, publication, and recovery as separate
capabilities injected at the composition root.

Acquire target ownership before opening the source snapshot. PostgreSQL workers
share one exported snapshot; its lifetime belongs to the source context. Persist
the immutable plan against invocation identity. Expired prepublication snapshots
require a new invocation and complete re-extraction.

Use isolated chunk attempts with bounded concurrency and encoded payload sizes.
Require an authoritative all-writer guard at each target mutation and through
server settlement; a local SQLite lease is not that guard.

Initially admit local ClickHouse Atomic databases with plain MergeTree tables.
Build and verify the full replacement generation, preserving outside-window and
NULL-window target rows and duplicate multiplicity. Refuse an intervening target
change under publication exclusion. Publish through one EXCHANGE and retain the
previous generation.

Persist publication intent before exchange. Reconcile generation UUIDs after lost
replies or restart; unknown outcomes fail closed. Postpublication recovery restores
the original plan without reacquiring a source snapshot.

Write durable evidence after verified publication and advance source state only
after evidence. Both callbacks enforce fencing and idempotency at their mutation
boundary. Unsupported authored legacy governance policies fail before I/O.

## Consequences

The standard runner refuses rolling manifests until a capability-aware Python
runtime factory is supplied. No bundled production all-writer guard or automatic
backup cleanup is implied.

Full-generation construction requires storage and work proportional to the target.
Encoded payload bounds do not imply an RSS bound. Multiset digests provide
probabilistic verification; synthetic tests additionally compare actual row
multisets. Performance claims require measured evidence.

Existing transport defaults and non-window execution remain separate. See
[Atomic rolling windows](../rolling-window.md) and
[the approved design](../feature-design-bounded-streaming-window-v1.md).

## Dependency ownership

Versioned plan and progress serialization belongs to the contract models.
Fenced CAS journal and invocation adapters own storage keys; filesystem receipt
adapters own atomic writes and fsync. The composition root supplies the executor's
journal factory and the wrapper's invocation store from one durable backend.

Row allocation preflight belongs to the RowBinary encoder. Typed multiset
aggregation has no I/O dependency. The sink consumes a binary-ingest port;
the existing HTTP adapter owns endpoint/format admission and request identity.
Window failures extend the canonical process-error contract.

The implementation remains blocked for merge while hard architecture fitness or
layer baseline checks fail. This decision does not change their thresholds,
exclusions, algorithms, or baseline; structural improvements require measured
validation rather than a new exemption.
