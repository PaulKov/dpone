# Bounded workspace handover execution

`WorkspaceHandoverExecutor` is the application-level implementation of the
[approved handover algorithm](feature-design-durable-workspace-handover.md).
It does not install SQL objects, register a channel, enable a watcher, or grant
permissions. A compatible protected store and verified artifact driver must be
composed explicitly before rollout.

## Recovery and operational outcomes

An existing pending claim is completed before consulting a newer publication.
Its stored occurrence UUID and original request survive loss of every local
cache file. Physical observation happens only after the applied predecessor is
durably retired. Once PREPARED exists, only the store's winning saved request is
replicated; another replica's proposal does not replace it.

Each cycle progresses at most two distinct transitions, including resumed work,
and performs at most 24 decision steps. Contention or unchanged store readback
returns `CONTINUATION_REQUIRED`, never successful convergence. An existing
watcher's next cycle performs fresh observation; the executor does not create a
second scheduler or sleep inside transactions.

Gateway errors such as waiting attempts or ambiguous commit acknowledgement
propagate unchanged. There is no mutation retry in the same cycle after an
exception. The next cycle reads the same protected channel to determine what
committed. Remote unavailability cannot be hidden by a healthy local pointer.

## Driver responsibilities

The driver verifies exact remote bytes, retained signatures, authorization
subjects, channel binding and current capability revocation. Historical
authorization is not replaced with today's Git head or another replica's full
authority fingerprint. Unsupported composed or non-workspace deployments are
rejected before retirement.

`replicate` writes artifacts and the local pointer only. In particular, a cache
materializer must disable its legacy external activation coordinator. The store
alone prepares and activates the managed occurrence. Credential-projection and
precommit authority verification remain mandatory during materialization.

No external artifact, filesystem or physical observation operation runs inside a
store transaction. Every store result and fresh read is checked against the
injected trusted channel. A lost compare-and-swap causes replanning from the
winner, never retirement of a stale locally observed predecessor.

`CONVERGED` means the latest remote observation agrees with exact ACTIVE shared
current and verified local pointer/artifacts, with no pending claim. It is not a
promise that another publisher cannot change remote state immediately afterward.

## Validation boundary

Stateful in-memory tests exercise ordering, crash recovery and contention. They
are not live SQL permission, transaction isolation or Kubernetes certification.
Those checks and explicit registration remain required before runtime enablement.
