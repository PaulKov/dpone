# ADR 0072: Shared workspace handover witness survives cache loss

Status: Accepted for implementation; live migration and release are not authorized

Date: 2026-09-23

## Context

The workspace admission protocol already protects exact activation occurrences,
physical guards and attempt quiescence. Native cache integration does not retire
the predecessor, so repeated activation conflicts. A local handover journal can
sequence one process but cannot survive loss of its ephemeral cache or coordinate
independent watcher replicas.

Remote desired-state publication is authoritative for selected immutable content.
Its previous publication is not necessarily the applied predecessor because the
reconciler permits skipped revisions. Deployment hashes also cannot distinguish
successive activations of identical content. Discovering guard owners fails for
disjoint write sets and the gap after predecessor retirement.

## Proposed decision

Add a protected shared execution witness next to existing SQL admission state:
one channel current/pending CAS row and immutable per-occurrence claim/request
history. Retain the existing activation/attempt/guard tables as the only physical
ownership authority. No second guard namespace or copied fencing epoch authority
is introduced.

The stable channel includes the trusted canonical desired-object identity,
registry scope, environment and source project/ref. It excludes watcher identity
and the full publication-authority fingerprint. The existing
`desired.source.occurrence_id` is the successor UUID shared by replicas.

Claim before predecessor retirement. Persist the exact original successor request
atomically with PREPARED reservation. Read complete historical coordinates,
resource/write subjects and owned epochs for retirement; do not reconstruct an
old observation from a changed physical catalog. RUNNING, COMMIT_UNKNOWN or absent
terminal/quiescence evidence retain ownership. Complete ACTIVE and shared current
in one protected transaction after the local pointer boundary. A lost cache
rebuilds from shared witness and retained immutable artifacts.

A durable claim authorizes completing its exact saga even when a later remote
publication appears. Finish the claim, then reconcile latest desired. Never
replace its UUID, abandon PREPARED, force-clear guards or report temporary applied
state as latest convergence. SQL and object-store operations are not one atomic
transaction. A requirement forbidding temporary application of an authorized but
subsequently superseded claim would need a different shared publication fence.

Existing records require explicit registrar-only registration/adoption. Persist
the complete closed input and database-derived actor/time receipt in the channel
row. Empty registration needs an explicit empty/retired-history attestation and
rejects any unbound non-RETIRED occurrence in a complete store inventory, including
disjoint resources. Adoption retains a typed immutable ACTIVE baseline with exact
historical desired/request bytes and complete guard epochs; it is not a synthetic
completed claim. Missing original bytes, ambiguous mapping, incomplete attempts,
or PREPARED/RETIRING cannot be adopted. Registration serializes inventory readback
with all writers and retries the exact registration UUID/input after unknown
commit. A cache pointer or previous publication alone cannot authorize adoption.
Local status and exported proof files are diagnostic replicas only.

Enforce the mutation boundary with fixed same-database owner-chained procedures,
not Python checks or caller-set session flags. Runtime/registrar principals have
only the required object-specific EXECUTE/read capabilities; no direct DML on
the two witness, five workspace, or shared guard tables, and no owner/DDL or
impersonation privileges. Request-only legacy procedures reject managed UUIDs.
Workspace activation/attempt/lifecycle mutations use the gateway. Three shared
guard procedures replace five guard-write sites in four semantic-refresh
adapters while preserving their existing transaction and non-guard logic; the
semantic procedures cannot acquire or release workspace owners. Other semantic
authority tables are not migrated. Two additional semantic transaction owners
must acquire the inventory barrier before earlier activation locks; workspace
attempt/replay locking must be reordered to the declared occurrence/guard/attempt
order. The write-site count is not a promise of only four changed files.
External attempt/pod identity context is loaded before the transaction; only
immutable inputs and fresh protected predicates are checked while holding locks.
Existing direct-DML binaries must be drained
and denied before channel enablement. This required compatibility slice is
explicitly larger than a cache-local fix; no existing admission procedure gateway
can be assumed. A parallel guard namespace and broad trigger interception are
rejected alternatives.

An idle channel already matching the exact latest UUID/body only verifies or
replicates its current snapshot; it does not create another claim, advance
revision/epochs or retire itself.

## Consequences

The design adds protected schema, typed claim/readback contracts and an explicit
migration and SQL-permission boundary. It supplies deterministic cold-cache recovery and replica
coordination while preserving existing physical fencing. A blocked old claim can
delay newer desired state until required terminal evidence or revoked authority
is resolved; elapsed time does not remove that safety condition.

Singleton v1 and composed-parent paths keep their existing semantics. Changing a
managed channel's authority mode is explicit and cannot silently use native
workspace retirement for a composed parent. Future credential-projection wires
are consumed through existing validated interfaces.

This ADR accepts the implementation design, not live migration or publication.
No live DDL, release or readiness claim follows from synthetic tests. The
[durable handover specification](../feature-design-durable-workspace-handover.md)
defines exact fields, transaction semantics, supersession behavior, adoption,
failure matrix and the required independent/live validation.
