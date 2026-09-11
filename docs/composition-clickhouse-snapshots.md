# Composition ClickHouse snapshots

`dpone.runtime.composition_snapshot.ClickHouseAtomicSnapshotPublisher` implements
the policy for publishing one complete, sealed generation in a single-node
ClickHouse Atomic database. It is an injectable integration component. Concrete
protected SQL intent storage, ClickHouse enrollment, writer closure, catalog
observation and worker adapters remain required; the component does not enable
public composition execution or certify a live route.

See the [approved execution specification](feature-specs/composition-activation-execution.md)
and [activation contract](composition-activation-contract.md) for complete parent
admission and the selected MSSQL-to-ClickHouse full_refresh cell.

## Identity and complete snapshots

The immutable intent binds the original parent request, exact scheduler attempt
and epochs, protected physical database and target slot, private generation,
independently reconciled source snapshot and two distinct issued user UUIDs.
The ingest user must already be closed and quiescent before preparation. A
caller-supplied hash or a constructed contract object cannot establish these
facts; the trusted authority adapter reopens the original evidence.

Before publication, the target name identifies old table A and the generation
name identifies new table B. The complete B replaces A through one EXCHANGE.
Rows absent from the new source disappear; an empty B produces an empty target.
An incremental merge or bounded interval replacement is not equivalent.

`dpone.contracts.composition_snapshot` provides strict intent and record
serialization. Documents reject missing, unknown, duplicate and noncanonical
members, invalid counters and unreachable state/revision combinations. Their
digests are SHA256 of the exact canonical UTF-8 bytes. Existing path-normalizing
fingerprints must not be used for these documents.

## Injected capabilities

Construct the publisher with keyword arguments `authority`, `store`, `catalog`
and `executor`, implementing the protocols in `dpone.ports.composition_snapshot`.

| Capability | Required concrete behavior |
|---|---|
| `SnapshotPublicationAuthority` | Reopen complete source, enrollment, current occurrence, attempt and epochs; close exact issued publishers and prove actual server quiescence |
| `SnapshotPublicationStore` | Persist complete canonical originals and transition history; atomically compare exact bytes/revisions and parent authority; independently read back commits |
| `ClickHouseSnapshotCatalog` | Freshly observe both names/UUIDs, complete physical design, side effects, typed B content and storage counters with sufficient catalog permissions |
| `ClickHouseSnapshotExecutor` | Dispatch one internally rendered EXCHANGE/query ID through protected issued credentials, serialized against closure, with no hidden retry |

The catalog must inspect the complete enrolled environment, including incoming
and outgoing materialized views, TTL, projections, computed/default columns,
mutations, row policies and ambient writer grants. A partial or cached view
cannot satisfy the protocol. Missing byte observations are errors, never zero.

`SnapshotLimits` separates source rows, cumulative source bytes, actual wire
bytes, generation storage, retained storage and total transient storage. The
previous target A counts toward retained and total storage along with previously
retained generations. Actual producers must enforce effective ceilings during
streaming as well as supply independently verified final observations.

## Publication and recovery

Call `prepare(attempt, generation_ref)` only with the original generation record
digest. The authority adapter reopens its evidence before the publisher persists
PREPARED. Only an acknowledged fresh exact PREPARED claim permits dispatch;
repeating a request or finding an old claim never grants another EXCHANGE.

| State | Revision and meaning |
|---|---|
| PREPARED | Exactly 1; no dispatch claim yet |
| EXCHANGE_INTENT | Exactly 2; the single claim is durable, but the SQL outcome is not known |
| PUBLISHED | At least 3; closed publisher plus independently observed B/A UUID pair and complete B parity |
| NOT_PUBLISHED | At least 2; closed publisher plus A/B; revision 2 permits explicit cancellation before any claim |
| COMMIT_UNKNOWN | At least 3; uncertain closure, observation or publication retains ownership |

`publish(intent_sha256)` claims once, rechecks authority and catalog, dispatches
at most once, then reconciles. A losing concurrent caller cannot close the
winner's publisher. If claim commit or independent readback is uncertain, the
call raises without dispatch and without asserting what storage committed.

Use `reconcile(intent_sha256)` to close the existing publisher and compare actual
UUIDs/content. It never issues credentials or retries SQL. Recovery requires the
original retained ownership in ACTIVE or RETIRING. Altered, missing or
unavailable tables/proofs remain unknown. Unknown preparation remains blocking
and cannot be converted into an unsupported PREPARED-to-unknown transition.

For an already terminal record, `publish` returns the protected historical
receipt without acquiring new authority. This is historical replay and does not
claim the target still contains B today. `reconcile` requires current retained
ownership even when reading a terminal record. A new publication requires a new
admitted attempt and verified generation.

The component retains A. It never drops tables, advances checkpoints, releases
parent fences or supplies the final business-outcome proof. These belong to the
complete protected worker protocol after publication and reconciliation.

## Evidence and current limits

Contract and injected-adapter tests cover empty/vanished snapshots, exact
identities, storage limits, concurrent claims, lost acknowledgements, unavailable
closure/catalog observations and explicit recovery. Such tests establish policy
behavior only. Actual delayed and pre-registration HTTP requests must be drained
by a concrete gate; HOST NONE, login absence or one empty process sample alone
does not prove that obligation. Full native/generated/ordinary current/provider
execution and live type/byte reconciliation remain unverified.
