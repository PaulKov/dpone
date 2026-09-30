# Feature design: protected ClickHouse candidate lifecycle and composition

- Status: APPROVED
- Owner: dpone maintainers
- Issue: next increment after PR #249
- Target release: no version assigned; staged capability, not route activation
- Parent: [approved authority design](feature-design-clickhouse-dpone-only-authority.md)
- Baseline inspected: `da1a1d9bd6fa352bc60560e064d03f70601f4202`

Last verified: 2026-09-30

Proposed amendment: [self-service table compatibility](feature-design-clickhouse-table-compatibility.md)
is `RESEARCHED`, not yet approved. It addresses server-rendered settings,
method-specific compatibility and version-safe recovery. This parent's approved
contract is not silently broadened; the amendment needs written approval and a
revised implementation plan before settings-aware production changes.

## Executive summary

The journal and native publisher exist, but callers still supply observations
and there is no durable candidate-writer lifecycle. Composing those pieces
directly would permit name collisions, leave pre-PREPARED recovery undefined,
and attempt to acquire the same non-reentrant execution lock twice.

This supplement closes those gaps for one protected Python publication. It
preserves the approved single-host, direct-node, dpone-only writer model and
method selector. It proposes explicit authority storage v2, serialized candidate
mutations, an irreversible seal, a narrow typed observer, and a session-aware
backend. It does not activate ODBC, finalize checkpoints, release owners, or
make successive full refreshes production-ready.

The maintainer approved this written supplement on 2026-09-30 with an explicit
`approved` response, including v2 without migration of existing journals. This
approves the design, not implementation completion or production certification.
The parent remains approved; its v1 foundation contract is preserved alongside
the explicit new v2 boundary. The previously selected Native execution method
remains unchanged. The [implementation plan](superpowers/plans/2026-09-30-clickhouse-protected-publication.md)
was separately reviewed and approved on 2026-09-30 before production edits.

## Personas and customer journey

| Persona | Need | Success signal |
|---|---|---|
| Platform engineer | Compose actual protected services without trusted caller flags | Readiness precedes source access; all candidate mutations are registered |
| Data engineer | Publish a complete snapshot without losing duplicate rows | Typed expected/observed parity and method-aware publication |
| Operator | Understand a failed first attempt | Original operation, exact failed boundary, retained resources and source-free inspection |

1. Discover the explicit Python capability and its unsupported route boundary in
   [publication methods](clickhouse-publication-methods.md).
2. Provision a new private persistent local store for a new enrollment. Supply
   the reviewed deployment ingress inventory, direct endpoint and separate
   observer/publisher credentials; do not place secrets in inventory artifacts.
3. Validate supported server, schema and principal visibility. Resolve target and
   candidate reservations before invoking the supplied source factory.
4. Create the candidate and load synchronous, registered requests. Close writer
   admission, join acknowledged completions and compare actual typed content
   with the accepted input before sealing.
5. Publish through the existing kernel. Report publication state separately from
   route completion; the owner and names remain reserved even after COMMITTED.
6. After interruption, inspect/recover the original operation without accepting
   any source argument. An unfinished prepublication operation stays quarantined.
7. Preserve the journal and candidate. There is no force-release, replay,
   migration, automatic cleanup or second operation on the same target here.

## Scope and compatibility

### Explicitly retained constraints

One POSIX host, persistent local SQLite storage, one direct ClickHouse node,
Atomic database and plain MergeTree. Source extraction may be parallel upstream;
candidate mutation ingress is serialized. Only the protected publisher may
mutate enrolled objects. No HA, lease expiry, retrying proxy, ON CLUSTER,
replication, Distributed, background TTL or independent writers are supported.

CLI, manifest/schema, connector registry, legacy full-refresh routes, package
version and release workflows are unchanged. Existing guarded-publication-v2
records and the eight-method kernel port retain their shape and semantics.

### Storage v2 and preservation of v1

Use explicit `dpone.clickhouse.authority.v2` provisioning/opening. Existing v1
provision/open defaults, records, inspection and native closure operations stay
available. They do not acquire new writer-seal guarantees. V1 still lacks a
complete protected observation backend; do not promise complete v1 outcome
recovery merely because its journal can be opened.

There is **no in-place migration or import** in this increment. V2 may enroll
only physical names never managed by another authority. Empty SQLite storage
does not prove an object is unowned. Prior or ambiguous ownership in the
deployment inventory blocks enrollment, including candidate names. Never create
a replacement v2 store to bypass a retained v1 owner or missing original store.

V1 and v2 readers reject mismatched/unknown versions. Do not add lifecycle
tables under the v1 identifier or change the v1 default to v2. A later migration
needs its own isolation, ownership-conflict and uncertain-request reconciliation
contract; future owner finalization alone is not sufficient. Downgrading does
not convert v2 files. Stop admissions and preserve them for a compatible reader.

### Proposed Python boundary

The composition provides two distinct operations:

- `publish_new(request, source_factory) -> PublicationRecord`: readiness and new
  acknowledged enrollment precede the first invocation of `source_factory`.
  The request contains typed candidate design and immutable operation identity,
  not arbitrary SQL, sessions, completion flags or precomputed caller seals.
- `recover_existing(operation_id) -> ProtectedPublicationStatus`: accepts no
  source callback. Before PREPARED it only inspects/quarantines. At or after
  PREPARED it delegates to source-free kernel recovery and protected observation.

Concrete imports and constructor signatures will be fixed in the implementation
plan. Dependencies remain explicit: authority, exclusion, catalog/row reader,
candidate transport, publisher transport, pinned evidence profile and enrollment
inventory. No default stock route constructs this backend.

Existing `AuthorityConflict`, `AuthorityError` and `PublicationUnknown` remain
compatible. New prepublication diagnostics distinguish `prepublication_retained`
from the kernel's publication UNKNOWN; do not invent PREPARED to represent an
unfinished load. All uncertain outcomes carry `safe_to_retry=False`.

## Durable identity and lifecycle

V2 keeps the existing publication history and adds cohesive protected records:

| Record | Immutable identity / monotonic facts |
|---|---|
| Namespace reservation | Deployment, pinned server, database, physical name, owning operation, role (target/candidate) |
| Candidate operation | Binding, design/profile digests, CREATE identity, admission state, expected typed aggregate |
| Candidate request | Operation, writer/request ID, sequence, kind, query ID, exact statement/schema and payload digest, row/byte counts |
| Request history | Revision, acceptance, send-state, terminal completion digest or retained uncertainty |
| Seal | Candidate UUID, accepted-request frontier and history digest, closed admission, expected/observed evidence and profile identity |

Acquire target and candidate reservations in the **same** `BEGIN IMMEDIATE`
transaction as the operation. A canonical physical name cannot be any other
operation's target or candidate. Names remain reserved after RENAME/EXCHANGE:
observed UUID changes do not transfer ownership. Cross-store reservations are an
enforced deployment prerequisite, not a capability SQLite can prove globally.

Every successful enrollment returns a volatile invocation capability. Existing
bindings and ambiguous commit readback never recreate it. Bind capabilities to
authority identity, operation, epoch, process/thread and active lifetime. Fork,
serialization, reconstruction and use after session close are invalid.

Request IDs and query IDs are deterministic from globally namespaced operation,
kind and monotonic sequence, with a domain distinct from publication query IDs.
They are correlation identities, not server-side exactly-once tokens. Register
exact immutable request metadata before handing a request to the transport.
Registration/send/closure use short durable transactions; never hold a SQLite
transaction over network I/O. Preserve WAL/FULL and file-identity checks.

```text
fresh acknowledged enrollment
  -> registered CREATE -> MAY_HAVE_SENT -> durable successful EOS
  -> OPEN -> [registered INSERT -> MAY_HAVE_SENT -> durable successful EOS]*
  -> CLOSED admission -> all accepted requests joined
  -> protected candidate observation and expected-content parity
  -> SEALED -> existing PREPARED/CLAIMED/publication lifecycle

any ambiguous mutation/commit -> retained prepublication operation
reopened before PREPARED     -> inspect only, never CREATE/INSERT/publication
```

### Request and admission rules

1. Read-only readiness verifies candidate absence before CREATE. Never adopt an
   existing table through `IF NOT EXISTS`. CREATE itself is a registered
   one-shot mutation; retain the operation if its response is uncertain.
2. Allow one synchronous CREATE/INSERT in flight per operation and no asynchronous
   queue. Source workers cannot call the transport or hold mutation credentials.
3. Each INSERT is one immutable bounded input batch. Validate its exact column
   order, types and values locally; compute expected typed evidence from the
   accepted values. No implicit lossy coercion, missing columns, server defaults,
   `INSERT SELECT`, arbitrary settings or asynchronous insert is allowed.
4. Acknowledged registration grants that invocation access only to its request.
   Before any mutation bytes, persist `not_started -> may_have_sent`. Lost ACK
   at either boundary forbids sending, even if readback finds the transition.
5. Consume the successful native EndOfStream and persist its request-bound
   completion before reporting success. Server exceptions, timeout, cancellation,
   partial response or disconnect do not establish successful completion.
   Never retry CREATE/INSERT automatically, including before observed effect.
6. Admission closure and registration race on a protected revision/CAS. Once
   CLOSED, reject every new request. Already accepted requests must reach durable
   successful completion before sealing. A provably unsent canceled request
   still aborts the load; it cannot be silently omitted from the snapshot.
7. Closing admission does not cancel a request already entitled to send. Join
   accepted work; if closure cannot be proved, retain it. No timeout or empty
   process list substitutes for join. Recovery may revoke provably unsent entry
   under exclusion, but cannot send it or call the source again.
8. Seal requires acknowledged CREATE, CLOSED admission, all accepted requests
   successfully joined, stable candidate identity/design and expected/observed
   typed parity. Seal publication is one immutable durable transition.
9. Only the same fresh invocation may continue from acknowledged seal to
   PREPARED. After process exit, even SEALED-without-PREPARED is inspection-only.
   This intentionally sacrifices availability in that crash window.

An empty source still creates and verifies an empty candidate; zero INSERTs is
valid only after normal source exhaustion. A source exception is not exhaustion.
There is no rollback DROP, delete-owner escape hatch or fresh retry identity.

## Protected observation profile

The first profile is deliberately narrower than ClickHouse itself. Persist its
identifier, server/driver version, canonical encoding identifier and supported
type/design grammar with the original operation and seal. Producer changes need
a new profile; never compare unrelated digest families as if interchangeable.

### Closed design grammar

Candidate design is a typed DTO rendered by a fixed grammar. Support ordinary
stored columns, plain MergeTree, an ordered list of sorting columns (or empty
tuple), a primary-key prefix of those columns, and partitioning by an empty
tuple or an ordered tuple of non-null integer/date columns. No free-form
expressions, DEFAULT/ALIAS/MATERIALIZED, explicit codecs, TTL, projections,
indices, constraints, sampling key, table comments or per-table SETTINGS are
accepted initially. Storage policy must be the pinned local default.

Observe the complete server-rendered definition plus ordered `system.columns`
and relevant `system.tables` fields. Parse only that closed grammar and reject
every unconsumed clause; do not regex-strip unknown SQL into an apparently
supported definition. Canonical design includes ordered names/types, engine,
partition/sort/primary keys, storage policy and the profile's fixed settings.
Exclude object names and UUIDs from the design digest; keep them as separate
identity evidence. Whitespace/quoting normalization must not discard literals.
Compare normalized definition against independent catalog fields; disagreement
or unavailable fields fails closed.

This supports both single-partition replacement and multi-partition EXCHANGE
without changing the selector. More expressive production designs need a
separately certified profile, not a permissive fallback.

### Typed content

Reuse the existing constant-space `TypedMultiset` algorithm
(`rowbinary-sha256-sum-v1`) and canonical RowBinary scalar encoder behind an
appropriately placed reusable boundary; preserve existing producer outputs.
Do not reuse the external-replication sorted-JSON digest: its encoding and
memory behavior differ. Schema/profile identity accompanies the multiset,
whose count and per-column null counts preserve duplicate multiplicity. SHA
parity remains probabilistic evidence, not mathematical equality or resistance
to maliciously chosen colliding data.

Initial allowlist: Bool, Int/UInt8/16/32/64, finite Float32/64, String,
FixedString, UUID, Date/Date32, DateTime with explicit UTC, DateTime64 with
explicit UTC and precision 0–6, Decimal32/64/128 and Nullable of those scalars.
Reject NaN/infinity, nested/array/map/tuple values, LowCardinality, enums,
aggregate types, Decimal256 and DateTime64 precision above six. Preserve the
sign bit of floating zero in RowBinary evidence. Decimal scale/range checks
must reject rounding; temporal values must be exactly representable at the
declared precision.

Use byte-preserving String reads. For FixedString, reconstruct the declared
width by zero-padding the driver's stripped suffix before canonical encoding;
test trailing zeroes, embedded NUL and invalid UTF-8 explicitly. No generic
`str(value)` conversion is a canonicalization rule.

Stream rows once to calculate count, multiset and distinct `_partition_id`
inventory from the same scan. Bound the partition set with an explicit positive
limit; exceeding any limit rejects the observation without truncation. Empty
tables have an empty inventory. Cross-check active-part inventory and exclude
zero-row parts consistently; contradictory inventory is not complete evidence.

### Exclusion and completeness

Hold the authority execution session across catalog-before, complete row stream
and catalog-after, plus seal validation. Validate table/database UUIDs, profile
and design on both sides. Reject active mutations, dependencies/materialized
views, row policies, unsupported topology, incomplete catalog privilege or any
unknown feature. Ordinary MergeTree background merges may change physical part
names, never the logical row multiset or partition set used for classification.

Catalog rechecks detect drift; they are not a fence against bypass credentials.
The reviewed deployment inventory enumerates every mutation job, principal,
maintenance/cleanup path and retired legacy route for both names. Bind its
revision/digest to enrollment; validate current supported grants and direct
endpoint. The operator must actually enforce it. A JSON assertion, signature or
digest does not prove there are no other writers. Docker evidence certifies
only the controlled test deployment, not the user's production grant inventory.

The candidate seal binds its **prepublication** UUID and typed content. During
post-publication observation, validate allowed RENAME/EXCHANGE/REPLACE identity
transitions using the frozen intent; do not incorrectly require the original
candidate name still to contain the sealed UUID after EXCHANGE or RENAME.
Closure and protected observation must precede outcome classification.

## Backend composition and lock lifetime

Use one explicit execution session inside `GuardedPublicationBackend.hold`.
The backend keeps the exact protected journal revision and newly acknowledged
volatile `DispatchGrant` for that invocation. Adapt `JournalEntry` to the
kernel's `PublicationRecord` without reconstructing authority from DTO fields.

Provide session-scoped publisher methods which validate the exact authority,
operation, current process/thread and active session. Existing standalone
publisher methods retain their lock-acquiring wrappers. The composed backend
calls scoped methods while already holding the lock; it must neither acquire a
second flock nor release exclusion between observation and send.

`claim` returns permission only from an acknowledged fresh CAS. `execute_once`
requires that same retained grant and exact intent. `close_and_drain` consumes
only protected transport history. Historical records, copied grants and
readback after uncertain commit cannot produce a new dispatch capability.
On leaving hold, discard invocation memory without releasing durable ownership.
No global sessions, thread-local service locators or generic reentrant locks.

Before PREPARED, the composition intercepts recovery; it must not pass an empty
`read()` to kernel `publish()` and accidentally start a new intent. At terminal
states the kernel returns historical resolution. Label it historical, not a
claim that the target has been freshly observed at the time of that read.

## Architecture, alternatives and quality

| Component | Responsibility / dependency direction |
|---|---|
| Contracts | Typed design, candidate status, request/seal/profile identities; no SDK |
| V2 authority adapter | Atomic namespace reservations, durable lifecycle and publication journal |
| Candidate gateway | Admission and single-request lifecycle behind injected transport/persistence ports |
| Observer policy | Closed design/type profile and evidence invariants; injected catalog/row reader |
| Native adapters | Fixed CREATE/INSERT/read operations and pinned protocol completion; no policy in SDK wrappers |
| Backend bridge | Exact kernel-to-journal/session/grant adaptation |
| Explicit composition root | Supplies concrete dependencies without changing stock route factories |

The same lifecycle covers empty/nonempty candidates and RENAME/REPLACE/EXCHANGE;
do not introduce a generic connector plugin registry for this one binding.
Adapters cannot import runtime implementations. Move shared pure evidence
primitives to an allowed canonical boundary with compatibility re-exports if
needed; do not make the observer import the window staging runtime.

| Alternative | Decision |
|---|---|
| Serialized candidate ingress | Adopt: smallest auditable join/closure contract; extraction may remain parallel |
| Parallel candidate requests | Defer until measured need; increases accepted-request and crash-state combinations |
| Separate lifecycle sidecar beside v1 | Reject: reservations and ownership would span non-atomic journals |
| Silent v1 extension | Reject: older code could omit new invariants |
| Explicit v2 with no migration | Adopt: preserves old originals; restricts new backend to new enrollment |
| Resume loading/dispatch from recovered seal | Reject for this profile: readback is not a fresh acknowledged capability |

Record an additive ADR before implementing this binding; ADR 0078 remains the
unbound-kernel decision. Decompose persistence, catalog parsing, typed evidence,
native loading and composition by responsibility. Use the checked-in
`docs/benchmarks/quality_budgets.yml`, import matrix and exact-cap ratchets.
No threshold increase or growth of existing debt is authorized. Module/graph
estimates and exact owned files belong in the reviewed implementation plan.

## Research and measurable outcome

Official sources checked 2026-09-30. Rolling ClickHouse documentation informs
the model; actual support must be tested on pinned ClickHouse 24.8.14.39 and
clickhouse-driver 0.2.10, not inferred from newer documentation.

- [ClickHouse system.tables](https://clickhouse.com/docs/reference/system-tables/tables)
  and [system.columns](https://clickhouse.com/docs/reference/system-tables/columns)
  expose definition, identity and column metadata. Adopt independent catalog
  checks; neither page promises our all-writer exclusion or an atomic snapshot
  across separate queries. Those are dpone deployment/design obligations.
- [INSERT documentation](https://clickhouse.com/docs/reference/statements/insert-into)
  describes additional buffering for asynchronous inserts. Reject that mode in
  this completion profile; this is not a claim that async inserts are generally
  unsafe.
- [Pinned driver types](https://github.com/mymarilyn/clickhouse-driver/blob/0.2.10/docs/types.rst)
  document microsecond DateTime64 resolution, byte-preserving String reads and
  FixedString suffix stripping. Adopt explicit precision limits and certified
  reconstruction rather than silently comparing lossy Python values.
- [Pinned driver completion](https://github.com/mymarilyn/clickhouse-driver/blob/0.2.10/clickhouse_driver/client.py)
  consumes insert EndOfStream and raises on exception/unexpected packets. Adopt
  pinned positive completion; a returned row count alone is not durable closure.
- [SQLite WAL](https://sqlite.org/wal.html) requires a common host and does not
  make transactions across attached databases atomic as a set. Adopt a single
  versioned authority; reject a separately committed lifecycle sidecar.

| Comparator | Applicability to this supplement |
|---|---|
| dlt, Airbyte, Fivetran | N/A: no comparison of ingestion products or their destination strategies is claimed; this is dpone's private authority protocol |
| Informatica, Pentaho, Microsoft SSIS | N/A: their orchestration/loading capabilities do not specify this journal's capability and migration contract |
| gusty, Astronomer Cosmos | N/A: DAG integration is outside this storage/publication boundary |
| Apache Beam | N/A: no distributed execution/checkpoint model is being replaced or benchmarked |

The broader [ODBC design](feature-design-mssql-columnar-range-parallelism-v2.md)
owns ingestion comparison and throughput goals. This supplement makes no
market-superiority or speed claim.

```yaml
axis: safe one-shot publication after proven candidate closure
scenario: competing admission, lost mutation ACK, catalog drift and restart
baseline: native publisher with fixture-supplied observations at da1a1d9b
metric: unsealed publications, duplicate sends, recovery source calls, silent row loss
target: zero in every required negative case
procedure: unit and process contracts followed by exact-commit owned Docker faults
artifact: test_artifacts/clickhouse-protected-publication-<run-id>/summary.json
limitations: finite test matrix; probabilistic digests; no production or power-loss proof
```

## Security, limits and diagnostics

Keep credentials, raw rows and SDK query/server logs out of public diagnostics.
Use private owned storage and bounded request sizes; explicit positive limits
cover batch rows/encoded bytes, column count, partition count, total observation
rows/bytes and request duration. Limit exhaustion aborts, never silently samples
or returns a complete observation. Byte accounting includes the canonical
payload; it is not a promise of a hard native-driver/Arrow/Parquet/RSS ceiling.
The separate ODBC pre-read admission gate remains required and unverified here.

Diagnostic JSON `dpone.clickhouse.protected-publication-status.v1` contains
schema/profile identity, operation/epoch/revision, admission/seal status,
accepted/succeeded/uncertain request counts and IDs, publication method/state,
historical-versus-fresh observation, reason code, `safe_to_retry`, and retained
resource names. Produce it from protected originals to a new path atomically,
without implicit overwrite. It is redacted inspection, never importable authority.

Use actionable reasons such as `candidate_namespace_occupied`,
`candidate_request_unclosed`, `candidate_content_mismatch`,
`preprepared_restart_retained`, `unsupported_observation_profile` and
`authority_version_mismatch`. Include the failing phase and original operation,
not advice to delete state or start another operation. Alert on retained unknown
work and capacity limits; no automatic expiry resolves them.

## Validation and certification

| Layer | Required proof | Artifact |
|---|---|---|
| Unit | Type framing, duplicates, NULL/empty/zero, binary suffixes, decimal/time precision, float zero, unsupported types, injected hash collisions | Focused JUnit and golden profile vectors |
| Contract | Complete closed-grammar catalog, immutable seal, namespace cross-role conflict, session/grant binding, missing visibility rejection | Fake-reader/transport negative-case matrix |
| Process | Admission-vs-close races, lost SQLite ACK, stale/forked sessions, pre-PREPARED restart, zero source/SQL replay | Linux subprocess outcomes and protected histories |
| Live | Real candidate CREATE/INSERT/seal, actual typed observation, all four selector outcomes, empty candidate, drift rejection | Owned Docker logs/JUnit and before/after rows/UUIDs |
| Live faults | In-flight writer plus close, response lost before/after effect, crash after seal before PREPARED, crash after terminal completion | Activated fault boundary, query counts and restart evidence |
| Compatibility | Existing v1 tests unchanged; v1/v2 cross-open rejected; old evidence vectors unchanged | Focused compatibility JUnit |
| Performance | Streaming observation and one-batch ingress; measured peak memory without an RSS guarantee | Fixture sizes, timings and measured memory |

The observer must detect actual drift, not merely a caller-supplied flag.
Collision injection checks count/multiplicity preservation and documents hash
limitations; it cannot prove collision-free equality. Source/replay sentinels
must be asserted outside catch-all paths so swallowed assertions cannot pass.

Evidence records exact commit/tree, commands, image digest, server/driver/Python/
SQLite/platform versions, inventory scope, profile vectors, durable histories,
fault placement, request identities/counts, source-call counts and checksums.
Do not reuse the prior 11 native-publisher live cases as observer/seal evidence:
their fixture supplies synthetic observations. Certification requires separate
actual-lifecycle cases with no skips and an independent review.

Run focused checks before repository lint/type/import/graph/module and full
non-live gates. Run docs checks, language tests and strict MkDocs. Docker Desktop
is authorized only for owned isolated resources; preserve evidence before
cleanup and never stop unrelated containers. TLS, production ingress controls,
power-loss, migration, owner release and the MSSQL/object-storage route remain
UNVERIFIED or N/A, not inferred PASS.

## Documentation, rollout and execution ownership

After implementation, add a tested first-run tutorial, exact Python/profile
reference and source-free recovery runbook. Update the stable publication
overview, journal/native guides and navigation together. Explain the first-run
only boundary, v1 preservation, new-enrollment requirement, pre-PREPARED crash
window, probabilistic parity and retained owner with runnable examples. Never
substitute `supplied_backend` or a caller boolean for actual composition.

Sequence implementation as one reviewed plan with independently verified steps:

1. Versioned lifecycle/reservations and source-free status.
2. Serialized native candidate transport, admission closure and durable join.
3. Closed-profile protected observation and immutable seal.
4. Session-aware backend composition and owned Docker certification.

Native execution keeps the root agent as the sole writer/integrator. Read-only
explorer, architect, test/certification and docs/UX reviewers have no write
ownership. The implementation task contract must enumerate new paths and shared
files before edits; root owns schemas, compatibility exports, fixtures,
`CHANGELOG.md`, `mkdocs.yml` and any shared composition files. No parallel writers
are authorized. A fresh-context independent reviewer must verify the final diff
and explicitly recheck every corrected finding before integration.

Rollout remains opt-in Python composition on newly enrolled test/deployment
subjects, not a route flag. On any failure, stop admissions and retain the
original store/resources; rollback the application only to a reader compatible
with that storage. No destructive rollback or automatic transfer to v1.
Production activation still requires controlled ownership release, method-aware
cleanup, checkpoint/evidence finalization and full route certification.

## Approval checklist

### Staged implementation: closed input/evidence profile

The first increment supplies `CandidateColumn`, `CandidateDesign` and
`ObservationLimits` in `dpone.contracts.clickhouse_observation`, plus
`ProtectedObservationProfile` in `dpone.adapters.clickhouse_observation_profile`.
It validates inputs locally; it does not read ClickHouse or certify publication.
The complete protected journey below remains dependent on the later plan tasks.

Supply exact immutable tuples and built-in scalar values. Integer inputs exclude
bool; float inputs must be finite, and Float32 must round-trip without rounding.
Timestamps use built-in `datetime` with immutable `datetime.timezone` at UTC;
normalize other timezone implementations explicitly before admission. DateTime64
supports scales 0–6 without truncation. Decimal width aliases normalize to
`Decimal(9, S)`, `Decimal(18, S)` or `Decimal(38, S)`; other precisions reject.
Byte-valued String preserves invalid UTF-8; FixedString pads zero suffixes.

All eight limits are explicit. `encode_row` enforces the encoded row bound;
`validate_batch` also enforces cumulative encoded bytes and row count before
allocating each wire payload. No limit truncates data. Batch-order payload
identity hashes an eight-byte unsigned little-endian length plus canonical bytes
per row. Multiset identity remains order independent with count/null multiplicity
and the unchanged `rowbinary-sha256-sum-v1` algorithm.

`render_candidate_create` and `parse_table_design` in
`dpone.adapters.clickhouse_design_grammar` share the closed design DTO. Object
name/UUID are separate from design identity. Unknown clauses, expressions,
comments and trailing statements reject rather than being removed. This grammar
still requires actual pinned-server verification in the observation increment.

Legacy runtime encoding imports remain available and preserve prior coercion,
wire bytes and multiset digests through shared contract primitives. No CLI,
manifest, stock route, authority schema default or release version changes here.

### Design approval

- [x] Existing approved deployment decisions are preserved.
- [x] New v2/non-migration compatibility boundary is explicit.
- [x] Candidate admission, closure, seal and pre-PREPARED failure behavior are defined.
- [x] Observer profile and session/grant composition have explicit limits.
- [x] Research distinguishes platform facts from dpone design obligations.
- [x] Tests, evidence, documentation, rollout and ownership are specified.
- [x] Maintainer approves this written supplement (2026-09-30).
- [x] Detailed implementation plan is written and reviewed before production edits (2026-09-30).
