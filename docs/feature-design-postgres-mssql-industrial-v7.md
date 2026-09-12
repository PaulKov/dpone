# Feature design: PostgreSQL → MSSQL industrial integration V7

> Migration status: historical source document retained for contract context. Source approvals, commit references, test counts and architecture exceptions do not establish approval or passing evidence for the current migration candidate. Current validation and public binding remain separately tracked.

- Status: APPROVED
- Owner: dpone maintainers
- Issue: maintainer-authorized V7 implementation roadmap
- Target release: R0–R8, each delivered by an independently approved scoped specification
Last verified: 2026-09-03

## Executive summary

dpone currently combines a capable PostgreSQL → MSSQL batch path with an unsafe
SQL-polling logical-slot reader and incomplete target-commit semantics. The SQL
reader advances a PostgreSQL logical slot before a durable target authority can
own feedback. The existing batch path also exposes legacy post-finalize quality
and avoidable data-plane amplification.

This roadmap develops three independent products:

```text
Correctness Core
+ Batch Engine V2
+ Incrementally promoted WAL capabilities
```

Production status belongs to an exact capability tuple. An unimplemented
journal, adapter, materialization, or HA topology cannot block a smaller proven
tuple and cannot inherit its certification. Approval of this roadmap authorizes
only release slices whose own specification is `APPROVED`; it is not a route
support or certification claim.

The first WAL GA target is PostgreSQL 16 `pgoutput` through one psycopg2
replication cursor, direct durable application to an ordinary SQL Server 2022
rowstore table in one standalone target database, current-state materialization,
stable fail-closed schema, one non-null integer/UUID primary key, and a supervised
Kubernetes service. Event history, DTC, journals, parallel apply, composite keys,
multi-table atomicity, streamed in-progress transactions, and `TRUNCATE` apply
remain separate future capabilities.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Data engineer | Declare PostgreSQL → MSSQL semantics without driver/slot knowledge | Physical CDC details leak into examples | Manifest contains only semantic `profile_ref`, bootstrap, materialization and key |
| Platform engineer | Resolve a small certified deployment tuple | Support is inferred from generic code paths | Activation resolves an exact tuple or fails closed before source I/O |
| Operator | Bootstrap, observe, pause and recover without losing acknowledged data | Slot, offset and target authority are disconnected | Status exposes durable, visible, feedback and server frontiers separately |
| Release owner | Promote evidence for one useful profile | A Cartesian matrix blocks every release | Exact commit/environment tuple receives independent evidence and status |

Journey: discover truthful capability → satisfy prerequisites → plan semantic
manifest → provision with a privileged control identity → bootstrap snapshot →
observe catch-up and live frontiers → diagnose blockers → recover/rebaseline →
upgrade only across compatible receipt generations.

## Scope

### In scope

- R0 fail-closed containment for the consuming SQL logical reader.
- R1 same-database Batch/XMin target unit-of-work, typed receipts, writer fence
  and generation-scoped row hashes.
- R2A exact one-pass target-ready scalar wire spike.
- R2 immutable snapshot-bound Batch Engine V2 and atomic publication.
- R3 independently certified SQL Server topology profiles.
- R4 one small WAL RC tuple with service-owned bootstrap, heartbeat barrier,
  transaction reducer, durable frontier and explicit feedback.
- R5 exact vendor-live WAL GA plus safe receipt compaction.
- R6 MSSQL journal, R7 object journal, and R8 additional adapters,
  materializations, keys and transaction groups as independent capabilities.

### Non-goals

- Treating approval, implementation, certification and activation as one status.
- Certifying every client × durability × topology combination together.
- Making Windows AG/DTC the minimum topology.
- Exposing physical slot/publication names, drivers or durability backends in
  the self-service manifest.
- Running a permanent replication consumer as an Airflow task.
- Claiming that WAL current-state materialization preserves event history.
- Automatically dropping, recreating or advancing a logical slot.

### Assumptions and constraints

- Every durable target effect has target-local authority committed in the same
  physical SQL Server session and transaction.
- PostgreSQL feedback never exceeds a target/journal authority permitted by the
  resolved semantic profile.
- Batch, XMin, WAL and maintenance writers are mutually fenced per target
  generation.
- Live integration requires an explicitly approved environment. Missing live
  evidence is `UNVERIFIED`, never `PASS`.
- All evidence and documentation references are repo-relative and bound to an
  exact implementation/environment digest.

## Public contract

### Status model

```yaml
spec_status: draft | researched | approved | superseded
implementation_status: absent | experimental | implemented
certification_status: unverified | local_pass | vendor_pass | expired
activation_status: blocked | explicit_opt_in | default
```

The four axes are independent. `APPROVED` never implies route availability.

### Self-service manifest

```yaml
source:
  type: postgres
  replication:
    mode: wal_cdc
    profile_ref: low_latency_current_state
    bootstrap:
      mode: snapshot_then_stream
sink:
  type: mssql
  table: {schema: dbo, name: orders}
  materialization:
    mode: current_state
    unique_key: [order_id]
    delete: hard_delete
```

The environment owns adapter, slot/publication and heartbeat identities,
durability, topology, receipt/frontier locations, service placement, capacity
and retention. A semantic profile requires at least:

```yaml
requires:
  source_delivery: at_least_once
  target_effect: effectively_once
  transaction_atomicity: route_visible_events
  cross_route_transaction_atomicity: not_supported
  schema_policy: stable_fail_closed
  maximum_ack_boundary: durable_target_database_authority
```

A resolved tuple that is weaker in any dimension is rejected.

### CLI

Existing `dpone cdc-plan` remains an admin diagnostic and cannot activate managed
WAL. Existing `dpone ops cdc-runtime-run` remains a bounded certification tool.
R4 adds manifest-driven plan/provision/bootstrap/status/pause/resume/rebaseline
commands. Read-only commands do not mutate. Destructive mutation requires
`--yes`; output supports JSON authority and deterministic Markdown projection;
credentials and physical identifiers are redacted.

### Python API

`PostgresLogicalCDCReader` remains import-compatible but its consuming SQL path
is activation-blocked in R0. Parser components remain experimental. At WAL GA,
the supported API is manifest-driven `CdcRouteControlService`; replication
cursor and physical authority ports remain internal.

### Artifacts and evidence

Every artifact has a versioned schema, exact route/capability/environment and
implementation digests, immutable identity, creation status, and retention
class. JSON is authoritative. Generated Markdown is a projection. Skipped,
mocked, stale or unavailable live checks remain `SKIP`/`UNVERIFIED`.

### Compatibility and migration

- Existing batch manifests stay in a compatibility profile and do not inherit
  Batch V2 certification.
- Consuming PostgreSQL SQL reader construction/import remains compatible, while
  `setup`, `read_batch`, and `drop_slot` fail before connector I/O.
- Batch/XMin V1 receipts are read/replay only after R1; all new writes use V2.
- Receipt dual-write is forbidden; writer-version cutover is a generation CAS.
- A previous binary may resume only when it understands current schemas and
  receipts; otherwise the route remains paused.

## Common authority contracts

Each target database owns canonical receipt headers and typed bodies, target
generation head, generation-scoped row hashes, CDC segments and compacted
frontier. A control database may contain only repairable projections.

```text
BEGIN in one physical MSSQL session
→ lock and verify target writer generation/epoch
→ target mutation
→ generation-scoped row-hash mutation
→ bounded deterministic target-local quality
→ canonical target-local receipt
→ checkpoint/frontier/head CAS
→ COMMIT
```

No external API, arbitrary user SQL or full-table scan executes inside this
transaction. `PostgresMssqlTypePolicy` owns logical normalization, target type,
loss policy, equality and SHA-256 canonical row hashing; physical Batch, XMin,
pgoutput and BCP codecs implement that policy independently.

Receipt headers bind receipt kind, effect key, route/target/generation,
contract version, source authority and committed database time. Typed bodies
cover generic Batch, XMin, CDC effect/progress, snapshot, publication, handoff,
segment and GC effects. Every visible committed pgoutput transaction has exactly
one effect or progress receipt.

## Release algorithms and gates

### R0 — safety containment

Before any source I/O, the legacy PostgreSQL SQL-polling reader rejects setup,
slot deletion and reads with one stable typed failure. It never invokes
`pg_logical_slot_get_*_changes`, `pg_replication_slot_advance`, automatic slot
drop/recreate, publication DDL, or an unsafe bypass. Parser-only APIs remain
experimental. Docs/examples/capability metadata call PostgreSQL WAL → MSSQL
unimplemented and activation-blocked. XMin is current-state polling only.

Acceptance: a real PostgreSQL test records `confirmed_flush_lsn` and
`restart_lsn`, attempts activation, observes the typed error, and proves both
slot fields and all target/state/receipt stores unchanged.

### R1 — Batch/XMin correctness GA

Scoped design: [PostgreSQL → MSSQL Batch/XMin correctness R1](feature-design-postgres-mssql-r1-correctness-v1.md)
(`APPROVED`; implementation and activation remain governed by its independently
approved provider children and current evidence). ADR 0056 is accepted.

The first GA topology is PostgreSQL 16 → SQL Server 2022 standalone, with
target, receipt, writer fence, row hash and checkpoint in one database.
Source/staging validation precedes the target transaction. Target mutation,
hash, bounded blocking quality, V2 receipt and checkpoint/head CAS are atomic.
Lost commit response is resolved by a fresh, source-free exact receipt probe.
Empty full refresh requires signed `allow_empty=true` authority.

### R2A/R2 — exact wire and Batch Engine V2

R2A spikes `mssql.bcp.prefixed-mixed.v1`: canonical PostgreSQL projection,
COPY BINARY framing, one-pass bounded encoder, exact length-prefixed BCP host
fields, format-file identity, private immutable artifact and BCP staging.
Supported scalar grammar is closed; numeric conversion never rounds; unsupported
or non-finite values fail negotiation. Python reference and compiled provider
share golden vectors and never silently fall back.

R2 opens one repeatable-read exporter, locks the source relation against DDL,
imports the snapshot into all planned lanes, performs bounded planning, seals an
immutable `BatchRunPlanV2`, then reads business payload. Parallelism requires an
exact non-null unique range key or lexicographic key plus unique tie-breaker;
otherwise `single_copy`. Runtime split, `ctid/OFFSET`, direct live
`DELETE/TRUNCATE → BCP`, partial publication, a second full typed file and
per-row dictionaries are forbidden.

Candidate chunks are privately staged, verified and published by action table,
partition switch or shadow rebuild. Strategy is selected before mutation from
rows/bytes, width, indexes, log amplification/throughput, partition alignment,
free space and lock budget. Visibility is run scope or the complete affected
partition set.

### R3 — topology ladder

Promote `ag_same_database`, `cross_database_receipt_repair`, Windows AG with
`DTC_SUPPORT=PER_DB`, in-doubt recovery and future cross-instance DTC
independently. DTC is not a dependency of R1/R2.

### R4 — WAL RC1

A supervised service owns the replication connection, source lease, heartbeat,
transaction assembly, target apply, feedback and owner epoch. Provisioner,
replication runtime, snapshot reader and heartbeat writer use distinct least-
privilege principals. Static signed authority pins source physical identity,
slot generation, publication semantics, relation/schema/replica identity,
heartbeat relation, pgoutput options and environment profile digest.

Bootstrap is snapshot-first (`bootstrap_wal_prefetch: off`). The coordinator
creates/adopts the slot and retains the exporting replication connection until
four extraction lanes, one warm spare and the snapshot-lease session import the
snapshot. After verified snapshot load, a committed heartbeat token defines the
cut. Transactions from the consistent point through the barrier end LSN `H` are
applied to the shadow in commit order, each with an effect/progress receipt and
contiguous sequence. Promotion atomically activates data and hash generations,
writes the handoff receipt, and changes the writer head to `wal/LIVE`. Feedback
cannot reach `H` before promotion. Incomplete shadows are never adoptable.

The GA tuple uses PostgreSQL 16, primary/UTF8, one ordinary logged relation plus
one control relation, one non-null `int2|int4|int8|uuid` primary key,
`REPLICA IDENTITY FULL`, publication operations including `TRUNCATE`, pgoutput
protocol 2 with binary/messages/two-phase/streaming disabled and origin `any`,
psycopg2 replication cursor, direct target durability, SQL Server 2022
standalone same-database ordinary disk rowstore, current-state materialization,
stable fail-closed schema and serial source-transaction apply.

Reducer reconstructs exact transaction initial/final rows, distinguishes NULL
from unchanged TOAST, checks final key uniqueness and target row-hash baseline,
then executes set-based delete/update/insert. A key cycle deletes the union of
affected initial/final keys before inserting all survivors. One source
transaction maps to one MSSQL transaction and is never split. Capacity limits
block oversize transactions without target effect, receipt or feedback.
Observed `TRUNCATE`, schema drift, poison transactions and target drift block the
contiguous frontier and require operator action/rebaseline.

Durable, local, server-observed and telemetry LSNs are separate. Only durable
target authority derives feedback. Keepalive never advances `flush_lsn`.
Startup compares server confirmed `S` and durable `D`: `S=D` starts; `S<D`
verifies generation/WAL and suppresses replay through `D`; `S>D` blocks until
exact authority is recovered or the route is rebaselined. Lost/invalidated slot
or generation mismatch fails closed.

### R5 — WAL GA and receipt compaction

The public RPO=0 claim is limited to the certified durability domain where the
target database retains committed target/receipt authority and the PostgreSQL
slot remains usable. Source slot loss, target PITR below `D`, media loss without
authority backup, or joint loss requires rebaseline.

Compaction closes contiguous `stream_sequence` segments by transaction count,
manifest bytes or age. An immutable ordered archive is conditionally created and
read back before an archived segment exists. After server-confirmed, PITR,
reconciliation, incident and legal-hold horizons, one target-local lock/CAS
advances the compacted frontier and records signed GC authorization. Cleanup is
idempotent. At every committed point either an individual receipt or a verified
compacted frontier suppresses replay; LSN arithmetic is never used as a
continuity proof.

### R6–R8

R6 adds MSSQL journal durability and serial asynchronous apply without mandatory
DTC. R7 adds immutable object journal plus versioned CAS `JournalFrontierPort`.
R8 independently adds libpq/helper clients, event log/SCD2, additional key
profiles, publication groups, multi-table atomicity and conflict-aware parallel
apply.

## Architecture

### Components and responsibilities

| Component | Responsibility | Dependency direction |
|---|---|---|
| Semantic/environment profile resolver | Resolve and reject weaker tuples | manifest/services → contracts |
| Target writer fence, receipt, hash and frontier ports | Capability-oriented target authority | runtime policies → ports |
| PostgreSQL replication/publication/heartbeat adapters | Physical PostgreSQL protocol and DDL/I/O | adapters → vendor SDK |
| MSSQL target adapters | Same-session transaction, target DML and metadata | adapters → ODBC/BCP |
| Batch planner/encoder/publication policies | Snapshot plan, exact wire, cost decision | runtime → contracts/ports |
| WAL assembler/reducer/applier | Transaction envelope and current-state effect | runtime → contracts/ports |
| Composition root | Select only certified environment tuple | app/bootstrap → adapters |

Canonical policies/models live under `dpone.contracts`, `dpone.ports`,
`dpone.runtime`, `dpone.manifest` and `dpone.dag`. PostgreSQL/MSSQL SQL and SDK
types stay under adapters. Legacy namespaces adapt or re-export only.

### Alternatives and tradeoffs

| Alternative | Decision |
|---|---|
| SQL logical-slot polling | Rejected: source advances before durable target authority |
| Nine-client/durability promotion gate | Rejected: unrelated cells block useful GA tuple |
| Windows AG/DTC first | Rejected: smallest same-database transaction is safer and deployable |
| Airflow long-running consumer | Rejected: Airflow remains bounded control plane |
| Runtime chunk split | Deferred until exact split receipt/coverage proof exists |
| Automatic large-transaction splitting | Rejected: violates source transaction atomicity |

### ADR requirement

Required before R1/R4 production merge because target authority, feedback and
service ownership are cross-layer decisions. Each scoped specification records
its ADR dependency and migration boundary.

### Quality-budget impact

No module may exceed repository budgets. Ports remain capability-oriented;
composition roots own concrete dependencies; each state machine, codec and SQL
adapter is split by stable responsibility, not arbitrary line count.

## Market comparison

R0 makes no superiority claim. R2/R4 approval must refresh official primary
sources for Debezium, AWS DMS, Qlik Replicate, Airbyte, Fivetran, Informatica and
SSIS; Beam is relevant only to processing semantics. dlt, Pentaho, gusty and
Astronomer Cosmos are recorded as relevant or `N/A` per the exact capability.
Adopted themes are snapshot/stream coordination, explicit source identity,
transaction metadata and observable retention. Rejected themes are opaque
acknowledgement, generic support inheritance and undocumented partial
materialization.

Normative protocol references are PostgreSQL 16 replication protocol, logical
replication protocol/message formats and COPY documentation, Microsoft BCP
prefix documentation, and psycopg2 replication feedback documentation. Exact
links are maintained in the release-scoped R2A/R4 specifications.

## Measurable differentiation

```yaml
axis: deterministic acknowledgement and replay authority
scenario: process loss after target commit and before PostgreSQL feedback
baseline: current SQL-polling logical-slot reader
metric: lost or duplicated target effects and source-free recovery outcome
target: zero lost effects, zero repeated target DML, restart from durable authority
procedure: exact-commit crash-injection and vendor-live slot/target probes
artifact: test_artifacts/postgres_mssql/<release>/certification.json
limitations: applies only to the exact certified durability domain and tuple
```

Batch throughput claims additionally use byte-domain efficiency and log/write
amplification, not row count alone.

## Security, privacy and operations

Principals are separated by DDL, replication, snapshot read and heartbeat write
authority. Batch artifacts/WAL spill use encrypted private volumes, `0700`
directories, `0600` files, bounded paths, symlink protection, capacity limits,
cleanup receipts and legal holds. Secrets and physical identifiers never appear
in public output. WAL/source disk, target log, receipt and archive horizons are
runtime admission inputs, not documentation-only advice.

## Test and certification plan

| Layer | Required proof |
|---|---|
| Unit/property | fencing, receipt identity, reducer, frontiers, wire grammar, coverage and state transitions |
| Contract | unsupported tuple rejection before I/O; JSON/Markdown and compatibility schemas |
| Integration | PostgreSQL slot containment; same-session MSSQL UoW; snapshot and barrier recovery |
| Fault | crash around DML/hash/receipt/frontier/barrier/promotion/feedback/GC |
| Performance | common byte domains, transport efficiency, target log amplification and reference fingerprint |
| Live certification | exact wheel/image, PG minor, SQL Server CU, soak, recovery and evidence digest |

Core properties: never acknowledge beyond durable authority; never commit a
target effect without canonical receipt; never apply an effect twice; never let
a stale epoch mutate; never publish partial Batch output or an incomplete
shadow; never skip a visible stream sequence; never let heartbeat cross a
blocker; never GC receipts before committed compacted authority.

## Documentation plan

Each release updates the first-success tutorial, semantic profile reference,
capability discovery, architecture/state diagrams, permissions guide,
recovery/rebaseline runbook, source-impact/capacity guide, certification,
migration, matrix and generated evidence reference. CJM covers discovery,
prerequisites, plan, provision, execution, observation, diagnosis, retry,
rebaseline, operation and upgrade.

## Rollout and rollback

```text
R0 containment
→ R1 Batch/XMin correctness
→ R2A exact wire spike
→ R2 Batch Engine V2
→ R3 optional topologies
→ R4 WAL RC1
→ R5 exact WAL GA1
→ R6/R7/R8 independent extensions
```

Each release uses a separate approved specification, task contract, PR,
fresh-context review and exact evidence. Unsupported tuples stay
activation-blocked. A durable effect is never undone by generic rollback;
recovery is receipt-aware.

## Agent execution plan

| Role | Ownership |
|---|---|
| Integrator | Shared schemas, capability registry, composition roots, navigation and changelog |
| Correctness implementer | Target UoW, receipts, writer fence and row hash |
| Batch implementer | Snapshot planner, encoder, BCP staging and publication |
| WAL implementer | Replication service, heartbeat, assembler, reducer and frontiers |
| Test certifier | Model tests, live profiles, performance/fault evidence |
| Docs/UX reviewer | Manifest UX, tutorials, runbooks, migration and CJM |

Parallel writers require disjoint worktrees and validated task contracts. Shared
semantic files have one integrator.

## Approval checklist

- [x] User problem and CJM are clear.
- [x] Algorithm and failure semantics are implementable without guessing at roadmap level.
- [x] Public contracts and compatibility are explicit.
- [x] Architecture and alternatives are justified.
- [x] Relevant market research is scoped to the releases where claims are made.
- [x] Claimed differentiation is measurable.
- [x] Tests, evidence, docs, rollout and rollback are complete at roadmap level.
- [x] Path ownership and integration plan are conflict-safe.
- [x] Maintainer changed status to `APPROVED` by explicitly requesting implementation of V7 on 2026-09-03.
