# Backfill

Resumable, chunked, idempotent historical loads for any source/sink route.

This guide covers the self-service workflow (`dpone backfill`), the manifest
contract, resume semantics, verification, and interval-aware Airflow
integration. For the strategy semantics see
[Load strategies → backfill](load-strategies.md#backfill).

## When to use backfill

- Initial historical load of a new route (`initial_backfill`).
- Range replays after upstream corrections (`range_replay`).
- Re-population after schema evolution (`schema_backfill`).
- Interval-driven idempotent scheduled loads (Airflow `catchup`/`backfill`).

### Large PostgreSQL-to-MSSQL initial load

Do not run `incremental_merge` hundreds of times against a growing live table.
Use a separate manual initial manifest that appends deterministic chunks to an
isolated shadow and publishes it only after exact validation:

```yaml
source:
  type: postgres
  connection_ref: postgres_oltp
  table: {schema: public, name: orders}
  options:
    incremental_strategy: xmin
    xmin_execution: {mode: initial, handoff_id: orders_v1}

sink:
  type: mssql
  connection_ref: mssql_dwh
  table: {schema: landing, name: orders}
  strategy:
    mode: backfill
    unique_key: [id]
    only_new_rows: false
    backfill:
      inner_mode: incremental_append
      backfill_id: orders_initial_typed_v1
      parallel_workers: 4
      retry_policy: non_committed
      lease_ttl_minutes: 60
      state:
        backend: audit_schema
        schema: system
        require_distributed_lock: true
      publication: {mode: shadow_swap, retain_backup: true, artifact_scope: campaign}
      chunk: {column: id, kind: uuid, buckets: 512}

state:
  type: mssql
  connection_ref: mssql_state
  atomicity: target_atomic
  provisioning: external
```

For target-atomic MSSQL, the four fixed lanes are four long-lived spawned
processes, not four Python threads. Each child lazily resolves authored
connection refs and keeps one independent source/sink/state connection set;
one child executes only one ODBC call path at a time. The parent owns the shared
pending queue, ledger CAS and synchronous campaign/chunk/operation lease
renewals. For MSSQL, the parent also holds the exact session-level campaign
application lock for the complete run. This preserves parallel throughput across processes without
concurrent `pyodbc` calls or a background ODBC heartbeat in any interpreter.
The process path intentionally overrides the authored legacy lease duration
with one internal 90-second TTL renewed every 30 seconds for all three lease
levels. During a single non-preemptible parent SQL phase, the session lock is
the continuous liveness fence; the parent proves `APPLOCK_MODE=Exclusive` on
that same session before refreshing even an elapsed durable timestamp. It never
reacquires a lost fence silently. Parent-process or pod teardown closes its SQL
session, releases the application lock, leaves at most the short durable TTL,
and is reclaimable before the five-minute Airflow retry.

This bound does not claim takeover through a node/network partition that leaves
the server-side SQL session alive. Do not bypass that session fence: wait for
SQL Server to retire it. A bounded partition-tolerant takeover requires a
separate epoch-fenced lease row and phase-level mutation fencing; the current
whole-ledger journal cannot add that safely as a heartbeat-thread patch.

A lane that reaches a durable chunk boundary immediately claims the next item
instead of waiting for the slowest lane in a global wave. On a known failure
the parent blocks new claims and bounds already-running peers; every untouched
or unclaimed chunk remains durable for the next resume. A target receipt is
committed with each append. Its invocation identity
uses the runtime-proven campaign `run_key`, so a pod loss after target commit
but before ledger CAS is replay-suppressed even when Airflow creates a new DAG
run. This also covers a native crash after operation-lease `unregister` but
before the child sends success: the parent retains the exact descriptor until
ledger CAS, probes the receipt on a fresh session, and repairs only the missing
ledger transition.

Process bootstrap contains raw authored config/connection refs and scheduler
context only; live connectors, resolved credentials and hydrated runtime
objects never cross IPC. `spawn` is mandatory. A non-serializable bootstrap,
missing entrypoint or legacy thread factory fails during a pure parent-side
preflight, before campaign leases, lifecycle/publication hooks, chunk claims or
source I/O. The lane runner repeats the serialization guard at its own boundary
so direct callers cannot bypass the deployment contract.

On POSIX, the child sends a containment READY immediately after `setsid` and
faulthandler setup, before resolving or entering the runtime opener. It is
accepted only when the process-group ID equals the exact spawned PID. A separate
runtime-ready message is mandatory before any chunk claim. The parent observes
process sentinels before issuing or redispatching work and terminates the whole
cached group, including BCP descendants, when bootstrap or a peer misses its
bound. Final evidence contains the post-join exit code rather than the earlier
containment snapshot.

The parent completes the full startup claim, serialization, dispatch-authority
preflight, receipt-authority warm-up and final owner proof before sending any
child command. If this phase fails, every claimed-but-unsent chunk is released
through its exact owner CAS and no source/sink I/O starts. Because that work can
consume part of the original lease, each exact command receives a newly issued
finite deadline and is serialized again. The parent proves every refreshed
chunk, drains ready peer callbacks, forces a campaign-session lease renewal,
then uses non-servicing readiness gates over every prepared lane sentinel and
active peer before the first pipe send. An already-observable prepared-lane exit
therefore aborts the whole unsent batch. No SQL or peer callback runs after that
final proof. If a peer frame is already ready,
the parent retains the exact unsent claims, services IPC, and refreshes/re-proves
that same batch; it does not record a failed chunk or consume another attempt.
The parent transfers retained-batch cleanup ownership before retrying it. A
callback exception or process-control signal therefore releases each unsent
exact owner once, while already-sent work remains receipt-recovery owned; a
failed ledger CAS becomes an explicit cleanup failure even when the same
diagnostic was recorded earlier.
Immediately before each pipe write, the parent latches that exact lane as
running. Because a pipe exception cannot prove whether zero bytes, a partial
frame or a complete frame crossed the kernel boundary, the current lane always
enters bounded quiescence and receipt recovery; only later unsent claims are
released directly.
After startup, ready IPC and process sentinels are drained
between per-lane coordinator, chunk and operation SQL callbacks, and only one
idle lane is redispatched per loop while peers remain active.

All MSSQL parent-control sessions run inside a five-second query-timeout scope:
the ledger session, its private campaign-fence session, the generic transaction
template and fresh sessions, target/state authority sessions and an MSSQL source
when present. Factory-created master authority sessions inherit the same cap.
The scope restores every previous connector setting when process-lane execution
ends and never reaches spawned source, BCP, staging or publication data paths.
An MSSQL process runtime without this capability fails before the first claim.
Authority IPC itself uses the smaller of the 30-second lease-renew interval and
one third of the remaining finite lease. Lost campaign or operation renewal is
latched on the exact lane before any co-ready terminal frame is drained; the
parent then follows the ordinary bounded drain, quiescence and receipt-recovery
path.

Before a new PostgreSQL-to-MSSQL campaign is created, the parent certifies one
versioned source/target column contract and binds it into both the campaign
`plan_hash` and `config_hash`. The ledger persists that contract. Each chunk
then derives its own exact AST binding in the parent before dispatch. An exact
committed-receipt replay can finish before PostgreSQL schema projection,
snapshot or row I/O and before the source/target business-schema preplan.
Infrastructure identity, topology and state-catalog checks still run. On a
receipt miss, the normal schema preplan revalidates the complete source and
target business schemas before data movement; the persisted contract avoids
only a separate portable-binding-specific projection.

The parent treats a batch of ready IPC frames and native process exits as one
observation boundary. It records every exited lane and closes dispatch before a
successful peer can claim more work. Before probing a possibly committed target
receipt, it terminates the lane's authenticated POSIX session/process group and
waits for executable descendants to disappear. On Linux, `Z`/`X` members do not
count as executable work; unreadable `/proc` evidence fails closed. Peer trees
are signalled together and consume one shared bounded shutdown deadline, not one
full timeout per connection; a subsequent receipt probe reuses the proven
quiescence instead of waiting for the same tree twice.

The campaign-scoped shadow is owned by a server-side run-key property and
derives its authority from the already registered live target. Its name
contains a deterministic digest of `backfill_id`, so a reviewed new generation
can coexist with an incomplete older shadow and retained backup without
deleting their audit evidence. Reusing the same `backfill_id` resolves the same
objects and resumes only non-committed chunks. Do not add a permanent
`dpone_target_identity` row for the temporary shadow. Before publication dpone
checks every committed chunk count, exact shadow row count, duplicate unique
keys, columns and indexes. It then renames live to the retained backup, shadow
to live, and writes the publication receipt in one transaction. Immediately
before rename, under one target-global lock, dpone rechecks the live generation
(`object_id + dpone-owned immutable UUID + predecessor receipt`) and all shadow
evidence; SQL Server object-id reuse therefore cannot create an ABA match. A
stale campaign fails without touching the newer live table. For an XMin initial
load, the new live head remains durably
`pending` until the checkpoint CAS closes the same publication identity under
that lock. Retries resume this handoff; another campaign cannot seed or replace
it out of order. Legacy recovery also accepts the exact pending-or-already-closed
head while holding both locks: it binds a previously committed unbound seed
receipt and closes the head in one transaction. Any different receipt fails
closed.

MSSQL `shadow_swap` is intentionally limited to an exactly reproducible object
surface. The live table must be an ordinary disk table with built-in, simple
columns and supported default-filegroup indexes. Dpone rejects CHECK
constraints, every inbound/outbound/self foreign key, triggers (including
disabled triggers), explicit object or column permissions, PK/UNIQUE
constraints, defaults, identity/computed/sparse/rowguid/generated or
alias/encrypted/masked columns, temporal/ledger/memory/filetable/graph tables,
CDC, change tracking, replication, row-level security, full-text indexes,
schema-bound dependents, non-dpone extended properties, and index options or
placement that its deterministic DDL cannot reproduce. Schema-level grants are
not object-local and remain the recommended permission model for this route.

Before creating or reusing a shadow, dpone reads the database-qualified exact
catalog and persists its canonical `object_contract_sha256` in the prepared
campaign. The loadable shadow is also checked: it may omit only deferred live
indexes. For the final cutover, dpone first takes the canonical physical-target
transaction lock shared with ordinary MSSQL DML, then the narrower publication
lock. While holding both locks, it re-proves the immutable target binding,
campaign fence and generation CAS, then rereads both objects. The live digest
must equal the prepared receipt and the shadow must match the complete live
contract before the first rename. An unsupported catalog or a DDL race
therefore fails without source reads during preparation or without renaming
during cutover. If a table needs one of the blocked surfaces, use a reviewed DBA
migration/cutover that explicitly recreates and verifies that surface; do not
bypass the guard.

This exact route receives compiler-issued Airflow retry authority for up to
three task retries. The authority is generated in the authenticated workload
pack; it cannot be self-declared in the DAG. Removing retained shadow
publication or enabling `only_new_rows` makes scheduler preflight fail closed
for any positive retry count.

Required runtime permissions for this explicit initial mode are target-schema
`CREATE TABLE`, `CREATE INDEX`, extended-property execution, and rename/alter
authority, plus the normal staging, target and state DML. Incremental manifests
remain separate and do not receive initial-publication behavior implicitly.

#### Required bounded local gate before DEV

Do not use a full production-sized plan as the first test. Before a backfill
change is deployed, run the same route, strategy, publication mode, state
backend and worker count against disposable local PostgreSQL and MSSQL, but
constrain the immutable plan to 10 representative chunks. Reducing the window
is allowed; replacing MSSQL with SQLite, changing `shadow_swap` to `direct`, or
reducing four lanes to one is not a valid pre-deploy proof.

The local gate should include skew: hold one of the first four chunks while a
faster lane completes at least two chunks on the same lane-scoped connections.
It should also inject one failure and prove that a new scheduler run resumes
every durable non-success chunk without repeating a target receipt.

Also inject a deterministic native child exit after target receipt commit and
operation unregister but before the success message. PASS requires zero new
dispatch after the exit, bounded peer cleanup, one receipt-based ledger
promotion and no second source `COPY` for that chunk.

The canonical gate injects a crash after the first target receipt commits but
before the campaign ledger CAS, then resumes from a new scheduler run:

```bash
docker compose -f docker/docker-compose.integration.yml up -d --wait postgres mssql
docker compose -f docker/docker-compose.integration.yml run --rm mssql-init
DPONE_RUN_INTEGRATION=1 uv run --extra postgres --extra mssql pytest \
  tests/integration/backfill/test_postgres_mssql_shadow_initial_integration.py \
  -q
```

PASS means exactly 10 committed chunks and 10 target rows, zero duplicate keys,
one source-free receipt replay, no remaining current-generation shadow, a
retained rollback table, preserved older-generation shadow evidence, and a
durable publication receipt. Only after this gate is stable does the
change proceed to DEV acceptance. For a production plan with thousands of
chunks, local testing remains bounded; DEV owns the full-volume validation.

## Quick start

1. Declare the campaign in the manifest:

```yaml
name: orders_backfill

source:
  type: mssql
  connection_id: mssql_oltp
  table:
    schema: dbo
    name: orders

sink:
  type: clickhouse
  connection_id: clickhouse_dwh
  table:
    schema: analytics
    name: orders
  strategy:
    mode: backfill
    backfill:
      inner_mode: partition_replace
      chunk:
        column: business_date
        from: "2025-01-01"
        to: "2025-12-31"
        step: 1d
      parallel_workers: 1
```

2. Review the deterministic plan (no data movement):

```bash
dpone backfill plan orders_backfill.yml
```

3. Execute; the run is dry-run by default, `--execute` loads pending chunks:

```bash
dpone backfill run orders_backfill.yml --execute
```

4. If the campaign is interrupted, resume all non-committed chunks:

```bash
dpone backfill resume orders_backfill.yml
```

5. Inspect progress at any time:

```bash
dpone backfill status orders_backfill.yml --format json
```

6. Operate an interrupted or long-running campaign:

```bash
dpone backfill retry-failed orders_backfill.yml --execute
dpone backfill cancel orders_backfill.yml --reason "source maintenance" --requested-by "data-ops"
dpone backfill doctor orders_backfill.yml
dpone backfill plan orders_backfill.yml --advisor
```

`dpone run orders_backfill.yml` is equivalent to `backfill run --execute`:
the runtime detects `mode: backfill` with a `chunk` block and orchestrates
the campaign automatically, which is what Airflow pods execute.

## Chunk windows

| Kind | Boundary semantics | Predicate |
|---|---|---|
| `date` | `to` inclusive; half-open windows | `column >= 'start' AND column < 'end'` |
| `timestamp` | `to` exclusive; half-open windows | `column >= 'start' AND column < 'end'` |
| `integer` | inclusive | `column >= start AND column <= end` |
| `uuid` | complete UUID keyspace split into adjacent buckets | indexed UUID ranges; final upper bound inclusive |

Steps: `Nh` (timestamp only), `Nd`, `Nw`, `Nmo`, `Ny`, or an integer for
`kind: integer`. Kind is inferred from `from` and can be overridden with
`chunk.kind`.

For a UUID primary key, do not discover mutable `min`/`max` values and do not
hash every row once per chunk. Use deterministic keyspace buckets instead:

```yaml
chunk:
  column: id
  kind: uuid
  buckets: 64
```

The planner converts the full 128-bit UUID domain into 64 adjacent ranges.
PostgreSQL compares the native UUID column to typed parameters, so its B-tree
primary key remains usable. SQL Server comparison evidence uses canonical
36-character BIN2 ordering because native `uniqueidentifier` sort order is not
the same contract as PostgreSQL UUID ordering. No row can fall between buckets,
and concurrent inserts still belong to exactly one resumable chunk.

Half-open windows guarantee adjacent chunks never overlap, so a re-run of one
chunk replaces exactly its own slice — the core idempotency invariant.

## Resume semantics and the ledger

Every campaign has a deterministic `run_key` derived from the dataset and the
full chunk configuration. Progress is persisted to a human-readable JSON
ledger:

```
.dpone/backfill/<run_key>.json
```

- Committed chunks are skipped on re-run; execution continues from the first
  non-committed chunk.
- Changing any boundary/step starts a fresh campaign (new `run_key`).
- Override the directory with `backfill.state_dir` or
  `DPONE_BACKFILL_STATE_DIR`; pin an explicit key with `backfill.backfill_id`
  or `--backfill-id`.
- Every chunk records its `idempotency_key`, attempts, row counts and the
  `run_id`/`load_id` of its ETL run for lineage.
- A pinned `backfill_id` cannot silently resume a different campaign shape:
  plan/config hash drift fails before data movement.
- Campaign and chunk leases prevent two workers from starting the same work
  concurrently. Expired running chunks are marked `lease_expired` and become
  retryable.
- A campaign lock is acquired before source IO. If the same campaign is already
  active, the second run fails fast instead of racing the same chunks.
- `resume` runs every non-committed chunk (`pending`, `failed`,
  `lease_expired`). `retry-failed` sets `retry_policy: failed_only` and runs only
  chunks whose last status is `failed`; pending future chunks are left untouched.
- Unknown `retry_policy` values fail before source IO. This is deliberate: a
  typo must not silently widen a targeted retry into a broader reload.
- `cancel` is cooperative: it blocks new chunks while already-running chunks
  either finish or expire by their lease TTL.

### Operator runbook

| Situation | Command | What moves data |
|---|---|---|
| First execution | `dpone backfill run manifest.yml --execute` | all planned chunks |
| Normal continuation after an interruption | `dpone backfill resume manifest.yml` | every non-committed chunk |
| Re-test only failed chunks after fixing a transient issue | `dpone backfill retry-failed manifest.yml --execute` | failed chunks only |
| Stop scheduling new chunks | `dpone backfill cancel manifest.yml --reason ... --requested-by ...` | no new chunks |
| Explain current state | `dpone backfill doctor manifest.yml` | no data movement |

`retry-failed` uses operation-level exit semantics: it exits with code `0` when
the selected failed chunks finish successfully, even if future chunks are still
`pending`. The full campaign status remains visible through `status`, `doctor`,
the ledger and Airflow XCom.

`doctor` is the self-service decision helper:

| Campaign state | Doctor next action |
|---|---|
| `running` chunks exist | inspect `status` and wait for lease TTL before retrying |
| `DPONE_BACKFILL_CAMPAIGN_LEASE_LOST` after TTL on MSSQL | do not clear the ledger or loop-retry; wait for SQL Server to retire the original session (or ask the DBA to verify it), then `resume` |
| `failed` chunks exist | run `retry-failed`, then inspect `status` |
| only `pending` chunks remain | run `resume` |
| all chunks are committed | no data movement action |

### State backends

Default CLI/dev state remains `local_file` (`.dpone/backfill`). Production
Airflow deployments should mirror campaign state into the audit schema:

```yaml
sink:
  strategy:
    mode: backfill
    backfill:
      state:
        backend: audit_schema
        schema: DWH_Tech
```

The audit-schema backend writes snapshots to:

- `DWH_Tech.__dpone__backfill_campaigns`;
- `DWH_Tech.__dpone__backfill_chunks`.

The SQL journal is authoritative; the local JSON ledger is only an operational
cache. MSSQL point transitions stay O(1) in SQL and do not rewrite a full local
file on every lease heartbeat. A terminal run does not serialize up to one
million chunk records merely to manufacture a cache path. `backfill.state_path`
is returned only when that file already exists;
`backfill.state_cache_status` is `authoritative` for local-file state,
`present_non_authoritative` for an existing SQL-backend cache, or `unavailable`;
an existing SQL cache is never presented as proof that it matches the latest
database revision. The
credential-free `backfill.state_authority` identifies the authoritative SQL
dialect, schema, journal tables and `run_key`. `status`, `doctor` and the advisor
can recover the latest state from the audit schema when the Airflow/Kubernetes
pod cache is gone, as long as the caller provides the sink connector through the
runtime/Python API.

No DAG-side dependency injection is required for the SQL backend. During
`dpone run`, the runtime builds the matching ClickHouse/Postgres/MSSQL state
store from the sink connector and `backfill.state` manifest block. During
read-only diagnostics (`plan --advisor`, `status`, `doctor`, `cancel`), dpone can
resolve the same sink connector from the manifest via the existing runtime
`SinkFactory`; embedded Python/API callers may still inject the connector
directly. If the backend is `audit_schema` but the sink connector is unavailable,
the run fails before source IO instead of falling back to local-only state
silently.
The run result includes `backfill.state_backend` so operators and acceptance
reports can distinguish local-only state from SQL-audited state.
It also includes `backfill.state_capabilities`:

| Field | Meaning |
|---|---|
| `backend` | `local_file` or `audit_schema` |
| `dialect` | file/clickhouse/postgres/mssql implementation |
| `durable_read` / `durable_write` | whether state survives the current process |
| `lock_scope` | current campaign/chunk lock scope |
| `distributed_lock` | whether the backend is certified for cross-pod mutual exclusion |
| `continuous_campaign_session_fence` | whether publication proves one database session lock end to end |

Current backend capability matrix:

| Backend | Dialect | Distributed campaign lock | Lock scope |
|---|---|---:|---|
| `local_file` | file | no | `process` |
| `audit_schema` | ClickHouse | no | `local_cache` |
| `audit_schema` | Postgres | yes | `postgres_advisory_campaign` |
| `audit_schema` | MSSQL | yes | `mssql_application_campaign` |

Postgres uses a session-level advisory campaign lock before mutating the local
ledger. MSSQL uses a session-level application lock through `sp_getapplock`.
If the external campaign lock is already held, the campaign does not start and
the local cache is left unchanged. ClickHouse audit state currently provides
durable read/write evidence only; this is safe for normal single-run Airflow
execution and diagnostics, but it is not advertised as a distributed lock.

For target-atomic MSSQL initial loads, that application lock remains on the
same parent connection through validation, deferred index creation and the
shadow swap; it is not released after the initial claim. This lets one blocking
MSSQL statement safely exceed the short durable lease while still excluding a
second coordinator. The publication transaction additionally locks and rereads
the latest campaign journal row, rejects cancellation/owner drift, and appends
the receipt without replacing newer chunk state. If the SQL session disappears,
renewal and publication fail closed and a later run resumes from durable target
receipts.

Campaign and chunk revisions are physically split. Campaign rows contain only
campaign metadata; their JSON size and heartbeat cost are O(1) with respect to
chunk count. Ordinary chunk claim/heartbeat/completion uses an indexed
latest-revision point read for that chunk. Full chunk hydration is reserved for
operator progress/result reads, while campaign takeover reads only latest
`running` chunks that must be orphaned.

If commit acknowledgement is lost after appending many chunk revisions, recovery
re-reads at most 1,000 chunk indexes per statement (1,001 parameters including
`run_key`, safely below SQL Server's 2,100-parameter ceiling). The verifier walks
those batches in constant additional memory, so the authored one-million-chunk
limit never produces one oversized `IN` query.

The first mutation of a legacy campaign row that still contains its complete
inline chunk array materializes every chunk into the split journal in the same
serializable transaction that appends the compact campaign revision. Existing
per-chunk revisions are overlaid before that write. A crash can therefore leave
either the original inline authority or the complete split authority, never a
compact campaign with a truncated chunk plan.

Publication and takeover also serialize on a separate transaction-owned
application gate. Under that gate, publication proves that the dedicated
session lock is still held and takes `UPDLOCK, HOLDLOCK` on the current campaign
row before any table rename. Cancellation racing with publication therefore
commits either before the fence and blocks publication, or after the publication
receipt and is rejected. Once publication is committed it is irreversible;
legacy cancellation state is normalized under the campaign fence even when the
XMin handoff was already committed. That normalization only repairs terminal
campaign status; committed chunks are never reloaded.

For a previously committed chunk, admission first probes the deterministic
MSSQL receipt key using the campaign's frozen portable-scope column contract
and the chunk's exact AST binding. An exact receipt returns before PostgreSQL
schema projection, snapshot setup or row I/O.

A ledger created before `0.74.27` has no campaign column contract. Its first
upgrade run performs one ordinary PostgreSQL/MSSQL catalog preflight under the
campaign lease, proves that the resulting candidate bindings cover its durable
historical operation scopes, persists a compatibility contract, and keeps the
existing `run_key`, `plan_hash`, and `config_hash`. Only after that one-time
bootstrap is the chunk dispatched. Exact receipt replay in the child still
precedes PostgreSQL schema projection/snapshot/row I/O and the business-schema
preplan; infrastructure identity and state-catalog checks remain mandatory. A
receipt miss executes the ordinary schema preplan before data movement. Do not
interpret receipt-before-business-schema-preplan as a claim that unfinished
work skips schema validation.

If your deployment requires a certified cross-pod campaign lock before source
IO, set:

```yaml
state:
  backend: audit_schema
  schema: DWH_Tech
  require_distributed_lock: true
```

For uncertified dialects this fails closed with a clear blocker instead of
pretending the lock is stronger than it is. For Postgres/MSSQL audit-schema
state it enforces the external-lock-backed route.

For CLI-only local usage, `local_file` remains the zero-dependency default. For
production Airflow/API usage, prefer `audit_schema`: it makes campaign state
visible in `DWH_Tech` together with `__dpone__loads`/`__dpone__load_steps` and
lets external diagnostics read the same source of truth as the runtime.

## Verification

Two layers, both automatic:

1. **Row-count parity** — each committed chunk compares extracted vs loaded
   rows; mismatches surface as a `verification.status: warning` section in the
   run result and the ledger.
2. **Reconciliation bridge** — after the last chunk commits, the orchestrator
   writes `<run_key>.execution.json` next to the ledger. Feed it to the
   existing evidence tooling for deep source/sink reconciliation:

```bash
dpone ops route-refresh-capture-snapshots \
  --execution .dpone/backfill/<run_key>.execution.json \
  --output-dir .dpone/backfill/verify ...
dpone ops route-refresh-verify ...
```

`dpone backfill plan --advisor` adds a conservative performance recommendation
without mutating the manifest. Profiles:

| Profile | Use when | Default behavior |
|---|---|---|
| `balanced` | normal production run | small bounded parallelism when many chunks exist |
| `worker_safety` | weak worker / tight disk or memory | one chunk at a time |
| `source_safety` | OLTP source protection | one chunk at a time |
| `speed` | certified source and sink capacity | bounded parallelism with source-pressure warning |

When a ledger already exists, the advisor becomes evidence-driven. By default it
reads persisted campaign state: committed/failed chunk counts, row counts,
attempts, `lease_expired` errors and per-chunk start/finish timestamps. You can
also attach external runtime evidence produced by acceptance, load-step audit or
integration matrix jobs:

```bash
dpone backfill plan orders_backfill.yml \
  --advisor \
  --advisor-evidence test_artifacts/acceptance/load_steps.json \
  --advisor-evidence test_artifacts/live_certification_vendor/backfill_matrix/certification_report.json \
  --format json
```

Accepted JSON shapes are intentionally small and connector-neutral:

- `{ "load_steps": [...] }` or a raw list of load-step records;
- `{ "chunks": [...] }` for an exported chunk ledger;
- `{ "matrix_report": {...} }`, `{ "certification_report": {...} }`, or a
  matrix report containing `total_cases`.

From that evidence it can recommend:

- increasing `chunk.step` when many small successful chunks are fast and stable;
- increasing `chunk.step` when many empty chunks prove the window is too narrow
  for the data distribution;
- increasing `parallel_workers` only up to the bounded cap;
- avoiding more parallelism when `sink_finalize` is the bottleneck compared with
  `source_read`;
- reducing to one worker when failed chunks exist;
- extending `lease_ttl_minutes` when workers expire leases before finishing.
- blocking throughput tuning when a vendor/live matrix report is red. In that
  case the recommendation becomes `resolve_vendor_certification_failures`,
  `risk=high`, and `recommended_max_parallel_chunks=1` until the matrix
  blockers are fixed.

Load-step rates are aggregated conservatively: if the same stage appears
several times, the advisor uses the lowest positive rows/sec for that stage.
That prevents one fast chunk from hiding a slow source read, finalize, cleanup
or network-bound stage.

The output is advisory by design. It includes `recommended_step`,
`recommended_max_parallel_chunks`, `recommended_lease_ttl_minutes`, `risk`,
`warnings`, `recommended_overrides` and the evidence summary used for the
recommendation, `evidence_sources` (`chunk_ledger`, `load_steps`,
`matrix_report`), but it never edits the manifest or silently widens a running
campaign. The evidence summary separates `failed_chunks` from
`failed_matrix_reports` / `failed_matrix_cases`; a green matrix report is not
counted as an empty chunk and cannot accidentally trigger a wider chunk window.
`recommended_overrides` is intentionally machine-readable so CI, MR comments
and runbooks can show a concrete manifest patch without applying it.

## Watermark safety

Chunked backfills never mutate incremental watermarks: extraction uses the
chunk predicate, not saved incremental state. When an incremental route needs
an explicit rewind before replaying history, use the gated operation:

```bash
dpone state rewind --backend postgres --state-type xmin analytics.orders \
  --to "2025-01-01" --reason "range replay" \
  --yes --approved-by "data-arch" \
  --evidence-output .dpone/state/rewind-orders.json
```

The rewind stays in preview mode until both `--yes` and `--approved-by` are
provided; the JSON payload doubles as approval evidence.

## Airflow integration

### Interval contract

The GitOps Airflow pack templates the DAG-run interval into every dpone pod
via `KubernetesPodOperator.env_vars` (rendered per task instance):

| Variable | Template |
|---|---|
| `DPONE_DAG_ID` | `{{ dag.dag_id }}` |
| `DPONE_DAG_RUN_ID` | `{{ run_id }}` |
| `DPONE_TRY_NUMBER` | `{{ ti.try_number }}` |
| `DPONE_LOGICAL_DATE` | `{{ logical_date }}` |
| `DPONE_INTERVAL_START` | `{{ data_interval_start }}` |
| `DPONE_INTERVAL_END` | `{{ data_interval_end }}` |

`dpone run` consumes the same variables (or explicit `--interval-start` /
`--interval-end` / `--execution-date` flags): they feed run-state identity and
resolve `{{ token }}` placeholders inside manifests.

### Idempotent interval runs (canonical pattern)

Template the chunk window from the interval so Airflow `catchup=True`, task
clears and `airflow dags backfill` each replace exactly their own slice:

```yaml
sink:
  strategy:
    mode: backfill
    backfill:
      inner_mode: partition_replace
      chunk:
        column: business_date
        from: "{{ data_interval_start }}"
        to: "{{ data_interval_end }}"
        step: 1d
```

Supported tokens: `data_interval_start`, `data_interval_end`, `logical_date`,
`ds` (logical date as `YYYY-MM-DD`), `dag_run_id`.

### Example DAGs

See `examples/dags/` in the repository root:

- `dpone_interval_catchup_dag.py` — interval-aware daily DAG with
  `catchup=True` (idempotent per-interval runs).
- `dpone_backfill_campaign_dag.py` — manually triggered, parameterized
  campaign DAG (`params: from/to/step`) driving `dpone backfill run --execute`.

### XCom evidence

The `gitops.airflow_xcom_summary` payload now carries the run interval and a
bounded backfill progress section (campaign counters, selected chunk count,
`operation_status` and `retry_policy` only — per-chunk details stay in the
runtime evidence and the ledger), so downstream tasks and monitoring see exactly
which window was loaded and whether the run was a normal resume or a targeted
failed-chunk retry. Schema:
[`airflow-xcom-summary.schema.json`](schemas/gitops/airflow-xcom-summary.schema.json).

### Bounded Airflow backfill mapping

The default `internal` mode keeps one Airflow task and lets dpone execute the
whole campaign. Use mapped modes only when operators need bounded per-range Grid
visibility:

| Mode | Airflow task instances | dpone execution |
| --- | --- | --- |
| `internal` | one | existing ledger and optional local workers |
| `visible` | one per chunk, at most 200 | one selected chunk per mapped task |
| `summary` | at most `max_items` | one ordered contiguous chunk range per mapped task |

```yaml
gitops:
  airflow:
    mapping:
      mode: summary
      max_items: 40
      max_active: 8
      pool: dpone_backfill
```

Mapped modes require a chunked backfill, `parallel_workers: 1`, a non-empty
Airflow pool and PostgreSQL or MSSQL `audit_schema` state with
`require_distributed_lock: true`. `max_items` is `1..200`; `max_active` is
`1..64` and cannot exceed `max_items`. Static check and pack build reject task
explosion, unsupported state and multiplied parallelism before publication.

The pack contains a deterministic mapping plan. Each mapped pod receives only
its range indexes and plan digests. Before connector I/O, runtime rebuilds the
chunk plan, verifies the hash and contiguous range, initializes the shared
ledger under a short lock, then acquires a distinct lease for every selected
chunk. Terminal transitions use owner compare-and-set, so a stale task attempt
cannot overwrite a newer owner. A summary item executes its chunks
sequentially.

Airflow task state is presentation and retry intent; the dpone ledger remains
authoritative. Clearing a successful mapped task safely skips chunks already
committed in the ledger. XCom contains a bounded `backfill.mapping` summary but
never the chunk ledger, predicates or row data. Full progress remains in state
and evidence.

### Data-aware scheduling (Assets)

Declare produced datasets in the workload catalog to schedule downstream DAGs
on data readiness:

```yaml
airflow:
  execution:
    outlets:
      - "clickhouse://analytics/orders"
```

The pack builder copies the block into `airflow-pack.json`; the scheduler-side
provider converts URIs into `Asset` (Airflow 3) or `Dataset` (Airflow 2.4+)
outlets on the runtime task.

## CLI reference

| Command | Purpose |
|---|---|
| `dpone backfill plan MANIFEST` | Deterministic chunk plan, no data movement |
| `dpone backfill run MANIFEST [--execute]` | Dry-run by default; `--execute` loads pending chunks |
| `dpone backfill resume MANIFEST` | Continue an interrupted campaign |
| `dpone backfill retry-failed MANIFEST` | Retry failed/non-committed chunks only |
| `dpone backfill status MANIFEST` | Ledger progress |
| `dpone backfill cancel MANIFEST` | Cooperative cancellation for existing campaign |
| `dpone backfill doctor MANIFEST` | Campaign health and next action hints |
| `dpone state rewind ... --to W` | Gated watermark rewind with evidence |

Window overrides work on every subcommand: `--column`, `--from`, `--to`,
`--step`, `--kind`, `--inner-mode`, `--parallel-workers`, `--max-chunks`,
`--state-dir`, `--backfill-id`. All subcommands support
`--format text|json|md`.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `backfill would produce N chunks, above max_chunks` | step too small for the window | increase `step` or `max_chunks` |
| `inner_mode=full_refresh ... only valid for a single-chunk plan` | truncate+load cannot run per chunk | use `partition_replace`/`replace`, or a single chunk |
| `Kafka backfill supports only keyed upsert replay` | topics are append-only | omit `inner_mode` or set `incremental_merge` |
| campaign restarted from chunk 1 | chunk config changed → new `run_key` | pin `--backfill-id` to reuse the previous ledger intentionally |
| `verification.status: warning` | extracted vs loaded row counts differ | inspect the mismatched chunks, then run the reconciliation bridge |
| `backfill campaign config hash changed` | pinned `backfill_id` points to a different chunk plan | use a new `backfill_id` or restore the original window |
| `lease_expired` | a worker died while a chunk was running | inspect logs, then run `dpone backfill retry-failed` |
| `DPONE_BACKFILL_CAMPAIGN_LEASE_LOST` persists after 90 seconds on MSSQL | a node/network partition may have preserved the original server-side session application lock | do not delete ownership evidence or loop-retry; have the DBA verify/retire the original session, then run `resume` |
| `cancel_requested` | operator stopped the campaign | resume with a new campaign id when ready |
