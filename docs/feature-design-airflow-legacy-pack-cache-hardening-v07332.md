# Airflow legacy pack cache hardening v1

Status: **IMPLEMENTED**
Target release: `0.73.32`
Approved source: canonical Airflow pack recovery and deployment plan
Research date: `2026-08-01`

Decision records:
[ADR 0040](adr/0040-airflow-legacy-pack-cache-authority.md) and
[ADR 0041](adr/0041-airflow-cache-retention-transaction.md).

Implementation evidence:

- `tests/test_airflow_cache_materializer.py`;
- `tests/test_airflow_cache_process_safety.py`;
- `tests/test_airflow_cache_sync_integrity.py`;
- `tests/test_airflow_cache_audit_closure.py`;
- `tests/test_airflow_cache_helm_profiles.py`;
- `tests/test_airflow_cache_shared_directory_mode.py`.

Impact note (`0.73.32` follow-up): shared/control directory modes remain
exact for process-owned inodes. Foreign-owned PVC roots (typical CSI
`uid 0`) are validated with an ownership-aware minimum-bit contract and
are never chmod'd, so layout init does not fail closed on sufficiently
permissive supersets such as `02777`. Private and shared-work trees keep
an exact foreign-owned contract. See ADR 0040 and
`docs/airflow-legacy-pack-cache-operations.md`.

## Problem and user journey

The mutable `latest/pack-index.json` watcher is a compatibility path while
installations migrate to exact release/deployment activation. It previously
allowed concurrent watchers to publish obsolete status, mixed incompatible
cache layouts, produced generation permissions unusable by another scheduler
UID, retained abandoned temporary bytes, and could report success above its
hard storage budget.

The platform operator needs one bounded journey:

1. configure a dedicated legacy cache root and limits;
2. let init/watch processes fetch and verify without blocking DAG parse;
3. atomically activate one immutable generation;
4. expose local authoritative evidence and optional Airflow diagnostics;
5. preserve last-known-good bytes on failure;
6. recover with stable reason codes and migrate to the exact cache.

## Public behavior

- Cache roots declare exactly one versioned layout:
  `exact_deployment_v1` or `legacy_pack_index_v1`.
- Historical unmarked roots are inferred once. Ambiguous or cross-layout roots
  fail before domain mutation.
- Legacy defaults are bounded: 512 MiB total, 10 MiB per pack/spec, 25 MiB
  index, three generations, 80/60 percent watermarks, and 30-minute abandoned
  stage TTL.
- `status/current-commit.json` is authoritative. Compatibility pointers must
  match it before parse, status success, or generation pruning.
- `dpone-airflow-pack-sync --once` returns `1` for a structured blocker and
  command failures, `0` for success or warning.
- Existing unmarked legacy roots, pack indexes, `cached://` references, and
  local fallback policy remain readable during the compatibility window.
- Shared Kubernetes deployments mount cache read-write only into the
  init/watcher writers and read-only into the scheduler or `dagProcessor`
  parser container. A common `fsGroup` supplies writer ownership; parser UID
  identity is not cache mutation authority.

Non-goals:

- mutable `latest` is not promoted into the exact deployment identity model;
- no network I/O is added to DAG parse;
- no generic cache plugin framework is introduced;
- this work does not certify a live Airflow installation.

## State and identity

```text
empty
-> fsGroup-managed stage(owner, heartbeat, attempted generation)
-> capacity reservation + active stage lease
-> verified candidate(inventory digest, index digest, bytes, files)
-> immutable generation
-> durable pending commit
-> compatibility pointers switched
-> durable commit receipt(commit UUID, sequence, generation, index digest)
-> pending commit cleared
-> diagnostic projection
```

Generation bytes are content-checked create-or-compare. A conflicting existing
generation is never replaced. The commit UUID is unique and the local sequence
is monotonic under the writer lease. A sync captures its base commit before
remote I/O; if another writer commits first, the delayed attempt cannot revert
current and reports `sync_superseded`.

## Algorithms

### Synchronize

1. Validate all limits before filesystem mutation or remote I/O.
2. Under the evidence and writer leases, initialize the layout and complete a
   previously prepared commit when its immutable generation is valid.
3. Read and validate the remote index without holding the parse lease.
4. Run safe retention before download. Authority or budget blockers stop the
   attempt before stage payload writes.
5. Reserve worst-case cache capacity from declared sizes, or from the bounded
   per-artifact limit when a historical index omits sizes.
6. Download into an owner-marked, fsGroup-managed stage while holding its shared attempt
   lease; refresh heartbeat after every bounded artifact.
7. Validate generation, paths, declared and actual sizes, checksums, and the
   complete tree inventory outside the parse lease.
8. Seal the candidate as fsGroup-managed `02770` directories and `0440` files
   and preverify any
   same-generation tree.
9. Under evidence then writer lease, revalidate layout/base commit,
   create-or-compare the generation, durably prepare the commit, switch both
   compatibility pointers, write the durable receipt, and clear pending state.
10. Run post-commit retention and enforce the hard budget.
11. Under the evidence lease, read actual current authority, write local
    status, publish the optional Airflow Variable, and update local diagnostic
    state. A stale attempt reports the newer generation.

### Retain

1. Measure immutable generations and identify expired stages outside the
   writer lease.
2. Protect every generation referenced by receipt, text pointer, or
   compatibility symlink. Any disagreement blocks generation pruning.
3. If above the high watermark, plan deletion down to the low watermark.
4. Under a short lease, compare the complete authority signature and
   revalidate both stage heartbeat and active attempt lease.
5. Rename eligible paths into private trash.
6. Delete detached bytes outside the lease.
7. Measure the root again. If it remains above the hard limit, emit a blocker.

### Recover

Recovery never edits a pointer, receipt, marker, pack, or DAG spec by hand.
Before new remote bytes are accepted, a single writer reads
`pending-commit.json`, verifies the generation receipt and index digest,
replays the compatibility pointers and receipt, then durably clears pending
state. Layout ambiguity, invalid pending evidence, persistent receipt
corruption, or immutable byte conflict moves to a new clean root while
preserving the old root as evidence.

## Concurrency, crash and failure semantics

- Remote I/O and recursive hashing/deletion never hold the parse-blocking
  exclusive lease.
- File contents and parent directories are fsynced before a successful commit
  is reported.
- Pointer-visible but receipt-incomplete crashes retain a durable pending
  marker and fail visible; no success evidence is emitted.
- The active stage lease protects slow reads across pods even when heartbeat
  TTL expires. Heartbeat and detachment revalidation close revival races.
- Capacity reservation prevents concurrent downloads from exceeding the
  configured artifact budget before payload writes.
- Evidence publication and commit share a non-parser lock, so a delayed
  Airflow metadata call cannot overwrite newer current identity.
- Lock release and diagnostic cleanup failures warn without replacing a
  primary exception or manufacturing rollback.
- Startup wrappers may fail open so ordinary Airflow DAGs still start. The
  provider loader fails visible when no trusted current dpone deployment exists.

## Components and dependency direction

- `cache_layout`: fixed filesystem layout identity;
- `cache_permissions`: shared control and fsGroup-managed stage mode policy;
- `cache_generation_files`: descriptor-safe hashing, durability, modes, delete;
- `cache_generation_lease`: attempt lifetime protection;
- `cache_generation_budget`: pre-download capacity reservation;
- `cache_generation_store`: stage, pending, recovery, and commit state machine;
- `cache_generation_retention`: plan/detach/delete budget policy;
- `cache_writer_coordination`: commit/evidence ordering;
- `cache_sync`: application orchestration;
- `cache_sync_evidence`: local and Airflow diagnostic projection;
- `cache_status` plus exact/layout status adapters: read-only diagnostics;
- `cli_sync`: thin argument and exit-code adapter.

Vendor SDKs remain behind `ArtifactReader`; base import and DAG parse do not
import S3, Airflow metadata, Vault, or connector clients unless the relevant
runtime path is selected.

## Compatibility and migration

Exact and legacy cache roots are intentionally incompatible. The compatibility
watcher remains supported for at least the published deprecation window, but
new deployments use exact immutable release/deployment IDs. Migration builds
and verifies the exact root beside the legacy root, switches the provider to
`current/airflow-index.json`, verifies loader acknowledgement and REST DAG IDs,
then removes the watcher.

## Observability

Evidence includes attempted and current generations, commit UUID/sequence,
index digest, downloaded pack/spec counts, cache bytes, warnings, blockers,
reason, and local authority marker. Secrets in exception messages are redacted.
The Airflow Variable is serialized with commits in one cache root, but it can
still lag after process termination or metadata outage. Operators compare it
to the local receipt before alerting.

## Test and rollout plan

- Unit: layout mismatch/ambiguity, policy validation, checksum conflict,
  pointer/receipt/index mismatch, restrictive umask modes, and CLI exits.
- Concurrency: delayed A versus committed B, heartbeat revival versus
  retention, slow active stage versus TTL, parser versus detach,
  same-generation create-or-compare, and serialized metadata projection.
- Fault injection: file/directory fsync, lock acquire/release, status cleanup,
  pending receipt recovery, over-budget protected current, abandoned stage.
- Regression: Airflow 2.10/3.x provider, exact cache materializer, dbt release
  materializer, DagBag/serialization, complete non-live suite.
- Dev: exact publish/materialize, restart recovery, expected DAG set, runtime
  smoke, one light MSSQL-to-ClickHouse workload.
- Prod: protected approval of the exact dev-certified deployment, canary,
  loader ACK/REST convergence, rollback evidence.

Live checks without the exact package/image/environment are `UNVERIFIED`, not
passes.

## Product comparison

Facts were checked against official sources on `2026-08-01`.

- Apache Airflow DAG serialization and DAG bundles establish that parse and UI
  consumers need stable local/versioned DAG representations. We adopt local
  parse and explicit version identity, while adding exact dpone evidence and a
  compatibility migration boundary:
  [DAG serialization](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-serialization.html),
  [DAG bundles](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html).
- Astronomer Cosmos documents compiled manifest parsing and caching. We adopt
  compiled local inputs and no parse-time remote I/O; dpone additionally binds
  release/deployment identity and bounded retention:
  [Cosmos parsing](https://astronomer.github.io/astronomer-cosmos/configuration/parsing-methods.html),
  [Cosmos caching](https://astronomer.github.io/astronomer-cosmos/optimize_performance/caching.html).
- dlt, Airbyte, Fivetran, Informatica, Pentaho, SSIS, gusty, and Apache Beam are
  `N/A` for the measurable cache-activation contract: they do not expose an
  equivalent Airflow scheduler local-cache pointer/receipt protocol. Their
  connector or ETL runtime behavior is not used to claim superiority here.

The measurable dpone target is: zero silent DAG disappearance after an
approved parse-authority restart, no stale-writer rollback, bounded cache at or
below policy, and exact desired/current evidence. Certification artifacts are
the sync status, loader acknowledgement, Airflow REST DAG inventory, and
restart smoke report.
