# Feature design: exact Airflow activation occurrence and convergence

- Status: IMPLEMENTED
- Owner: dpone maintainers
- Issue: Canonical Airflow Pack Recovery And Deployment
- Target release: v0.73.21
- Approval authority: user-approved canonical recovery plan
- Last reviewed: 2026-07-27

## Problem and outcome

An immutable `deployment_id` identifies artifact content, not one activation
event. The same deployment can become current more than once after rollback or
recovery. DAG visibility is also insufficient evidence because an unchanged
`dag_id` can remain in Airflow serialized metadata from an older parse.

The control plane therefore needs occurrence identity and an ordered evidence
protocol:

```text
exact promotion event
  -> local materialization
  -> UUIDv4 current activation
  -> complete local DAG parse
  -> acknowledgement v2
  -> durable verified history
  -> bounded retention
  -> Airflow REST convergence
```

Success means that current pointer, index, promotion audit, loader
acknowledgement, expected DAG set, and authoritative Airflow metadata agree.
A missing, legacy, partial, or mismatched acknowledgement is visible and cannot
authorize deletion.

## Personas and journey

| Persona | Need | Product behavior |
| --- | --- | --- |
| Airflow operator | Recover after pod restart without restoring an archive | Materialize exact immutable IDs from object storage |
| Platform engineer | Distinguish replay from a new activation of the same bytes | Persist one UUIDv4 per committed switch |
| Release engineer | Know whether all generated DAGs were parsed | Require exact ACK v2 and Airflow REST convergence |
| Incident responder | Preserve rollback data during an incomplete parse | Disable destructive retention while blockers exist |

The author merges a DAG-repository change. CI publishes one immutable release
and deployment, then sends the exact promotion evidence and source publish job
identity to infrastructure. The parse component activates the local cache,
loads the index, acknowledges the occurrence, and publishes status. CI accepts
the rollout only after Airflow REST shows the expected DAG set and exact
status.

## Public contracts

### Current pointer

New writers add:

```json
{
  "schema": "dpone.current-pointer.v1",
  "activation_id": "12345678-1234-4234-9234-123456789abc"
}
```

`activation_id` is a lowercase UUIDv4 assigned once to a trusted promotion
occurrence. Every pod-local `current` switch for that occurrence writes the
same value. Standalone local promotion generates a UUID when no desired-state
occurrence exists. It is opaque, is not used for ordering, and is not derived
from path, inode, clock, deployment digest, or process identity.
Readers retain support for legacy pointers without the field, but those
pointers do not have exact-occurrence capability.

The bounded precommit attestation stores the deterministic activation-guard
digest. It distinguishes an idempotent replay from a new promotion event that
selects the same deployment bytes.

### Loader acknowledgement

- `dpone.airflow_loader_ack.v1` is the immutable historical wire without
  activation identity. It is read-only diagnostic migration evidence.
- `dpone.airflow_loader_ack.v2` requires UUIDv4 `activation_id`, exact
  release/deployment/index identities, sorted unique loaded/skipped/error
  arrays, `fatal`, and an offset-aware timestamp.
- New provider versions write v2 only. A parse without exact activation
  identity fails closed instead of manufacturing v1 evidence.

Only matching v2 can certify convergence, append verified history, or enable
destructive retention.

### Status authority

An integration must choose one authoritative parsing component for cache status:

| Airflow line | Parse authority | UI/API component |
| --- | --- | --- |
| 2.10 | `scheduler` unless a separate DAG processor is explicitly deployed | `webserver` |
| 3.x | `dagProcessor` | `apiServer` |

Non-authoritative components may keep a local cache but do not overwrite the
cluster status variable.

## Algorithms

### Activation

1. Read exact promotion evidence and activation guard.
2. Materialize immutable release/deployment bytes without changing `current`.
3. Re-read evidence and guard.
4. Validate the desired-state `source.occurrence_id` as canonical UUIDv4 before
   any snapshot or permission mutation, then use it as the activation UUID.
   For a standalone local promotion, generate and validate a UUIDv4 through
   the injected factory.
5. Under the cache promotion lock, build and verify the sealed activation
   snapshot. The content-addressed snapshot is safe to retain before the
   mutable commit boundary.
6. Validate local current CAS and revalidate the remote guard before committing
   the pointer. A CAS mismatch or superseded guard reports possible state
   mutation because the immutable snapshot may already exist even though
   `current` remains unchanged.
7. Durably write pointer and promotion audit containing the same occurrence.
8. Atomically replace the relative `current` symlink.
9. A repeated guard/deployment/occurrence tuple is `already_current`; a new
   desired-state occurrence for the same deployment creates a new activation
   shared by every participating local cache.

### Parse and acknowledgement

1. Hold a shared cache lock while resolving current and reading all indexed DAG
   artifacts.
2. Capture the pointer UUID and index digest from the same pinned snapshot.
3. Load valid DAGs and isolate per-DAG failures according to provider policy.
4. Atomically and durably write ACK v2 under a separately configured writable
   `ack_root` before releasing the shared cache lock. The ACK root must not be
   equal to, contain, resolve through, or live below the cache root.
5. The cache writer may mount ACK read-only for retention evidence; it must not
   let ACK publication mutate cache authority.
6. Never call S3, Vault, Kubernetes, Airflow Variables, or metadata DB from
   parse.

### Finalization and retention

One exclusive lock covers:

```text
fresh symlink/pointer/index/audit read
  -> ACK snapshot
  -> desired == current == ACK v2
  -> fsynced occurrence history upsert
  -> complete retention plan validation
  -> bounded deletions
```

Any blocker makes the cycle read-only. Legacy v1 history is migrated into a
separate diagnostic list; exact history is keyed by `activation_id`. Partial
deletion exposes failed paths, deleted counters, over-budget state, and
`state_may_have_changed`.

The implemented exact-history file is
`.retention-activation-history.v2.json`. It is an atomically replaced, fsynced,
bounded local control file keyed by UUIDv4 activation identity. A conflicting
binding or a capacity/write failure blocks deletion. This history is durable
for the lifetime of the pod-local cache; it is not represented as
cross-pod/global evidence after an `emptyDir` replacement. Cluster rollout
evidence remains the immutable promotion/ACK/status/REST set.

### Cluster convergence

Kubernetes ConfigMap projection is explicitly eventual. The local guard
linearizes one pod transaction but is not a global lease. CI waits through
Airflow REST until the authoritative status matches promotion evidence,
current and ACK UUIDs match, every expected DAG is visible, and no loader
import error exists.

## Failure and recovery semantics

| Failure | Mutation | Result |
| --- | --- | --- |
| Materialization/hash error | None | Last-known-good stays current |
| Pointer/audit/current mismatch | Unknown control state | Recovery latch; no retry or retention |
| ACK missing, v1, stale, fatal, or incomplete | None after activation | Warning; all rollback generations retained |
| History fsync failure | No retention | Finalization blocker |
| Partial delete | Some non-current entries may be gone | Structured mutation evidence and recovery action |
| Stale ConfigMap projection | Pod-local activation may temporarily lag | Latest convergence gate stays blocked until reconciliation |

Cache is a bounded `emptyDir`; a new pod has no local last-known-good. Startup
sync is fail-open for ordinary Airflow processes, while the generated loader is
fail-visible when no trustworthy canonical cache exists.

## Architecture and dependency direction

- `dpone.runtime` owns pointer, materializer, audit, and injected UUID factory.
- `dpone_airflow_pack` owns parse-safe capture and ACK v2 writer.
- Infrastructure owns eventual reconciliation, retention composition, status
  publication, and Airflow REST verification.
- DAG repositories own declarative source and a thin formal-provider loader.

No Kubernetes Lease abstraction is added: it cannot make independent
pod-local `emptyDir` caches atomic. A globally atomic cutover would require a
separate central rollout authority and per-pod quorum design.

## Compatibility and migration

1. Deploy readers that accept pointer v1 with or without UUID and ACK v1/v2.
2. Deploy pointer and ACK v2 writers.
3. Re-activate legacy current deployments to obtain UUIDv4 identity. A
   schema-valid historical desired-state v1 envelope with a non-v4 UUID stays
   readable for diagnostics but must be republished before activation.
4. Wait for matching ACK v2.
5. Enable durable v2 history and destructive retention.
6. Remove legacy deployment lanes only after seven days of green restart
   evidence.

Rollback keeps exact immutable artifacts and changes desired IDs through a new
reviewed source commit and publish occurrence. Infrastructure verifies the
canonical source branch head and uses Kubernetes `resourceVersion` CAS; it
never rewrites `current` manually or orders events by GitLab job ID.

## Test and certification plan

- Unit: injected UUID factory, repeated exact promotion, v1 diagnostic reader,
  v2 fail-closed writer, history migration, ACK blockers, partial retention.
- Concurrency: shared parse lock versus exclusive promotion/finalization and
  stale A/B cycle.
- Matrix: provider/DagBag on Airflow 2.10 and 3.2 with matching authority.
- Integration: immutable publish, materialize, activate, parse, ACK,
  finalization, restart, outage, corruption, rollback.
- Live dev: controlled `dagProcessor` restart, all expected DAG IDs through
  REST, runtime smoke, and one lightweight MSSQL-to-ClickHouse workload.

Skipped live checks remain `UNVERIFIED`; deterministic tests do not imply
cluster certification.

## Market comparison

| System | Relevant pattern | Adopted or rejected | Source checked 2026-07-27 |
| --- | --- | --- | --- |
| Apache Airflow | Versioned DAG bundles and serialized DAG contract | Adopt exact version/evidence; retain a provider-compatible cache for 2.10/3.x | [DAG bundles](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html), [serialization](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-serialization.html) |
| Astronomer Cosmos | Parse compiled artifacts from local/cacheable inputs | Adopt compiled local parse; reject remote I/O in parse | [Parsing modes](https://astronomer.github.io/astronomer-cosmos/configuration/parsing-methods.html), [caching](https://astronomer.github.io/astronomer-cosmos/optimize_performance/caching.html) |
| dlt, Airbyte, Fivetran | Control-plane state and run evidence | Conceptually relevant to evidence, but no Airflow parse-cache activation contract | N/A for scheduler activation |
| Informatica, Pentaho, SSIS | Enterprise deployment repositories | No comparable OSS Airflow local-cache occurrence protocol | N/A |
| gusty, Apache Beam | DAG authoring/execution | No comparable immutable Airflow parse-cache activation contract | N/A |

Measurable target: after a controlled parse-component restart, the exact
expected DAG set is restored from immutable IDs with zero parse-time network
calls, zero silent empty loads, and no destructive retention before matching
ACK v2. Evidence is the promotion record, ACK, status v2, retention history,
and REST convergence artifact.

## Implementation evidence

The `0.73.21` release candidate is covered by:

- pointer and activation occurrence tests in
  `tests/test_airflow_cache_activation_occurrence.py`;
- cache/process safety and exact promotion tests in
  `tests/test_airflow_cache_process_safety.py`,
  `tests/test_airflow_cache_promotion_guard.py`, and
  `tests/test_airflow_exact_activation_real.py`;
- ACK v1/v2 compatibility tests in `tests/test_airflow_loader_ack.py`;
- loader isolation, zero-DAG prevention, and preview materialization tests in
  `tests/test_airflow_dag_loader.py` and
  `tests/test_airflow_dag_preview_materialization.py`;
- Airflow 2.10, 2.11, 3.2, and 3.3 compatibility cells in
  `.github/workflows/airflow-pack-compat.yml`.

Local validation on 2026-07-27 passed Ruff, formatting, mypy, import rules,
layer metrics, module size, generated references, public Airflow contracts,
strict documentation build, the complete non-live pytest suite, closed package
inventory, archive inspection, fresh installation, and Twine validation.

Controlled dev `dagProcessor` restart, exact A-to-B-to-A rollback, and Airflow
REST confirmation of the eight expected declarative DAGs remain `UNVERIFIED`
until the exact released packages and protected infrastructure reference are
deployed. Those checks are rollout evidence, not a prerequisite for marking
the implementation complete, and they remain release-to-environment blockers
for declaring the dev deployment certified.
