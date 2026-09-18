# ClickHouse cluster publication reference

This reference is for platform engineers, operators, auditors, and connector
maintainers who need the exact public behavior of bounded ClickHouse cluster
`full_refresh`. For a guided first run, start with
[ClickHouse cluster full-refresh publication](clickhouse-cluster-publication.md).

## Manifest contract

The cluster configuration is nested under
`sink.options.physical_design.storage.clickhouse.cluster`.

| Field | Type | Default | Contract |
| --- | --- | --- | --- |
| `name` | non-empty string | none | ClickHouse cluster inspected and used for cluster-scoped DDL |
| `ddl_scope` | `local` or `cluster` | existing configuration default | Cluster publication requires `cluster` |
| `replication_mode` | `internal` or `external` | `internal` | Explicitly selects the physical staging and generation protocol |
| `external_content_row_budget` | positive integer | `100000` | Bounds the rows sealed and re-observed for exact typed-content proof in external mode |

All cluster full-refresh modes also require:

- `sink.strategy.mode: full_refresh`;
- a positive `sink.strategy.max_source_bytes`;
- target-local staging;
- no managed `access_table`; and
- a direct table engine admitted for the selected mode.

There is no manifest setting for force replay, fallback, member omission,
authority deletion, or cleanup by age or prefix.

## Mode matrix

| Property | Internal mode | External mode |
| --- | --- | --- |
| Selection | omitted or `replication_mode: internal` | explicit `replication_mode: external` |
| Inventory | uniform `internal_replication=true` | uniform `internal_replication=false` |
| Engine | direct `Replicated*MergeTree` | direct non-replicated `*MergeTree` |
| Physical generation | shared replicated identity | distinct member UUIDs bound into one logical generation |
| Staging | one admitted connection, then ClickHouse replication | direct load to every required member from the same sealed artifact |
| Authority/receipt | existing V1 codecs | separately versioned external codecs |
| Fallback | none | none |

Omission remains backward compatible and selects internal mode. An external
topology remains blocked until it is selected explicitly.

## Static decision output

`dpone check` and `dpone plan` share the same static admission policy. The
external decision is additive to the existing result:

```json
{
  "requested": "cluster",
  "selected": "cluster_external",
  "replication_mode": "external",
  "runtime_admission_required": true,
  "no_fallback": true
}
```

A static blocker produces a non-zero validation result. JSON validation errors
use stable codes on stdout according to the existing check/plan contract.
Runtime failures occur after live admission begins, are emitted on stderr, and
also exit non-zero. No error path changes the request to local publication.

## Runtime admission

Runtime requires the exact current inventory:

- one shard;
- at least two unique `(shard_num, replica_num)` members;
- uniform replication flags matching the declared mode;
- no missing, extra, duplicate, unreachable, or ambiguous member;
- Atomic databases and compatible direct table engines everywhere;
- consistent KeeperMap facade and target authority view;
- direct member connection capability; and
- canonical typed-content digest support within configured budgets.

The coordinator connection may use HTTP. External member staging is deliberately
native and uses the admitted `system.clusters` port unless an injected endpoint
resolver translates it for the runtime network. The resolver output, not a later
catalog refresh, is part of the fenced inventory snapshot. Admission opens all
resolved member connections before extraction. Default lineage projection is
not supported on this route; set `options.lineage: false`.

External mode currently admits replayable in-memory rows and uncompressed CSV
or TSV file artifacts without a bulk text codec. Unsupported, streaming,
compressed, native-wire, or decoder-dependent artifacts fail before candidate
mutation; dpone does not silently switch transport or publication mode.

`skip_unavailable_shards=1` and partial catalog results are never admission
evidence.

## External identity contract

The protocol derives stable identities without endpoint text:

```text
target_key     = H(cluster, database, target)
operation_id   = H(protocol_version, scheduler_invocation, target_key,
                   normalized_plan_digest)
member_id      = H(shard_num, replica_num)
generation_id  = H(operation_id, artifact_sha256, schema_digest, row_count)
candidate_name = bounded(target + "__dpone_ext_" + operation_id_prefix)
```

Worker try number and host order do not change the operation. Inventory drift
changes the inventory digest and fences the existing operation; it does not
authorize a second operation.

## State and retry contract

| Phase | Meaning | Same-operation retry behavior |
| --- | --- | --- |
| `LOCKED` | Keeper target fence acquired before source I/O | validate the same operation and continue extraction or safe abort |
| `STAGING` | artifact, baseline, and member map are bound | observe each exact candidate; accept an exact match or replace only an owned unpublished partial candidate |
| `STAGED` | every required member is `READY` | revalidate all identities and acquire one publication permit |
| `PUBLICATION_DISPATCHING` | one correlated cluster DDL was permitted | observe only the bound queue entry and every member; never redispatch |
| `COMMITTED` | every target exposes its desired member generation | resume separately fenced predecessor cleanup |
| `CLEANUP_DISPATCHING` | exact predecessor cleanup is unresolved | reconcile the bound cleanup entry and object identities |
| `COMPLETED` | publication and required cleanup are proven | a new operation requires a version-checked transition |
| `ABORTED` | unsupported pre-publication operation ended after exact owned cleanup | a new operation requires a version-checked transition; this is not success |

There is no lease expiry or authority stealing.

## Receipt and evidence contract

External authority schema:
`dpone.clickhouse.cluster-external-full-refresh.v1`.

External receipt schema:
`dpone.clickhouse.cluster-external-full-refresh-receipt.v1`.

The retained authority binds:

- replication mode, operation ID, fence token, phase, dispatch epoch, and
  Keeper version;
- inventory and normalized-plan digests;
- artifact SHA-256, byte size, row count, schema digest, and generation ID;
- ordered opaque per-member stage, publication, and cleanup records;
- per-member target and candidate UUIDs and canonical content digests; and
- exact publication and cleanup queue identity.

Evidence and errors never include hostnames, addresses, credentials, source
SQL, row values, or local filesystem paths. A synthetic evidence receipt must
identify its scope as `local_synthetic` and bind the exact commit and fixture
configuration. It is not live certification.

## Publication and cleanup semantics

After the all-member readiness barrier, only an acknowledged Keeper CAS plus an
exact version-plus-one read grants an in-memory publication permit. Dpone sends
one `EXCHANGE TABLES ... ON CLUSTER` for an existing target or one `RENAME TABLE
... ON CLUSTER` for an absent target. It binds the command to one exact
distributed-DDL entry through an opaque `log_comment` token.

Success requires the desired generation on every required member. A terminal
mixed result, missing queue evidence, unknown UUID, content divergence, or
inventory drift is not success and never authorizes cleanup.

Cleanup is independently fenced. It removes only candidates that exactly match
the recorded per-member predecessor UUIDs, then proves their absence everywhere
before completion.

## Stable external failure pages

- [`CLICKHOUSE_CLUSTER_EXTERNAL_MODE_MISMATCH`](errors/CLICKHOUSE_CLUSTER_EXTERNAL_MODE_MISMATCH.md)
- [`CLICKHOUSE_CLUSTER_EXTERNAL_ENGINE_UNSUPPORTED`](errors/CLICKHOUSE_CLUSTER_EXTERNAL_ENGINE_UNSUPPORTED.md)
- [`CLICKHOUSE_CLUSTER_EXTERNAL_INVENTORY_INCOMPLETE`](errors/CLICKHOUSE_CLUSTER_EXTERNAL_INVENTORY_INCOMPLETE.md)
- [`CLICKHOUSE_CLUSTER_EXTERNAL_ARTIFACT_UNSUPPORTED`](errors/CLICKHOUSE_CLUSTER_EXTERNAL_ARTIFACT_UNSUPPORTED.md)
- [`CLICKHOUSE_CLUSTER_EXTERNAL_STAGING_DIVERGED`](errors/CLICKHOUSE_CLUSTER_EXTERNAL_STAGING_DIVERGED.md)
- [`CLICKHOUSE_CLUSTER_EXTERNAL_PUBLICATION_PARTIAL`](errors/CLICKHOUSE_CLUSTER_EXTERNAL_PUBLICATION_PARTIAL.md)
- [`CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_UNKNOWN`](errors/CLICKHOUSE_CLUSTER_EXTERNAL_CLEANUP_UNKNOWN.md)

Use the [recovery runbook](clickhouse-cluster-publication-runbook.md) before any
manual action.

## Compatibility and limitations

- Existing local and internal manifests are unchanged.
- V1 internal authority and receipts remain readable and recoverable.
- An unresolved V1 record blocks external mode.
- A completed V1 record requires a version-checked migration transition before
  external mode can own the same target key.
- Multi-shard, mixed-mode, one-member, Replicated-database, Shared-database, and
  Distributed-target publication are unsupported.
- Publication proves convergence, not instantaneous cluster-wide read atomicity.
- Live external certification is `UNVERIFIED` until an approved environment
  supplies current topology, grants, retention, and failure-injection evidence.

See [ADR 0069](adr/0069-clickhouse-external-replication-publication.md) for the
architecture decision and [ADR 0067](adr/0067-clickhouse-cluster-publication-authority.md)
for internal-mode authority.
