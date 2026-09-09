# Production readiness guide

This guide describes the dpone hardening layer that moves a pipeline from a fast
local run to an auditable production-grade integration.

## CLI UX

The readiness commands are top-level and self-service:

```bash
dpone doctor --format json
dpone certify --format json
dpone schema-plan --source source-columns.json --target target-columns.json --table dbo.orders --mode widening --format json
dpone cdc-plan --backend postgres_logical --schema public --table orders --slot-name dpone_orders --publication-name dpone_publication
dpone ops certification-run --source postgres --sink mssql --strategy snapshot_diff
dpone ops contract-check --rows-json '[{"id":1}]' --contract-json '{"required_columns":["id"]}'
dpone ops rollback-plan --sink mssql --target landing.orders --load-id 01JLOAD0000000000000000000
```

There is no separate `production` mode. Production readiness is a set of checks,
plans, manifests, and evidence that can be used with any runtime.

Operational controls are grouped under [`dpone ops`](ops-cli.md). The package
namespace is `dpone.ops.*`, not `dpone.production.*`, to keep the public
API environment-neutral.

## Resumable partition manifests

Partitioned loads can be paired with a JSON manifest that records every range:

- `pending`, `running`, `success`, `failed`, `skipped` status;
- range bounds and inclusive upper-bound policy;
- loaded/extracted row counts;
- artifact path and optional checksum;
- retry attempts and last error.

Use `JsonPartitionManifestStore` for local/CI runs. The same manifest shape can
be mirrored into PostgreSQL or MSSQL state tables for orchestrated production
runs.

## Schema evolution

`SchemaComparator` compares source and target columns under an explicit policy:

- `strict`: any drift is breaking;
- `additive`: nullable additions are safe, incompatible changes are breaking;
- `widening`: type widening is safe, narrowing/incompatible changes are breaking.

Automatic runtime schema evolution is enabled by default. Safe DDL is applied
before staging is created, while drop/rename/narrowing/incompatible type changes
fail by default. If an incompatible type change must continue ingesting data, set
`sink.options.schema_evolution.on_type_change: new_column` to route source values
into `__dpone__nc__<original_column_name>`.

See [Schema evolution](schema-evolution.md) for the full source -> sink
matrix, examples, CLI planning, and runbooks.

## Observability

`dpone observability metrics-export` converts `dpone run --format json` output
into Prometheus text exposition, OpenTelemetry-compatible JSON, a machine
summary, a Markdown report, and a `metrics_index.json` checksum manifest. The exporter is dependency-light by design:
local CI can produce artifacts without a running collector, while production
jobs can ship the same files to Prometheus, OpenTelemetry Collector, or vendor
tooling.

Recommended production labels:

- `env`;
- `pipeline`;
- `source`;
- `sink`;
- `strategy`;
- `ci_run_id` or scheduler run id.

`ErrorClassifier` provides a small but explicit taxonomy for retry and alert
routing:

- `transient`;
- `authorization`;
- `data_quality`;
- `unknown`.

See [Runtime observability](observability.md) for copy-paste commands, artifact
contracts, CI pattern, and runbooks.

## Certification matrix

`dpone certify` reports the default connector/capability matrix. A connector is
not considered production-certified until required capabilities have pass/fail
evidence from tests, live gates, or benchmarks.

Required evidence currently includes high-throughput DB flows, state backends,
schema evolution, and soak/stress checks.

## CDC contracts

`CDCConfig` and `CDCOffset` define stable state shapes for:

- PostgreSQL logical replication;
- MSSQL CDC;
- MSSQL Change Tracking.

The runtime includes bounded live CDC readers for PostgreSQL logical decoding,
SQL Server CDC, and SQL Server Change Tracking. Readers emit the same Source ->
Sink artifact contracts as batch extract strategies and persist typed offsets
after successful sink load.

CDC replay/idempotency hardening adds deterministic event identities, duplicate
batch detection, operator replay plans, and an offset commit gate. Use
`dpone cdc replay-plan` before rewinding CDC state. State advancement remains
blocked until the sink load is committed and idempotency checks pass.

## Kafka production readiness

Kafka is supported as bounded batch source/sink through `dpone[kafka]`. Source runs fix offset boundaries before consumption and persist offsets only after a successful sink load. Sink runs default to at-least-once delivery, with optional idempotent producer mode. Use Schema Registry for Avro, JSON Schema, and Protobuf production contracts.

## Object storage staging

Large file-based loads should use provider-neutral object storage manifests for
S3, GCS, Azure Blob, or the local CI emulator. Every object records URI, size,
SHA-256 checksum, format, compression, and cleanup policy. The manifest can be
attached to run artifacts and certification evidence before target-native load
jobs consume staged files.

Production rules:

- use per-pipeline and per-run prefixes;
- grant target readers least-privilege read/list access on the staging prefix;
- keep `retain` for replay/certification evidence and `delete_on_success` for
  routine batch jobs;
- verify checksums before target load when the source/export tool supports it.

See [Object storage staging](object-storage-staging.md) for examples and
runbooks.

## Related docs

- [`dpone ops`](ops-cli.md) documents operational CLI controls, artifacts, and runbooks.
- [Runtime observability](observability.md) documents Prometheus and OpenTelemetry artifact export.
- [Object storage staging](object-storage-staging.md) documents S3/GCS/Azure staging manifests and cleanup policy.
- [Postgres XMin incremental strategy](postgres-xmin.md) documents XMin state, safety rules, and operational runbooks.
- [CDC Runtime](cdc.md) documents CDC replay planning, idempotency, and offset commit safety.
- [Type mapping matrix](type-mapping-matrix.md) documents cross-system type conversion policy.
- [Load strategies](load-strategies.md) documents append, upsert/merge, replace, and full refresh behavior by sink.
- [Source -> sink matrix](source-sink-matrix.md) documents supported connector combinations and links to per-flow guides.
