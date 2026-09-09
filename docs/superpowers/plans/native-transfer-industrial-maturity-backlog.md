# Native transfer industrial maturity backlog

This artifact tracks follow-up work beyond the first adaptive native transfer
runtime release. The first release targets local POSIX `work_dir`, bytes-aware
slicing, eager cleanup, source-impact diagnostics, and self-service plan output.

## Object-backed transfer store

Goal: support S3, GCS, Azure Blob, and MinIO as transfer stores without changing
connector export/load ports.

Design direction:

- introduce a `TransferStore` port with `put`, `get`, `open_writer`,
  `open_reader`, `delete`, and `stat`;
- keep `ArtifactLease` semantics for both local and object-backed artifacts;
- add multipart upload support for large slices;
- support server-side encryption and bucket/path policy evidence;
- keep local POSIX as the default implementation.

User value:

- cross-pod resume becomes possible;
- weak workers no longer need large local disks;
- artifacts can survive pod eviction when policy requires it.

## Zero-file streaming pipe

Goal: avoid local files when source and sink tools support compatible
stdin/stdout streaming.

Design direction:

- add `StreamingSliceExporter` and `StreamingStagingLoader` ports;
- support bounded pipes with backpressure and byte counters;
- retain evidence with row counts, byte counts, checksums, and command spans;
- use file-based slices as fallback when either connector lacks streaming.

User value:

- minimal disk footprint;
- lower latency from source export to sink ingest;
- same correctness contract as file-backed transfer.

## Adaptive source throttling

Goal: protect production sources from transfer jobs that scan too aggressively.

Design direction:

- add optional `SourceLoadGuard` port;
- collect source wait events, query duration, rows/sec, and error rates;
- adjust `export_workers`, slice size, and retry backoff dynamically;
- expose hard guardrails in manifest policy.

User value:

- fewer production incidents from high-impact extracts;
- safer self-service workloads;
- explainable throttling evidence.

## Automatic index and boundary recommendations

Goal: give actionable source-side advice before a large transfer runs.

Design direction:

- add connector-specific `BoundaryAdvisor` implementations;
- inspect source metadata for indexes, statistics, key distribution, and
  nullable boundary columns;
- classify recommendations by safety and DBA impact;
- include generated SQL skeletons and explain-plan hints in `dpone plan`.

User value:

- faster path from slow view/table to production-safe partitioning;
- fewer ad hoc DBA conversations;
- clear tradeoffs for new indexes, persisted computed columns, and materialized
  boundaries.

## Visual run timeline and evidence explorer

Goal: make large transfers debuggable without reading raw logs.

Design direction:

- emit a normalized timeline artifact with partition, slice, export, load,
  cleanup, checkpoint, and gate spans;
- add a local HTML evidence explorer;
- support Airflow links and GitOps artifact index integration;
- keep the JSON artifact stable for external UIs.

User value:

- faster root cause analysis;
- visible progress and ETA;
- easy comparison between batch, pipelined, and streaming runs.

## Managed rollback and replay UI contract

Goal: make failed or partially staged runs recoverable through a UI-safe
contract.

Design direction:

- define `RunReplayPlan` and `RunRollbackPlan` artifacts;
- expose safe replay choices: `reexport`, `staging_if_verified`, and
  future `object_store_if_verified`;
- include target unchanged/staging retained evidence;
- keep destructive finalization actions behind explicit policy gates.

User value:

- predictable operator workflow after failures;
- no manual staging table archaeology;
- consistent UI/CLI behavior.

## Cross-pod resume

Goal: resume transfer execution when Kubernetes schedules the retry on another
worker.

Design direction:

- move durable checkpoints to `runtime.storage.checkpoint_dir` or object store;
- attach run id, schema hash, query hash, partition id, slice id, and staging
  marker to each checkpoint;
- never trust local files after eager cleanup;
- require store-backed artifacts for any file reuse policy.

User value:

- resilient long-running transfers;
- safe retries after pod eviction;
- clear boundary between local cache and durable state.

## Connector SDK for slicing and storage ports

Goal: make new connectors implement adaptive transfer without copying MSSQL or
ClickHouse internals.

Design direction:

- document `SlicePlanner`, `SliceExporter`, `PartitionStagingLoader`,
  `SourceImpactInspector`, and `TransferStore` ports;
- provide reference adapters for MSSQL BCP, PostgreSQL COPY, and ClickHouse
  HTTP/client ingest;
- add conformance tests for split/retry, cleanup, checkpoint, and staging
  finalization;
- keep connector modules thin and below the project SLOC gate.

User value:

- faster connector development;
- consistent UX across sources and sinks;
- fewer pair-specific one-off implementations.
