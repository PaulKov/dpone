# Stage a validated character file in ClickHouse

This guide is for data engineers integrating an already exported, contract-validated
character file with a ClickHouse sink. The explicit Python API prepares a separate
RowBinary file and returns a verified staging handle. It preserves text controls,
NULL versus empty values, and the chosen binary representation.

Use this API when your application owns the export and staging lifecycle. Ordinary
`dpone run`, manifests, `stage_payload`, and automatic source selection do not activate
it. The PostgreSQL-to-ClickHouse route keeps its existing behavior. Local tests cover
this API; live ClickHouse value certification and performance measurement remain
**UNVERIFIED**.

## Before you start

- Use an uncompressed, headerless `FileExportArtifact` with format `mssql-delimited`,
  the default `BulkTextCodec` version 2, an exact exported row count and a genuine
  `FileContractValidationReceipt`. The format name describes the producer grammar.
- Keep the original source file immutable and available throughout the call.
- Select `client` or `http` explicitly in `LoadConfig.options.clickhouse_bulk`.
  Client requires `clickhouse-client`; HTTP uses the standard-library HTTP transport.
- Use one directly addressed ClickHouse node, an existing **Atomic** database, and
  permissions to inspect server/database/table/column identity, create a local
  MergeTree table, insert, count, drop, and cancel the exact query ID.
- Reserve a private local work directory and explicit spool capacity. Keep other
  writers and external DDL out of this API's `__dpone_b02_` staging namespace.
- Configure the sink connector, selected transport, and later finalizer for the
  same node/database. Probes observe the controlled transport's endpoint; they do
  not inspect an already-open finalizer socket or guarantee future reconnects.

Distributed/replicated staging, load balancers, automatic failover, async inserts,
permissive parser settings, projections and unsupported types are outside this API.

## First staging call

The function below accepts your existing sink, load configuration and completed
export. Run it only in your application's approved target environment. Contract
validation is performed by the genuine producer before creating the wrapper.

```python
from pathlib import Path
from typing import Sequence

from dpone.config.load_config import LoadConfig
from dpone.readiness.schema_contracts import SchemaContract
from dpone.runtime.etl.contract_artifacts import ContractValidatedFileArtifact
from dpone.runtime.etl.file_contract_validation import validate_mssql_delimited_file_contract
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.governance.ports import StagedLoadHandle
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.runtime.sinks.clickhouse_validated_file_models import ClickHouseValidatedFilePolicy
from dpone.runtime.sinks.load_payload import LoadPayload


def stage_export(
    sink: ClickHouseSink,
    config: LoadConfig,
    original: FileExportArtifact,
    schema: Sequence[tuple[str, str]],
    contract: SchemaContract,
    work_directory: Path,
) -> StagedLoadHandle:
    validate_mssql_delimited_file_contract(original, schema=schema, contract=contract)
    wrapped = ContractValidatedFileArtifact(
        original, contract=contract, schema=schema,
        run_id="character-file-stage", load_id="character-file-stage",
    )
    return sink.stage_validated_file(
        config,
        LoadPayload(wrapped, schema),
        policy=ClickHouseValidatedFilePolicy(
            work_directory=work_directory,
            max_spool_bytes=1_073_741_824,
        ),
    )
```

For an ordered source schema `[("id", "int"), ("note", "nvarchar(max) nullable"),
("payload", "varbinary(max) nullable")]`, these are valid **LoadConfig options**:

```yaml
clickhouse_bulk:
  mode: http
  http:
    timeout_seconds: 3600
type_fidelity:
  binary_encoding: none
physical_design:
  columns:
    id:
      target_type:
        clickhouse: Int32
    note:
      target_type:
        clickhouse: Nullable(String)
    payload:
      target_type:
        clickhouse: Nullable(String)
```

These options are not a new manifest activation mechanism. HTTP defaults to port
8123 and non-TLS unless explicitly configured in `clickhouse_bulk.http`; client
inherits the sink connector's host, port, database and TLS defaults. Select the
same node explicitly when these defaults do not match your deployment.

On success, inspect `handle.staged_rows` and
`handle.metadata["validated_file_consumption"]`. The record has `outcome="staged"`,
exact prepared/observed counts and a `journal_directory`. The source remains owned
by its existing lifecycle; the temporary RowBinary spool has been removed. The
private staging table is transferred to the existing staging/finalization owner.
No source checkpoint, target promotion, publication or certification occurs here.
An empty export still creates and counts its empty table, without submitting INSERT.

For a successful exploratory call, dispose of the returned staging table through
its existing owner when you finish inspecting the handle:

```python
handle = stage_export(sink, config, original, schema, contract, work_directory)
try:
    print(handle.staged_rows)
    print(handle.metadata["validated_file_consumption"]["journal_directory"])
finally:
    sink.abort_staged_load(handle)
```

Here `sink`, `config`, `original`, `schema`, `contract` and `work_directory` are the
prepared application inputs described above. Source cleanup stays with its existing
owner. For a real load, hand the successful handle to your existing governed
finalization flow instead of aborting it.

For a local reproducible example without a database, from a repository checkout
with development dependencies, run:

```bash
uv run pytest tests/test_b02_clickhouse_file_value_oracle.py -q
```

Those tests exercise both real adapters against local protocol peers and independently
decode their output. They do not certify an external ClickHouse server.

## Representation reference

| Source type | Required target family | Value rule |
| --- | --- | --- |
| char/varchar/nchar/nvarchar/text/ntext/json/xml/string | String, optionally Nullable | Exact UTF-8; no trimming, quote removal or JSON reserialization |
| producer-recognized binary/varbinary/image | String, optionally Nullable | Decode source hex first; `none` preserves bytes, `hex` emits lowercase ASCII, `base64` emits padded RFC4648 ASCII |
| tinyint/smallint/int/bigint | UInt8/Int16/Int32/Int64 respectively, optionally Nullable | Exact integer and range; fractions rejected |
| bit | Bool, optionally Nullable | Only source `0` and `1` |
| decimal(p,s)/numeric(p,s) | matching Decimal(p,s), optionally Nullable | Explicit precision 1–76 and scale 0–precision; finite, exact representation |
| float, temporal, UUID, nested, FixedString or mismatched target | Unsupported | Rejected before CREATE, including empty exports |

A raw empty source field means NULL. The nonempty codec empty marker means empty
text or empty bytes. Literal `\N`, `NULL`, spaces, quotes and backslashes remain
data. NULL cannot populate a non-nullable target. Use canonical binary spellings:
for example `varbinary(max) nullable` is recognized by the producer; ambiguous
`binary nullable` is rejected. Binary `none` here preserves raw bytes, whereas some
legacy row paths use hexadecimal text; consumers must choose deliberately.

## Capacity and time reference

All numeric policy values are positive finite integers; booleans, zero, negative
values and `None` are rejected. Limits count bytes, not characters.

| Policy field | Default |
| --- | --- |
| `work_directory: Path` | Required; explicit local directory |
| `max_spool_bytes` | Required; includes spool and all journal event bytes |
| `max_source_bytes` | 4,294,967,296 bytes (4 GiB) per verification scan |
| `max_record_bytes` | 16,777,216 bytes (16 MiB), including terminating LF |
| `min_free_bytes` | 1,073,741,824 bytes (1 GiB) reserve |
| `preparation_timeout_seconds` | 3600 seconds, one deadline from attempt entry through pre-CREATE checks |
| `verification_timeout_seconds` | 3600 seconds, one deadline after INSERT/no-INSERT through COUNT, metadata, file checks and completion |

The selected transport's `timeout_seconds` defaults to 3600 for this API. Control
confirmation has a separate 30-second bound; local process abort/join uses 5 seconds
per phase. Buffers are at most 1 MiB per chunk, 64 KiB combined response capture and
256 KiB per journal event. Capacity is refreshed while writing; external disk use
can still cause failure. Time limits are cooperative around fixed-size file reads,
including EOF, and bounded lock waits. They cannot preempt a stalled OS filesystem
call. This limitation also applies to filesystem publication and fsync.

Only explicit `client` (including `clickhouse-client` and `native_client` aliases)
or `http` is admitted. `RowBinary`, synchronous inserts and zero tolerated parser
errors are fixed. Arbitrary settings, legacy ClickHouse option aliases, `auto`,
Python/native-driver modes and unbounded transport timeouts are rejected.

## Diagnose and recover

The API prints no progress or CLI envelope. Capture the original exception and
inspect `error.details.get("validated_file_consumption")` when available. Admission
and fidelity errors use code `DPONE_CLICKHOUSE_FILE_CONSUMPTION_BLOCKED`, with
`blocker`, `phase`, and safe column/one-based row ordinal when applicable. Original
source, integrity, connector, timeout and cancellation exceptions remain primary.
Errors before journal creation may have no journal record.

| Signal | Next action |
| --- | --- |
| `explicit_mode_required`, `transport_unsupported`, `settings_unsupported` | Correct the explicit selected transport and finite settings before another call |
| `codec_profile_unsupported`, `source_type_unsupported`, `target_representation_unsupported` | Check the original producer/profile and representation table; do not relabel or forward encoded source bytes |
| `value_not_representable` | Correct the reported row/column or choose an approved representation; values are excluded from the journal |
| Source receipt/integrity error, `schema_changed`, `transport_changed` | Investigate changed file, receipt, schema or endpoint; regenerate and validate through the producer |
| `resource_limit` or verification timeout | Check byte budgets, free-space reserve and phase timing; retained resources still require reconciliation |
| `staging_count_mismatch` | Treat the attempt as failed; matching transport bytes alone do not prove row authority |
| `attempt_journal_unavailable` | Restore reliable storage; do not infer durability from a merely visible event file |
| `retained_unknown` or `cleanup_failed` outcome | Stop retries; follow the inventory procedure below |

For `failed_cleaned`, either no remote mutation was submitted, or submitted
execution was confirmed terminal and ownership checked where necessary. Any
created staging/spool resources have been removed. A later explicit call
creates a new attempt. It never resumes a partial attempt or deduplicates globally.
For `retained_unknown`/`cleanup_failed`:

1. Retain the journal and named spool. Find the exact server/database, query ID,
   planned table, UUID and `dpone-b02:<attempt_id>` ownership comment in the last
   durably confirmed record. Unknown fields remain null.
2. Have the operator establish the exact query's terminal state on that same node.
   Local exit, a closed socket, HTTP 200, an empty process list or an empty KILL
   response alone is insufficient. Do not issue wildcard cancellation.
3. Verify table UUID, ownership marker, engine and namespace before manual cleanup.
   A crash before durable UUID evidence requires investigation; do not infer ownership.
4. Reconcile/remove only those owned resources under your operational controls.
   Archive the journal after resolution, then start a fresh explicit attempt.

There is no automatic recovery/delete command, retry integration or crash-cleanup
guarantee. UUID inspection and DROP are separate operations; exclude external DDL
races. Completed journals also remain until caller-controlled archival.

Upgrade by adopting this explicit API and its caller together. To roll back, stop
new calls, reconcile outstanding attempts and move caller/package together; do not
fallback to raw encoded file forwarding.

See [developer contracts and evidence](developer-validated-clickhouse-file-staging.md),
[data-contract runtime](data-contract-runtime.md), [connector SDK](connector-sdk.md)
and the unchanged [Postgres-to-ClickHouse route](source-sink/postgres-to-clickhouse.md).
