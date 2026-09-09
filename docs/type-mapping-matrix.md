# Type Mapping Matrix

This document defines the default type conversion policy for dpone source -> sink flows. It complements the per-flow guides in [Source -> sink matrix](source-sink-matrix.md).

## Table of contents

- [Principles](#principles)
- [Logical target defaults](#logical-target-defaults)
- [Generate pair-specific matrix with CLI](#generate-pair-specific-matrix-with-cli)
- [Certification profiles](#certification-profiles)
- [Source-specific caveats](#source-specific-caveats)
- [PostgreSQL caveats](#postgresql)
- [MSSQL / SQL Server caveats](#mssql-sql-server)
- [ClickHouse caveats](#clickhouse)
- [Generic REST API caveats](#generic-rest-api)
- [Kafka caveats](#kafka)
- [Per-flow docs](#per-flow-docs)

## Principles

- Prefer lossless native types when source and sink share the same semantics.
- Prefer string/JSON landing for vendor-specific, semi-structured, spatial, hierarchy, variant, enum/domain/composite, array/range, and protobuf-only values unless a manifest mapper is explicit.
- Never auto-apply narrowing. Narrowing is a breaking schema evolution change.
- Preserve numeric precision and scale. If the sink cannot represent the source precision, fail or land as string/generated column.
- Preserve timestamps intentionally: choose UTC instant, local wall-clock, or string fidelity in the manifest.
- Use `__dpone__nc__<column>` only for explicit incompatible type-change continuation.
- Explicit [schema contracts](schema-contracts.md) override inferred logical types.
- Target-specific [physical design](physical-design.md) overrides render concrete sink DDL types.

## Logical target defaults

<div class="wide-table" markdown="1">

| Logical type | MSSQL | PostgreSQL | ClickHouse | BigQuery | Kafka |
| --- | --- | --- | --- | --- | --- |
| boolean | bit | boolean | UInt8/Bool | BOOL | boolean/schema field |
| small integer | smallint/int | smallint/integer | Int16/Int32 | INT64 | int32/int64 |
| big integer | bigint | bigint | Int64 | INT64 | int64 |
| unsigned big integer | decimal(20,0) | numeric(20,0) | UInt64 | NUMERIC/STRING | decimal/string |
| decimal(p,s) | decimal(p,s) | numeric(p,s) | Decimal(p,s) | NUMERIC/BIGNUMERIC | decimal/string |
| float/double | float | double precision | Float64 | FLOAT64 | double |
| short string | nvarchar(n) | varchar/text | String | STRING | string |
| long text | nvarchar(max) | text | String | STRING | string |
| binary | varbinary(max) | bytea | String/base64 | BYTES | bytes/base64 |
| date | date | date | Date/Date32 | DATE | string/logical date |
| timestamp | datetime2 | timestamp/timestamptz | DateTime64 | TIMESTAMP/DATETIME | timestamp logical/string |
| JSON object | nvarchar(max) | jsonb | String/JSON | JSON/STRING | JSON object/value schema |
| array/list | nvarchar(max) JSON | array/jsonb | Array/String | REPEATED/JSON/STRING | array/schema field |
| map/object | nvarchar(max) JSON | jsonb | Map/String | JSON/RECORD/STRING | map/schema field |
| UUID/GUID | uniqueidentifier | uuid | UUID/String | STRING | string/logical uuid |
| enum/domain | nvarchar/text | native/text | String/Enum | STRING | string/enum schema |
| spatial/hierarchy | nvarchar(max)/varbinary(max) | text/extension type | String | GEOGRAPHY/STRING | string/bytes |

</div>

## Generate pair-specific matrix with CLI

Use `dpone schema type-matrix` when you need an explainable source -> sink
decision table for a concrete integration. This is the quickest way to debug
false schema-evolution type changes without reading connector code.

```bash
dpone schema type-matrix \
  --source mssql \
  --sink clickhouse \
  --source-type "int nullable" \
  --source-type "nvarchar(510) nullable" \
  --source-type "datetime" \
  --format md
```

The output includes the source type, target type, native transport, schema
evolution compatibility, lossless status, and runbook link. Supported
pair-specific profiles currently include:

| Source | Sink | Profile | Use case |
| --- | --- | --- | --- |
| MSSQL | ClickHouse | `mssql_to_clickhouse_lossless_v1` | Native `bcp queryout` -> ClickHouse TSV/HTTP loads. |
| MSSQL | PostgreSQL | `mssql_to_postgres_native_v1` | MSSQL streaming SELECT → Postgres COPY CSV loads (Batch ETL; wide vendor-live for FR/append/merge/replace/partition_replace/snapshot_diff/scd2/backfill; `varbinary` as `\x` hex CSV for `bytea`). |
| MSSQL | BigQuery | `mssql_to_bigquery_analytics_v1` | MSSQL streaming SELECT → BigQuery CSV staging loads (Batch ETL; wide vendor-live for FR/append/merge/replace/partition_replace/snapshot_diff/scd2/backfill; `varbinary` as base64 CSV for BYTES). |
| PostgreSQL | MSSQL | `postgres_to_mssql_native_v2` | PostgreSQL COPY/export -> MSSQL bcp loads. |
| MySQL | MSSQL | `mysql_to_mssql_native_v1` | MySQL streaming SELECT -> MSSQL character BCP loads (Batch ETL; wide vendor-live for FR/append/merge/replace/partition_replace/snapshot_diff/scd2/backfill; BLOB/varbinary N/A on character BCP). |
| MySQL | PostgreSQL | `mysql_to_postgres_native_v1` | MySQL streaming SELECT -> Postgres COPY CSV loads (Batch ETL; wide vendor-live for FR/append/merge/replace/partition_replace/snapshot_diff/scd2/backfill). |
| MySQL | ClickHouse | `mysql_to_clickhouse_analytics_v1` | MySQL streaming SELECT -> ClickHouse TabSeparated loads. |
| MySQL | BigQuery | `mysql_to_bigquery_analytics_v1` | MySQL streaming SELECT -> BigQuery CSV staging loads (Batch ETL; wide vendor-live for FR/append/merge/replace/partition_replace/snapshot_diff/scd2/backfill). |
| PostgreSQL | BigQuery | `postgres_to_bigquery_analytics_v1` | PostgreSQL COPY CSV -> BigQuery staging loads (Batch ETL; wide vendor-live for FR/append/merge/replace/partition_replace/snapshot_diff/scd2/backfill; `bytea` as base64 CSV for BYTES). |
| MSSQL | MSSQL | `mssql_to_mssql_identity_v1` | Same-family MSSQL landing (Batch ETL; wide vendor-live for FR/append/merge/replace/partition_replace/snapshot_diff/scd2/backfill; `varbinary` via hex character BCP; bare `tinyint`/`datetime` dialect collisions remain hermetic-only). |
| PostgreSQL | PostgreSQL | `postgres_to_postgres_identity_v1` | Same-family PostgreSQL landing (Batch ETL; wide vendor-live for FR/append/merge/replace/partition_replace/snapshot_diff/scd2/backfill; `bytea` identity). |
| PostgreSQL | ClickHouse | `postgres_to_clickhouse_analytics_v1` | Analytics landing from PostgreSQL into ClickHouse. |
| ClickHouse | MSSQL | `clickhouse_to_mssql_landing_v1` | Conservative ClickHouse analytical data landing into MSSQL. |

When a runtime schema plan already reports type changes, use
`dpone schema explain` with the same source/target column JSON to combine the
raw schema comparator and the pair-specific matrix:

```bash
dpone schema explain \
  --source source-columns.json \
  --target clickhouse-columns.json \
  --source-system mssql \
  --sink-system clickhouse \
  --format md
```

The output shows the raw source/target types, expected pair-specific target
type, whether the target matches that profile, and the diagnostic reason. This
is the preferred first step when a plan looks like “all columns changed type”
but the source and target are actually using expected cross-dialect types such
as `int` -> `Nullable(Int32)` or `nvarchar(510)` -> `Nullable(String)`.

## Certification profiles

`dpone` treats the critical type matrices as executable contracts, not prose.
The registry in `dpone.type_system.source_sink.certification` defines fixture
rows with source type, expected canonical type, target type, transport
encoding, lossless status, nullability behavior and decision category.

Decision categories exposed by `dpone schema type-matrix`, `dpone schema
explain`, physical DDL planning and tests:

| Category | Meaning |
| --- | --- |
| `auto_inferred` | Source metadata or sample inference produced a safe default mapping. |
| `explicit_logical_contract` | `schema_contract.columns` won over source/sample inference. |
| `explicit_physical_override` | `physical_design.columns.<column>.target_type.<sink>` won over all other decisions. |
| `compatible_widening` | Existing target type can safely receive the source type through widening. |
| `incompatible_requires_policy` | The source type is vendor-specific or semantically ambiguous and needs a contract, quarantine or variant-column policy. |
| `quarantine_required` | The value shape is unsafe for direct target writes and should go through quarantine. |
| `variant_column_required` | Continuation requires `__dpone__nc__<column>` because the target contract changed incompatibly. |

Precedence is always:

1. Explicit physical target override.
2. Explicit logical schema contract.
3. Source metadata or Schema Registry metadata.
4. Sample inference.
5. Safe fallback or quarantine/variant policy.

Route type-matrix suites currently cover (live certification is separate;
hermetic/contract-ready rows are marked explicitly):

| Route | Suite | What is certified |
| --- | --- | --- |
| MSSQL -> ClickHouse | `mssql_to_clickhouse_lossless_v2` | Integer, decimal/money, float, bit, UUID, date/time, `datetime`, `datetime2`, `smalldatetime`, `datetimeoffset`, string/unicode, binary, `rowversion`, nullable behavior and policy-required vendor types. |
| MSSQL -> PostgreSQL | `mssql_to_postgres_native_v1` | Integer family (`tinyint`→`smallint`), numeric/decimal, bit→boolean, float/real, date/time/`datetime2`→timestamp, `datetimeoffset`→timestamptz, nvarchar/varchar/text, UUID, `varbinary`→`bytea`, and explicit-contract-required spatial/`sql_variant`/XML landing. Hermetic type matrix; wide vendor-live covers FR/append/merge/replace/partition_replace/snapshot_diff/scd2/backfill with typed spot checks including `encode(c_varbinary,'hex')='010203'`. |
| MSSQL -> BigQuery | `mssql_to_bigquery_analytics_v1` | Integer family→INT64, NUMERIC/BIGNUMERIC, BOOL, DATE/DATETIME/TIMESTAMP/TIME, STRING, `varbinary`→BYTES (base64 CSV), uniqueidentifier→STRING, and explicit-contract-required spatial/`sql_variant`. Hermetic type matrix; wide vendor-live covers FR/append/merge/replace/partition_replace/snapshot_diff/scd2/backfill with typed spot checks including `TO_HEX(c_blob)='010203'`. |
| Postgres -> MSSQL | `postgres_to_mssql_native_v2` | Integer, numeric, bool, UUID, `bytea`, date/time, timestamp, `timestamptz`, JSON/JSONB, text/varchar and explicit-contract-required arrays/enums/ranges. |
| MySQL -> MSSQL | `mysql_to_mssql_native_v1` | Integer family, decimal, bool/`tinyint(1)`, date/time/datetime, text/varchar, blob/binary, JSON, and explicit-contract-required ENUM/SET/spatial. |
| MySQL -> PostgreSQL | `mysql_to_postgres_native_v1` | Integer family, numeric, bool/`tinyint(1)`, date/time/datetime→timestamp, text/varchar, bytea, JSON→jsonb, and explicit-contract-required ENUM/SET/spatial. |
| MySQL -> ClickHouse | `mysql_to_clickhouse_analytics_v1` | Integer family (incl. unsigned), Decimal, Bool/`tinyint(1)`, date/time/datetime→DateTime64, String/text, binary hex→String, JSON→String, and explicit-contract-required ENUM/SET/spatial. Wide vendor-live covers FR/append/merge/replace/partition_replace/backfill with typed spot checks via `system.columns` (`snapshot_diff`/`scd2` N/A on ClickHouse staged load). |
| MySQL -> BigQuery | `mysql_to_bigquery_analytics_v1` | Integer family→INT64, NUMERIC/BIGNUMERIC, BOOL/`tinyint(1)`, DATE/DATETIME/TIMESTAMP/TIME, STRING/BYTES, JSON, and explicit-contract-required ENUM/SET/spatial/`BIGINT UNSIGNED`. Hermetic type matrix; wide vendor-live covers FR/append/merge/replace/partition_replace/snapshot_diff/scd2/backfill with typed spot checks (contract-required types remain hermetic-only). |
| Postgres -> BigQuery | `postgres_to_bigquery_analytics_v1` | Integer family→INT64, NUMERIC/BIGNUMERIC, BOOL, DATE/DATETIME/TIMESTAMP/TIME, STRING, `bytea`→BYTES (base64 CSV), JSON/JSONB→JSON, UUID→STRING, and explicit-contract-required arrays/composites/geometry. Hermetic type matrix; wide vendor-live covers FR/append/merge/replace/partition_replace/snapshot_diff/scd2/backfill with typed spot checks including `TO_HEX(c_blob)='010203'`. |
| ClickHouse -> MSSQL | `clickhouse_to_mssql_landing_v1` | Signed/unsigned integers, Decimal, Float, Bool, String, UUID, Date, DateTime64, nullable/LowCardinality wrappers and explicit-contract-required arrays/maps/tuples/enums/high-precision values. |

Run contract certification locally:

```bash
uv run pytest -m type_matrix_certification tests/test_type_matrix_certification.py -q
```

Run the manual local-live profile with Docker services:

```bash
gh workflow run "Live certification" \
  -f profile=type_matrix_certification \
  -f row_count=10000
```

Expected evidence artifacts for the manual profile:

| Artifact | Purpose |
| --- | --- |
| `type_matrix_decisions.json` | Machine-readable route decisions and categories. |
| `physical_ddl_plan.sql` | Target DDL evidence for physical contracts. |
| `schema_evolution_rerun.json` | Rerun proof that expected mappings do not create false-positive breaking changes. |
| `typed_reconciliation.json` | Typed equality/hash evidence for temporal and nullability policies. |
| `live_fixture_summary.md` | Human-readable summary for release review. |

## Source-specific caveats

### PostgreSQL

- Arrays, ranges, multiranges, enums, domains, composites, geometric types, bit/varbit, tsvector/tsquery, pg_lsn, oid/reg* and extension/custom types require explicit mapping outside PostgreSQL targets.
- `jsonb` should land as target JSON when the target has a first-class JSON type, otherwise string JSON.
- `numeric` precision/scale must be preserved or the load fails.

#### PostgreSQL -> MSSQL detailed defaults

| PostgreSQL source type | MSSQL target type | Bulk representation | Notes |
| --- | --- | --- | --- |
| `smallint` | `smallint` | integer text | Preserves signed 16-bit range. |
| `integer` | `int` | integer text | Compatible with nullable target `int`. |
| `bigint` | `bigint` | integer text | Preserves signed 64-bit range. |
| `numeric(p,s)` | `decimal(p,s)` | decimal text | Fails or requires explicit contract if precision/scale cannot be represented. |
| `real` / `double precision` | `real` / `float` | float text | Use decimal for exact financial values. |
| `boolean` | `bit` | `0`/`1` text | Nullable booleans remain nullable. |
| `text` / `varchar(n)` | `nvarchar(max)` by default | `BulkTextCodec` text | Use physical design overrides for bounded `nvarchar(n)`. |
| `uuid` | `uniqueidentifier` | UUID text | Uses SQL Server native GUID type. |
| `json` / `jsonb` | `nvarchar(max)` | JSON text via `BulkTextCodec` | Use schema contracts for stricter shape. |
| `bytea` | `varbinary(max)` | binary-safe representation | Requires explicit certified binary-safe path for character bcp loads. |
| `date` | `date` | ISO date text | Preserves date semantics. |
| `timestamp without time zone` | `datetime2(6)` | timestamp text | Timezone-naive; no implicit conversion. |
| `timestamp with time zone` | `datetimeoffset(6)` | offset timestamp text | Preserves instant and offset semantics. |
| `time without time zone` | `time(6)` | time text | Preserves microsecond precision. |
| arrays/ranges/custom types | `nvarchar(max)` by default | JSON/text via `BulkTextCodec` | Production loads should declare `schema_contract`. |

Arrays, ranges, domains, enums and other custom PostgreSQL types are marked
`incompatible_requires_policy` in the certification suite even when the fallback
landing type is `nvarchar(max)`. This prevents accidental semantic loss: use an
explicit `schema_contract`, a physical target override, quarantine, or a
documented JSON/text landing contract.

### MSSQL / SQL Server

- `rowversion/timestamp` is a binary concurrency token, not a business timestamp.
- `sql_variant`, `hierarchyid`, `geometry`, `geography`, deprecated `text/ntext/image`, and CLR/user-defined types require explicit mapping.
- `uniqueidentifier` maps to native UUID/GUID where available.
- MSSQL -> ClickHouse uses the `mssql_to_clickhouse_lossless_v1` profile: `decimal(p,s)` and `numeric(p,s)` map to `Decimal(p,s)`, `money` maps to `Decimal(19,4)`, `smallmoney` maps to `Decimal(10,4)`, `datetime` maps to `DateTime64(3)`, `datetime2(p)` maps to `DateTime64(p)`, `smalldatetime` maps to `DateTime64(0)`, and `uniqueidentifier` maps to `UUID`.
- MSSQL `datetime`, `datetime2`, and `smalldatetime` support `type_fidelity.temporal.naive_timestamp.transfer_encoding`: `auto`, `text`, or `epoch`. `epoch` is a transport encoding for faster ClickHouse `DateTime64(p)` inserts; target storage remains `DateTime64(p)`.
- `datetimeoffset(p)` uses `type_fidelity.temporal.offset_timestamp`: default `utc_instant` maps to `DateTime64(p, 'UTC')`; use `fixed_timezone` for BI local time, `preserve_offset` to add `__dpone__tz_offset_minutes__<column>`, or `preserve_text` for raw audit text.
- `binary`, `varbinary`, and `rowversion` need `source.options.type_fidelity.binary_encoding: hex` or `base64` if downstream consumers require byte-readable exactness.
- `time(p)` maps to `String` by default. Use `source.options.type_fidelity.time_encoding: seconds_since_midnight` to land it as `UInt32` seconds when sub-second precision is not required.
- Use ODBC Driver 18 and bcp UTF-8/codepage settings for reliable Unicode movement.

#### MSSQL -> ClickHouse detailed defaults

| MSSQL source type | ClickHouse target type | Native transport | Schema evolution compatibility |
|---|---|---|---|
| `tinyint` | `UInt8` | numeric TSV | compatible with `Nullable(UInt8)` when source nullable |
| `smallint` | `Int16` | numeric TSV | compatible with `Nullable(Int16)` when source nullable |
| `int` | `Int32` | numeric TSV | compatible with `Nullable(Int32)` when source nullable |
| `bigint` | `Int64` | numeric TSV | compatible with `Nullable(Int64)` when source nullable |
| `decimal(p,s)` / `numeric(p,s)` | `Decimal(p,s)` | decimal TSV | compatible when precision/scale match |
| `nvarchar(n)` / `varchar(n)` | `String` | escaped TSV text | compatible with `Nullable(String)` when source nullable |
| `datetime` | `DateTime64(3)` | epoch ticks by default on native path | compatible with `Nullable(DateTime64(3))` |
| `datetime2(p)` | `DateTime64(p)` | epoch ticks by default on native path | compatible with `Nullable(DateTime64(p))` |
| `smalldatetime` | `DateTime64(0)` | epoch ticks by default on native path | compatible with `Nullable(DateTime64(0))` |
| `datetimeoffset(p)` | policy-dependent | UTC/fixed/text/offset companion | compatible according to `offset_timestamp` mode |
| `rowversion` / `timestamp` | `String` | hex/base64 when configured | binary token, not a temporal type |

Vendor-specific SQL Server types such as `sql_variant`, `hierarchyid`,
`geometry` and `geography` are marked `incompatible_requires_policy`. They can
still be landed as text/binary by explicit contract, but they are not treated as
ordinary auto-inferred strings.

### ClickHouse

- `Nullable(T)` maps to nullable target columns; `LowCardinality(T)` is treated as `T` for most sinks.
- `Array`, `Map`, `Nested`, `Tuple`, `AggregateFunction` need explicit landing policy outside ClickHouse; `SimpleAggregateFunction(f, T)` unwraps to `T`.
- Avoid mutation-based correction for target tables; prefer staging/shadow replacement or tombstone/sign modeling.

#### ClickHouse -> MSSQL full inventory (`clickhouse_to_mssql_landing_v1`)

Canonical registry:
`dpone.type_system.source_sink.clickhouse_mssql.classify_clickhouse_type`,
shared by the runtime DDL mapper and `dpone schema type-matrix --source
clickhouse --sink mssql`. Policy: every ClickHouse type is either **mapped**
(deterministic, width/precision exact, round-trips through schema-evolution
compatibility so chunk re-runs stay idempotent) or **explicitly unsupported**
(deterministic configuration error naming the reason and a source-side
workaround). There is no silent fallback for the ClickHouse dialect; the
`nvarchar(max)` safe-fallback (with a runtime warning) applies only to
non-ClickHouse dialects.

Mapped types:

| ClickHouse | MSSQL | Notes |
| --- | --- | --- |
| `Int8`, `Int16` | `smallint` | `tinyint` is unsigned, so `Int8` keeps its sign in `smallint` |
| `Int32` | `int` | |
| `Int64` | `bigint` | |
| `UInt8` | `tinyint` | |
| `UInt16` | `int` | |
| `UInt32` | `bigint` | |
| `UInt64` | `decimal(20,0)` | full unsigned range preserved |
| `Float32` / `BFloat16` | `real` | `BFloat16` widens losslessly |
| `Float64` | `float` | |
| `Decimal(P,S)` (P <= 38), `Decimal32/64/128(S)` | `decimal(P,S)` | precision/scale exact |
| `String` | `nvarchar(max)` | lossless escaped text transport |
| `FixedString(N)` | `nvarchar(N)` | text semantics; base64-encode binary payloads on the source |
| `Date`, `Date32` | `date` | |
| `DateTime`, `DateTime32`, `DateTime(tz)` | `datetime2(0)` | timezone metadata is not preserved |
| `DateTime64(P<=7[,tz])` | `datetime2(P)` | default precision 3 |
| `DateTime64(8..9)` | `datetime2(7)` | documented sub-100ns truncation |
| `Bool` | `bit` | |
| `UUID` | `uniqueidentifier` | |
| `Enum8` / `Enum16` | `nvarchar(max)` | values land as enum names |
| `IPv4` | `varchar(15)` | canonical dotted-quad text: portable, diff/bcp-safe |
| `IPv6` | `varchar(45)` | canonical RFC 5952 text |
| `Nullable(T)` / `LowCardinality(T)` | mapping of `T` | wrappers unwrap recursively |
| `SimpleAggregateFunction(f, T)` | mapping of `T` | stores plain `T` values |

Explicitly unsupported (configuration error + workaround):

| ClickHouse | Reason | Workaround on the source |
| --- | --- | --- |
| `Int128/256`, `UInt128/256` | exceed `decimal(38,0)` | `CAST(col AS Decimal(38,0))` or `toString(col)` |
| `Decimal(P>38,S)`, `Decimal256(S)` | exceed MSSQL `decimal(38)` | cast to `Decimal(38,S)` or `toString(col)` |
| `Array`, `Tuple`, `Map`, `Nested` | no relational equivalent; extraction emits Python literals | `toJSONString(col) AS col` |
| `JSON`, `Object('json')` | experimental, no deterministic text form | `toJSONString(col)` |
| `Point`, `Ring`, `Polygon`, `MultiPolygon`, `LineString`, `MultiLineString` | no MSSQL scalar equivalent in this route | `toJSONString(col)` or `wkt(col)` |
| `AggregateFunction(...)` | engine-internal binary state | `finalizeAggregation(col)` |
| `Interval*` | not storable table columns | `toString(col)` |
| `Variant(...)`, `Dynamic` | experimental, not deterministic across chunks | `toString(col)` / `toJSONString(col)` |
| `Time`, `Time64` | no exact MSSQL mapping in this route | `toString(col)` |
| `Nothing` | carries no values | drop from the source projection |

Physical value fidelity (boundary integers, negative decimals, microsecond
`DateTime64`, non-ASCII/emoji strings, NULLs) is certified end-to-end in
`tests/integration/backfill/test_backfill_clickhouse_mssql_type_fidelity_integration.py`;
the full contract inventory lives in `tests/test_mssql_type_mapper_contracts.py`.

### Generic REST API

- Explicit `columns` beat sampled inference.
- Volatile objects/arrays should land as JSON/string until the contract stabilizes.
- Date/time parsing must be configured because API formats often drift.

### Kafka

- Schema Registry gives the strongest type contract for Avro, JSON Schema, and Protobuf.
- Without Schema Registry, JSON payloads are inferred from samples and should land conservatively.
- Tombstones and `op=delete` require explicit delete handling.
- MySQL → Kafka wide live certification validates CSV→JSON wire payloads (stringly
  cells) in `tests/integration/mysql/test_mysql_to_kafka_vendor_live_integration.py`;
  there is no PairTypeMatrix profile for this route.

## Per-flow docs

See [Source -> sink matrix](source-sink-matrix.md) for links to every source -> sink guide.

## Related docs

- [Schema evolution](schema-evolution.md) explains how type widening and incompatible type changes are handled at runtime.
- [Type inference](type-inference.md) explains source metadata, samples, confidence, and fallback behavior.
- [Schema contracts](schema-contracts.md) explains explicit logical column contracts and enforcement modes.
- [Physical design](physical-design.md) explains concrete target DDL overrides, indexes, partitions, and storage hints.
- [Load strategies](load-strategies.md) documents which write strategies are supported per sink and source -> sink pair.
- [Source -> sink matrix](source-sink-matrix.md) links every supported flow to a dedicated implementation guide.
