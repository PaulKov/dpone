# SQL Server managed catalog transport v1

**Audience:** connector developers and maintainers implementing the managed dbt
physical catalog provider.

This reference defines the implemented immutable catalog records and strict
`decode_catalog_result` boundary in
`dpone.contracts.dbt_mssql_physical_catalog_wire`. It defines transport validation;
SQL acquisition, physical comparison, authenticated admission and publication
remain separate implementation responsibilities. The managed SQL provider is not
yet qualified or available as a complete publishing route.

Each call returns one resultset for one of nine closed kinds. Acquire HEADER,
the required kinds, then HEADER again within the admitted transaction, without
intervening mutation; compare both headers separately. Use `SET NOCOUNT ON` and
no additional diagnostic resultsets. The decoder does not establish these
server-side properties.

The exact column order and primitive representations below are the v1 wire
contract. Direct construction of the frozen DTOs is unchecked; providers must
use the decoder at their acquisition boundary. Forbidden-property semantics and
admission phase/role enums belong to the future physical provider, not this
transport. An unknown property code never grants physical acceptance.

## Decoder input and budgets

Input is one detached immutable tuple of row tuples, an expected row kind, expected
positive SQL-int object ID, and caller-supplied `max_rows` and
`max_definition_bytes`. Both budgets must be exact positive Python ints (not bool).
`max_rows` bounds data rows per resultset; the single empty marker remains legal.
Reject an outer/inner non-tuple rather than coercing arbitrary iterables.
Acquisition must separately enforce bounds before materializing a resultset;
decoding an already allocated tuple does not bound database/network acquisition.

Allowed kinds: HEADER, TABLE, COLUMN, INDEX, INDEX_COLUMN, PARTITION, DEPENDENCY,
FORBIDDEN_PROPERTY, COUNT. A decoder returns typed immutable data, never an
observation verdict. It does not query SQL, infer missing values or normalize names.

## Common ordered prefix

Every row begins with these five NOT NULL columns:

| Column | SQL type | Constraint |
|---|---|---|
| wire_version | smallint | exact integer 1 |
| row_kind | varchar(24) | exact expected enum string |
| object_id | int | positive, equals expected object ID |
| row_ordinal | int | 1..row_count for data; 0 only for empty marker |
| row_count | int | nonnegative, <= max_rows, identical across rows |

Nonempty resultsets contain exactly row_count rows in contiguous ordinal order
1..row_count. Reject duplicate/missing/reordered ordinals, repeated mismatched
counts, version/kind/object mismatches and unexpected tuple length.

An empty collection has exactly one row with ordinal=0, count=0 and ALL detail
fields NULL. No other mixed or partial marker is valid. HEADER, TABLE and COUNT
must contain exactly one data row and cannot use an empty marker. An empty SQL
resultset is invalid for every kind; it is not proof of absence or visibility.

## Ordered detail columns

All fields are NOT NULL on data rows unless marked `?`. Every detail field is NULL
on a valid empty marker. SQL type ranges and Python representations follow below.

### HEADER

`database_id int`, `database_guid uniqueidentifier`,
`object_create_time char(27)`, `object_modify_time char(27)`,
`schema_name nvarchar(128)`, `object_name nvarchar(128)`, `object_type char(2)`,
`column_count int`, `index_count int`, `index_column_count int`,
`partition_count int`, `dependency_count int`, `forbidden_property_count int`.

All six collection counts are nonnegative and <= max_rows. They are transport
claims only. Cross-call header equality and count comparison belong to subsequent
acquisition/comparison, not a single-resultset decoder.

### TABLE

`schema_id int`, `schema_name nvarchar(128)`, `object_name nvarchar(128)`,
`object_type char(2)`, `object_create_time char(27)`,
`object_modify_time char(27)`, `is_memory_optimized bit`, `durability tinyint`,
`temporal_type tinyint`, `is_filetable bit`, `is_node bit`, `is_edge bit`,
`ledger_type tinyint`, `lob_data_space_id int`, `filestream_data_space_id int`.

### COLUMN

`column_id int`, `name nvarchar(128)`, `system_type_id tinyint`, `user_type_id int`,
`type_schema nvarchar(128)`, `type_name nvarchar(128)`, `max_length smallint`,
`precision tinyint`, `scale tinyint`, `collation_name nvarchar(128)?`,
`is_nullable bit`, `is_ansi_padded bit`, `is_identity bit`, `is_computed bit`,
`is_sparse bit`, `is_column_set bit`, `is_hidden bit`,
`generated_always_type tinyint`, `encryption_type int?`, `is_masked bit`,
`default_object_id int`, `rule_object_id int`.

Preserve max_length=-1 as a catalog fact; transport acceptance does not admit LOBs.

### INDEX

`index_id int`, `name nvarchar(128)?`, `type tinyint`, `type_desc nvarchar(60)`,
`is_unique bit`, `is_primary_key bit`, `is_unique_constraint bit`,
`is_disabled bit`, `is_hypothetical bit`, `has_filter bit`,
`filter_definition nvarchar(max)?`, `data_space_id int`,
`data_space_name nvarchar(128)?`, `data_space_type char(2)?`.

Preserve heap index_id=0 and NULL name. Filter definition's UTF-16LE byte length
must not exceed max_definition_bytes; never truncate it.

### INDEX_COLUMN

`index_id int`, `index_column_id int`, `column_id int`, `key_ordinal tinyint`,
`partition_ordinal tinyint`, `is_descending_key bit`, `is_included_column bit`,
`column_store_order_ordinal int`.

### PARTITION

`index_id int`, `partition_number int`, `partition_id bigint`, `hobt_id bigint`,
`data_compression tinyint`, `data_compression_desc nvarchar(60)`,
`data_space_id int`, `data_space_name nvarchar(128)?`, `data_space_type char(2)?`.

### DEPENDENCY

`direction varchar(8)`, `referencing_id int`, `referencing_minor_id int`,
`referenced_id int?`, `referenced_minor_id int`,
`referenced_server_name nvarchar(128)?`,
`referenced_database_name nvarchar(128)?`,
`referenced_schema_name nvarchar(128)?`,
`referenced_entity_name nvarchar(128)?`, `is_schema_bound_reference bit`,
`is_caller_dependent bit`, `is_ambiguous bit`.

Direction is exactly INBOUND or OUTBOUND. Preserve nullable unresolved targets;
transport decoding cannot declare them supported dependencies.

### FORBIDDEN_PROPERTY

`property_code varchar(40)`, `related_object_id int?`, `related_column_id int?`.

For this slice property_code is bounded nonempty ASCII transport text only.
No semantic closed-code enum or interpretation is registered by this decoder.
Every received code is preserved. Later physical admission must fail closed on
unrecognized codes; defining codes and catalog queries is a separate prerequisite.

### COUNT

`row_count_exact bigint` (nonnegative). Represents the procedure's actual COUNT_BIG
claim. Decoder acceptance alone cannot establish that COUNT_BIG was executed.

## Primitive conversion

- Integer fields require type(value) is int; bool, float, Decimal and strings
  reject. Enforce SQL tinyint 0..255, smallint -32768..32767,
  int -2147483648..2147483647, bigint -9223372036854775808..9223372036854775807.
  Apply the stricter envelope/count bounds stated above. Physical validity of
  other catalog numeric values is deferred to comparison; never silently repair.
- SQL bit fields require exact Python bool, the qualified pyodbc representation.
  Reject integer 0/1 and string equivalents; acquisition through another driver
  needs an explicit reviewed conversion boundary, not decoder coercion.
- uniqueidentifier requires exact uuid.UUID; acquisition must configure that
  driver representation or explicitly adapt before this decoder. No string guess.
- Creation/modification timestamps use exact canonical ASCII char(27) text
  YYYY-MM-DDTHH:MM:SS.fffffff, retaining all seven fractional digits. Validate
  Gregorian date and clock ranges (years0001..9999, no leap seconds/timezone),
  preserve the exact string; reject datetime objects and shortened/reformatted
  strings. The SQL producer must explicitly emit this lossless representation.
  Python datetime may validate the date portion but must not round/replace the
  retained seven-digit value. This closes the earlier microsecond precision gap.
- Strings require exact str, valid UTF-16LE encoding (reject unpaired surrogates),
  and SQL nvarchar length measured in UTF-16 code units. Preserve spelling and
  case. Non-definition name/description fields are nonempty; char(2) fields are
  exactly two ASCII characters; varchar fields are ASCII within their declared
  maximum. Do not strip whitespace or lowercase values.
- None is legal only for explicitly nullable detail fields or the complete
  empty marker. No default value is synthesized.

## Meaningful transport tests

One valid tuple per kind; real zero-data markers for collection kinds; row_count
zero for a real COUNT summary; heap index zero/NULL name; all legal null branches.
Then wrong kind/version/object/count, empty resultset, missing/extra columns,
mixed markers, duplicate/gapped/reordered ordinals, count/max_rows overflow,
integer boundaries and bool confusion, bit coercion, wrong datetime/UUID types,
Unicode SQL-width and definition-byte boundaries. Mutate one field at a time
from a baseline-valid case. No synthetic test is SQL visibility/transaction proof.

[Native execution overview](native-generation-execution.md) ·
[Architecture decision](adr/0065-trusted-isolated-native-generation-execution.md)

## Executable acquisition and comparison

The [catalog acquisition guide](dbt-mssql-physical-catalog-acquisition.md) describes
the separate signed SQL producer, bounded incremental reader, exact structural
comparison and operator recovery. These consumers enforce additional invariants;
wire decoding alone still grants no visibility, admission or execution authority.
