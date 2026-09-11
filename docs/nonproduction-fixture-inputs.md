# Finite fixture inputs and canonical recipes

This reference is for developers implementing the approved synthetic
[qualification plans](nonproduction-qualification-plans.md). Canonical recipe
functions describe the two fixed wide fixtures. An internal input original binds
the expected seed steps for one route to exact source files. These pure values
and SQL text do not seed a database, observe a generation or reserve export bytes.
See the [authority overview](nonproduction-composition-authority.md) for the
complete campaign and its currently unavailable execution path.

**Implementation status:** this page describes the approved isolated fixture
candidate. The input and recipe modules are not present in the integrated branch
at `dfccfad`. Independent correctness review passed, but architecture checks still
block integration. The APIs below are candidate interfaces; they are not yet
importable from the current checkout or an installed release.

## Expected input, observed generation and export bound

| Evidence | What it establishes |
|---|---|
| Fixture input original | Exact declared recipe, parameters, implementation files and preceding seed steps |
| Future protected generation evidence | Actual seed outcome, object/helper identity and closure of every writer through export |
| Future export-bound derivation | Complete bytes for that generation, exact projection and actual codec before export starts |

These obligations are cumulative. A known key range does not prove successful
insertion or writer closure. A row count does not bound every encoded column.
Repeatable-read extraction alone does not exclude an independent writer. Unknown
seed outcomes must retain ownership and charges; retrying by table name cannot
replace recovery evidence.

## Canonical recipe APIs

Runtime modules own each literal inventory once. Legacy tool/test wrappers adapt
the immutable records to their existing DTO classes, lists and SQL producers.
Runtime never imports a fixture tool or test module.

| Module and API | Result |
|---|---|
| `dpone.runtime.nonproduction_bcp_fixture_recipe.bcp_fixture_columns(column_count)` | Tuple of `BcpFixtureColumn(name, mssql_type, insert_expression)` |
| `render_bcp_finite_insert(source, row_count=...)` in that module | SQL text for the fixed 202-column MSSQL seed; no execution |
| `dpone.runtime.nonproduction_postgres_fixture_recipe.postgres_fixture_columns()` | Tuple of 128 `PostgresFixtureColumn(name, pg_ddl, pg_type, seed_sql)` records |
| `dpone.runtime.nonproduction_postgres_fixture_rows.postgres_seed_rows(seed_step)` | Two initial SQL-value vectors or one watermark vector |

`render_bcp_finite_insert` requires an exact MSSQL table
`NonproductionPlanObject` and strict integer row count from 0 through 100000.
It validates the source before the row count. No arbitrary SQL, schema creation,
DROP, column-count selector or implicit object name is accepted.

Its five fixed decimal digits enumerate `gs = 1 + a + 10*b + 100*c + 1000*d +
10000*e`, for digits 0–9. The selector converts `gs` to SQL `bigint` **before**
any original column expression runs, filters `1 <= gs <= row_count`, and orders
by `gs`. This gives the intended exact prefix and an empty selection for zero.
Early conversion matters: an expression such as `CAST(gs*1000003 AS bigint)`
can overflow an integer intermediate at key 2148 despite its outer cast.
Original column expressions are preserved. Actual SQL generation and exported
payload equality remain live acceptance obligations.

PostgreSQL steps are exactly `initial` and `watermark_3`. Initial vectors retain
the existing first row and second row with key 2, 11:00 watermark, `row-two`,
NULL for nullable columns and original expressions otherwise. The watermark
vector uses key 3, 12:00 and `row-three`; it is one additional row, not the
cumulative three-row dataset. The four serial/identity expressions explicitly
retain 71, 72, 73 and 74. No sequence-consumption claim is inferred from them.
Values remain SQL expressions; the API does not approximate their server results.

## Closed input original

`dpone.contracts.nonproduction_fixture_input.NonproductionFixtureInput` uses
schema `dpone.nonproduction-fixture-input.v1`. This internal metadata original
adds no signed authority family. Its exact required fields are:

| Field | Meaning |
|---|---|
| `schema` | Exact wire value `dpone.nonproduction-fixture-input.v1`; emitted by the codec, not a constructor argument |
| `profile` | Existing BCP-wide or PostgreSQL-wide profile |
| `fixture_id`, `source_object_id` | Exact logical plan references |
| `parameters` | Existing strict `row_count` integer or `watermark_key_3` boolean |
| `recipe_original` | Complete selected fixture recipe descriptor |
| `seed_program` | `mssql_bcp_decimal_prefix_v1` or `postgres_wide_explicit_rows_v1` |
| `implementation_originals` | Exact canonical recipe source descriptor set |
| `seed_steps` | Ordered `{work_item_id, seed_step}` references for this generation |

The BCP implementation set is exactly
`src/dpone/runtime/nonproduction_bcp_fixture_recipe.py`. The PostgreSQL set is
exactly `src/dpone/runtime/nonproduction_postgres_fixture_recipe.py` and
`src/dpone/runtime/nonproduction_postgres_fixture_rows.py`. Descriptors are sorted
by path and retain exact digest and byte size. The legacy recipe descriptor
continues to name its existing producer path. A protected source-bundle reader
must eventually acquire and verify all these originals.

Seed references are in generation order, not lexical ID order. BCP has one
initial step. PostgreSQL has its initial step and optionally its enabled
watermark step if that step precedes the selected route. IDs are unique.
Parameters use an immutable tuple of pairs internally; serialization emits the
closed parameter object. Descriptors use the existing `NonproductionPlanOriginal`.

The document and original descriptors retain the existing 1 MiB bounds and
canonical UTF-8 JSON rules. Unknown members, duplicate/noncanonical bytes,
coerced types, unsupported program/path combinations and invalid step mixtures
reject. No SQL, generated row values, asserted observations, byte maxima, clock,
epochs, owner, success flag or permission is embedded. It contains no self,
fixture-plan, qualification-plan, parent-item or grant digest.

## Construction and complete readback comparison

Use `describe_fixture_input(fixture_plan, fixture_id=..., seed_items=...,
implementation_originals=...)` to construct the expected input from a complete
fixture plan and one or two exact seed-item originals. This construction precedes
the containing route, qualification plan and grant, avoiding a hash cycle.
It validates the selected fixed recipe and step values without claiming campaign
validity or independently acquired source bytes.

`to_dict()`, `to_bytes()`, `input_sha256` and
`from_bytes(raw, expected_sha256=...)` provide the strict original interface.
`require_route(originals, route_work_item_id=...)` takes the complete existing
`NonproductionQualificationPlanOriginals` pair and returns the exact selected
route after these comparisons:

1. Revalidate the complete pair and select the route by its exact work-item ID.
2. Match fixture, source, profile, parameters, recipe and accounting profile.
   Match this input's digest and byte length to the route's input descriptor.
3. Traverse the route's predecessor closure, within the existing 64-item bound.
   Find every declared writer to the full selected source identity, regardless
   of purpose or the writer's fixture label.
4. Require precisely this fixture's initial seed and its applicable optional
   PostgreSQL watermark seed. Reject any unexplained ancestor source writer,
   even when the broader plan legitimately declares that effect.
5. Compare the exact ordered step references. A future descendant writer is not
   part of this route's expected generation; protected runtime admission must
   still prevent it from mutating the source during the export.

The input descriptor's path is a future acquisition location; it is not a field
inside the input's own bytes. Pair comparison supplies no authentication,
reservation, current worker identity or source-seal proof.

New fixed reasons are `fixture_input_profile`, `fixture_input_originals`,
`fixture_input_steps`, `fixture_input_reference`, `fixture_input_writers`,
`fixture_recipe_source`, `fixture_recipe_rows` and `fixture_recipe_step`.
Existing primitive/plan errors remain unchanged. Invalid schema or noncanonical
JSON retains `document`; invalid raw-byte size/type retains `document_budget`.
Correct the producer or unsupported generation rather than rewriting a digest,
discarding a predecessor or accepting a smaller source set.

## Compatibility and remaining implementation

The existing BCP tool keeps its original DTO/list exports, variable counts,
SQL text, events and seven-digit TOP/ROW_NUMBER selector. Its historical Python
slice/range/comparison behavior remains unchanged, including the existing
200-column caller. The new finite renderer has a separate strict API.

The existing PostgreSQL fixture keeps its five-field `WideColumn`, mapper calls,
constants, exact five initial SQL statements and watermark statement. Its
arbitrary legacy `row_id` still uses `str(row_id)`. Existing mapper and connector
exceptions propagate. Legacy conditional enum creation and CASCADE remain
confined to that producer; they are not a guarded seed implementation.

A future protected PostgreSQL renderer must explicitly bind all four sequence
objects and the enum, preserve serial/identity semantics, and prove actual
backend/version/helper behavior. BCP and PostgreSQL need actual fresh seed
transactions, outcome recovery, source leases and exact complete native/COPY
byte bounds. Native dbt output needs its own independently derived finite bound.
The recipes and input codec enable none of these operations or public factories.

Moved implementation source bytes acquire new hashes. New source bundles and
grants must pin those originals; retained production/native-v2 documents and
historical evidence keep their original bytes and digests. Developer tests
compare pre-edit DTO values and SQL/event originals, exercise strict failures and
the full decimal domain, and check mutation detection. Offline text and value
tests do not certify live SQL or the complete current/provider/worker campaign.
