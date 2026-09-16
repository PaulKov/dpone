# Preview managed SQL Server candidate DDL

This reference is for platform engineers and adapter developers who already have
an immutable [physical model plan](dbt-mssql-physical-plans.md). The renderer turns
that plan into deterministic SQL text for creating its candidate and applying
one physical layout. It does not connect to SQL Server or execute the text.

A preview is useful for reviewing the exact column order, nullability, collation,
filegroup and generation-derived object names before integration with the managed
writer. It is not proof of plan membership, authenticated originals, schema
ownership, available resources, permissions, catalog equality or enrollment.

## Render a preview

For actual plans, first use the existing canonical plan-set decoder and retain the
validated model. This self-contained example uses synthetic identities solely to
show the rendering API; it is not an executable enrollment or provisioning input.

```python
from hashlib import sha256

from dpone.contracts.dbt_mssql_physical_rendering import (
    render_candidate_create,
    render_candidate_layout,
)
from dpone.contracts.dbt_mssql_physical import (
    AbsentPredecessor,
    PhysicalFilegroup,
    PhysicalModelPlan,
    PhysicalModelSpec,
    PhysicalRelation,
)
from dpone.contracts.mssql_type_contract import MssqlCatalogColumn
from dpone.contracts.native_identity import OriginalRef

spec = PhysicalModelSpec(
    model_unique_id="model.example.orders",
    source_graph_sha256="sha256:" + sha256(b"example graph").hexdigest(),
    relation=PhysicalRelation("example", "models", "orders"),
    columns=(
        MssqlCatalogColumn("id", "int", False),
        MssqlCatalogColumn("label", "nvarchar(40)", True, "Latin1_General_100_BIN2"),
    ),
    layout="rowstore_page",
    filegroup=PhysicalFilegroup(1, "PRIMARY"),
    resource_bounds=OriginalRef(
        "examples/profile", "sha256:" + sha256(b"example profile").hexdigest()
    ),
)
plan = PhysicalModelPlan(
    "10000000-0000-0000-0000-000000000001", spec, AbsentPredecessor()
)
create_sql = render_candidate_create(plan)
layout_sql = render_candidate_layout(plan)
print(create_sql)
print(layout_sql)
```

The first string creates only the derived `dpone_c_...` candidate in the plan's
explicit database and schema, with the columns in their original order. It names
the selected filegroup and explicitly requests heap compression `NONE`. The
second string requests one offline PAGE rebuild with `MAXDOP = 1`. Neither string
addresses the logical target `orders`, a helper or a backup.

## API and layout contract

Both functions accept an exact `PhysicalModelPlan`. They reuse the existing strict
single-model decoder behind the plan-set codec, including canonical type spelling,
derived names, nested spec digest and plan digest. They reject a tampered record
without changing it. There is no separate, weaker SQL type parser.

| Function | Result |
| --- | --- |
| `render_candidate_create(plan)` | One semicolon-terminated `CREATE TABLE` string for an empty heap with `DATA_COMPRESSION = NONE`. |
| `render_candidate_layout(plan)` | One semicolon-terminated layout statement, or Python `None` for `rowstore_none`. |

| Selected layout | Layout statement |
| --- | --- |
| `rowstore_none` | None; the caller must verify the heap remains NONE. |
| `rowstore_row` | One table rebuild with ROW compression, `ONLINE = OFF`, `MAXDOP = 1`. |
| `rowstore_page` | One table rebuild with PAGE compression, `ONLINE = OFF`, `MAXDOP = 1`. |
| `columnstore` | One ordinary clustered columnstore index using the plan's derived index name, explicit COLUMNSTORE compression, `ONLINE = OFF`, `MAXDOP = 1`, and the selected filegroup. |

Every database, schema, candidate, column, filegroup and index identifier
is bracket-quoted; closing brackets are escaped. Unicode and exact spelling are
retained. The filegroup's numeric ID is not substituted for its name: an admission
consumer must independently resolve and compare both identities.

Collation uses SQL's separate literal-name grammar: for example,
`COLLATE SQL_Latin1_General_CP1_CI_AS`, without identifier brackets or string quotes.
The renderer accepts only a full ASCII token matching `[A-Za-z][A-Za-z0-9_]{0,127}`,
retaining its exact spelling. Whitespace, Unicode, comments, delimiters and SQL
fragments are rejected. This lexical safety check does not prove that a collation
exists or is supported: the admission consumer must verify the selected name
against the SQL Server catalog before execution. No name is normalized or repaired.

The renderer accepts the existing canonical finite physical type grammar, except
`varchar(max)`, `nvarchar(max)` and `varbinary(max)`, which the managed physical
policy does not support. XML, CLR/alias types, rowversion/timestamp, malformed
sizes and SQL fragments are rejected by the existing type contract. Noncanonical
spellings such as `INT`, `integer` or bare `datetime2` are not silently rewritten.
Character columns require an explicit collation; other columns have none.

SQL's `default` filegroup alias and `database_default` collation alias are rejected,
including case and surrounding-whitespace variants. No filegroup or collation
fallback is selected. Other identifier spelling and server-collation equivalence,
column-name uniqueness, row width, the admitted column limit and runtime type
qualification remain the admission consumer's responsibility.

## Scope and recovery

The renderer never synthesizes a SELECT, INSERT, key, filter or deduplication step.
It does not alter row multiplicity. Loading rows, observing the resulting layout,
renaming the candidate, cleaning up an owned helper and committing a publication
belong to the separately admitted model transaction described in
[ADR 0065](adr/0065-trusted-isolated-native-generation-execution.md).

A `PhysicalPlanError` identifies an invalid or unsupported rendering input. Return
to the canonical plan producer or selected policy and correct that input; do not
patch derived names/digests, switch to a fallback layout, or execute partial text.
Repeating a render of the same valid plan returns the same text, but does not
establish that repeating its SQL execution is safe. No execution retry, receipt,
connection, credentials or transaction settlement exists in this API.

This additive API leaves existing dbt materializations, macros and plan bytes
unchanged. It is a preview/rendering boundary; the complete managed writer and
its installed qualification remain separate work.

## Validation and SQL references

Run the focused representation and regression checks with:

```bash
uv run pytest tests/test_dbt_mssql_physical_rendering.py \
  tests/test_dbt_mssql_physical.py tests/test_dbt_mssql_type_contract.py -q
```

The tests cover all four layouts, quoted/Unicode identifiers, exact type dimensions,
ordered columns, derived identities, invalid input and the runnable example above.
These tests do not execute SQL Server. Live syntax, layout readback and transaction
qualification are **UNVERIFIED** by this renderer-only slice.

Microsoft references consulted on 2026-09-16 for the SQL Server disk-based-table
syntax:

- [COLLATE](https://learn.microsoft.com/en-us/sql/t-sql/statements/collations?view=sql-server-ver16): literal Windows or SQL collation names, separate from the `database_default` alias; catalog enumeration through `fn_helpcollations`. The bounded ASCII check above is renderer policy, not catalog certification.
- [CREATE TABLE](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-table-transact-sql?view=sql-server-ver16): explicit filegroup and table compression; the special `default` alias.
- [ALTER TABLE](https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-table-transact-sql?view=sql-server-ver16): table rebuild, compression, offline operation and MAXDOP.
- [CREATE COLUMNSTORE INDEX](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-columnstore-index-transact-sql?view=sql-server-ver16): ordinary clustered columnstore creation, compression, filegroup and build options.

Continue with the [physical plan contract](dbt-mssql-physical-plans.md) and
[physical catalog acquisition](dbt-mssql-physical-catalog-acquisition.md) for the
separate identity and observation boundaries.
