"""Deterministic expansion of the SQL2022 catalog package, without deployment.

The installer authenticates the retained profile/schema binding, package bytes,
registration, certificate and finite permission inventory. Renderer arguments
only represent those claims. This producer does not grant model-plan admission.
"""

from dataclasses import fields

from dpone.adapters.dbt_mssql_physical_catalog_properties import forbidden_property_query
from dpone.contracts import dbt_mssql_physical_catalog_rows as rows
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_registration_codec import physical_runtime_registration_digest
from dpone.contracts.dbt_mssql_physical_registration_values import DedicatedObserver
from dpone.contracts.dbt_mssql_physical_validation import require_physical_identifier, require_sql_positive_integer
from dpone.contracts.mssql_object_name import native_control_schema

ENTRY = "physical_catalog_v1"


def _literal(value: str) -> str:
    return "N'" + value.replace("'", "''") + "'"


def _details(record: type[rows.CatalogRow]) -> tuple[str, ...]:
    return tuple(field.name for field in fields(record)[5:])


def _projection(record: type[rows.CatalogRow], alias: str, **expressions: str) -> str:
    return ",".join(f"{expressions.get(name, alias + '.[' + name + ']')} AS [{name}]" for name in _details(record))


def _dependency_query() -> str:
    # Never deduplicate: one self-reference legitimately occurs in both directions.
    projection = _projection(rows.DependencyRow, "d", direction="'OUTBOUND'")
    outgoing = f"SELECT {projection} FROM sys.sql_expression_dependencies d WHERE d.referencing_id=@object_id"
    projection = _projection(rows.DependencyRow, "d", direction="'INBOUND'")
    incoming = f"""SELECT {projection} FROM sys.sql_expression_dependencies d
WHERE d.referenced_id=@object_id OR (d.referenced_id IS NULL
 AND d.referenced_server_name IS NULL
 AND (d.referenced_database_name IS NULL OR d.referenced_database_name=DB_NAME())
 AND (d.referenced_schema_name IS NULL OR d.referenced_schema_name=@model_schema)
 AND d.referenced_entity_name=@object_name)"""
    return outgoing + "\nUNION ALL\n" + incoming


def _collections() -> tuple[tuple[str, type[rows.CatalogRow], str, str], ...]:
    return (
        (
            "COLUMN",
            rows.ColumnRow,
            "SELECT "
            + _projection(rows.ColumnRow, "c", type_schema="s.name", type_name="t.name")
            + " FROM sys.columns c JOIN sys.types t ON t.user_type_id=c.user_type_id"
            + " JOIN sys.schemas s ON s.schema_id=t.schema_id WHERE c.object_id=@object_id",
            "column_id",
        ),
        (
            "INDEX",
            rows.IndexRow,
            "SELECT "
            + _projection(rows.IndexRow, "i", data_space_name="s.name", data_space_type="s.type")
            + " FROM sys.indexes i LEFT JOIN sys.data_spaces s ON s.data_space_id=i.data_space_id"
            + " WHERE i.object_id=@object_id",
            "index_id",
        ),
        (
            "INDEX_COLUMN",
            rows.IndexColumnRow,
            "SELECT "
            + _projection(rows.IndexColumnRow, "c")
            + " FROM sys.index_columns c WHERE c.object_id=@object_id",
            "index_id,index_column_id",
        ),
        (
            "PARTITION",
            rows.PartitionRow,
            "SELECT "
            + _projection(
                rows.PartitionRow,
                "p",
                data_space_id="i.data_space_id",
                data_space_name="s.name",
                data_space_type="s.type",
            )
            + " FROM sys.partitions p JOIN sys.indexes i ON i.object_id=p.object_id AND i.index_id=p.index_id"
            + " LEFT JOIN sys.data_spaces s ON s.data_space_id=i.data_space_id WHERE p.object_id=@object_id",
            "index_id,partition_number,partition_id",
        ),
        (
            "DEPENDENCY",
            rows.DependencyRow,
            _dependency_query(),
            ",".join(
                "["
                + name
                + "]"
                + (" COLLATE Latin1_General_100_BIN2" if name == "direction" or name.endswith("_name") else "")
                for name in _details(rows.DependencyRow)
            ),
        ),
        (
            "FORBIDDEN_PROPERTY",
            rows.ForbiddenPropertyRow,
            forbidden_property_query(),
            "property_code COLLATE Latin1_General_100_BIN2,related_object_id,related_column_id",
        ),
    )


def _materialize(kind: str, query: str) -> str:
    limit = "@dependency_limit" if kind == "DEPENDENCY" else "@column_limit" if kind == "COLUMN" else "@row_limit"
    # TOP has no ORDER BY: the bounded overflow sample need not be deterministic.
    # Only a complete, within-budget collection is sorted and exposed afterwards.
    return f"""IF @kind IN ('HEADER','{kind}')
BEGIN
SELECT TOP (CONVERT(bigint,{limit})+1) * INTO #catalog_{kind} FROM ({query}) facts;
IF (SELECT COUNT_BIG(*) FROM #catalog_{kind})>{limit}
 THROW 51463, 'DPONE_CATALOG_ROW_BOUND_EXCEEDED', 1;
END"""


def _emit(kind: str, record: type[rows.CatalogRow], order: str) -> str:
    columns = ",".join("[" + name + "]" for name in _details(record))
    nulls = ",".join(f"CAST(NULL AS {field.metadata['sql']})" for field in fields(record)[5:])
    return f"""IF @kind='{kind}'
BEGIN
 IF EXISTS (SELECT 1 FROM #catalog_{kind})
  SELECT 1,'{kind}',@object_id,CONVERT(int,ROW_NUMBER() OVER (ORDER BY {order})),
   CONVERT(int,COUNT_BIG(*) OVER ()),{columns} FROM #catalog_{kind} ORDER BY {order};
 ELSE SELECT 1,'{kind}',@object_id,0,0,{nulls};
 RETURN;
END"""


def catalog_procedure(
    registration: MssqlPhysicalRuntimeRegistration,
    *,
    catalog_sql: bytes,
    model_schema: str,
    model_schema_id: int,
    model_schema_owner_id: int,
    catalog_certificate_thumbprint: bytes,
) -> str:
    """Render one registration-specific fixed signed module from package bytes.

    Registration limits are compiled only alongside exact source-authenticated
    registration digest equality. Runtime arguments cannot replace those limits
    or select an arbitrary namespace. No source module is changed/countersigned.
    Full dependency visibility and exclusion of privileged DDL are provisioning
    preconditions; local catalog queries cannot discover dynamic/cross-DB users.
    """
    if type(registration) is not MssqlPhysicalRuntimeRegistration:
        raise ValueError("catalog producer requires exact registration")
    registration.__post_init__()
    if type(registration.principals.observer) is not DedicatedObserver:
        raise ValueError("initial catalog cell requires a dedicated observer")
    if type(catalog_sql) is not bytes or not catalog_sql:
        raise ValueError("authenticated catalog package bytes are required")
    local = native_control_schema(registration.local_schema)
    require_physical_identifier(model_schema, "model_schema")
    require_sql_positive_integer(model_schema_id, "model_schema_id")
    if type(model_schema_owner_id) is not int or model_schema_owner_id != 1:
        raise ValueError("catalog schema requires the dbo owner baseline")
    if model_schema.casefold() in {
        "dbo",
        "sys",
        "information_schema",
        local.casefold(),
        registration.control_schema.casefold(),
    }:
        raise ValueError("catalog requires a separate model-data schema")
    if type(catalog_certificate_thumbprint) is not bytes or len(catalog_certificate_thumbprint) != 20:
        raise ValueError("catalog requires the observed 20-byte certificate thumbprint")
    collection_queries = _collections()
    limits = registration.limits
    timestamp = lambda field: f"CONVERT(char(27),CONVERT(datetime2(7),t.{field}),126)"  # noqa: E731
    table_projection = _projection(
        rows.TableRow,
        "t",
        schema_name="@model_schema",
        object_name="t.name",
        object_type="t.type",
        object_create_time=timestamp("create_date"),
        object_modify_time=timestamp("modify_date"),
    )
    substitutions = {
        "LOCAL_SCHEMA": local,
        "ENTRY": ENTRY,
        "REGISTRATION_ID": registration.registration_id,
        "REGISTRATION_DIGEST": physical_runtime_registration_digest(registration),
        "MODEL_SCHEMA": _literal(model_schema),
        "MODEL_SCHEMA_ID": str(model_schema_id),
        "CERTIFICATE_THUMBPRINT": catalog_certificate_thumbprint.hex(),
        "ROW_LIMIT": str(limits.max_catalog_rows),
        "DEPENDENCY_LIMIT": str(limits.max_dependency_rows),
        "COLUMN_LIMIT": str(min(limits.max_columns, limits.max_catalog_rows)),
        "DEFINITION_LIMIT": str(limits.max_definition_utf16_bytes),
        "MATERIALIZE": "\n".join(_materialize(kind, query) for kind, _, query, _ in collection_queries),
        "COLLECTION_OUTPUTS": "\n".join(_emit(kind, record, order) for kind, record, _, order in collection_queries),
        "TABLE_PROJECTION": table_projection,
    }
    template = catalog_sql.decode("utf-8")
    for key, value in substitutions.items():
        marker = "{{" + key + "}}"
        if marker not in template:
            raise ValueError("catalog package omits a required substitution")
        template = template.replace(marker, value)
    if "{{" in template:
        raise ValueError("catalog package contains unknown substitutions")
    return template
