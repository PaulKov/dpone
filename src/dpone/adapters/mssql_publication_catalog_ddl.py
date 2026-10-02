"""SQL Server dialect adapter for a reviewed catalog plan; never execute DDL."""

from dpone.contracts.mssql_object_name import safe_mssql_identifier

SLOT_TABLE = "dpone_cluster_publication_authority"
EVENT_TABLE = "dpone_cluster_publication_events"
COLLATION = "Latin1_General_100_BIN2"
# name, SQL type, catalog max_length, nullable, collation, scale
SLOT_COLUMNS = (
    ("slot_key", "char", 64, 0, COLLATION, 0),
    ("binding_digest", "binary", 32, 0, None, 0),
    ("revision", "bigint", 8, 0, None, 0),
    ("operation_id", "nvarchar", 256, 0, COLLATION, 0),
    ("phase", "varchar", 32, 0, COLLATION, 0),
    ("payload", "varbinary", -1, 0, None, 0),
    ("payload_sha256", "binary", 32, 0, None, 0),
    ("write_id", "uniqueidentifier", 16, 0, None, 0),
)
EVENT_COLUMNS = SLOT_COLUMNS + (
    ("previous_sha256", "binary", 32, 1, None, 0),
    ("origin", "varchar", 32, 0, COLLATION, 0),
    ("provenance", "varbinary", -1, 0, None, 0),
    ("provenance_sha256", "binary", 32, 0, None, 0),
    ("created_utc", "datetime2", 8, 0, None, 7),
)


def quote_identifier(value: str) -> str:
    """Accept one validated SQL identifier, never an expression or path."""
    if not isinstance(value, str) or not safe_mssql_identifier(value) or len(value) > 128:
        raise ValueError("invalid publication catalog identifier")
    return f"[{value}]"


def immutable_event_trigger(*, schema: str) -> str:
    """Reject event rewrites, including UPDATE/DELETE issued outside dpone."""
    prefix = quote_identifier(schema)
    return (
        f"CREATE TRIGGER {prefix}.[dpone_publication_events_immutable]\n"
        f"ON {prefix}.[{EVENT_TABLE}] AFTER UPDATE, DELETE AS\n"
        "BEGIN\n SET NOCOUNT ON;\n"
        " THROW 51071, 'publication events are append-only', 1;\nEND;"
    )


def render_publication_catalog_ddl(*, database: str, schema: str) -> str:
    """Produce reviewable SQL batches; existing objects deliberately fail apply.

    GO is an operator/client batch separator, not a DBAPI SQL statement. A
    schema-apply service must execute these reviewed batches transactionally and
    admit the resulting exact catalog before any publication writer starts.
    """
    db, namespace = quote_identifier(database), quote_identifier(schema)
    tables = []
    for name, columns, constraint in (
        (SLOT_TABLE, SLOT_COLUMNS, "CONSTRAINT [pk_publication_authority] PRIMARY KEY CLUSTERED ([slot_key])"),
        (
            EVENT_TABLE,
            EVENT_COLUMNS,
            "CONSTRAINT [pk_publication_events] PRIMARY KEY CLUSTERED ([slot_key], [revision]),\n"
            " CONSTRAINT [uq_publication_write] UNIQUE ([write_id])",
        ),
    ):
        rendered = ",\n ".join(_column(*column) for column in columns)
        tables.append(f"CREATE TABLE {namespace}.[{name}] (\n {rendered},\n {constraint}\n);")
    return f"USE {db};\nGO\n" + "\nGO\n".join([*tables, immutable_event_trigger(schema=schema)]) + "\nGO\n"


def _column(name: str, kind: str, length: int, nullable: int, collation: str | None, scale: int) -> str:
    if kind in {"char", "varchar", "nvarchar", "binary", "varbinary"}:
        size = "max" if length == -1 else str(length // 2 if kind == "nvarchar" else length)
        kind += f"({size})"
    elif kind == "datetime2":
        kind += f"({scale})"
    suffix = f" COLLATE {collation}" if collation else ""
    return f"[{name}] {kind}{suffix} {'NULL' if nullable else 'NOT NULL'}"
