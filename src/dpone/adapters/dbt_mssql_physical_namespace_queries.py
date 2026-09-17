"""Finite namespace SQL from retained discovery bytes, without caller authority.

The same assertions serve METADATA discovery and authenticated BUILD attach.
Callers supply their own current authority predicates before observing metadata;
these pure fragments never authenticate a plan, caller, or retained source.
"""

from dpone.adapters.dbt_mssql_physical_catalog_queries import _literal
from dpone.adapters.dbt_mssql_physical_catalog_v2_queries import _reject_difference
from dpone.adapters.native_generation_mssql_json import scalar
from dpone.contracts.dbt_mssql_physical_validation import require_physical_identifier, require_sql_positive_integer
from dpone.contracts.mssql_object_name import native_control_schema

_ANCHORS = (
    "DECLARE @objects TABLE(",
    "DECLARE @guard_epoch bigint,@control_id int,@control_sid varbinary(85);",
    "IF ISNULL(HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','VIEW DEFINITION'),0)<>1",
    "SELECT CONVERT(smallint,1),@registration_id,@registration_digest,",
)


def _shape(document: str, fields: dict[str, int]) -> str:
    values = ",".join(f"(N'{key}',{kind})" for key, kind in fields.items())
    return f"""IF {document} IS NULL OR ISNULL(ISJSON({document},OBJECT),0)<>1
 OR (SELECT COUNT(*) FROM OPENJSON({document}))<>{len(fields)}
 OR EXISTS (SELECT 1 FROM (VALUES {values}) e(name,kind) WHERE NOT EXISTS
 (SELECT 1 FROM OPENJSON({document}) j WHERE CONVERT(varbinary(max),j.[key])=CONVERT(varbinary(max),e.name)
 AND DATALENGTH(j.[key])=DATALENGTH(e.name) AND j.type=e.kind))
 THROW 51480, 'DPONE_DISCOVERY_SHAPE_INVALID', 1;"""


def _canonical_object(document: str, fields: dict[str, int]) -> str:
    parts = ["CONVERT(nvarchar(max),N'{')"]
    for index, (name, kind) in enumerate(sorted(fields.items())):
        value = scalar(document, "$." + name)
        parts.append(f"N'{',' if index else ''}\"{name}\":'")
        parts.append(
            f"JSON_QUERY({document},'$.{name}')"
            if kind in (4, 5)
            else f"CONVERT(nvarchar(max),TRY_CONVERT(bigint,{value}))"
            if kind == 2
            else f"(N'\"'+REPLACE(STRING_ESCAPE({value},'json'),NCHAR(92)+N'/',N'/')+N'\"')"
        )
    parts.append("N'}'")
    return (
        "IF "
        + _reject_difference(document, "(" + "+".join(parts) + ")")
        + "\n THROW 51480, 'DPONE_DISCOVERY_NONCANONICAL', 1;"
    )


def _render_namespace(
    *,
    discovery_sql: bytes,
    model_schema: str,
    model_schema_id: int,
    control_schema: str,
) -> tuple[str, tuple[str, str], tuple[str, str]]:
    if type(discovery_sql) is not bytes or not discovery_sql:
        raise ValueError("authenticated discovery package bytes are required")
    require_physical_identifier(model_schema, "model_schema")
    require_sql_positive_integer(model_schema_id, "model_schema_id")
    control = native_control_schema(control_schema)
    if model_schema.casefold() in {"dbo", "sys", "information_schema", control.casefold()}:
        raise ValueError("namespace requires a separate model schema")
    source = discovery_sql.decode("utf-8")
    if any(source.count(anchor) != 1 for anchor in _ANCHORS):
        raise ValueError("discovery namespace requires exact finite span boundaries")
    offsets = tuple(source.index(anchor) for anchor in _ANCHORS)
    if offsets != tuple(sorted(offsets)):
        raise ValueError("discovery namespace span boundaries are reversed")
    raw = (source[offsets[0] : offsets[1]], source[offsets[2] : offsets[3]])
    maps = (
        {
            "OBJECT_SHAPE": _shape("@item", dict(model_unique_id=1, role=1, name=1)),
            "OBJECT_CANONICAL": _canonical_object("@item", dict(model_unique_id=1, role=1, name=1)),
        },
        {"MODEL_SCHEMA": _literal(model_schema), "MODEL_SCHEMA_ID": str(model_schema_id), "CONTROL_SCHEMA": control},
    )
    marker_counts = {
        "OBJECT_SHAPE": 1,
        "OBJECT_CANONICAL": 1,
        "MODEL_SCHEMA": 2,
        "MODEL_SCHEMA_ID": 3,
        "CONTROL_SCHEMA": 1,
    }
    rendered = []
    for span, replacements in zip(raw, maps, strict=True):
        for name, value in replacements.items():
            marker = "{{" + name + "}}"
            if span.count(marker) != marker_counts[name]:
                raise ValueError("discovery namespace requires exact substitution count: " + name)
            span = span.replace(marker, value)
        if "{{" in span or "}}" in span:
            raise ValueError("discovery namespace contains unresolved substitutions")
        rendered.append(span)
    return source, raw, (rendered[0], rendered[1])


def physical_namespace_sql(
    *,
    discovery_sql: bytes,
    model_schema: str,
    model_schema_id: int,
    control_schema: str,
) -> tuple[str, str]:
    """Return input and observation fragments; no authentication, SQL execution or I/O.

    Inputs are retained discovery source bytes and already admitted coordinates.
    The caller defines @json.objects, @json.filegroup_name and @row_limit from
    authenticated plans/registration. Observation requires the caller's owned
    transaction and current P/G/caller predicates. Attach supplies only its
    current model's three names and compares the observed filegroup ID to its
    enrolled identity. Full-set namespace checks belong to enrollment.
    """
    return _render_namespace(
        discovery_sql=discovery_sql,
        model_schema=model_schema,
        model_schema_id=model_schema_id,
        control_schema=control_schema,
    )[2]


def _replace_namespace_sql(
    *,
    discovery_sql: bytes,
    model_schema: str,
    model_schema_id: int,
    control_schema: str,
) -> str:
    source, raw, rendered = _render_namespace(
        discovery_sql=discovery_sql,
        model_schema=model_schema,
        model_schema_id=model_schema_id,
        control_schema=control_schema,
    )
    for before, after in zip(raw, rendered, strict=True):
        source = source.replace(before, after, 1)
    return source
