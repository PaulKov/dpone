"""Finite P-only discovery SQL expansion using existing authority validators."""

from dpone.adapters.dbt_mssql_physical_catalog_queries import _literal
from dpone.adapters.dbt_mssql_physical_catalog_v2_queries import _canonical, _links, _reject_difference, _shapes
from dpone.adapters.dbt_mssql_physical_source_queries import _caller, _pin, _projections, _quote
from dpone.adapters.native_generation_mssql_json import decode_bytes, scalar, utf8
from dpone.adapters.native_generation_mssql_owner import physical_owner
from dpone.contracts.dbt_mssql_physical_registration import database_pin_payload
from dpone.contracts.dbt_mssql_physical_validation import require_physical_identifier, require_sql_positive_integer
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityPin
from dpone.contracts.mssql_object_name import native_control_schema
from dpone.contracts.native_delivery_json import MAX_NATIVE_JSON_TOKENS

ENTRY = "physical_discover_absent_v1"
HELPER = "physical_control_require_owner_v1"


def _decode(source: str, target: str) -> str:
    return (
        decode_bytes(source, target)
        + "\nIF "
        + _reject_difference(utf8(target), source)
        + "\n THROW 51480, 'DPONE_DISCOVERY_UTF8_INVALID', 1;"
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


def _request() -> str:
    shapes = {
        "@json": dict(
            schema=1,
            subject=5,
            workspace_attempt=5,
            guard=5,
            generation_id=1,
            invocation_id=1,
            filegroup_name=1,
            objects=4,
        ),
        "JSON_QUERY(@json,'$.workspace_attempt')": dict(
            activation_id=1, attempt_id=1, workflow_id=1, write_subjects=4, request_sha256=1
        ),
        "JSON_QUERY(@json,'$.guard')": dict(guard_id=1, fencing_epoch=2),
    }
    guards = [_shape(doc, fields) + "\n" + _canonical_object(doc, fields) for doc, fields in shapes.items()]
    for name in ("generation_id", "invocation_id"):
        value = scalar("@json", "$." + name)
        guards.append(
            "IF "
            + _reject_difference(value, f"LOWER(CONVERT(nvarchar(36),TRY_CONVERT(uniqueidentifier,{value})))")
            + "\n THROW 51480, 'DPONE_DISCOVERY_CORRELATION_INVALID', 1;"
        )
    guards.append(
        "IF "
        + _reject_difference(
            utf8("JSON_QUERY(@json,'$.subject')"), utf8("JSON_QUERY(@registration,'$.platform_subject')")
        )
        + "\n THROW 51480, 'DPONE_DISCOVERY_SUBJECT_MISMATCH', 1;"
    )
    guards.append(
        "IF "
        + _reject_difference(scalar("@json", "$.schema"), "N'dpone.mssql-physical-discovery-request.v1'")
        + "\n THROW 51480, 'DPONE_DISCOVERY_SCHEMA_INVALID', 1;"
    )
    # Closed shapes count every punctuation/string/number as native preflight does:
    # root25 + scalars4 + PLATFORM53 + attempt(21+2*S) + guard9 + objects(1+14*N).
    # Nonempty string subjects and three-string objects are enforced separately.
    guards.append(f"""IF NOT EXISTS (SELECT 1 FROM OPENJSON(@json,'$.workspace_attempt.write_subjects'))
 OR EXISTS (SELECT 1 FROM OPENJSON(@json,'$.workspace_attempt.write_subjects') WHERE type<>1)
 OR CONVERT(bigint,113)+2*(SELECT COUNT_BIG(*) FROM OPENJSON(@json,'$.workspace_attempt.write_subjects'))
 +14*(SELECT COUNT_BIG(*) FROM OPENJSON(@json,'$.objects'))>{MAX_NATIVE_JSON_TOKENS}
 THROW 51480, 'DPONE_DISCOVERY_TOKEN_BOUND', 1;""")
    return "\n".join(guards)


def discovery_procedures(
    *,
    discovery_sql: bytes,
    model_database: MssqlDatabaseAuthorityPin,
    local_schema: str,
    control_database: str,
    control_schema: str,
    model_schema: str,
    model_schema_id: int,
    model_schema_owner_id: int,
    discovery_certificate_thumbprint: bytes,
) -> dict[str, str]:
    """Render authenticated installation coordinates; this function authenticates none."""
    if type(discovery_sql) is not bytes or not discovery_sql:
        raise ValueError("authenticated discovery package bytes are required")
    database_pin_payload(model_database)
    local, control = native_control_schema(local_schema), native_control_schema(control_schema)
    require_physical_identifier(model_schema, "model_schema")
    require_sql_positive_integer(model_schema_id, "model_schema_id")
    if type(model_schema_owner_id) is not int or model_schema_owner_id != 1:
        raise ValueError("discovery requires dbo schema owner baseline")
    if model_schema.casefold() in {"dbo", "sys", "information_schema", local.casefold(), control.casefold()}:
        raise ValueError("discovery requires a separate model schema")
    if type(discovery_certificate_thumbprint) is not bytes or len(discovery_certificate_thumbprint) != 20:
        raise ValueError("discovery requires observed 20-byte certificate thumbprint")
    replacements = {
        "LOCAL_SCHEMA": local,
        "CONTROL_SCHEMA": control,
        "CONTROL_DATABASE": _quote(control_database),
        "MODEL_SCHEMA": _literal(model_schema),
        "MODEL_SCHEMA_ID": str(model_schema_id),
        "MODEL_DATABASE_ID": str(model_database.database_id),
        "MODEL_DATABASE_GUID": str(model_database.database_guid),
        "MODEL_DATABASE_NAME": _literal(model_database.database_name),
        "MODEL_DATABASE_CREATE_TOKEN": model_database.create_token,
        "MODEL_CALLER": _caller(local),
        "CONTROL_CALLER": _caller(control),
        "MODEL_PIN": _pin("model", local),
        "CONTROL_PIN": _pin("control", control),
        "REGISTRATION_DECODE": _decode("@payload", "@registration"),
        "REQUEST_DECODE": _decode("@request", "@json"),
        "REQUEST_CHECKS": _request(),
        "PROJECTION_CHECK": _projections(),
        "PHYSICAL_OWNER": physical_owner(control),
        "BINDING_DECODE": _decode("@binding_payload", "@binding"),
        "BINDING_SHAPES": _shapes(),
        "BINDING_CANONICAL": _canonical(),
        "BINDING_LINKS": _links(model_database, model_schema, model_schema_id, catalog_module_schema=local),
        "OBJECT_SHAPE": _shape("@item", dict(model_unique_id=1, role=1, name=1)),
        "OBJECT_CANONICAL": _canonical_object("@item", dict(model_unique_id=1, role=1, name=1)),
        "AUTHORITY_LOCATOR": utf8(scalar("@registration", "$.control_authority.locator")),
        "AUTHORITY_DIGEST": utf8(scalar("@registration", "$.control_authority.sha256")),
    }
    for name, kind in [("MODEL", "SPVC"), ("CONTROL", "CPVC")]:
        replacements[
            name + "_SIGNATURE"
        ] = f"""IF (SELECT COUNT(*) FROM sys.crypt_properties WHERE class=1 AND major_id=@@PROCID)<>1
 OR NOT EXISTS (SELECT 1 FROM sys.crypt_properties WHERE class=1 AND major_id=@@PROCID
 AND crypt_type='{kind}' AND thumbprint=0x{discovery_certificate_thumbprint.hex()})
 THROW 51480, 'DPONE_DISCOVERY_SIGNATURE_INVALID', 1;"""
    result = discovery_sql.decode("utf-8")
    for name, value in replacements.items():
        marker = "{{" + name + "}}"
        if marker not in result:
            raise ValueError("discovery package omits required substitution: " + name)
        result = result.replace(marker, value)
    parts = result.split("\n-- DPONE MODULE BOUNDARY\n")
    if "{{" in result or len(parts) != 2:
        raise ValueError("discovery requires exact finite module inventory")
    return {ENTRY: parts[0], HELPER: parts[1]}
