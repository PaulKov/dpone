"""Deterministic expansion of the finite source-only bridge SQL package.

Coordinates are privileged deployment inputs. Public execution has exactly
three UUID inputs; SQL callers cannot select an endpoint, role or payload.
"""

from dpone.adapters.dbt_mssql_physical_registration_schema import COLUMNS
from dpone.adapters.native_generation_mssql_json import decode_bytes, scalar, shape, utf8
from dpone.adapters.native_generation_mssql_owner import physical_owner
from dpone.adapters.native_generation_mssql_queries import _canonical_executor
from dpone.contracts.dbt_mssql_physical_validation import require_physical_identifier
from dpone.contracts.mssql_object_name import native_control_schema

ENTRY = "physical_require_source_v1"
HELPER = "physical_control_require_source_v1"


def _quote(value: str) -> str:
    return "[" + require_physical_identifier(value, "database_name").replace("]", "]]") + "]"


def _projection(name: str, kind: str) -> str:
    """Map every frozen storage projection to its canonical registration path."""
    path = "$." + name
    if name == "payload":
        return "@payload"
    if name == "registration_digest":
        return "CONVERT(varbinary(71),'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',@payload),2)))"
    if name in {"platform_subject", "trusted_profile_subject", "trusted_toolchain_subject"}:
        path = "$." + name.replace("_subject", ".subject") if name != "platform_subject" else path
        return utf8(f"JSON_QUERY(@registration,'{path}')")
    for prefix in ("control_authority", "capacity_authority", "trusted_profile", "trusted_toolchain"):
        if name in {prefix + "_locator", prefix + "_digest"}:
            middle = ".reference" if prefix.startswith("trusted") else ""
            path = "$." + prefix + middle + (".locator" if name.endswith("locator") else ".sha256")
    for namespace in ("control", "model"):
        if name.startswith(namespace + "_database_"):
            suffix = name[len(namespace + "_database_") :]
            path = f"$.{namespace}_database." + (suffix if suffix == "create_token" else "database_" + suffix)
    if name in {
        "control_program_id",
        "control_program_sha256",
        "package_bundle_sha256",
        "macro_authority_sha256",
        "physical_policy",
    }:
        path = "$.program." + name
    if name.startswith("max_"):
        path = "$.limits." + name
    if name == "observer_mode":
        path = "$.principals.observer.mode"
    if name == "observer_permission_contract_sha256":
        return "COALESCE(" + utf8(scalar("@registration", "$.principals.observer.permission_contract_sha256")) + ",0x)"
    for role in ("metadata", "build", "observer"):
        for namespace in ("control", "model"):
            for suffix, field in (("principal_id", "principal_id"), ("sid", "sid_hex")):
                if name == f"{role}_{namespace}_{suffix}":
                    base = f"$.principals.{role}"
                    expression = scalar("@registration", f"{base}.{namespace}.{field}")
                    if role == "observer":
                        expression = (
                            "CASE JSON_VALUE(@registration,'$.principals.observer.mode') "
                            "WHEN 'DEDICATED' THEN "
                            + scalar("@registration", f"{base}.mapping.{namespace}.{field}")
                            + " WHEN 'SHARE_METADATA' THEN "
                            + scalar("@registration", f"$.principals.metadata.{namespace}.{field}")
                            + " WHEN 'SHARE_BUILD' THEN "
                            + scalar("@registration", f"$.principals.build.{namespace}.{field}")
                            + " END"
                        )
                    return f"TRY_CONVERT({kind},{expression}" + (",2)" if suffix == "sid" else ")")
    value = scalar("@registration", path)
    return utf8(value) if kind.startswith("varbinary") else f"TRY_CONVERT({kind},{value})"


def _projections() -> str:
    clauses = []
    for name, kind, _ in COLUMNS:
        expression = _projection(name, kind)
        clauses.append(
            f"({expression} IS NOT NULL AND CONVERT(varbinary(max),r.{name})=CONVERT(varbinary(max),{expression}) "
            f"AND DATALENGTH(r.{name})=DATALENGTH({expression}))"
        )
    return " AND\n".join(clauses)


def _caller(schema: str) -> str:
    return f"""DECLARE @caller_id int=USER_ID(), @caller_sid varbinary(85);
SELECT @caller_sid=sid FROM sys.database_principals
 WHERE principal_id=@caller_id AND principal_id>4 AND type='S' AND authentication_type=1;
IF @caller_sid IS NULL OR SUSER_SID() IS NULL OR SUSER_SID(ORIGINAL_LOGIN()) IS NULL
 OR @caller_sid<>SUSER_SID() OR DATALENGTH(@caller_sid)<>DATALENGTH(SUSER_SID())
 OR SUSER_SID()<>SUSER_SID(ORIGINAL_LOGIN()) OR DATALENGTH(SUSER_SID())<>DATALENGTH(SUSER_SID(ORIGINAL_LOGIN()))
 OR IS_SRVROLEMEMBER('sysadmin')<>0 OR IS_MEMBER('db_owner')<>0
 OR HAS_PERMS_BY_NAME(N'{schema}',N'SCHEMA',N'ALTER')<>0
 THROW 51420, 'DPONE_PHYSICAL_SOURCE_CALLER_INVALID', 1;"""


def _pin(namespace: str, schema: str) -> str:
    return f"""IF NOT EXISTS (SELECT 1 FROM sys.databases d JOIN sys.database_recovery_status r
 ON r.database_id=d.database_id WHERE d.database_id=DB_ID()
 AND d.database_id=TRY_CONVERT(int,JSON_VALUE(@registration,'$.{namespace}_database.database_id'))
 AND CONVERT(varbinary(max),d.name)=CONVERT(varbinary(max),JSON_VALUE(@registration,'$.{namespace}_database.database_name'))
 AND DATALENGTH(d.name)=DATALENGTH(JSON_VALUE(@registration,'$.{namespace}_database.database_name'))
 AND CONVERT(datetime2(7),d.create_date)=TRY_CONVERT(datetime2(7),JSON_VALUE(@registration,'$.{namespace}_database.create_token'))
 AND r.database_guid=TRY_CONVERT(uniqueidentifier,JSON_VALUE(@registration,'$.{namespace}_database.database_guid')))
 OR CONVERT(varbinary(max),JSON_VALUE(@registration,'$.{"local" if namespace == "model" else "control"}_schema'))<>CONVERT(varbinary(max),N'{schema}')
 THROW 51421, 'DPONE_PHYSICAL_SOURCE_DATABASE_INVALID', 1;"""


def source_procedures(
    *, admission_sql: bytes, model_database: str, local_schema: str, control_database: str, control_schema: str
) -> dict[str, str]:
    """Expand retained package bytes into the exact two deployed module bodies.

    The provisioner authenticates these package bytes before calling this pure
    function and records both the producer digest and separate expansion hashes.
    """
    local, control = native_control_schema(local_schema), native_control_schema(control_schema)
    template = admission_sql.decode("utf-8")
    replacements = {
        "MODEL_DATABASE": _quote(model_database),
        "CONTROL_DATABASE": _quote(control_database),
        "LOCAL_SCHEMA": local,
        "CONTROL_SCHEMA": control,
        "MODEL_CALLER": _caller(local),
        "CONTROL_CALLER": _caller(control),
        "MODEL_PIN": _pin("model", local),
        "CONTROL_PIN": _pin("control", control),
        "REGISTRATION_DECODE": decode_bytes("@payload", "@registration"),
        "REQUEST_DECODE": decode_bytes("@request", "@json"),
        "EXECUTOR_DECODE": decode_bytes("@executor", "@binding"),
        "PHYSICAL_OWNER": physical_owner(control),
        "PROJECTION_CHECK": _projections(),
        "REQUEST_SHAPE": shape(
            "@json", dict(schema=1, subject=5, workspace_attempt=5, guard=5, profile=5, command=5, requested_bytes=2)
        ),
        "EXECUTOR_SHAPE": shape(
            "@binding",
            dict(schema=1, generation_id=1, guard_epoch=2, invocation_id=1, reservation=5, profile=5, command=5),
        ),
        "CANONICAL_EXECUTOR": _canonical_executor(),
        "REQUEST_SUBJECT": utf8("JSON_QUERY(@json,'$.subject')"),
    }
    for name, document, path in (
        ("AUTHORITY_LOCATOR", "@registration", "$.control_authority.locator"),
        ("AUTHORITY_DIGEST", "@registration", "$.control_authority.sha256"),
        ("PROFILE_LOCATOR", "@registration", "$.trusted_profile.reference.locator"),
        ("PROFILE_DIGEST", "@registration", "$.trusted_profile.reference.sha256"),
        ("CAPACITY_LOCATOR", "@registration", "$.capacity_authority.locator"),
        ("CAPACITY_DIGEST", "@registration", "$.capacity_authority.sha256"),
        ("REQUEST_PROFILE_LOCATOR", "@json", "$.profile.locator"),
        ("REQUEST_PROFILE_DIGEST", "@json", "$.profile.sha256"),
        ("BINDING_RESERVATION_LOCATOR", "@binding", "$.reservation.locator"),
        ("BINDING_RESERVATION_DIGEST", "@binding", "$.reservation.sha256"),
        ("BINDING_PROFILE_LOCATOR", "@binding", "$.profile.locator"),
        ("BINDING_PROFILE_DIGEST", "@binding", "$.profile.sha256"),
    ):
        replacements[name] = utf8(scalar(document, path))
    replacements["REQUEST_SHAPE"] += shape(
        "JSON_QUERY(@json,'$.subject')", dict(schema=1, scope=1, authority=5, generation_id=1)
    )
    for document, fields in (("@json", ("profile", "command")), ("@binding", ("profile", "command", "reservation"))):
        key = "REQUEST_SHAPE" if document == "@json" else "EXECUTOR_SHAPE"
        replacements[key] += "\n" + "\n".join(
            shape(f"JSON_QUERY({document},'$.{field}')", dict(locator=1, sha256=1)) for field in fields
        )
    for name, value in replacements.items():
        template = template.replace("{{" + name + "}}", value)
    if "{{" in template:
        raise ValueError("unknown source bridge package substitution")
    parts = template.split("\n-- DPONE MODULE BOUNDARY\n")
    if len(parts) != 2:
        raise ValueError("source bridge requires the exact finite module inventory")
    return {ENTRY: parts[0], HELPER: parts[1]}
