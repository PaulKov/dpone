"""Stable v2 catalog programme backed by protected per-registration bindings.

The module contains only cohort/database/schema/certificate coordinates. Source
admission authenticates existing registration projections; immutable binding
provisioning authenticates selected policy/member provenance. Runtime compares
these facts in one transaction and never claims to re-read an original archive.
"""

from dpone.adapters.dbt_mssql_physical_catalog_queries import (
    _collections,
    _emit,
    _literal,
    _materialize,
    _projection,
)
from dpone.adapters.native_generation_mssql_json import decode_bytes, scalar, utf8
from dpone.contracts import dbt_mssql_physical_catalog_rows as rows
from dpone.contracts.dbt_mssql_physical_registration import database_pin_payload
from dpone.contracts.dbt_mssql_physical_validation import require_physical_identifier, require_sql_positive_integer
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityPin
from dpone.contracts.mssql_object_name import native_control_schema
from dpone.contracts.native_project_documents import NATIVE_POLICY_MEMBER

ENTRY = "physical_catalog_v2"
_REFERENCE = dict(locator=1, sha256=1)
_SUBJECT = dict(schema=1, scope=1, authority=5, platform_policy_sha256=1)
_AUTHORITY = dict(
    environment=1,
    release_id=1,
    deployment_id=1,
    release_sha256=1,
    deployment_sha256=1,
    binding_set_sha256=1,
    connection_registry_sha256=1,
    credential_runtime_sha256=1,
    authority_subject_sha256=1,
)
_SHAPES = {
    "$": dict(
        schema=1,
        registration_id=1,
        registration_sha256=1,
        platform_subject=5,
        trusted_profile=5,
        profile_name=1,
        workflow_id=1,
        policy_member=5,
        project_archive_sha256=1,
        model_database_name=1,
        model_schema=1,
        resource_bounds=5,
        model_schema_id=2,
        model_schema_owner_id=2,
        catalog_module_sha256=1,
    ),
    "$.platform_subject": _SUBJECT,
    "$.platform_subject.authority": _AUTHORITY,
    "$.trusted_profile": dict(reference=5, subject=5),
    "$.trusted_profile.reference": _REFERENCE,
    "$.trusted_profile.subject": _SUBJECT,
    "$.trusted_profile.subject.authority": _AUTHORITY,
    "$.policy_member": _REFERENCE,
    "$.resource_bounds": _REFERENCE,
}


def _object(path: str) -> str:
    return "@binding" if path == "$" else f"JSON_QUERY(@binding,'{path}')"


def _reject_difference(left: str, right: str) -> str:
    """SQL NULL is never evidence of equality; compare exact bytes and length."""
    return (
        f"({left} IS NULL OR {right} IS NULL OR CONVERT(varbinary(max),{left})<>CONVERT(varbinary(max),{right}) "
        f"OR DATALENGTH({left})<>DATALENGTH({right}))"
    )


def _shapes() -> str:
    guards = []
    for path, fields in _SHAPES.items():
        document = _object(path)
        expected = ",".join(f"(N'{name}',{kind})" for name, kind in fields.items())
        guards.append(f"""IF {document} IS NULL OR ISNULL(ISJSON({document},OBJECT),0)<>1
 OR (SELECT COUNT(*) FROM OPENJSON({document}))<>{len(fields)}
 OR EXISTS (SELECT 1 FROM (VALUES {expected}) e(name,kind) WHERE NOT EXISTS
 (SELECT 1 FROM OPENJSON({document}) j WHERE CONVERT(varbinary(max),j.[key])=CONVERT(varbinary(max),e.name)
 AND DATALENGTH(j.[key])=DATALENGTH(e.name) AND j.type=e.kind))
 THROW 51471, 'DPONE_CATALOG_BINDING_SHAPE_INVALID', 1;""")
    return "\n".join(guards)


def _canonical() -> str:
    guards = []
    for path, fields in _SHAPES.items():
        parts = ["CONVERT(nvarchar(max),N'{')"]
        for index, (name, kind) in enumerate(sorted(fields.items())):
            field_path = path + "." + name
            value = scalar("@binding", field_path)
            parts.append(f"N'{',' if index else ''}\"{name}\":'")
            if kind == 5:
                parts.append(_object(field_path))
            elif kind == 2:
                parts.append(f"CONVERT(nvarchar(max),TRY_CONVERT(int,{value}))")
            else:
                parts.append(f"(N'\"'+REPLACE(STRING_ESCAPE({value},'json'),NCHAR(92)+N'/',N'/')+N'\"')")
                guards.append(f"""IF {value} IS NULL OR DATALENGTH({utf8(value)}) NOT BETWEEN 1 AND 4096
 THROW 51471, 'DPONE_CATALOG_BINDING_STRING_INVALID', 1;""")
        parts.append("N'}'")
        guards.append(
            "IF "
            + _reject_difference(_object(path), "(" + "+".join(parts) + ")")
            + "\n THROW 51471, 'DPONE_CATALOG_BINDING_NONCANONICAL', 1;"
        )
    return "\n".join(guards)


def _links(model_database: MssqlDatabaseAuthorityPin, model_schema: str, schema_id: int) -> str:
    value = lambda path: scalar("@binding", "$." + path)  # noqa: E731
    checks = [
        _reject_difference(value("schema"), "N'dpone.mssql-physical-catalog-binding.v1'"),
        _reject_difference(value("registration_id"), "LOWER(CONVERT(nvarchar(36),@registration_id))"),
        _reject_difference(utf8(value("registration_sha256")), "@registration_digest"),
        _reject_difference(utf8(_object("$.platform_subject")), "@platform_subject"),
        _reject_difference(utf8(_object("$.trusted_profile.subject")), "@profile_subject"),
        _reject_difference(utf8(value("trusted_profile.reference.locator")), "@profile_locator"),
        _reject_difference(utf8(value("trusted_profile.reference.sha256")), "@profile_digest"),
        _reject_difference(_object("$.resource_bounds"), _object("$.trusted_profile.reference")),
        _reject_difference(value("policy_member.sha256"), value("platform_subject.platform_policy_sha256")),
        _reject_difference(value("policy_member.locator"), _literal(NATIVE_POLICY_MEMBER)),
        _reject_difference(value("model_database_name"), _literal(model_database.database_name)),
        _reject_difference(value("model_schema"), _literal(model_schema)),
        _reject_difference(value("model_schema_id"), f"CONVERT(nvarchar(max),{schema_id})"),
        _reject_difference(value("model_schema_owner_id"), "N'1'"),
        _reject_difference(
            value("catalog_module_sha256"),
            "CONVERT(nvarchar(max),'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',CONVERT(varbinary(max),OBJECT_DEFINITION(@@PROCID))),2)))",
        ),
    ]
    for path in ("platform_subject", "trusted_profile.subject"):
        checks.extend(
            (
                _reject_difference(value(path + ".schema"), "N'dpone.native-original-subject.v1'"),
                _reject_difference(value(path + ".scope"), "N'PLATFORM'"),
            )
        )
    for path in ("project_archive_sha256", "policy_member.sha256", "registration_sha256", "catalog_module_sha256"):
        digest = value(path)
        checks.append(
            f"({digest} IS NULL OR DATALENGTH({digest})<>142 OR LEFT({digest},7) COLLATE Latin1_General_100_BIN2<>N'sha256:' "
            f"OR SUBSTRING({digest},8,64) COLLATE Latin1_General_100_BIN2 LIKE N'%[^0-9a-f]%')"
        )
    for path in ("profile_name", "workflow_id", "policy_member.locator"):
        checks.append(f"({value(path)} IS NULL OR LEN(LTRIM(RTRIM({value(path)})))=0)")
    return "IF " + "\n OR ".join(checks) + "\n THROW 51472, 'DPONE_CATALOG_BINDING_LINK_MISMATCH', 1;"


def catalog_procedure_v2(
    *,
    catalog_sql: bytes,
    local_schema: str,
    model_database: MssqlDatabaseAuthorityPin,
    model_schema: str,
    model_schema_id: int,
    model_schema_owner_id: int,
    catalog_certificate_thumbprint: bytes,
) -> str:
    """Expand stable authenticated deployment inputs, never registration values.

    Deliberately reuse v1's finite query/wire fragments without altering its
    deployment or trust chain. V2 has a new entry and separately provisioned
    certificate. This renderer does not authenticate input provenance.
    """
    if type(catalog_sql) is not bytes or not catalog_sql:
        raise ValueError("authenticated catalog package bytes are required")
    local = native_control_schema(local_schema)
    database_pin_payload(model_database)
    require_physical_identifier(model_schema, "model_schema")
    require_sql_positive_integer(model_schema_id, "model_schema_id")
    if type(model_schema_owner_id) is not int or model_schema_owner_id != 1:
        raise ValueError("catalog schema requires the dbo owner baseline")
    if model_schema.casefold() in {"dbo", "sys", "information_schema", local.casefold()}:
        raise ValueError("catalog requires a separate model-data schema")
    if type(catalog_certificate_thumbprint) is not bytes or len(catalog_certificate_thumbprint) != 20:
        raise ValueError("catalog requires the observed 20-byte certificate thumbprint")
    queries = _collections()
    timestamp = lambda field: f"CONVERT(char(27),CONVERT(datetime2(7),t.{field}),126)"  # noqa: E731
    substitutions = {
        "LOCAL_SCHEMA": local,
        "ENTRY": ENTRY,
        "MODEL_SCHEMA": _literal(model_schema),
        "MODEL_SCHEMA_ID": str(model_schema_id),
        "MODEL_DATABASE_ID": str(model_database.database_id),
        "MODEL_DATABASE_GUID": str(model_database.database_guid),
        "MODEL_DATABASE_CREATE_TOKEN": model_database.create_token,
        "MODEL_DATABASE_NAME": _literal(model_database.database_name),
        "CERTIFICATE_THUMBPRINT": catalog_certificate_thumbprint.hex(),
        "BINDING_DECODE": decode_bytes("@binding_payload", "@binding")
        + "\nIF "
        + _reject_difference(utf8("@binding"), "@binding_payload")
        + "\n THROW 51471, 'DPONE_CATALOG_BINDING_UTF8_INVALID', 1;",
        "BINDING_SHAPES": _shapes(),
        "BINDING_CANONICAL": _canonical(),
        "BINDING_LINKS": _links(model_database, model_schema, model_schema_id),
        "MATERIALIZE": "\n".join(_materialize(kind, query) for kind, _, query, _ in queries),
        "COLLECTION_OUTPUTS": "\n".join(_emit(kind, record, order) for kind, record, _, order in queries),
        "TABLE_PROJECTION": _projection(
            rows.TableRow,
            "t",
            schema_name="@model_schema",
            object_name="t.name",
            object_type="t.type",
            object_create_time=timestamp("create_date"),
            object_modify_time=timestamp("modify_date"),
        ),
    }
    template = catalog_sql.decode("utf-8")
    for key, replacement in substitutions.items():
        marker = "{{" + key + "}}"
        if marker not in template:
            raise ValueError("catalog package omits a required substitution")
        template = template.replace(marker, replacement)
    if "{{" in template:
        raise ValueError("catalog package contains unknown substitutions")
    return template
