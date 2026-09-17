"""Finite managed enrollment SQL templates, without execution or authority claims."""

from dpone.adapters.dbt_mssql_physical_enrollment_tables import enrollment_tables as enrollment_tables
from dpone.adapters.dbt_mssql_physical_enrollment_validation_queries import (
    _expand as _expand,
)
from dpone.adapters.dbt_mssql_physical_enrollment_validation_queries import (
    enrollment_document_checks,
)
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityPin

ENROLL = "physical_enroll_plan_set_v1"
READ = "physical_read_plan_enrollment_v1"
CONTROL = "physical_control_require_enrollment_v1"


def enrollment_document_sql(*, enrollment_sql: bytes) -> str:
    """Produce bounded native/closed envelope checks from finite package sections."""
    sections = _sections(enrollment_sql)
    return enrollment_document_checks(native_sql=sections[6], component_sql=sections[7])


def _differences(pairs: list[tuple[str, str]]) -> str:
    from dpone.adapters.dbt_mssql_physical_catalog_v2_queries import _reject_difference

    return (
        "IF "
        + "\n OR ".join(_reject_difference(left, right) for left, right in pairs)
        + "\n THROW 51601, 'DPONE_ENROLLMENT_BYTES_MISMATCH', 1;"
    )


def _enrollment_links(*, control: bool = False) -> str:
    from dpone.adapters.native_generation_mssql_json import scalar, utf8

    def value(path: str) -> str:
        return scalar("@enrollment", "$." + path)

    def obj(path: str) -> str:
        return f"JSON_QUERY(@enrollment,'$.{path}')"

    pairs = [
        (value("schema"), "N'dpone.mssql-physical-enrollment.v1'"),
        (value("subject.schema"), "N'dpone.native-original-subject.v1'"),
        (value("subject.scope"), "N'GENERATION'"),
        (obj("subject.authority"), "JSON_QUERY(@registration,'$.platform_subject.authority')"),
        (obj("executor.reservation"), obj("reservation")),
        (obj("executor.command"), obj("command.reference")),
        (obj("executor.profile"), obj("plan_set.payload.profile")),
        (
            utf8(value("plan_set.reference.sha256")),
            "CONVERT(varbinary(71),'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',"
            + utf8(obj("plan_set.payload"))
            + "),2)))",
        ),
        (
            utf8(value("command.reference.sha256")),
            "CONVERT(varbinary(71),'sha256:'+LOWER(CONVERT(varchar(64),HASHBYTES('SHA2_256',"
            + utf8(obj("command.payload"))
            + "),2)))",
        ),
    ]
    if control:
        pairs.extend(
            [
                (obj("subject"), "JSON_QUERY(@request_json,'$.subject')"),
                (obj("plan_set.payload.workspace_attempt"), "JSON_QUERY(@request_json,'$.workspace_attempt')"),
                (obj("plan_set.payload.guard"), "JSON_QUERY(@request_json,'$.guard')"),
                (obj("executor.profile"), "JSON_QUERY(@request_json,'$.profile')"),
                (obj("executor.command"), "JSON_QUERY(@request_json,'$.command')"),
                (utf8(obj("executor")), "@executor"),
                (utf8(value("reservation.locator")), "@reservation_locator"),
                (utf8(value("reservation.sha256")), "@reservation_digest"),
            ]
        )
    else:
        pairs.extend(
            [
                (value("registration.id"), "LOWER(CONVERT(nvarchar(36),@registration_id))"),
                (utf8(value("registration.sha256")), "@registration_digest"),
                (utf8(value("catalog_binding_sha256")), "@binding_digest"),
                (value("subject.generation_id"), "LOWER(CONVERT(nvarchar(36),@generation))"),
                (value("plan_set.payload.schema"), "N'dpone.mssql-physical-plan-set.v1'"),
                (value("plan_set.payload.generation_id"), value("subject.generation_id")),
                (value("plan_set.payload.runtime_registration_id"), value("registration.id")),
                (obj("plan_set.payload.model_database"), "JSON_QUERY(@registration,'$.model_database')"),
                (obj("executor.profile"), "JSON_QUERY(@binding,'$.resource_bounds')"),
                (value("plan_set.payload.workspace_attempt.workflow_id"), scalar("@binding", "$.workflow_id")),
                (utf8(obj("executor")), "@source_executor"),
                (value("command.payload.schema"), "N'dpone.trusted-dbt-command-plan.v1'"),
                (value("command.payload.phase"), "N'BUILD'"),
                (value("command.payload.executor_invocation_id"), "LOWER(CONVERT(nvarchar(36),@expected_invocation))"),
            ]
        )
        for name, other in [
            ("generation_id", "subject.generation_id"),
            ("invocation_id", "executor.invocation_id"),
            ("runtime_registration_id", "registration.id"),
        ]:
            pairs.append((scalar("@managed_vars", "$.__dpone_managed." + name), value(other)))
        pairs.append(("JSON_QUERY(@managed_vars,'$.__dpone_managed.plan_set')", obj("plan_set.reference")))
    return _differences(pairs)


def _sections(enrollment_sql: bytes) -> tuple[str, ...]:
    if type(enrollment_sql) is not bytes or not enrollment_sql:
        raise ValueError("authenticated enrollment package bytes are required")
    remaining = enrollment_sql.decode("utf-8")
    parts = []
    for boundary in (
        "ENROLLMENT TABLE",
        "CONTEXT",
        "VALIDATION",
        "MODEL LINK",
        "NAMESPACE",
        "NATIVE JSON",
        "COMPONENT",
    ):
        marker = "\n-- DPONE " + boundary + " BOUNDARY\n"
        if remaining.count(marker) != 1:
            raise ValueError("enrollment package requires exact finite source sections")
        head, remaining = remaining.split(marker)
        parts.append(head)
    return (*parts, remaining)


def _renderer_parts(
    *,
    enrollment_sql: bytes,
    discovery_sql: bytes,
    model_database: MssqlDatabaseAuthorityPin,
    model_schema: str,
    model_schema_id: int,
    control_database: str,
    control_schema: str,
    enrollment_certificate_thumbprint: bytes,
    build: bool = False,
) -> tuple[tuple[str, ...], dict[str, str]]:
    from dpone.adapters.dbt_mssql_physical_catalog_queries import _literal
    from dpone.adapters.dbt_mssql_physical_catalog_v2_queries import _canonical, _links, _shapes
    from dpone.adapters.dbt_mssql_physical_namespace_queries import physical_namespace_sql
    from dpone.adapters.dbt_mssql_physical_source_queries import _caller, _pin, _projections, _quote
    from dpone.adapters.native_generation_mssql_json import decode_bytes, scalar, utf8
    from dpone.contracts.dbt_mssql_physical_registration import database_pin_payload
    from dpone.contracts.mssql_object_name import native_control_schema
    from dpone.contracts.native_delivery_json import encode_native_delivery_json

    database_pin_payload(model_database)
    control = native_control_schema(control_schema)
    if model_schema.casefold() in {"dpone_physical", "dbo", "sys", "information_schema", control.casefold()}:
        raise ValueError("enrollment requires a dedicated model schema")
    if type(enrollment_certificate_thumbprint) is not bytes or len(enrollment_certificate_thumbprint) != 20:
        raise ValueError("enrollment requires an observed 20-byte certificate thumbprint")
    parts = _sections(enrollment_sql)
    namespace_input, namespace_assertion = physical_namespace_sql(
        discovery_sql=discovery_sql, model_schema=model_schema, model_schema_id=model_schema_id, control_schema=control
    )
    replacements = {
        "SOURCE_CONTEXT": parts[2],
        "ENROLLMENT_VALIDATE": parts[3],
        "ENROLLMENT_LINKS": _enrollment_links() + "\n" + parts[4],
        "NAMESPACE_ALL": parts[5],
        "NAMESPACE_FILTER": "WHERE models.position=@selected_position" if build else "",
        "NAMESPACE_INPUT": namespace_input,
        "NAMESPACE_ASSERTION": namespace_assertion,
        "MODEL_CALLER": _caller("dpone_physical"),
        "CONTROL_CALLER": _caller(control),
        "MODEL_PIN": _pin("model", "dpone_physical")
        + "\n"
        + _differences(
            [
                (
                    "JSON_QUERY(@registration,'$.model_database')",
                    _literal(encode_native_delivery_json(database_pin_payload(model_database)).decode("utf-8")),
                )
            ]
        ),
        "CONTROL_PIN": _pin("control", control),
        "CONTROL_DATABASE": _quote(control_database),
        "CONTROL_SCHEMA": control,
        "MODEL_SCHEMA": _literal(model_schema),
        "MODEL_DATABASE_NAME": _literal(model_database.database_name),
        "PROJECTION_CHECK": _projections().replace("@payload", "@registration_payload"),
        "REQUIRED_ROLE": "build" if build else "metadata",
        "BINDING_SHAPES": _shapes(),
        "BINDING_CANONICAL": _canonical(),
        "BINDING_LINKS": _links(model_database, model_schema, model_schema_id, catalog_module_schema="dpone_physical"),
        "DOCUMENT_CHECKS": enrollment_document_sql(enrollment_sql=enrollment_sql),
        "CONTROL_LINKS": _enrollment_links(control=True),
        "MODEL_ID": scalar("@model_document", "$.spec.model_unique_id"),
        "NAMESPACE_MODEL_ID": "model_identity.model_unique_id",
    }
    for key, source, target in [
        ("REGISTRATION_DECODE", "@registration_payload", "@registration"),
        ("BINDING_DECODE", "@binding_payload", "@binding"),
        ("ENROLLMENT_DECODE", "@payload", "@enrollment"),
        ("CONTROL_ENROLLMENT_DECODE", "@enrollment_payload", "@enrollment"),
        ("REQUEST_DECODE", "@request", "@request_json"),
    ]:
        replacements[key] = decode_bytes(source, target) + "\n" + _differences([(utf8(target), source)])
    for key, expression in [
        ("AUTHORITY_LOCATOR", scalar("@registration", "$.control_authority.locator")),
        ("AUTHORITY_DIGEST", scalar("@registration", "$.control_authority.sha256")),
        ("PLAN_LOCATOR", scalar("@enrollment", "$.plan_set.reference.locator")),
        ("PLAN_DIGEST", scalar("@enrollment", "$.plan_set.reference.sha256")),
        ("COMMAND_LOCATOR", scalar("@enrollment", "$.command.reference.locator")),
        ("COMMAND_DIGEST", scalar("@enrollment", "$.command.reference.sha256")),
        ("ENROLLMENT_SUBJECT", "JSON_QUERY(@enrollment,'$.subject')"),
        ("MODEL_ID_BYTES", "@current_model_id"),
        ("UNSIGNED_SPEC_BYTES", "@unsigned_spec"),
        ("UNSIGNED_MODEL_BYTES", "@unsigned_model"),
        ("NAMESPACE_BYTES", "@json"),
    ]:
        replacements[key] = utf8(expression)
    for key, role in [("CANDIDATE", "CANDIDATE"), ("HELPER", "HELPER"), ("CCI", "CCI")]:
        replacements[key + "_NAME_BYTES"] = utf8(
            "(@name_payload+N'" + role + '","schema":"dpone.mssql-physical-object-name.v1"}' + "')"
        )
    for key, kind in [("MODEL", "SPVC"), ("CONTROL", "CPVC")]:
        replacements[
            key + "_SIGNATURE"
        ] = f"""IF (SELECT COUNT_BIG(*) FROM sys.crypt_properties WHERE class=1 AND major_id=@@PROCID)<>1
 OR NOT EXISTS(SELECT 1 FROM sys.crypt_properties WHERE class=1 AND major_id=@@PROCID
 AND crypt_type='{kind}' AND thumbprint=0x{enrollment_certificate_thumbprint.hex()})
 THROW 51602, 'DPONE_ENROLLMENT_CALLER_INVALID', 1;"""
    return parts, replacements


def enrollment_procedures(
    *,
    enrollment_sql: bytes,
    discovery_sql: bytes,
    model_database: MssqlDatabaseAuthorityPin,
    model_schema: str,
    model_schema_id: int,
    control_database: str,
    control_schema: str,
    enrollment_certificate_thumbprint: bytes,
) -> dict[str, str]:
    """Render exact enroll/read/control bodies from authenticated package inputs."""
    parts, replacements = _renderer_parts(
        enrollment_sql=enrollment_sql,
        discovery_sql=discovery_sql,
        model_database=model_database,
        model_schema=model_schema,
        model_schema_id=model_schema_id,
        control_database=control_database,
        control_schema=control_schema,
        enrollment_certificate_thumbprint=enrollment_certificate_thumbprint,
    )
    modules = parts[1].split("\n-- DPONE MODULE BOUNDARY\n")
    if len(modules) != 3:
        raise ValueError("enrollment requires its exact three-module inventory")
    return {name: _expand(sql, replacements) for name, sql in zip((ENROLL, READ, CONTROL), modules, strict=True)}
