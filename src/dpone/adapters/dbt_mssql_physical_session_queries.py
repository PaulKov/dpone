"""Finite managed connection observation SQL; no SQL execution or certification."""

from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityPin

CONNECTION_OBSERVER = "physical_observe_current_connection_v1"
ATTACH = "physical_attach_session_v1"


def connection_observer_procedure(*, session_sql: bytes, connection_certificate_thumbprint: bytes) -> str:
    """Render the fixed self-only DMV observer with its distinct signing identity.

    The caller authenticates retained package bytes and observes the installed
    certificate. A supplied thumbprint is not evidence of installed permissions.
    """
    if type(session_sql) is not bytes or not session_sql:
        raise ValueError("authenticated session package bytes are required")
    if type(connection_certificate_thumbprint) is not bytes or len(connection_certificate_thumbprint) != 20:
        raise ValueError("connection observer requires an observed 20-byte certificate thumbprint")
    template = _session_parts(session_sql)[0]
    marker = "{{CONNECTION_CERTIFICATE}}"
    if template.count(marker) != 1:
        raise ValueError("connection observer requires its exact certificate substitution")
    result = template.replace(marker, "0x" + connection_certificate_thumbprint.hex())
    if "{{" in result:
        raise ValueError("connection observer contains unknown substitutions")
    return result


def _session_parts(session_sql: bytes) -> tuple[str, str]:
    import re

    if type(session_sql) is not bytes or not session_sql:
        raise ValueError("authenticated session package bytes are required")
    template = session_sql.decode("utf-8")
    allowed = {
        "CONNECTION_CERTIFICATE",
        "SOURCE_CONTEXT",
        "ENROLLMENT_VALIDATE",
        "NAMESPACE_ALL",
        "MODEL_INPUT_BYTES",
        "PLAN_INPUT_BYTES",
        "PLAN_REFERENCE_MATCH",
        "MODEL_MATCH",
    }
    if set(re.findall(r"\{\{([^}]+)\}\}", template)) != allowed:
        raise ValueError("session package requires its exact substitution inventory")
    parts = template.split("\n-- DPONE MODULE BOUNDARY\n")
    if len(parts) != 2:
        raise ValueError("session package requires exact observer/attach module inventory")
    return parts[0], parts[1]


def session_procedures(
    *,
    session_sql: bytes,
    enrollment_sql: bytes,
    discovery_sql: bytes,
    model_database: MssqlDatabaseAuthorityPin,
    model_schema: str,
    model_schema_id: int,
    control_database: str,
    control_schema: str,
    enrollment_certificate_thumbprint: bytes,
    connection_certificate_thumbprint: bytes,
) -> dict[str, str]:
    """Render real BUILD attach and its separately signed self-only observer.

    Namespace assertions cover only the selected model's three names. Full
    model/command/original linkage remains checked against immutable enrollment.
    This emits no transaction-bind, reset, retry or receipt endpoint.
    """
    from dpone.adapters.dbt_mssql_physical_catalog_v2_queries import _reject_difference
    from dpone.adapters.dbt_mssql_physical_enrollment_queries import _differences, _expand, _renderer_parts
    from dpone.adapters.native_generation_mssql_json import scalar, utf8

    if enrollment_certificate_thumbprint == connection_certificate_thumbprint:
        raise ValueError("connection observer requires a separate certificate")
    observer = connection_observer_procedure(
        session_sql=session_sql, connection_certificate_thumbprint=connection_certificate_thumbprint
    )
    _, replacements = _renderer_parts(
        enrollment_sql=enrollment_sql,
        discovery_sql=discovery_sql,
        model_database=model_database,
        model_schema=model_schema,
        model_schema_id=model_schema_id,
        control_database=control_database,
        control_schema=control_schema,
        enrollment_certificate_thumbprint=enrollment_certificate_thumbprint,
        build=True,
    )
    replacements.update(
        {
            "MODEL_INPUT_BYTES": utf8("@model_unique_id"),
            "PLAN_INPUT_BYTES": utf8("@plan_set_locator"),
            "PLAN_REFERENCE_MATCH": _differences(
                [
                    ("@plan_set_locator", scalar("@enrollment", "$.plan_set.reference.locator")),
                    ("CONVERT(nvarchar(max),@plan_set_sha256)", scalar("@enrollment", "$.plan_set.reference.sha256")),
                ]
            ),
            "MODEL_MATCH": "NOT "
            + _reject_difference("@model_unique_id", scalar("models.document", "$.spec.model_unique_id")),
        }
    )
    return {CONNECTION_OBSERVER: observer, ATTACH: _expand(_session_parts(session_sql)[1], replacements)}
