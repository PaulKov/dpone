"""Render the explicit workspace gateway permission cutover, without executing it.

The installer must render/install and independently verify every fixed gateway
procedure first. This module alone does not create a usable gateway or certify
SQL permissions. Application connections never run provisioning SQL.
"""

from __future__ import annotations

import re

WORKSPACE_GATEWAY_SESSION_OPTIONS = """SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET NUMERIC_ROUNDABORT OFF;"""

WORKSPACE_PROTECTED_TABLES = (
    "dbt_workspace_channels",
    "dbt_workspace_handover_claims",
    "dbt_workspace_activations",
    "dbt_workspace_activation_guards",
    "dbt_workspace_activation_write_subjects",
    "dbt_workspace_attempts",
    "dbt_workspace_attempt_guards",
    "semantic_refresh_guards",
)
_READ_PROCEDURES = frozenset({"workspace_channel_read", "workspace_lifecycle_read", "workspace_attempt_read"})
_ATTEMPT_PROCEDURES = frozenset({"workspace_attempt_admit", "workspace_attempt_terminalize"})
_ROLE_CAPABILITIES = {
    "dpone_workspace_runtime": _READ_PROCEDURES
    | _ATTEMPT_PROCEDURES
    | {
        "workspace_handover_claim",
        "workspace_handover_begin_retirement",
        "workspace_handover_finalize_retirement",
        "workspace_handover_prepare",
        "workspace_handover_complete",
    },
    "dpone_workspace_legacy": _READ_PROCEDURES
    | _ATTEMPT_PROCEDURES
    | {
        "workspace_legacy_prepare",
        "workspace_legacy_activate",
        "workspace_legacy_begin_retirement",
        "workspace_legacy_finalize_retirement",
    },
    "dpone_workspace_registrar": _READ_PROCEDURES
    | {
        "workspace_registration_inventory",
        "workspace_register_empty",
        "workspace_adopt_active",
    },
    "dpone_semantic_guard_runtime": frozenset(
        {"semantic_guard_seed", "semantic_guard_acquire", "semantic_guard_release"}
    ),
}


def workspace_gateway_identifier(value: str) -> str:
    """Accept only bounded simple identifiers before interpolating installer SQL."""
    if not isinstance(value, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", value) is None:
        raise ValueError("workspace gateway SQL identifier is invalid")
    return value


def render_workspace_gateway_security(control_schema: str) -> str:
    """Create disjoint EXECUTE roles and deny direct protected mutation atomically.

    No login/user membership is automatically granted. The platform must separately
    bind reviewed principals and audit their effective privileges, including inherited
    roles and other callable modules, before enabling any channel.
    """
    schema = workspace_gateway_identifier(control_schema)
    procedures = sorted(
        set().union(
            *_ROLE_CAPABILITIES.values(),
            {
                "workspace_json_escape",
                "workspace_json_utf8_sha256",
                "workspace_json_canonical",
                "workspace_request_require",
            },
        )
    )
    statements = [
        WORKSPACE_GATEWAY_SESSION_OPTIONS,
        "SET XACT_ABORT ON;",
        "BEGIN TRANSACTION;",
        _ownership_preconditions(schema, procedures),
    ]
    for role, allowed in _ROLE_CAPABILITIES.items():
        statements.append(
            f"""
IF DATABASE_PRINCIPAL_ID(N'{role}') IS NULL
    CREATE ROLE [{role}] AUTHORIZATION [dbo];
IF NOT EXISTS (
    SELECT 1 FROM sys.database_principals
    WHERE name = N'{role}' AND type = N'R'
      AND owning_principal_id = DATABASE_PRINCIPAL_ID(N'dbo')
)
    THROW 51000, 'workspace gateway role authority differs', 1;
DENY ALTER, TAKE OWNERSHIP ON SCHEMA::[{schema}] TO [{role}];
""".strip()
        )
        for table in WORKSPACE_PROTECTED_TABLES:
            statements.append(
                f"DENY INSERT, UPDATE, DELETE, ALTER, TAKE OWNERSHIP ON OBJECT::[{schema}].[{table}] TO [{role}];"
            )
        for procedure in procedures:
            action = "GRANT" if procedure in allowed else "DENY"
            statements.append(f"{action} EXECUTE ON OBJECT::[{schema}].[{procedure}] TO [{role}];")
    statements.append(
        f"GRANT SELECT ON OBJECT::[{schema}].[semantic_refresh_guards] TO [dpone_semantic_guard_runtime];"
    )
    statements.append("COMMIT TRANSACTION;")
    return "\n".join(statements)


def _ownership_preconditions(schema: str, procedures: list[str]) -> str:
    required = [(name, "U") for name in WORKSPACE_PROTECTED_TABLES] + [(name, "P") for name in procedures]
    checks = [f"OBJECT_ID(N'[{schema}].[{name}]', N'{kind}') IS NULL" for name, kind in required]
    names = ", ".join(f"N'{name}'" for name, _ in required)
    roles = ", ".join(f"N'{role}'" for role in _ROLE_CAPABILITIES)
    return f"""
IF SCHEMA_ID(N'{schema}') IS NULL OR {" OR ".join(checks)}
    THROW 51000, 'workspace gateway object is missing', 1;
IF EXISTS (
    SELECT 1 FROM sys.schemas AS namespace
    JOIN sys.database_principals AS principal ON principal.principal_id = namespace.principal_id
    WHERE namespace.name = N'{schema}' AND principal.name IN ({roles})
)
    THROW 51000, 'workspace gateway schema owner is a runtime role', 1;
IF EXISTS (
    SELECT 1 FROM sys.objects AS object
    JOIN sys.schemas AS namespace ON namespace.schema_id = object.schema_id
    WHERE namespace.name = N'{schema}' AND object.name IN ({names})
      AND COALESCE(object.principal_id, namespace.principal_id) <> namespace.principal_id
)
    THROW 51000, 'workspace gateway ownership chain differs', 1;
IF EXISTS (
    SELECT 1 FROM sys.sql_modules AS module
    JOIN sys.objects AS object ON object.object_id = module.object_id
    WHERE object.schema_id = SCHEMA_ID(N'{schema}') AND object.name IN ({names})
      AND (module.execute_as_principal_id IS NOT NULL OR module.definition IS NULL
           OR module.uses_ansi_nulls <> 1 OR module.uses_quoted_identifier <> 1)
)
    THROW 51000, 'workspace gateway module execution context differs', 1;
""".strip()
