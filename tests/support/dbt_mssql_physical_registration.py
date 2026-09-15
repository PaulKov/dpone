"""Synthetic registration inputs; no installed authority or credentials."""

from dataclasses import asdict
from uuid import UUID

from dpone.contracts.dbt_mssql_physical_registration_values import (
    DatabasePrincipal,
    DatabaseRoleMapping,
    DedicatedObserver,
    PlatformSelection,
    ProgramAuthority,
    RegisteredLimits,
    RegisteredPrincipals,
)
from dpone.contracts.dbt_workspace_runtime_authority import DbtWorkspaceRuntimeAuthority
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityPin
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import NativePlatformOriginalSubject


def registration_inputs():
    """Return fresh constructor inputs with explicit limits and dedicated roles."""
    digest = "sha256:" + "a" * 64
    authority = DbtWorkspaceRuntimeAuthority.build(
        environment="test",
        release_id=digest,
        deployment_id=digest,
        release_sha256=digest,
        deployment_sha256=digest,
        binding_set_sha256=digest,
        connection_registry_sha256=digest,
        credential_runtime_sha256=digest,
    )
    subject = NativePlatformOriginalSubject(authority, digest)
    pin = MssqlDatabaseAuthorityPin(
        "example",
        5,
        "2024-02-29T12:00:00.1234567",
        UUID("10000000-0000-0000-0000-000000000001"),
    )

    def role(number, sid):
        principal = DatabasePrincipal(number, sid)
        return DatabaseRoleMapping(principal, principal)

    return {
        "registration_id": "20000000-0000-0000-0000-000000000001",
        "platform_subject": subject,
        "control_authority": OriginalRef("originals/control", "sha256:" + "1" * 64),
        "trusted_profile": PlatformSelection(
            OriginalRef("originals/profile", "sha256:" + "2" * 64),
            NativePlatformOriginalSubject(authority, "sha256:" + "b" * 64),
        ),
        "trusted_toolchain": PlatformSelection(
            OriginalRef("originals/toolchain", "sha256:" + "3" * 64),
            NativePlatformOriginalSubject(authority, "sha256:" + "c" * 64),
        ),
        "qualification_policy_id": "example-policy",
        "control_connection_ref": "control",
        "model_connection_ref": "model",
        "service_authority_sha256": digest,
        "control_database": pin,
        "model_database": pin,
        "control_schema": "runtime_control",
        "local_schema": "runtime_local",
        "program": ProgramAuthority("sha256:" + "d" * 64, "sha256:" + "e" * 64, "sha256:" + "f" * 64),
        "capacity_authority": OriginalRef("originals/capacity", "sha256:" + "4" * 64),
        "limits": RegisteredLimits(1024, 4096, 10, 2048, 10, 256),
        "principals": RegisteredPrincipals(role(5, "aa"), role(6, "bb"), DedicatedObserver(role(7, "cc"))),
    }


def registration_document():
    """Expected wire vector independent of registration projection and codec."""
    digest = "sha256:" + "a" * 64
    subject = {
        "schema": "dpone.native-original-subject.v1",
        "scope": "PLATFORM",
        "authority": asdict(registration_inputs()["platform_subject"].authority),
        "platform_policy_sha256": digest,
    }
    pin = {
        "database_name": "example",
        "database_id": 5,
        "create_token": "2024-02-29T12:00:00.1234567",
        "database_guid": "10000000-0000-0000-0000-000000000001",
    }

    def role(number, sid):
        return {"control": {"principal_id": number, "sid_hex": sid}, "model": {"principal_id": number, "sid_hex": sid}}

    # Fresh nested objects prevent mutation tests from accidentally changing
    # another field that merely has equal contents in this synthetic example.
    import copy

    return copy.deepcopy(
        {
            "schema": "dpone.mssql-physical-runtime-registration.v1",
            "registration_id": "20000000-0000-0000-0000-000000000001",
            "platform_subject": subject,
            "control_authority": {"locator": "originals/control", "sha256": "sha256:" + "1" * 64},
            "trusted_profile": {
                "reference": {"locator": "originals/profile", "sha256": "sha256:" + "2" * 64},
                "subject": {**copy.deepcopy(subject), "platform_policy_sha256": "sha256:" + "b" * 64},
            },
            "trusted_toolchain": {
                "reference": {"locator": "originals/toolchain", "sha256": "sha256:" + "3" * 64},
                "subject": {**copy.deepcopy(subject), "platform_policy_sha256": "sha256:" + "c" * 64},
            },
            "qualification_policy_id": "example-policy",
            "control_connection_ref": "control",
            "model_connection_ref": "model",
            "service_authority_sha256": digest,
            "control_database": pin.copy(),
            "model_database": pin.copy(),
            "control_schema": "runtime_control",
            "local_schema": "runtime_local",
            "program": {
                "control_program_id": "dpone.mssql-physical-control.v1",
                "control_program_sha256": "sha256:" + "d" * 64,
                "package_bundle_sha256": "sha256:" + "e" * 64,
                "macro_authority_sha256": "sha256:" + "f" * 64,
                "physical_policy": "sqlserver-table-physical-v1",
            },
            "capacity_authority": {"locator": "originals/capacity", "sha256": "sha256:" + "4" * 64},
            "limits": {
                "max_metadata_bytes": 1024,
                "max_generation_bytes": 4096,
                "max_catalog_rows": 10,
                "max_definition_utf16_bytes": 2048,
                "max_dependency_rows": 10,
                "max_columns": 256,
            },
            "principals": {
                "metadata": role(5, "aa"),
                "build": role(6, "bb"),
                "observer": {"mode": "DEDICATED", "mapping": role(7, "cc")},
            },
        }
    )
