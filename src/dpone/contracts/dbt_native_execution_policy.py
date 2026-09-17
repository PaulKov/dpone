"""Closed native composition premises inside an authenticated v4 policy.

This is a structural/policy contract. Actual deployment registration, retained
subjects, runtime qualification, credentials and physical allocation are verified
by their application consumers; a valid mapping grants none of those capabilities.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import fields
from hashlib import sha256
from typing import Any

from dpone.contracts.dbt_mssql_physical_collation import (
    PHYSICAL_COLLATION_PATTERN,
    require_physical_collation_token,
)
from dpone.contracts.dbt_mssql_physical_validation import require_physical_identifier
from dpone.contracts.dbt_publish_schema_contract_common import (
    DIGEST,
    IDENTIFIER,
    NONBLANK_TOKEN,
    nullable,
    object_schema,
)
from dpone.contracts.dbt_workspace_runtime_authority import DbtWorkspaceRuntimeAuthority
from dpone.contracts.mssql_object_name import native_control_schema
from dpone.contracts.native_delivery_json import MAX_NATIVE_JSON_BYTES, encode_native_delivery_json
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import (
    NativeOriginalStorageAuthority,
    NativePlatformOriginalSubject,
    decode_native_original_storage_authority,
    decode_native_original_subject,
)

_POSITIVE = {"type": "integer", "minimum": 1}
_SQL_BIGINT = {**_POSITIVE, "maximum": 9223372036854775807}
_TEXT = {"type": "string", "minLength": 1, "maxLength": 4096}


def original_reference_schema() -> dict[str, Any]:
    """Use existing native locator/digest coordinates; semantic validation is exact."""
    return object_schema(("locator", "sha256"), {"locator": _TEXT, "sha256": DIGEST})


def _platform_subject_schema() -> dict[str, Any]:
    authority = {
        field.name: NONBLANK_TOKEN if field.name == "environment" else DIGEST
        for field in fields(DbtWorkspaceRuntimeAuthority)
    }
    return object_schema(
        ("schema", "scope", "authority", "platform_policy_sha256"),
        {
            "schema": {"const": "dpone.native-original-subject.v1"},
            "scope": {"const": "PLATFORM"},
            "authority": object_schema(tuple(authority), authority),
            "platform_policy_sha256": DIGEST,
        },
    )


def _selection_schema() -> dict[str, Any]:
    return object_schema(
        ("reference", "subject"),
        {
            "reference": original_reference_schema(),
            "subject": nullable(_platform_subject_schema()),
        },
    )


def _storage_schema() -> dict[str, Any]:
    integers = {"retention_days", "max_artifact_bytes"}
    flags = {"conditional_create_authorized", "require_object_lock"}
    properties = {
        field.name: _POSITIVE if field.name in integers else {"const": True} if field.name in flags else _TEXT
        for field in fields(NativeOriginalStorageAuthority)
    }
    properties.update(
        schema={"const": "dpone.native-original-storage-authority.v1"},
        provider={"const": "s3"},
        object_lock_mode={"const": "COMPLIANCE"},
    )
    return object_schema(tuple(properties), properties)


def native_execution_schema() -> dict[str, Any]:
    """Describe only concrete control/original/qualification/resource inputs."""
    limit_names = (
        "max_metadata_bytes",
        "original_io_chunk_bytes",
        "original_io_total_budget_seconds",
        "command_termination_allowance_seconds",
        "total_termination_budget_seconds",
    )
    limits = {name: _POSITIVE for name in limit_names}
    limits["max_metadata_bytes"] = {**_POSITIVE, "maximum": MAX_NATIVE_JSON_BYTES}
    return object_schema(
        ("control", "originals", "trusted_execution", "generation", "limits"),
        {
            "control": object_schema(
                ("connection_ref", "schema", "authority"),
                {
                    "connection_ref": NONBLANK_TOKEN,
                    "schema": IDENTIFIER,
                    "authority": original_reference_schema(),
                },
            ),
            "originals": object_schema(
                ("connection_ref", "authority", "policy"),
                {
                    "connection_ref": NONBLANK_TOKEN,
                    "authority": original_reference_schema(),
                    "policy": _storage_schema(),
                },
            ),
            "trusted_execution": object_schema(
                ("profile", "toolchain", "qualification_policy_id", "qualification"),
                {
                    "profile": _selection_schema(),
                    "toolchain": _selection_schema(),
                    "qualification_policy_id": NONBLANK_TOKEN,
                    "qualification": _selection_schema(),
                },
            ),
            "generation": object_schema(
                ("storage_root", "capacity_authority", "max_generation_bytes"),
                {
                    "storage_root": _selection_schema(),
                    "capacity_authority": original_reference_schema(),
                    "max_generation_bytes": _SQL_BIGINT,
                },
            ),
            "limits": object_schema(limit_names, limits),
            "physical_catalog_limits": _physical_catalog_limits_schema(),
            "physical_collation": object_schema(
                ("name",),
                {
                    "name": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 128,
                        "pattern": "^" + PHYSICAL_COLLATION_PATTERN + "$",
                    }
                },
            ),
            "physical_filegroup": object_schema(
                ("name",), {"name": {"type": "string", "minLength": 1, "maxLength": 128}}
            ),
        },
    )


def _physical_catalog_limits_schema() -> dict[str, Any]:
    """Explicit platform allocation choices; presence does not certify a route."""
    properties = {
        name: {**_POSITIVE, "maximum": 2147483647}
        for name in ("max_catalog_rows", "max_definition_utf16_bytes", "max_dependency_rows")
    }
    properties["max_columns"] = {"type": "integer", "const": 256}
    return object_schema(tuple(properties), properties)


def validate_native_execution_policy(value: dict[str, Any], *, serialized_payload_max_bytes: int | None) -> None:
    """Apply cross-field semantics after closed JSON Schema validation.

    Null upstream subject means actually issued under the selected full-policy
    PLATFORM subject. An explicit retained PLATFORM subject must be preserved by
    consumers. Neither option can relabel an original into GENERATION scope.
    """
    control, originals, trusted, generation, limits = (
        value[name]
        for name in (
            "control",
            "originals",
            "trusted_execution",
            "generation",
            "limits",
        )
    )
    catalog = value.get("physical_catalog_limits")
    if catalog is not None and catalog["max_dependency_rows"] > catalog["max_catalog_rows"]:
        raise ValueError("physical catalog dependency limit exceeds its catalog row ceiling")
    if "physical_filegroup" in value:
        require_physical_filegroup_name(value)
    if "physical_collation" in value:
        require_physical_collation_name(value)
    native_control_schema(control["schema"])
    _reference(control["authority"])
    authority_ref = _reference(originals["authority"])
    policy_bytes = encode_native_delivery_json(originals["policy"])
    decode_native_original_storage_authority(policy_bytes)
    if authority_ref.sha256 != "sha256:" + sha256(policy_bytes).hexdigest():
        raise ValueError("native storage authority differs from complete inline policy bytes")
    _reference(generation["capacity_authority"])
    for selection in (trusted["profile"], trusted["toolchain"], trusted["qualification"], generation["storage_root"]):
        _reference(selection["reference"])
        if selection["subject"] is not None:
            subject = decode_native_original_subject(encode_native_delivery_json(selection["subject"]))
            if type(subject) is not NativePlatformOriginalSubject:
                raise ValueError("native upstream selection requires its actual PLATFORM subject")
    if limits["original_io_chunk_bytes"] > limits["max_metadata_bytes"]:
        raise ValueError("native original chunk limit exceeds its metadata ceiling")
    if serialized_payload_max_bytes is not None and serialized_payload_max_bytes > generation["max_generation_bytes"]:
        raise ValueError("serialized payload ceiling exceeds the generation allocation ceiling")


def require_physical_filegroup_name(native_execution: Mapping[str, Any]) -> str:
    """Require the exact policy-selected name without SQL/default inference.

    Preparation consumers call this only after authenticating the complete policy.
    This pure accessor proves representation, not original authenticity, database
    identity, observed data-space ID/type, visibility or permission to allocate.
    Missing selection rejects instead of choosing PRIMARY or the database default.
    """
    selection = native_execution.get("physical_filegroup")
    if not isinstance(selection, Mapping) or set(selection) != {"name"}:
        raise ValueError("physical_filegroup requires a closed object with an explicit name")
    return require_physical_identifier(selection["name"], "physical_filegroup.name")


def require_physical_collation_name(native_execution: Mapping[str, Any]) -> str:
    """Read exact selection after authenticating the complete selected policy.

    This accessor validates representation only. It cannot establish SQL catalog
    availability, helper-output agreement, generation ownership or route authority.
    """
    selection = native_execution.get("physical_collation")
    if not isinstance(selection, Mapping) or set(selection) != {"name"}:
        raise ValueError("physical_collation requires a closed object with an explicit name")
    return require_physical_collation_token(selection["name"])


def _reference(value: dict[str, str]) -> OriginalRef:
    return OriginalRef(value["locator"], value["sha256"])
