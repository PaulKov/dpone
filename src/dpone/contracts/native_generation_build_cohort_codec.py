"""Closed canonical metadata for actual build artifacts; no raw artifact rewrite."""

from __future__ import annotations

from typing import Literal, cast

from dpone.contracts.native_delivery_json import (
    NativeJsonValue,
    decode_native_delivery_json,
    encode_native_delivery_json,
)
from dpone.contracts.native_generation_build_cohort import NativeBuildArtifact, NativeBuildArtifactInventory
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_source_custody import NativeSourceCustodyError
from dpone.contracts.native_source_custody_codec import decode_source_executor_binding, encode_source_executor_binding

_SCHEMA = "dpone.native-build-artifact-inventory.v1"
_FIELDS = {
    "schema",
    "executor",
    "command",
    "toolchain",
    "termination",
    "execution_pack",
    "build_evidence",
    "dbt_invocation_id",
    "artifacts",
}


def encode_native_build_artifact_inventory(value: NativeBuildArtifactInventory) -> bytes:
    """Encode references and exact sizes, excluding the inventory's own reference."""
    if type(value) is not NativeBuildArtifactInventory:
        raise NativeSourceCustodyError("expected exact build artifact inventory")
    value.__post_init__()
    return encode_native_delivery_json(
        {
            "schema": _SCHEMA,
            "executor": decode_native_delivery_json(encode_source_executor_binding(value.executor)),
            "command": _ref(value.command),
            "toolchain": _ref(value.toolchain),
            "termination": _ref(value.termination),
            "execution_pack": _ref(value.execution_pack),
            "build_evidence": _ref(value.build_evidence),
            "dbt_invocation_id": value.dbt_invocation_id,
            "artifacts": [
                {
                    "role": artifact.role,
                    "relative_path": artifact.relative_path,
                    "size_bytes": artifact.size_bytes,
                    "original": _ref(artifact.original),
                }
                for artifact in value.artifacts
            ],
        }
    )


def decode_native_build_artifact_inventory(payload: bytes) -> NativeBuildArtifactInventory:
    """Reject altered membership and noncanonical bytes; authenticate outside codec."""
    value = _object(decode_native_delivery_json(payload), _FIELDS)
    if value["schema"] != _SCHEMA:
        raise NativeSourceCustodyError("unsupported build artifact inventory schema")
    artifacts = value["artifacts"]
    if type(artifacts) is not list or len(artifacts) != 2:
        raise NativeSourceCustodyError("inventory requires exactly two artifact entries")
    result = NativeBuildArtifactInventory(
        executor=decode_source_executor_binding(encode_native_delivery_json(value["executor"])),
        command=_decode_ref(value["command"]),
        toolchain=_decode_ref(value["toolchain"]),
        termination=_decode_ref(value["termination"]),
        execution_pack=_decode_ref(value["execution_pack"]),
        build_evidence=_decode_ref(value["build_evidence"]),
        dbt_invocation_id=_string(value["dbt_invocation_id"]),
        artifacts=tuple(_artifact(item) for item in artifacts),
    )
    if encode_native_build_artifact_inventory(result) != payload:
        raise NativeSourceCustodyError("build artifact inventory bytes must be canonical")
    return result


def _artifact(value: NativeJsonValue) -> NativeBuildArtifact:
    raw = _object(value, {"role", "relative_path", "size_bytes", "original"})
    role, size = raw["role"], raw["size_bytes"]
    if type(role) is not str or role not in {"MANIFEST", "RUN_RESULTS"} or type(size) is not int:
        raise NativeSourceCustodyError("artifact requires an exact role and integer size")
    return NativeBuildArtifact(
        cast(Literal["MANIFEST", "RUN_RESULTS"], role),
        _string(raw["relative_path"]),
        size,
        _decode_ref(raw["original"]),
    )


def _object(value: NativeJsonValue, fields: set[str]) -> dict[str, NativeJsonValue]:
    if type(value) is not dict or set(value) != fields:
        raise NativeSourceCustodyError("build inventory payload requires exact fields")
    return value


def _string(value: NativeJsonValue) -> str:
    if type(value) is not str:
        raise NativeSourceCustodyError("build inventory text must be an exact string")
    return value


def _ref(value: OriginalRef) -> dict[str, NativeJsonValue]:
    return {"locator": value.locator, "sha256": value.sha256}


def _decode_ref(value: NativeJsonValue) -> OriginalRef:
    raw = _object(value, {"locator", "sha256"})
    return OriginalRef(_string(raw["locator"]), _string(raw["sha256"]))
