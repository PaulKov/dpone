"""Exact observed build artifacts linked to one trusted invocation.

Shapes are not authority. The producer validates actual files and the locked
execution pack; consumers independently authenticate every original and outcome.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Literal, cast

from dpone.contracts.native_delivery_json import (
    NativeJsonValue,
    decode_native_delivery_json,
    encode_native_delivery_json,
)
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_source_custody import (
    NativeSourceCustodyError,
    SourceExecutorBinding,
    decode_source_executor_binding,
    encode_source_executor_binding,
)


@dataclass(frozen=True, slots=True)
class NativeBuildArtifact:
    """One bounded actual file; path is relative to the captured OUTPUT root."""

    role: Literal["MANIFEST", "RUN_RESULTS"]
    relative_path: str
    size_bytes: int
    original: OriginalRef

    def __post_init__(self) -> None:
        names = {"MANIFEST": "manifest.json", "RUN_RESULTS": "run_results.json"}
        if type(self.role) is not str or self.role not in names:
            raise NativeSourceCustodyError("unknown build artifact role")
        if type(self.relative_path) is not str or not self.relative_path or len(self.relative_path) > 4096:
            raise NativeSourceCustodyError("build artifact requires a bounded relative path")
        path = PurePosixPath(self.relative_path)
        if (
            path.is_absolute()
            or ".." in path.parts
            or path.as_posix() != self.relative_path
            or "\\" in self.relative_path
            or any(ord(c) < 32 for c in self.relative_path)
            or path.name != names[self.role]
        ):
            raise NativeSourceCustodyError("build artifact path differs from its confined role")
        if type(self.size_bytes) is not int or not 1 <= self.size_bytes <= 9223372036854775807:
            raise NativeSourceCustodyError("build artifact size must be an exact positive SQL bigint")
        _references(self.original)


@dataclass(frozen=True, slots=True)
class NativeBuildArtifactInventory:
    """Observed dbt ID mapping plus the complete fixed two-artifact cohort.

    The observed dbt invocation ID is not the trusted pre-dispatch UUID. Actual
    bytes from held output roots establish their mapping in the trusted producer.
    """

    executor: SourceExecutorBinding
    command: OriginalRef
    toolchain: OriginalRef
    termination: OriginalRef
    execution_pack: OriginalRef
    build_evidence: OriginalRef
    dbt_invocation_id: str
    artifacts: tuple[NativeBuildArtifact, ...]

    def __post_init__(self) -> None:
        if type(self.executor) is not SourceExecutorBinding:
            raise NativeSourceCustodyError("build inventory requires an exact executor")
        self.executor.__post_init__()
        _references(self.command, self.toolchain, self.termination, self.execution_pack, self.build_evidence)
        if self.command != self.executor.command:
            raise NativeSourceCustodyError("build inventory command differs from admission")
        label = self.dbt_invocation_id
        if (
            type(label) is not str
            or not 0 < len(label) <= 128
            or label != label.strip()
            or any(ord(c) < 32 for c in label)
        ):
            raise NativeSourceCustodyError("build inventory requires a canonical observed dbt identity")
        if type(self.artifacts) is not tuple or len(self.artifacts) != 2:
            raise NativeSourceCustodyError("build inventory requires exactly two immutable artifacts")
        for artifact in self.artifacts:
            if type(artifact) is not NativeBuildArtifact:
                raise NativeSourceCustodyError("build inventory requires exact artifact records")
            artifact.__post_init__()
        if tuple(item.role for item in self.artifacts) != ("MANIFEST", "RUN_RESULTS"):
            raise NativeSourceCustodyError("build inventory requires ordered manifest and run results")
        if len({PurePosixPath(item.relative_path).parent for item in self.artifacts}) != 1:
            raise NativeSourceCustodyError("build artifacts must share the admitted target directory")


def _references(*references: OriginalRef) -> None:
    for reference in references:
        if type(reference) is not OriginalRef:
            raise NativeSourceCustodyError("build cohort requires exact original references")
        reference.__post_init__()


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
