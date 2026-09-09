"""Validation and error primitives for semantic-refresh runtime proofs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


class SemanticRefreshRuntimeProofError(RuntimeError):
    """Stable fail-closed runtime proof error consumed by the worker gate."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def model_ids(value: object) -> tuple[str, ...]:
    """Validate the canonical selected-model closure."""

    if (
        not isinstance(value, tuple)
        or not value
        or value != tuple(sorted(set(value)))
        or any(not isinstance(item, str) or not item.startswith("model.") for item in value)
    ):
        raise unverified("runtime selected-model closure is not canonical")
    return value


def string_array(value: object) -> tuple[str, ...]:
    """Validate a closed, sorted string sequence."""

    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        raise unverified("runtime macro dependency closure is unavailable")
    result = tuple(sorted(set(value))) if all(isinstance(item, str) and item for item in value) else ()
    if len(result) != len(value):
        raise unverified("runtime macro dependency closure is not canonical")
    return result


def mapping(value: object, field: str) -> Mapping[str, Any]:
    """Require one mapping value."""

    if not isinstance(value, Mapping):
        raise unverified(f"{field} must be an object")
    return value


def required_text(value: object, field: str) -> str:
    """Require non-empty text."""

    if not isinstance(value, str) or not value.strip():
        raise unverified(f"{field} must be non-empty text")
    return value


def relation(value: object) -> tuple[str, str, str]:
    """Normalize one three-part relation for policy comparison."""

    if not isinstance(value, tuple) or len(value) != 3 or any(not isinstance(item, str) or not item for item in value):
        raise ValueError("catalog relation identity must have three exact parts")
    return value[0].casefold(), value[1].casefold(), value[2].casefold()


def status_error(status: object, message: str) -> SemanticRefreshRuntimeProofError:
    """Map proof status to the stable public failure code."""

    return drift(message) if status == "NONCONFORMANT" else unverified(message)


def drift(message: str) -> SemanticRefreshRuntimeProofError:
    """Build a deterministic drift error."""

    return SemanticRefreshRuntimeProofError("DPONE_DBT_V2_PROOF_DRIFT", message)


def unverified(message: str) -> SemanticRefreshRuntimeProofError:
    """Build a deterministic unavailable-proof error."""

    return SemanticRefreshRuntimeProofError("DPONE_DBT_V2_PROOF_UNVERIFIED", message)


def validate_manifest_toolchain(
    manifest: Mapping[str, Any],
    *,
    expected_dbt_version: str,
) -> None:
    """Require the exact promoted SQL Server dbt toolchain."""

    metadata = mapping(manifest.get("metadata"), "manifest.metadata")
    if metadata.get("dbt_version") != expected_dbt_version or metadata.get("adapter_type") != "sqlserver":
        raise drift("runtime dbt manifest differs from the pinned SQL Server toolchain")


def model_node(nodes: Mapping[str, Any], model_id: str) -> Mapping[str, Any]:
    """Load one exact selected model node from the runtime manifest."""

    node = mapping(nodes.get(model_id), model_id)
    if node.get("unique_id") != model_id or node.get("resource_type") != "model":
        raise unverified("runtime manifest omitted an exact selected model")
    return node
