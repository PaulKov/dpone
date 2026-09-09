"""Canonical, dependency-free identity for compact dpone Airflow packs.

The identity binds the complete JSON pack by default. Only the self-referential
fingerprint and approved top-level report metadata are excluded. The helpers
perform no I/O and import no Airflow or runtime package.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
from collections.abc import Mapping
from typing import Any, TypeAlias

PACK_IDENTITY_SCHEMA = "dpone.airflow-pack-identity.v1"

_EXCLUDED_TOP_LEVEL_FIELDS = frozenset(
    {
        "pack_fingerprint",
        "producer",
        "meta",
        "artifact_dir",
        "output_path",
        "bundle_path",
        "next_actions",
        "warnings",
        "blockers",
    }
)
_CANONICAL_SHA256 = re.compile(r"sha256:[0-9a-f]{64}")

PackSource: TypeAlias = Mapping[str, Any] | str | bytes | bytearray


class PackIdentityError(ValueError):
    """Raised when a pack cannot satisfy the canonical identity contract."""


def parse_pack_json(source: str | bytes | bytearray) -> dict[str, Any]:
    """Parse one UTF-8 JSON pack without accepting duplicate keys or constants.

    This parser is provided for byte-oriented producer and consumer boundaries.
    Parsing through it preserves JSON arrays in their declared order and rejects
    duplicate object keys before a mapping can silently overwrite them.
    """

    if isinstance(source, str):
        text = source
    elif isinstance(source, (bytes, bytearray)):
        try:
            text = bytes(source).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise PackIdentityError("workload pack must be one UTF-8 JSON object") from exc
    else:
        raise PackIdentityError("workload pack source must be JSON text or UTF-8 bytes")
    try:
        payload = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_constant,
        )
    except PackIdentityError:
        raise
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise PackIdentityError("workload pack must be one valid UTF-8 JSON object") from exc
    if not isinstance(payload, dict):
        raise PackIdentityError("workload pack must be one UTF-8 JSON object")
    return _copy_json_object(payload)


def identity_payload(pack: PackSource) -> dict[str, Any]:
    """Return the validated whole-pack identity payload.

    Exclusions apply only at the pack's top level. Identically named nested
    fields, arrays, and unknown future fields remain identity-bearing.
    """

    payload = _pack_object(pack)
    return {key: value for key, value in payload.items() if key not in _EXCLUDED_TOP_LEVEL_FIELDS}


def compute_pack_fingerprint(pack: PackSource) -> str:
    """Derive the canonical lowercase SHA-256 identity for ``pack``."""

    return _fingerprint(_pack_object(pack))


def verify_pack_fingerprint(pack: PackSource) -> str:
    """Validate the declared algorithm and claim, then return the derived value.

    Returning the independently derived value lets consumers bind evidence and
    downstream comparisons without reusing an unverified embedded claim.
    """

    payload = _pack_object(pack)
    _validate_pack_identity(payload.get("pack_identity"))
    claim = payload.get("pack_fingerprint")
    if not isinstance(claim, str) or _CANONICAL_SHA256.fullmatch(claim) is None:
        raise PackIdentityError("pack_fingerprint must be canonical lowercase sha256:<64 hex>")
    derived = _fingerprint(payload)
    if not hmac.compare_digest(claim, derived):
        raise PackIdentityError("pack_fingerprint does not match the derived workload pack identity")
    return derived


def _fingerprint(pack: Mapping[str, Any]) -> str:
    payload = {key: value for key, value in pack.items() if key not in _EXCLUDED_TOP_LEVEL_FIELDS}
    return "sha256:" + hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def _canonical_json_bytes(payload: Mapping[str, Any]) -> bytes:
    try:
        encoded = json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        return encoded.encode("utf-8")
    except (RecursionError, TypeError, UnicodeEncodeError, ValueError) as exc:
        raise PackIdentityError("workload pack contains a value that is not canonical JSON") from exc


def _pack_object(pack: PackSource) -> dict[str, Any]:
    if isinstance(pack, (str, bytes, bytearray)):
        return parse_pack_json(pack)
    if not isinstance(pack, Mapping):
        raise PackIdentityError("workload pack must be a JSON object")
    return _copy_json_object(pack)


def _copy_json_object(payload: Mapping[str, Any]) -> dict[str, Any]:
    try:
        result = _copy_json_value(payload, active_containers=set())
    except RecursionError as exc:
        raise PackIdentityError("workload pack JSON is too deeply nested") from exc
    assert isinstance(result, dict)
    return result


def _copy_json_value(value: Any, *, active_containers: set[int]) -> Any:
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, str):
        try:
            value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise PackIdentityError("workload pack JSON strings must contain valid Unicode") from exc
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise PackIdentityError("workload pack contains a non-finite JSON number")
        return value
    if isinstance(value, Mapping):
        return _copy_json_mapping(value, active_containers=active_containers)
    if isinstance(value, list):
        return _copy_json_list(value, active_containers=active_containers)
    raise PackIdentityError("workload pack contains a value that is not JSON")


def _copy_json_mapping(
    value: Mapping[Any, Any],
    *,
    active_containers: set[int],
) -> dict[str, Any]:
    _enter_container(value, active_containers)
    try:
        result: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise PackIdentityError("workload pack JSON objects must use string keys")
            result[key] = _copy_json_value(item, active_containers=active_containers)
        return result
    finally:
        active_containers.remove(id(value))


def _copy_json_list(value: list[Any], *, active_containers: set[int]) -> list[Any]:
    _enter_container(value, active_containers)
    try:
        return [_copy_json_value(item, active_containers=active_containers) for item in value]
    finally:
        active_containers.remove(id(value))


def _enter_container(value: object, active_containers: set[int]) -> None:
    identity = id(value)
    if identity in active_containers:
        raise PackIdentityError("workload pack contains a recursive non-JSON value")
    active_containers.add(identity)


def _unique_object(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise PackIdentityError("workload pack contains a duplicate JSON key")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise PackIdentityError(f"workload pack contains non-finite JSON constant {value}")


def _validate_pack_identity(value: object) -> None:
    if not isinstance(value, Mapping) or set(value) != {"schema"} or value.get("schema") != PACK_IDENTITY_SCHEMA:
        raise PackIdentityError("pack_identity must be exactly {'schema': 'dpone.airflow-pack-identity.v1'}")


__all__ = [
    "PACK_IDENTITY_SCHEMA",
    "PackIdentityError",
    "compute_pack_fingerprint",
    "identity_payload",
    "parse_pack_json",
    "verify_pack_fingerprint",
]
