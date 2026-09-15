"""Closed canonical codecs for upstream-authenticated invocation environment."""

from __future__ import annotations

from dataclasses import fields
from typing import Literal, cast
from uuid import UUID

from dpone.contracts.dbt_toolchain import DbtToolchainContract
from dpone.contracts.native_delivery_json import (
    NativeJsonValue,
    decode_native_delivery_json,
    encode_native_delivery_json,
)
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_source_custody import NativeSourceCustodyError
from dpone.contracts.native_trusted_dbt_environment import (
    TrustedDbtOwnedRoot,
    TrustedDbtQualification,
    TrustedDbtToolchain,
)


def _ref(value: OriginalRef) -> dict[str, NativeJsonValue]:
    return {"locator": value.locator, "sha256": value.sha256}


def _read_ref(value: NativeJsonValue) -> OriginalRef:
    body = _object(value, {"locator", "sha256"})
    return OriginalRef(_string(body["locator"]), _string(body["sha256"]))


def _object(value: NativeJsonValue, expected: set[str]) -> dict[str, NativeJsonValue]:
    if type(value) is not dict or set(value) != expected:
        raise NativeSourceCustodyError("environment record requires exactly its declared fields")
    return value


def _string(value: NativeJsonValue) -> str:
    if type(value) is not str:
        raise NativeSourceCustodyError("environment string must be exact")
    return value


def _integer(value: NativeJsonValue) -> int:
    if type(value) is not int:
        raise NativeSourceCustodyError("environment integer must be exact")
    return value


def encode_trusted_dbt_toolchain(value: TrustedDbtToolchain) -> bytes:
    """Preserve the existing seven-field dbt contract without a second hash recipe."""
    if type(value) is not TrustedDbtToolchain:
        raise NativeSourceCustodyError("expected a trusted toolchain")
    value.__post_init__()
    return encode_native_delivery_json(
        {
            "schema": value.schema,
            "dbt_contract": dict(value.dbt_contract.to_dict()),
            "runtime_identity": _ref(value.runtime_identity),
        }
    )


def decode_trusted_dbt_toolchain(payload: bytes) -> TrustedDbtToolchain:
    """Decode a record, without inferring qualification from version strings."""
    body = _object(decode_native_delivery_json(payload), {"schema", "dbt_contract", "runtime_identity"})
    contract = _object(body["dbt_contract"], {field.name for field in fields(DbtToolchainContract)})
    value = TrustedDbtToolchain(
        cast(Literal["dpone.trusted-dbt-toolchain.v1"], _string(body["schema"])),
        DbtToolchainContract(**{key: _string(item) for key, item in contract.items()}),
        _read_ref(body["runtime_identity"]),
    )
    _canonical(payload, encode_trusted_dbt_toolchain(value))
    return value


def encode_trusted_dbt_qualification(value: TrustedDbtQualification) -> bytes:
    """Encode exact plan/profile/toolchain membership and retained provenance."""
    if type(value) is not TrustedDbtQualification:
        raise NativeSourceCustodyError("expected a trusted qualification")
    value.__post_init__()
    return encode_native_delivery_json(
        {
            "schema": value.schema,
            "policy_id": value.policy_id,
            "profile": _ref(value.profile),
            "toolchain": _ref(value.toolchain),
            "command_plan": _ref(value.command_plan),
            "evidence": [_ref(item) for item in value.evidence],
        }
    )


def decode_trusted_dbt_qualification(payload: bytes) -> TrustedDbtQualification:
    """Reject open fields and aliases; the authorized producer owns qualification."""
    body = _object(
        decode_native_delivery_json(payload),
        {
            "schema",
            "policy_id",
            "profile",
            "toolchain",
            "command_plan",
            "evidence",
        },
    )
    evidence = body["evidence"]
    if type(evidence) is not list:
        raise NativeSourceCustodyError("qualification evidence must be an array")
    value = TrustedDbtQualification(
        cast(Literal["dpone.trusted-dbt-qualification.v1"], _string(body["schema"])),
        _string(body["policy_id"]),
        _read_ref(body["profile"]),
        _read_ref(body["toolchain"]),
        _read_ref(body["command_plan"]),
        tuple(_read_ref(item) for item in evidence),
    )
    _canonical(payload, encode_trusted_dbt_qualification(value))
    return value


def encode_trusted_dbt_owned_root(value: TrustedDbtOwnedRoot) -> bytes:
    """Encode directory identity, never an arbitrary path as an authority grant."""
    if type(value) is not TrustedDbtOwnedRoot:
        raise NativeSourceCustodyError("expected a trusted owned root")
    value.__post_init__()
    return encode_native_delivery_json(
        {
            "schema": value.schema,
            "role": value.role,
            "executor_invocation_id": str(value.executor_invocation_id),
            "root_id": value.root_id,
            "device": value.device,
            "inode": value.inode,
            "content_inventory": None if value.content_inventory is None else _ref(value.content_inventory),
        }
    )


def decode_trusted_dbt_owned_root(payload: bytes) -> TrustedDbtOwnedRoot:
    """Require exact canonical UUID, integer identities and role-dependent fields."""
    body = _object(
        decode_native_delivery_json(payload),
        {
            "schema",
            "role",
            "executor_invocation_id",
            "root_id",
            "device",
            "inode",
            "content_inventory",
        },
    )
    value = TrustedDbtOwnedRoot(
        cast(Literal["dpone.trusted-dbt-owned-root.v1"], _string(body["schema"])),
        cast(Literal["PROJECT", "OUTPUT", "PROFILE"], _string(body["role"])),
        UUID(_string(body["executor_invocation_id"])),
        _string(body["root_id"]),
        _integer(body["device"]),
        _integer(body["inode"]),
        None if body["content_inventory"] is None else _read_ref(body["content_inventory"]),
    )
    _canonical(payload, encode_trusted_dbt_owned_root(value))
    return value


def _canonical(payload: bytes, encoded: bytes) -> None:
    if encoded != payload:
        raise NativeSourceCustodyError("environment bytes must be canonical")
