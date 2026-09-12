"""Typed failure carrier and iterative canonical preflight for Attestation V2."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, fields
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, TypeVar
from uuid import UUID

from dpone.contracts.mssql_r1_v3_errors import (
    MssqlR1V3ContractError,
    canonical_bytes,
    decode_canonical_bytes,
    expect_bytes,
    expect_enum,
    expect_int,
    expect_text,
    expect_tuple,
)
from dpone.contracts.mssql_r1_v3_provider_attestation_enums import (
    MssqlR1ProviderAttestationFailureReasonV2,
    MssqlR1ProviderAttestationRecoveryActionV2,
)

CAP_LEAF = 262144
CAP_STATEMENT = 2097152
CAP_QUERY = 3145728
CAP_POST_INSTALL = 4194304
CAP_REGISTRY = 3145728
CAP_STABLE_SCHEMA = 4194304
CAP_STABLE_BINDING = 4194304
CAP_CATALOG = 12582912
MAX_NESTED_DEPTH = 16
MAX_NESTED_TOTAL = 4096
MAX_UNNAMED_SEQUENCE = 64
_FAILURE_DOMAIN = b"dpone-r1-provider-attestation-failure-v2\0"
_FIXED = {ord(b"n"): 1, ord(b"t"): 1, ord(b"f"): 1, ord(b"u"): 17, ord(b"i"): 9}
Reason = MssqlR1ProviderAttestationFailureReasonV2
Recovery = MssqlR1ProviderAttestationRecoveryActionV2
ModelT = TypeVar("ModelT")
_TABLE: dict[Reason, tuple[str, Recovery, str]] = {
    Reason.ATTESTATION_DOMAIN_INVALID: (
        "decode",
        Recovery.REGENERATE_ATTESTATION_INPUT,
        "Attestation bytes do not match the required contract.",
    ),
    Reason.ATTESTATION_BOUNDS_EXCEEDED: (
        "semantic_bounds",
        Recovery.REDUCE_OBSERVATION_SCOPE,
        "Attestation input exceeds the certified bound.",
    ),
    Reason.ATTESTATION_REGISTRY_MISMATCH: (
        "registry",
        Recovery.RECONCILE_RENDERER_REGISTRY,
        "Attestation statement registry does not match the approved provider.",
    ),
    Reason.ATTESTATION_RESULT_ARM_MISMATCH: (
        "registry",
        Recovery.REGENERATE_ATTESTATION_INPUT,
        "Attestation result kind does not match its statement.",
    ),
    Reason.ATTESTATION_PROJECTION_MISMATCH: (
        "projection",
        Recovery.REGENERATE_ATTESTATION_INPUT,
        "Stable attestation projection is inconsistent.",
    ),
    Reason.ATTESTATION_SCHEMA_INVENTORY_MISMATCH: (
        "projection",
        Recovery.REPAIR_OR_REBASELINE_PROVIDER,
        "Observed schema inventory differs from the approved provider.",
    ),
    Reason.ATTESTATION_PERMISSION_CLOSURE_MISMATCH: (
        "authority",
        Recovery.REPAIR_OR_REBASELINE_PROVIDER,
        "Observed permissions differ from the approved provider.",
    ),
    Reason.ATTESTATION_CERTIFICATE_MISMATCH: (
        "authority",
        Recovery.REPAIR_OR_REBASELINE_PROVIDER,
        "Observed certificate identity differs from the approved provider.",
    ),
    Reason.ATTESTATION_BINDING_INVENTORY_MISMATCH: (
        "authority",
        Recovery.REPAIR_OR_REBASELINE_PROVIDER,
        "Observed binding inventory differs from the approved provider.",
    ),
    Reason.ATTESTATION_TARGET_IDENTITY_MISMATCH: (
        "authority",
        Recovery.SELECT_SUPPORTED_TARGET,
        "Target identity differs from the approved target.",
    ),
    Reason.ATTESTATION_AUTHORITY_SPLICE: (
        "authority",
        Recovery.REGENERATE_ATTESTATION_INPUT,
        "Attestation authorities do not belong to one provider generation.",
    ),
}


def attestation_fail(reason: Reason, *, phase: str | None = None) -> None:
    """Raise the closed attestation failure for one exact reason and phase."""

    table_phase, recovery, message = _TABLE[reason]
    resolved = phase if reason is Reason.ATTESTATION_BOUNDS_EXCEEDED and phase is not None else table_phase
    if reason is Reason.ATTESTATION_BOUNDS_EXCEEDED and resolved not in {"raw_bounds", "semantic_bounds"}:
        resolved = "semantic_bounds"
    raise MssqlR1ProviderAttestationError(MssqlR1ProviderAttestationFailureV2(reason, resolved, recovery, message))


@dataclass(frozen=True, slots=True)
class MssqlR1ProviderAttestationFailureV2:
    reason: MssqlR1ProviderAttestationFailureReasonV2
    phase: str
    recovery_action: MssqlR1ProviderAttestationRecoveryActionV2
    redacted_message: str

    def __post_init__(self) -> None:
        if not isinstance(self.reason, Reason) or not isinstance(self.recovery_action, Recovery):
            raise MssqlR1V3ContractError("attestation failure discriminators are inexact")
        table_phase, recovery, message = _TABLE[self.reason]
        allowed = (
            {"raw_bounds", "semantic_bounds"} if self.reason is Reason.ATTESTATION_BOUNDS_EXCEEDED else {table_phase}
        )
        if self.phase not in allowed or self.recovery_action is not recovery or self.redacted_message != message:
            raise MssqlR1V3ContractError("attestation failure is not the closed table row")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _FAILURE_DOMAIN,
            (self.reason, self.phase, self.recovery_action, self.redacted_message),
        )

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ProviderAttestationFailureV2:
        values = list(decode_attestation_model(payload, _FAILURE_DOMAIN, 4, CAP_LEAF))
        values[0] = expect_enum(Reason, values[0], "reason")
        values[2] = expect_enum(Recovery, values[2], "recovery")
        try:
            return cls(
                values[0],  # type: ignore[arg-type]
                expect_text(values[1], "phase"),
                values[2],  # type: ignore[arg-type]
                expect_text(values[3], "message"),
            )
        except MssqlR1ProviderAttestationError:
            raise
        except MssqlR1V3ContractError:
            attestation_fail(Reason.ATTESTATION_DOMAIN_INVALID)
            raise AssertionError("unreachable") from None


class MssqlR1ProviderAttestationError(MssqlR1V3ContractError):
    """Exact raised wrapper whose public surface is the typed failure only."""

    def __init__(self, failure: MssqlR1ProviderAttestationFailureV2) -> None:
        if type(failure) is not MssqlR1ProviderAttestationFailureV2:
            raise MssqlR1V3ContractError("attestation error requires the closed failure carrier")
        self.failure = failure
        super().__init__(failure.redacted_message)

    def __str__(self) -> str:
        return self.failure.redacted_message


def preflight_provider_attestation_canonical_v2(payload: bytes, domain: bytes, field_count: int, cap: int) -> None:
    """Reject illegal V3 frames before the recursive decoder allocates values."""

    if not isinstance(payload, bytes) or len(payload) == 0 or len(payload) > cap:
        attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="raw_bounds")
    if not isinstance(domain, bytes) or not payload.startswith(domain):
        attestation_fail(Reason.ATTESTATION_DOMAIN_INVALID)
    cursor = len(domain)
    seen = 0
    members = 0
    stack: list[tuple[str, int, int, int]] = []
    while cursor < len(payload) or stack:
        if not stack:
            if cursor + 4 > len(payload):
                attestation_fail(Reason.ATTESTATION_DOMAIN_INVALID)
            size = int.from_bytes(payload[cursor : cursor + 4], "big")
            start = cursor + 4
            end = start + size
            if end > len(payload):
                attestation_fail(Reason.ATTESTATION_DOMAIN_INVALID)
            stack.append(("value", start, end, 0))
            cursor = end
            seen += 1
            continue
        kind, start, end, depth = stack.pop()
        if kind == "seq":
            if start >= end:
                continue
            if start + 4 > end:
                attestation_fail(Reason.ATTESTATION_DOMAIN_INVALID)
            child_size = int.from_bytes(payload[start : start + 4], "big")
            child_start = start + 4
            child_end = child_start + child_size
            if child_end > end:
                attestation_fail(Reason.ATTESTATION_DOMAIN_INVALID)
            members += 1
            if members > MAX_NESTED_TOTAL:
                attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
            if child_end < end:
                stack.append(("seq", child_end, end, depth))
            stack.append(("value", child_start, child_end, depth))
            continue
        if start >= end:
            attestation_fail(Reason.ATTESTATION_DOMAIN_INVALID)
        tag = payload[start]
        length = end - start
        expected = _FIXED.get(tag)
        if expected is not None:
            if length != expected:
                attestation_fail(Reason.ATTESTATION_DOMAIN_INVALID)
            continue
        if tag in {ord(b"b"), ord(b"s")}:
            if length < 9:
                attestation_fail(Reason.ATTESTATION_DOMAIN_INVALID)
            size = int.from_bytes(payload[start + 1 : start + 9], "big")
            if start + 9 + size != end:
                attestation_fail(Reason.ATTESTATION_DOMAIN_INVALID)
            continue
        if tag == ord(b"q"):
            if depth >= MAX_NESTED_DEPTH:
                attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
            stack.append(("seq", start + 1, end, depth + 1))
            continue
        attestation_fail(Reason.ATTESTATION_DOMAIN_INVALID)
    if seen != field_count:
        attestation_fail(Reason.ATTESTATION_DOMAIN_INVALID)


def canonical_encoded_size_v1(domain: bytes, field_values: tuple[object, ...]) -> int:
    """Return V3 framed length without materializing the encoded payload."""

    return len(domain) + sum(4 + _value_size(item) for item in field_values)


def _value_size(value: object) -> int:
    if value is None or isinstance(value, bool):
        return 1
    if isinstance(value, Enum):
        return _value_size(value.value)
    if isinstance(value, UUID):
        return 17
    if isinstance(value, bytes):
        return 9 + len(value)
    if isinstance(value, str):
        return 9 + len(value.encode("utf-8"))
    if isinstance(value, int) and not isinstance(value, bool):
        return 9
    if isinstance(value, datetime):
        utc = timezone(timedelta(0))
        return _value_size(value.astimezone(utc).isoformat(timespec="microseconds").replace("+00:00", "Z"))
    if isinstance(value, tuple):
        return 1 + sum(4 + _value_size(item) for item in value)
    raise MssqlR1V3ContractError(f"unsupported canonical V3 value: {type(value).__name__}")


def decode_attestation_model(payload: bytes, domain: bytes, field_count: int, cap: int) -> tuple[object, ...]:
    """Preflight, then decode one exact-domain attestation model payload."""

    preflight_provider_attestation_canonical_v2(payload, domain, field_count, cap)
    try:
        return decode_canonical_bytes(payload, domain, field_count=field_count)
    except MssqlR1ProviderAttestationError:
        raise
    except MssqlR1V3ContractError:
        attestation_fail(Reason.ATTESTATION_DOMAIN_INVALID)
        raise AssertionError("unreachable") from None


def emit_canonical(domain: bytes, field_values: tuple[object, ...], cap: int) -> bytes:
    """Encode after rejecting a prospective size outside the model cap."""

    size = canonical_encoded_size_v1(domain, field_values)
    if size <= 0 or size > cap:
        attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
    payload = canonical_bytes(domain, field_values)
    if len(payload) != size or len(payload) > cap:
        attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
    return payload


def expect_attestation_enum(enum_type: type, value: object, field: str):
    """Decode one closed attestation discriminator without leaking codec errors."""

    try:
        return expect_enum(enum_type, value, field)
    except MssqlR1ProviderAttestationError:
        raise
    except MssqlR1V3ContractError:
        attestation_fail(Reason.ATTESTATION_DOMAIN_INVALID)
        raise AssertionError("unreachable") from None


def map_contract(action: Callable[[], ModelT]) -> ModelT:
    """Map an upstream contract error onto the closed attestation decode failure."""

    try:
        return action()
    except MssqlR1ProviderAttestationError:
        raise
    except MssqlR1V3ContractError:
        attestation_fail(Reason.ATTESTATION_DOMAIN_INVALID)
        raise AssertionError("unreachable") from None


def require_digest32(value: object, field: str) -> bytes:
    if not isinstance(value, bytes) or len(value) != 32:
        attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
    return value  # type: ignore[return-value]


def require_nonzero_uuid(value: object, field: str) -> UUID:
    if not isinstance(value, UUID) or value.int == 0:
        attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
    return value  # type: ignore[return-value]


def require_sql_positive(value: object, field: str) -> int:
    try:
        number = expect_int(value, field)
    except MssqlR1V3ContractError:
        attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        raise AssertionError("unreachable") from None
    if isinstance(value, bool) or not 1 <= number <= 9_223_372_036_854_775_807:
        attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
    return number


def require_bounded_text(value: object, field: str, *, maximum: int = 128) -> str:
    try:
        text = expect_text(value, field)
    except MssqlR1V3ContractError:
        attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        raise AssertionError("unreachable") from None
    encoded = text.encode("utf-8")
    if not 1 <= len(encoded) <= maximum or "\x00" in text:
        attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
    return text


def require_opaque_utf8(value: object, field: str, *, maximum: int) -> bytes:
    raw = expect_bytes(value, field)
    if not raw or len(raw) > maximum or b"\x00" in raw:
        attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
    try:
        raw.decode("utf-8")
    except UnicodeDecodeError:
        attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
    return raw


def require_canonical_unique(values: tuple[object, ...], key: Callable[[Any], object], field: str) -> None:
    keys = tuple(key(item) for item in values)
    if keys != tuple(sorted(keys)) or len(set(keys)) != len(keys):  # type: ignore[type-var]
        attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")


def decode_nested_bytes(payload: object, loader: Callable[[bytes], ModelT], field: str) -> ModelT:
    return map_contract(lambda: loader(expect_bytes(payload, field)))


def decode_nested_tuple(
    payload: object,
    loader: Callable[[bytes], ModelT],
    field: str,
    *,
    maximum: int,
    exact: int | None = None,
    key: Callable[[ModelT], object] | None = None,
) -> tuple[ModelT, ...]:
    try:
        items = expect_tuple(payload, field)
    except MssqlR1V3ContractError:
        attestation_fail(Reason.ATTESTATION_DOMAIN_INVALID)
        raise AssertionError("unreachable") from None
    if exact is not None and len(items) != exact:
        attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
    if len(items) > maximum:
        attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
    decoded = tuple(decode_nested_bytes(item, loader, field) for item in items)
    if key is not None:
        require_canonical_unique(decoded, key, field)
    return decoded


def bound_imported_graph(root: object) -> None:
    """Cap unnamed imported sequences so a new upstream arm cannot grow unbounded."""

    seen: set[int] = set()
    members = 0

    def walk(value: object, depth: int, field: str | None) -> None:
        nonlocal members
        if depth > MAX_NESTED_DEPTH:
            attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
        marker = id(value)
        if marker in seen:
            return
        if hasattr(type(value), "__dataclass_fields__") and not isinstance(value, type):
            seen.add(marker)
            for item in fields(value):  # type: ignore[arg-type]
                walk(getattr(value, item.name), depth + 1, item.name)
            return
        if isinstance(value, tuple | list):
            members += len(value)
            if members > MAX_NESTED_TOTAL:
                attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
            cap = _named_sequence_cap(field)
            if len(value) > cap:
                attestation_fail(Reason.ATTESTATION_BOUNDS_EXCEEDED, phase="semantic_bounds")
            for item in value:
                walk(item, depth + 1, None)

    walk(root, 0, None)


def _named_sequence_cap(field: str | None) -> int:
    # fmt: off
    named = {"ordered_module_signature_digests": 32, "ordered_permission_edge_digests": 64, "ordered_trigger_identities": 16, "ordered_signatures": 16, "ordered_schemas": 8, "ordered_principals": 32, "ordered_role_memberships": 32, "ordered_table_objects": 32, "ordered_core_module_objects": 32, "ordered_binding_modules": 6, "ordered_shared_certificates": 2, "ordered_observed_permission_paths": 256, "ordered_expected_paths": 256, "ordered_observed_paths": 256, "ordered_missing_paths": 256, "ordered_forbidden_paths": 256, "ordered_core_signatures": 32, "ordered_binding_signatures": 6, "ordered_binding_prefix_inventory": 64, "ordered_statement_results": 7, "ordered_statements": 7, "ordered_module_definitions": 6, "ordered_observed_schemas": 8, "ordered_observed_objects": 64, "ordered_observed_principals": 32, "ordered_observed_role_memberships": 32, "ordered_certificate_observations": 2, "ordered_forbidden_membership_observations": 32, "ordered_modules": 6}
    # fmt: on
    return named.get(field or "", MAX_UNNAMED_SEQUENCE)


# fmt: off
__all__ = (
    "CAP_CATALOG", "CAP_LEAF", "CAP_POST_INSTALL", "CAP_QUERY", "CAP_REGISTRY",
    "CAP_STABLE_BINDING", "CAP_STABLE_SCHEMA", "CAP_STATEMENT",
    "MssqlR1ProviderAttestationError", "MssqlR1ProviderAttestationFailureV2",
    "attestation_fail", "bound_imported_graph", "canonical_encoded_size_v1",
    "decode_attestation_model", "decode_nested_bytes", "decode_nested_tuple",
    "emit_canonical", "expect_attestation_enum", "map_contract",
    "preflight_provider_attestation_canonical_v2", "require_bounded_text",
    "require_canonical_unique", "require_digest32", "require_nonzero_uuid",
    "require_opaque_utf8", "require_sql_positive",
)
# fmt: on
