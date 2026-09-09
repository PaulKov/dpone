"""Connector-neutral native acceleration policy, registry, and evidence."""

from __future__ import annotations

import importlib
import platform
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from time import perf_counter
from typing import Any

NATIVE_ACCELERATION_SCHEMA_VERSION = "dpone.native_transfer.acceleration.v1"
NATIVE_ACCELERATION_BATCH_SCHEMA_VERSION = "dpone.native_transfer.acceleration-batch.v1"
PYTHON_REFERENCE_BACKEND = "python_reference"
NATIVE_ACCELERATED_BACKEND = "native_accelerated"


@dataclass(frozen=True, slots=True)
class NativeAccelerationPolicy:
    """User-facing optional native acceleration policy."""

    mode: str = "auto"

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> NativeAccelerationPolicy:
        return cls(mode=_mode((value or {}).get("mode")))

    def to_dict(self) -> dict[str, str]:
        return {"mode": self.mode}


@dataclass(frozen=True, slots=True)
class NativeAccelerationDecision:
    """Pure decision describing the selected transcode backend."""

    requested_mode: str
    selected_backend: str
    source_format: str
    target_format: str
    backend_id: str | None = None
    accelerator_version: str | None = None
    available: bool = False
    certified: bool = False
    fallback_reason: str | None = None
    warning_codes: tuple[str, ...] = ()
    blocker_codes: tuple[str, ...] = ()

    @property
    def blocked(self) -> bool:
        return bool(self.blocker_codes)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["schema_version"] = NATIVE_ACCELERATION_SCHEMA_VERSION
        payload["warning_codes"] = list(self.warning_codes)
        payload["blocker_codes"] = list(self.blocker_codes)
        return payload


@dataclass(slots=True)
class NativeAccelerationEvidence:
    """Runtime evidence for a native acceleration transcode stream."""

    requested_mode: str
    selected_backend: str
    source_format: str
    target_format: str
    backend_id: str | None = None
    accelerator_version: str | None = None
    available: bool = False
    certified: bool = False
    fallback_reason: str | None = None
    warning_codes: list[str] = field(default_factory=list)
    blocker_codes: list[str] = field(default_factory=list)
    rows: int = 0
    decoded_bytes: int = 0
    encoded_bytes: int = 0
    block_count: int = 0
    duration_seconds: float = 0.0
    rows_per_second: float = 0.0
    failure_code: str | None = None

    @classmethod
    def from_decision(cls, decision: NativeAccelerationDecision) -> NativeAccelerationEvidence:
        return cls(
            requested_mode=decision.requested_mode,
            selected_backend=decision.selected_backend,
            source_format=decision.source_format,
            target_format=decision.target_format,
            backend_id=decision.backend_id,
            accelerator_version=decision.accelerator_version,
            available=decision.available,
            certified=decision.certified,
            fallback_reason=decision.fallback_reason,
            warning_codes=list(decision.warning_codes),
            blocker_codes=list(decision.blocker_codes),
        )

    def finish(self, *, rows: int, decoded_bytes: int, encoded_bytes: int, block_count: int, started_at: float) -> None:
        self.rows = rows
        self.decoded_bytes = decoded_bytes
        self.encoded_bytes = encoded_bytes
        self.block_count = block_count
        self.duration_seconds = max(0.0, perf_counter() - started_at)
        self.rows_per_second = rows / self.duration_seconds if self.duration_seconds else 0.0

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["schema_version"] = NATIVE_ACCELERATION_SCHEMA_VERSION
        return payload


@dataclass(frozen=True, slots=True)
class NativeAccelerationBatch:
    """One provider-authored Native block with its encoder row authority."""

    payload: bytes
    rows: int


class NativeAccelerationRegistry:
    """Resolve optional native acceleration backends without importing connectors."""

    def __init__(self, module_loader: Callable[[], Any] | None = None) -> None:
        self._module_loader = module_loader or _load_optional_module

    def decide(
        self,
        *,
        policy: NativeAccelerationPolicy,
        source_format: str,
        target_format: str,
        source_types: Sequence[str],
    ) -> NativeAccelerationDecision:
        normalized_source = str(source_format)
        normalized_target = str(target_format)
        if policy.mode == "off":
            return _python_decision(
                policy=policy,
                source_format=normalized_source,
                target_format=normalized_target,
                warning="native_acceleration_disabled",
            )
        if not _route_supports_acceleration(normalized_source, normalized_target):
            return self._blocked_or_fallback(
                policy=policy,
                source_format=normalized_source,
                target_format=normalized_target,
                reason="native_acceleration_route_unsupported",
            )

        module = self._load_module()
        if module is None:
            return self._blocked_or_fallback(
                policy=policy,
                source_format=normalized_source,
                target_format=normalized_target,
                reason="native_acceleration_package_missing",
            )
        if not callable(getattr(module, "transcode_batches", None)):
            return self._blocked_or_fallback(
                policy=policy,
                source_format=normalized_source,
                target_format=normalized_target,
                reason="native_acceleration_batch_contract_missing",
            )

        backend = _matching_backend(module, normalized_source, normalized_target, source_types)
        if backend is None:
            return self._blocked_or_fallback(
                policy=policy,
                source_format=normalized_source,
                target_format=normalized_target,
                reason="native_acceleration_backend_unsupported",
            )
        if not bool(backend.get("certified")):
            return self._blocked_or_fallback(
                policy=policy,
                source_format=normalized_source,
                target_format=normalized_target,
                reason="native_acceleration_backend_uncertified",
            )

        revision = backend.get("native_wire_revision")
        if type(revision) is not int or revision != 2:
            return self._blocked_or_fallback(
                policy=policy,
                source_format=normalized_source,
                target_format=normalized_target,
                reason="native_acceleration_profile_revision_unsupported",
            )

        return NativeAccelerationDecision(
            requested_mode=policy.mode,
            selected_backend=NATIVE_ACCELERATED_BACKEND,
            source_format=normalized_source,
            target_format=normalized_target,
            backend_id=str(backend.get("backend_id") or "unknown"),
            accelerator_version=str(getattr(module, "__version__", "unknown")),
            available=True,
            certified=True,
        )

    def transcode(self, *, decision: NativeAccelerationDecision, request: Mapping[str, Any]) -> Iterable[bytes]:
        module = self._load_module()
        if module is None:
            raise RuntimeError("native_acceleration_package_missing")
        transcode = getattr(module, "transcode", None)
        if transcode is None:
            raise RuntimeError("native_acceleration_transcode_missing")
        return (bytes(item) for item in transcode(dict(request)))

    def transcode_batches(
        self, *, decision: NativeAccelerationDecision, request: Mapping[str, Any]
    ) -> Iterable[NativeAccelerationBatch]:
        """Return the additive closed batch contract used for runtime evidence."""

        module = self._load_module()
        if module is None:
            raise RuntimeError("native_acceleration_package_missing")
        transcode_batches = getattr(module, "transcode_batches", None)
        if transcode_batches is None:
            raise RuntimeError("native_acceleration_batch_transcode_missing")
        return (_native_batch(item) for item in transcode_batches(dict(request)))

    def doctor(self) -> dict[str, Any]:
        module = self._load_module()
        decision = self.decide(
            policy=NativeAccelerationPolicy(),
            source_format="mssql-bcp-native",
            target_format="Native",
            source_types=("int",),
        )
        payload = decision.to_dict()
        payload["python_platform"] = platform.platform()
        payload["package"] = "dpone-native-accel"
        payload["importable"] = module is not None
        return payload

    def _load_module(self) -> Any | None:
        try:
            return self._module_loader()
        except Exception:  # noqa: BLE001 - optional accelerator must never break Python fallback.
            return None

    def _blocked_or_fallback(
        self,
        *,
        policy: NativeAccelerationPolicy,
        source_format: str,
        target_format: str,
        reason: str,
    ) -> NativeAccelerationDecision:
        if policy.mode == "required":
            return NativeAccelerationDecision(
                requested_mode=policy.mode,
                selected_backend=PYTHON_REFERENCE_BACKEND,
                source_format=source_format,
                target_format=target_format,
                fallback_reason=reason,
                blocker_codes=("native_acceleration_required_unavailable", reason),
            )
        return _python_decision(policy=policy, source_format=source_format, target_format=target_format, warning=reason)


def _mode(value: Any) -> str:
    normalized = str(value or "auto").strip().lower().replace("-", "_")
    return normalized if normalized in {"auto", "off", "required"} else "auto"


def _native_batch(value: object) -> NativeAccelerationBatch:
    if (
        not isinstance(value, Mapping)
        or frozenset(value) != {"schema_version", "payload", "rows"}
        or value.get("schema_version") != NATIVE_ACCELERATION_BATCH_SCHEMA_VERSION
    ):
        raise RuntimeError("native_acceleration_batch_invalid")
    payload = value.get("payload")
    rows = value.get("rows")
    if (
        not isinstance(payload, bytes | bytearray)
        or not payload
        or not isinstance(rows, int)
        or isinstance(rows, bool)
        or rows <= 0
    ):
        raise RuntimeError("native_acceleration_batch_invalid")
    return NativeAccelerationBatch(payload=bytes(payload), rows=rows)


def _python_decision(
    *,
    policy: NativeAccelerationPolicy,
    source_format: str,
    target_format: str,
    warning: str,
) -> NativeAccelerationDecision:
    return NativeAccelerationDecision(
        requested_mode=policy.mode,
        selected_backend=PYTHON_REFERENCE_BACKEND,
        source_format=source_format,
        target_format=target_format,
        fallback_reason=warning,
        warning_codes=(warning,),
    )


def _route_supports_acceleration(source_format: str, target_format: str) -> bool:
    return source_format == "mssql-bcp-native" and target_format == "Native"


def _matching_backend(
    module: Any,
    source_format: str,
    target_format: str,
    source_types: Sequence[str],
) -> Mapping[str, Any] | None:
    raw_capabilities: Any = getattr(module, "capabilities", lambda: {})()
    capabilities: Mapping[str, Any] = raw_capabilities if isinstance(raw_capabilities, Mapping) else {}
    for backend in capabilities.get("backends", ()) or ():
        if not isinstance(backend, Mapping):
            continue
        if backend.get("source_format") != source_format or backend.get("target_format") != target_format:
            continue
        supported = {str(item).lower() for item in backend.get("supported_types", ()) or ()}
        if supported and not {_root_type(item) for item in source_types}.issubset(supported):
            continue
        return backend
    return None


def _root_type(value: str) -> str:
    normalized = str(value).strip().lower()
    normalized = normalized.replace(" nullable", "")
    return normalized.split("(", 1)[0].strip()


def _load_optional_module() -> Any | None:
    try:
        return importlib.import_module("dpone_native_accel")
    except ImportError:
        return None


__all__ = [
    "NATIVE_ACCELERATION_BATCH_SCHEMA_VERSION",
    "NATIVE_ACCELERATION_SCHEMA_VERSION",
    "NATIVE_ACCELERATED_BACKEND",
    "PYTHON_REFERENCE_BACKEND",
    "NativeAccelerationBatch",
    "NativeAccelerationDecision",
    "NativeAccelerationEvidence",
    "NativeAccelerationPolicy",
    "NativeAccelerationRegistry",
]
