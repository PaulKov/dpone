"""Direct sink-native ingest protocol contracts.

The core package only defines the runtime contract. Optional providers, such as
``dpone-native-accel``, own protocol-specific implementations and certification.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any

from dpone.runtime.clickhouse_bulk_options_models import ClickHouseBulkOptions

DIRECT_INGEST_SCHEMA_VERSION = "dpone.native_transfer.direct_ingest.v1"


@dataclass(frozen=True, slots=True)
class DirectIngestRouteRequest:
    """Pure direct-ingest route request."""

    bulk_options: ClickHouseBulkOptions
    input_format: str
    route_certified: bool = False
    certification_mode: str = "certified_only"


@dataclass(frozen=True, slots=True)
class DirectIngestDecision:
    """Decision for the native TCP implementation backend."""

    requested_backend: str
    selected_backend: str | None
    input_format: str
    certified: bool = False
    provider_available: bool = False
    fallback_reason: str | None = None
    release_gate: str = "green"
    warnings: tuple[str, ...] = ()
    blockers: tuple[str, ...] = ()

    def to_evidence(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["schema_version"] = DIRECT_INGEST_SCHEMA_VERSION
        payload["warnings"] = list(self.warnings)
        payload["blockers"] = list(self.blockers)
        return payload


@dataclass(frozen=True, slots=True)
class ClickHouseDirectNativeCredentials:
    """Credentials for a direct ClickHouse Native protocol provider."""

    host: str
    port: int
    database: str
    user: str
    password: str | None = None
    secure: bool = False

    def to_provider_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_redacted_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        if payload.get("password"):
            payload["password"] = "***"
        return payload


@dataclass(frozen=True, slots=True)
class ClickHouseDirectNativeOptions:
    """Provider-neutral ClickHouse direct Native ingest options."""

    compression: str = "lz4"
    timeout_seconds: int = 3600
    query_id: str | None = None
    settings: Mapping[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "compression": self.compression,
            "timeout_seconds": self.timeout_seconds,
            "query_id": self.query_id,
            "settings": dict(self.settings or {}),
        }


@dataclass(frozen=True, slots=True)
class ClickHouseDirectNativeResult:
    """Runtime result returned by a direct Native protocol provider."""

    rows: int = 0
    query_id: str | None = None
    compressed_bytes: int | None = None
    backend: str = "direct"
    compression: str = "lz4"
    provider_version: str | None = None

    def to_evidence(self) -> dict[str, Any]:
        return {
            "schema_version": DIRECT_INGEST_SCHEMA_VERSION,
            "selected_backend": self.backend,
            "compression": self.compression,
            "rows": self.rows,
            "query_id": self.query_id,
            "compressed_bytes": self.compressed_bytes,
            "provider_version": self.provider_version,
        }


class DirectIngestResolver:
    """Resolve direct protocol eligibility without touching connectors."""

    def __init__(self, provider_loader: Callable[[], Any] | None = None) -> None:
        self._provider_loader = provider_loader or _load_optional_provider

    def decide(self, request: DirectIngestRouteRequest) -> DirectIngestDecision:
        requested = _backend(request.bulk_options.native_tcp.backend)
        if requested == "client":
            return _decision(requested=requested, selected="client", input_format=request.input_format)
        provider = self._provider()
        backend = _matching_backend(provider, request.input_format)
        if requested == "direct":
            return _forced_direct_decision(request, provider, backend)
        if (
            backend is not None
            and bool(backend.get("certified"))
            and _route_is_accepted(request)
            and _has_insert_backend(provider)
        ):
            warnings = _uncertified_route_warnings(request)
            return _decision(
                requested=requested,
                selected="direct",
                input_format=request.input_format,
                certified=request.route_certified,
                provider_available=True,
                warnings=warnings,
            )
        reason = _fallback_reason(
            provider,
            backend,
            route_certified=request.route_certified,
            certification_mode=request.certification_mode,
        )
        return _decision(
            requested=requested,
            selected="client",
            input_format=request.input_format,
            provider_available=provider is not None,
            fallback_reason=reason,
            warnings=(reason,),
        )

    def _provider(self) -> Any | None:
        try:
            return self._provider_loader()
        except Exception:  # noqa: BLE001 - direct provider must fail closed into client fallback.
            return None


class ClickHouseDirectNativeRunner:
    """Thin provider runner for direct ClickHouse Native ingest."""

    def __init__(
        self,
        *,
        credentials: ClickHouseDirectNativeCredentials,
        options: ClickHouseDirectNativeOptions,
        provider_loader: Callable[[], Any] | None = None,
    ) -> None:
        self._credentials = credentials
        self._options = options
        self._provider_loader = provider_loader or _load_optional_provider

    def insert_stream(
        self, table: str, columns: Sequence[str], chunks: Iterable[bytes]
    ) -> ClickHouseDirectNativeResult:
        provider = self._provider_loader()
        if provider is None or not hasattr(provider, "insert_clickhouse_native"):
            raise RuntimeError("direct_ingest_provider_missing")
        raw = provider.insert_clickhouse_native(
            {
                "table": table,
                "columns": list(columns),
                "byte_stream": chunks,
                "credentials": self._credentials.to_provider_dict(),
                "options": self._options.to_dict(),
            }
        )
        payload = raw if isinstance(raw, Mapping) else {}
        return ClickHouseDirectNativeResult(
            rows=int(payload.get("rows") or 0),
            query_id=str(payload["query_id"]) if payload.get("query_id") is not None else self._options.query_id,
            compressed_bytes=int(payload["compressed_bytes"]) if payload.get("compressed_bytes") is not None else None,
            compression=self._options.compression,
            provider_version=str(getattr(provider, "__version__", "unknown")),
        )


def _forced_direct_decision(
    request: DirectIngestRouteRequest,
    provider: Any | None,
    backend: Mapping[str, Any] | None,
) -> DirectIngestDecision:
    blockers: list[str] = []
    if provider is None:
        blockers.append("direct_ingest_provider_missing")
    elif not _has_insert_backend(provider):
        blockers.append("direct_ingest_provider_insert_missing")
    elif backend is None:
        blockers.append("direct_ingest_backend_unsupported")
    elif not bool(backend.get("certified")):
        blockers.append("direct_ingest_backend_uncertified")
    if not _route_is_accepted(request):
        blockers.append("direct_ingest_route_certification_missing")
    if blockers:
        return _decision(
            requested="direct",
            selected=None,
            input_format=request.input_format,
            provider_available=provider is not None,
            fallback_reason=blockers[0],
            blockers=("direct_ingest_required_unavailable", *tuple(blockers)),
        )
    return _decision(
        requested="direct",
        selected="direct",
        input_format=request.input_format,
        certified=request.route_certified,
        provider_available=True,
        warnings=_uncertified_route_warnings(request),
    )


def _decision(
    *,
    requested: str,
    selected: str | None,
    input_format: str,
    certified: bool = False,
    provider_available: bool = False,
    fallback_reason: str | None = None,
    warnings: Sequence[str] = (),
    blockers: Sequence[str] = (),
) -> DirectIngestDecision:
    return DirectIngestDecision(
        requested_backend=requested,
        selected_backend=selected,
        input_format=input_format,
        certified=certified,
        provider_available=provider_available,
        fallback_reason=fallback_reason,
        release_gate="blocked" if blockers else "warning" if warnings else "green",
        warnings=tuple(warnings),
        blockers=tuple(blockers),
    )


def _matching_backend(provider: Any | None, input_format: str) -> Mapping[str, Any] | None:
    if provider is None or not hasattr(provider, "direct_ingest_capabilities"):
        return None
    raw = provider.direct_ingest_capabilities()
    capabilities: Mapping[str, Any] = raw if isinstance(raw, Mapping) else {}
    for backend in capabilities.get("backends", ()) or ():
        if not isinstance(backend, Mapping):
            continue
        if str(backend.get("input_format") or "").lower() == input_format.lower():
            return backend
    return None


def _fallback_reason(
    provider: Any | None,
    backend: Mapping[str, Any] | None,
    *,
    route_certified: bool,
    certification_mode: str = "certified_only",
) -> str:
    if provider is None:
        return "direct_ingest_provider_missing"
    if backend is None:
        return "direct_ingest_backend_unsupported"
    if not _has_insert_backend(provider):
        return "direct_ingest_provider_insert_missing"
    if not bool(backend.get("certified")):
        return "direct_ingest_backend_uncertified"
    if not route_certified and not _allows_uncertified_route(certification_mode):
        return "direct_ingest_route_certification_missing"
    return "direct_ingest_unknown_fallback"


def _backend(value: Any) -> str:
    normalized = str(value or "auto").strip().lower().replace("-", "_")
    return normalized if normalized in {"auto", "direct", "client"} else "auto"


def _has_insert_backend(provider: Any | None) -> bool:
    return bool(provider is not None and callable(getattr(provider, "insert_clickhouse_native", None)))


def _route_is_accepted(request: DirectIngestRouteRequest) -> bool:
    return request.route_certified or _allows_uncertified_route(request.certification_mode)


def _allows_uncertified_route(certification_mode: str) -> bool:
    return str(certification_mode or "").strip().lower() == "advisory"


def _uncertified_route_warnings(request: DirectIngestRouteRequest) -> tuple[str, ...]:
    if request.route_certified:
        return tuple()
    if _allows_uncertified_route(request.certification_mode):
        return ("direct_ingest_route_uncertified_advisory",)
    return tuple()


def _load_optional_provider() -> Any | None:
    try:
        return importlib.import_module("dpone_native_accel")
    except ImportError:
        return None


__all__ = [
    "DIRECT_INGEST_SCHEMA_VERSION",
    "ClickHouseDirectNativeCredentials",
    "ClickHouseDirectNativeOptions",
    "ClickHouseDirectNativeResult",
    "ClickHouseDirectNativeRunner",
    "DirectIngestDecision",
    "DirectIngestResolver",
    "DirectIngestRouteRequest",
]
