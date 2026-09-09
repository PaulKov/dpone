"""CDC handoff profile catalog built from route matrix metadata."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from dpone.ops.cdc.models import CdcHandoffProfile
from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.ops.routes.models import RouteKey

_REQUIRED_CDC_EVIDENCE: tuple[str, ...] = (
    "cdc_snapshot_boundary",
    "cdc_window",
    "retention_preflight",
    "cdc_apply_correctness",
    "delete_semantics",
    "typed_cdc_hash",
    "schema_drift_governance",
)


@dataclass(frozen=True, slots=True)
class CdcRouteMetadata:
    """Route-specific CDC metadata kept outside services and policies."""

    source_backend: str
    sink_apply_mode: str
    snapshot_boundary_kind: str
    offset_kind: str
    native_apply_path: str
    required_evidence: tuple[str, ...] = _REQUIRED_CDC_EVIDENCE
    default_slo_hints: Mapping[str, object] | None = None


_CDC_ROUTES: Mapping[str, CdcRouteMetadata] = {
    "mssql_to_clickhouse": CdcRouteMetadata(
        source_backend="mssql_cdc",
        sink_apply_mode="clickhouse_replacing_merge_tree",
        snapshot_boundary_kind="mssql_lsn",
        offset_kind="mssql_lsn",
        native_apply_path="mssql_cdc_to_clickhouse_typed_staging_apply",
        default_slo_hints={
            "apply_lag_seconds_max": 300,
            "duplicate_events_max": 0,
            "delete_semantics": "physical_or_replacing_tombstone",
        },
    ),
}


class CdcHandoffCatalog:
    """Lookup CDC handoff profiles for matrix-backed source -> sink CDC routes."""

    def __init__(
        self,
        *,
        route_catalog: RouteProfileCatalog,
        route_metadata: Mapping[str, CdcRouteMetadata] = _CDC_ROUTES,
    ) -> None:
        self._route_catalog = route_catalog
        self._route_metadata = route_metadata

    @classmethod
    def default(cls) -> CdcHandoffCatalog:
        return cls(route_catalog=RouteProfileCatalog.default())

    def get(self, route: RouteKey) -> CdcHandoffProfile | None:
        if route.strategy != "cdc":
            return None
        route_profile = self._route_catalog.get(route)
        metadata = self._route_metadata.get(route.pair_id)
        if route_profile is None or metadata is None:
            return None
        return CdcHandoffProfile(
            route=route,
            docs_link=route_profile.docs_link,
            source_backend=metadata.source_backend,
            sink_apply_mode=metadata.sink_apply_mode,
            snapshot_boundary_kind=metadata.snapshot_boundary_kind,
            offset_kind=metadata.offset_kind,
            native_apply_path=metadata.native_apply_path,
            required_evidence=tuple(dict.fromkeys(metadata.required_evidence)),
            default_slo_hints=dict(metadata.default_slo_hints or {}),
        )

    def profiles(self) -> tuple[CdcHandoffProfile, ...]:
        return tuple(
            profile
            for route_profile in self._route_catalog.profiles()
            if (profile := self.get(route_profile.key)) is not None
        )
