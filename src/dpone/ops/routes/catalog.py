"""Route profile catalog built from existing matrix and strategy metadata."""

from __future__ import annotations

from collections.abc import Mapping

from dpone.integration_matrix import DEFAULT_INTEGRATION_MATRIX, IntegrationMatrix, IntegrationMatrixCase
from dpone.ops.routes.models import RouteKey, RouteProfile
from dpone.strategy_intelligence.certification import StrategyCertificationMatrixService
from dpone.strategy_intelligence.models import StrategyCertificationEntry

_BASE_REQUIRED_EVIDENCE: tuple[str, ...] = (
    "matrix_case",
    "manifest_example",
    "strategy_plan",
    "quality_reconciliation",
    "run_artifact",
    "route_execution_ledger",
    "state_promotion",
    "docs_runbook",
)

_ROUTE_REQUIRED_EVIDENCE: Mapping[str, tuple[str, ...]] = {
    "postgres_to_mssql": (
        "route_schema_evolution",
        "route_reconciliation_repair",
        "lossless_transport_contract",
        "benchmark_slo",
        "resume_checkpoint",
        "type_matrix",
    ),
    "mssql_to_clickhouse": (
        "route_schema_evolution",
        "route_reconciliation_repair",
        "type_fidelity",
        "typed_hash",
        "wide_type_certification",
        "benchmark_slo",
        "schema_evolution",
    ),
    "clickhouse_to_mssql": (
        "route_schema_evolution",
        "route_reconciliation_repair",
        "type_matrix",
        "bcp_bulk_readiness",
        "source_boundary_profile",
        "benchmark_slo",
        "schema_evolution",
    ),
}

_DEFAULT_SLO_HINTS: Mapping[str, Mapping[str, object]] = {
    "postgres_to_mssql": {"rows_per_second_min": 80000, "phase": "target_load_finalize"},
    "mssql_to_clickhouse": {"rows_per_second_min": 30000, "phase": "source_export"},
    "clickhouse_to_mssql": {"rows_per_second_min": 15000, "phase": "target_load_finalize"},
}


class RouteProfileCatalog:
    """Lookup profiles for any source -> sink -> strategy matrix case."""

    def __init__(
        self,
        *,
        matrix: IntegrationMatrix,
        strategy_entries: Mapping[tuple[str, str, str], StrategyCertificationEntry],
    ) -> None:
        self._matrix = matrix
        self._strategy_entries = strategy_entries
        self._cases = {RouteKey.of(case.source, case.sink, case.strategy): case for case in matrix.cases}

    @classmethod
    def default(cls) -> RouteProfileCatalog:
        strategy_matrix = StrategyCertificationMatrixService().build()
        entries = {
            (entry.source_type, entry.sink_type, entry.strategy_mode): entry for entry in strategy_matrix.entries
        }
        return cls(matrix=DEFAULT_INTEGRATION_MATRIX, strategy_entries=entries)

    def get(self, key: RouteKey) -> RouteProfile | None:
        case = self._cases.get(key)
        if case is None:
            return None
        strategy_entry = self._strategy_entry(key)
        return self._profile(case, strategy_entry)

    def profiles(self) -> tuple[RouteProfile, ...]:
        return tuple(
            profile
            for key in sorted(self._cases, key=lambda item: item.case_id)
            if (profile := self.get(key)) is not None
        )

    def _strategy_entry(self, key: RouteKey) -> StrategyCertificationEntry:
        strategy_key = (key.source, key.sink, _strategy_certification_mode(key.strategy))
        if strategy_key in self._strategy_entries:
            return self._strategy_entries[strategy_key]
        return StrategyCertificationEntry(
            source_type=key.source,
            sink_type=key.sink,
            strategy_mode=key.strategy,
            status="not_supported",
            native_fast_path="streaming_rows",
            required_evidence=(),
        )

    @staticmethod
    def _profile(case: IntegrationMatrixCase, strategy_entry: StrategyCertificationEntry) -> RouteProfile:
        key = RouteKey.of(case.source, case.sink, case.strategy)
        return RouteProfile(
            key=key,
            docs_link=f"docs/{case.guide}",
            install_extras=tuple(case.install_extras),
            required_profiles=tuple(case.required_profiles),
            live_profiles=tuple(case.live_profiles),
            local_service_supported=case.local_service_supported,
            external_credentials_required=case.external_credentials_required,
            certification_status=strategy_entry.status,
            native_fast_path=strategy_entry.native_fast_path,
            required_evidence=_required_evidence(key, strategy_entry),
            default_slo_hints=dict(_DEFAULT_SLO_HINTS.get(key.pair_id, {})),
        )


def route_key_from_mapping(values: Mapping[str, object]) -> RouteKey:
    """Build a route key from a route-like mapping."""

    return RouteKey.of(str(values.get("source", "")), str(values.get("sink", "")), str(values.get("strategy", "")))


def _strategy_certification_mode(strategy: str) -> str:
    if strategy == "cdc":
        return "cdc_apply"
    return strategy


def _required_evidence(key: RouteKey, strategy_entry: StrategyCertificationEntry) -> tuple[str, ...]:
    values = [
        *_BASE_REQUIRED_EVIDENCE,
        *strategy_entry.required_evidence,
        *_ROUTE_REQUIRED_EVIDENCE.get(key.pair_id, ()),
    ]
    return tuple(dict.fromkeys(item for item in values if item))
