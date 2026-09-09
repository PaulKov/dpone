"""Public integration matrix models and case simulation contracts."""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from typing import Any

from dpone.integration_matrix_constants import (
    BASE_LOAD_STRATEGIES,
    COMMON_PRODUCTION_STRATEGIES,
    DB_TARGET_PRODUCTION_STRATEGIES,
    PARTITION_REPLACE_SINKS,
    SINK_FAMILIES,
    SOURCE_FAMILIES,
    SOURCE_SPECIFIC_STRATEGIES,
    MatrixRow,
)
from dpone.integration_matrix_constants import (
    EXTRAS_BY_FAMILY as _EXTRAS_BY_FAMILY,
)
from dpone.integration_matrix_constants import (
    LIVE_PROFILE_BY_FAMILY as _LIVE_PROFILE_BY_FAMILY,
)
from dpone.integration_matrix_constants import (
    PROFILE_BY_FAMILY as _PROFILE_BY_FAMILY,
)
from dpone.integration_matrix_counts import (
    _merge_unique,
)
from dpone.integration_matrix_manifest import example_manifest_for_case
from dpone.integration_matrix_selection import matrix_case_selected
from dpone.integration_matrix_simulation import simulate_mock_strategy_behavior_for_case


@dataclass(frozen=True, slots=True)
class MatrixStrategyBehaviorResult:
    """Deterministic mock execution result for one matrix strategy case."""

    case_id: str
    source: str
    sink: str
    strategy: str
    target_before: tuple[MatrixRow, ...]
    source_rows: tuple[MatrixRow, ...]
    expected_rows: tuple[MatrixRow, ...]
    actual_rows: tuple[MatrixRow, ...]
    target_before_count: int
    source_row_count: int
    expected_row_count: int
    actual_row_count: int
    expected_checksum: str
    actual_checksum: str
    configured_row_count: int
    change_ratio: float
    delete_ratio: float
    changed_row_count: int
    deleted_row_count: int
    quality_checks: tuple[str, ...]
    passed: bool
    notes: tuple[str, ...]

    @property
    def wide_column_count(self) -> int:
        if not self.source_rows:
            return 0
        return len([key for key in self.source_rows[0] if key.startswith("wide_")])

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, indent=2) + "\n"


@dataclass(frozen=True, slots=True)
class IntegrationMatrixCase:
    """One source -> sink strategy case in the manual certification matrix."""

    source: str
    sink: str
    strategy: str
    guide: str
    install_extras: tuple[str, ...]
    required_profiles: tuple[str, ...]
    live_profiles: tuple[str, ...]
    local_service_supported: bool
    external_credentials_required: bool = False

    @property
    def pair_id(self) -> str:
        return f"{self.source}_to_{self.sink}"

    @property
    def case_id(self) -> str:
        return f"{self.pair_id}__{self.strategy}"

    @property
    def sink_strategy(self) -> str:
        if self.strategy in {"xmin", "cdc"}:
            return "incremental_merge"
        return self.strategy

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["pair_id"] = self.pair_id
        data["case_id"] = self.case_id
        return data

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, indent=2) + "\n"

    def example_manifest(self, *, name_prefix: str = "integration_matrix") -> dict[str, Any]:
        """Build a minimal credential-free manifest fragment for preflight validation."""
        return example_manifest_for_case(self, name_prefix=name_prefix)

    def simulate_mock_strategy_behavior(
        self,
        *,
        row_count: int | None = None,
        change_ratio: float | None = None,
        delete_ratio: float | None = None,
    ) -> MatrixStrategyBehaviorResult:
        """Execute a deterministic in-memory strategy model for certification preflight.

        This does not replace service-specific integration tests. It proves that every
        source/sink/strategy case has an explicit, artifact-producing semantic contract:
        replace means bounded replacement, merge means key-based upsert, append means no
        target mutation, and Kafka sinks produce events rather than mutating a table.
        """
        return simulate_mock_strategy_behavior_for_case(
            self,
            result_type=MatrixStrategyBehaviorResult,
            row_count=row_count,
            change_ratio=change_ratio,
            delete_ratio=delete_ratio,
        )


@dataclass(frozen=True, slots=True)
class IntegrationMatrix:
    """Immutable collection of source -> sink strategy certification cases."""

    cases: tuple[IntegrationMatrixCase, ...]

    @classmethod
    def build_default(cls) -> IntegrationMatrix:
        cases: list[IntegrationMatrixCase] = []
        for source in SOURCE_FAMILIES:
            for sink in SINK_FAMILIES:
                guide = f"source-sink/{source}-to-{sink}.md"
                extras = _merge_unique(_EXTRAS_BY_FAMILY[source], _EXTRAS_BY_FAMILY[sink])
                profiles = _merge_unique((_PROFILE_BY_FAMILY[source],), (_PROFILE_BY_FAMILY[sink],))
                live_profiles = _merge_unique((_LIVE_PROFILE_BY_FAMILY[source],), (_LIVE_PROFILE_BY_FAMILY[sink],))
                local_service_supported = sink != "bigquery"
                strategies = [
                    *BASE_LOAD_STRATEGIES,
                    *COMMON_PRODUCTION_STRATEGIES,
                    *SOURCE_SPECIFIC_STRATEGIES.get(source, ()),
                ]
                if sink in PARTITION_REPLACE_SINKS:
                    strategies.extend(("partition_replace", *DB_TARGET_PRODUCTION_STRATEGIES))
                for strategy in strategies:
                    cases.append(
                        IntegrationMatrixCase(
                            source=source,
                            sink=sink,
                            strategy=strategy,
                            guide=guide,
                            install_extras=extras,
                            required_profiles=profiles,
                            live_profiles=live_profiles,
                            local_service_supported=local_service_supported,
                        )
                    )
        return cls(cases=tuple(cases))

    def for_pair(self, source: str, sink: str) -> tuple[IntegrationMatrixCase, ...]:
        return tuple(case for case in self.cases if case.source == source and case.sink == sink)

    def unique_pairs(self) -> tuple[IntegrationMatrixCase, ...]:
        seen: set[tuple[str, str]] = set()
        result: list[IntegrationMatrixCase] = []
        for case in self.cases:
            key = (case.source, case.sink)
            if key in seen:
                continue
            seen.add(key)
            result.append(case)
        return tuple(result)

    def selected(self, env: Mapping[str, str] | None = None) -> tuple[IntegrationMatrixCase, ...]:
        source = env or os.environ
        return tuple(case for case in self.cases if matrix_case_selected(case, env=source))

    def local_service_cases(self) -> tuple[IntegrationMatrixCase, ...]:
        return tuple(case for case in self.cases if case.local_service_supported)

    def documented_contract_cases(self) -> tuple[IntegrationMatrixCase, ...]:
        return tuple(case for case in self.cases if not case.local_service_supported)

    def to_json(self, cases: Iterable[IntegrationMatrixCase] | None = None) -> str:
        selected = tuple(cases or self.cases)
        return json.dumps([case.to_dict() for case in selected], ensure_ascii=False, sort_keys=True, indent=2) + "\n"


__all__ = [
    "IntegrationMatrix",
    "IntegrationMatrixCase",
    "MatrixStrategyBehaviorResult",
    "matrix_case_selected",
]
