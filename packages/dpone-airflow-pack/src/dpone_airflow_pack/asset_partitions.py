"""Parse-safe Airflow asset partition capability adapter."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import reduce
from operator import and_
from typing import Any

PARTITION_KEY_ENV = "DPONE_PARTITION_KEY"
PARTITION_DIMENSION_ENV = "DPONE_PARTITION_DIMENSION"
PARTITION_MODE_ENV = "DPONE_PARTITION_MODE"
PARTITION_KEY_TEMPLATE = (
    "{{ dag_run.partition_key if dag_run is defined and dag_run "
    "and dag_run.partition_key is defined and dag_run.partition_key else '' }}"
)


@dataclass(frozen=True, slots=True)
class AirflowPartitionCapabilities:
    cron_partition_timetable: type[Any] | None
    partitioned_asset_timetable: type[Any] | None
    identity_mapper: type[Any] | None

    @property
    def native(self) -> bool:
        return all(
            item is not None
            for item in (
                self.cron_partition_timetable,
                self.partitioned_asset_timetable,
                self.identity_mapper,
            )
        )

    @property
    def partial(self) -> bool:
        available = sum(
            item is not None
            for item in (
                self.cron_partition_timetable,
                self.partitioned_asset_timetable,
                self.identity_mapper,
            )
        )
        return 0 < available < 3


@dataclass(frozen=True, slots=True)
class PartitionScheduleMaterialization:
    handled: bool
    value: Any
    mode: str | None


def detect_partition_capabilities() -> AirflowPartitionCapabilities:
    """Load only Airflow's public SDK; absence means an older supported version."""

    try:
        from airflow.sdk import CronPartitionTimetable, IdentityMapper, PartitionedAssetTimetable

        return AirflowPartitionCapabilities(
            cron_partition_timetable=CronPartitionTimetable,
            partitioned_asset_timetable=PartitionedAssetTimetable,
            identity_mapper=IdentityMapper,
        )
    except ImportError:
        return AirflowPartitionCapabilities(None, None, None)


def materialize_partition_schedule(
    *,
    schedule: object,
    partition_plan: object,
    assets: Sequence[Any],
    capabilities: AirflowPartitionCapabilities | None = None,
) -> PartitionScheduleMaterialization:
    """Materialize a native schedule or explicitly select the safe downgrade."""

    if not isinstance(partition_plan, Mapping):
        return PartitionScheduleMaterialization(handled=False, value=None, mode=None)
    selected = capabilities or detect_partition_capabilities()
    if selected.partial:
        raise ValueError(
            "DPONE_AIRFLOW_PARTITION_CAPABILITY_INVALID: Airflow partition SDK is only partially available"
        )
    if not selected.native:
        return PartitionScheduleMaterialization(
            handled=False,
            value=None,
            mode="degraded_unpartitioned",
        )
    mode = str(partition_plan.get("mode") or "").strip()
    if mode == "cron_producer":
        if not isinstance(schedule, str) or not schedule.strip():
            raise ValueError("DPONE_AIRFLOW_PARTITION_CAPABILITY_INVALID: cron_producer requires a cron schedule")
        assert selected.cron_partition_timetable is not None
        value = selected.cron_partition_timetable(
            schedule,
            timezone=_required_text(partition_plan, "timezone"),
            key_format=_required_text(partition_plan, "key_format"),
        )
        return PartitionScheduleMaterialization(handled=True, value=value, mode="native")
    if mode == "asset_consumer":
        if not assets:
            raise ValueError("DPONE_AIRFLOW_PARTITION_CAPABILITY_INVALID: asset_consumer requires at least one asset")
        assert selected.partitioned_asset_timetable is not None
        assert selected.identity_mapper is not None
        condition = reduce(and_, assets) if len(assets) > 1 else assets[0]
        value = selected.partitioned_asset_timetable(
            assets=condition,
            default_partition_mapper=selected.identity_mapper(),
        )
        return PartitionScheduleMaterialization(handled=True, value=value, mode="native")
    raise ValueError(f"DPONE_AIRFLOW_PARTITION_CAPABILITY_INVALID: unsupported partition mode {mode!r}")


def partition_materialization_mode(partition_plan: object) -> str | None:
    if not isinstance(partition_plan, Mapping):
        return None
    capabilities = detect_partition_capabilities()
    if capabilities.partial:
        raise ValueError(
            "DPONE_AIRFLOW_PARTITION_CAPABILITY_INVALID: Airflow partition SDK is only partially available"
        )
    return "native" if capabilities.native else "degraded_unpartitioned"


def partition_env_vars_from_pack(
    pack: Mapping[str, Any],
    *,
    partition_plan: object = None,
    materialization_mode: str = "degraded_unpartitioned",
) -> dict[str, str]:
    """Return one bounded runtime context from the DAG plan and pack metadata."""

    identities = set(_pack_partitions(pack))
    planned = _plan_partition(partition_plan)
    if planned is not None:
        identities.add(planned)
    if not identities:
        return {}
    if len(identities) != 1:
        raise ValueError(
            "DPONE_AIRFLOW_PARTITION_CAPABILITY_INVALID: DAG and workload pack contain mixed partition contracts"
        )
    dimension, _, _, _, _, _ = next(iter(identities))
    if materialization_mode not in {"native", "degraded_unpartitioned"}:
        raise ValueError("DPONE_AIRFLOW_PARTITION_CAPABILITY_INVALID: invalid task partition materialization mode")
    return {
        PARTITION_KEY_ENV: PARTITION_KEY_TEMPLATE,
        PARTITION_DIMENSION_ENV: dimension,
        PARTITION_MODE_ENV: materialization_mode,
    }


def _pack_partitions(pack: Mapping[str, Any]) -> tuple[tuple[str, str, str, str, str, str], ...]:
    airflow = pack.get("airflow")
    execution = airflow.get("execution") if isinstance(airflow, Mapping) else None
    parsed: list[tuple[str, str, str, str, str, str]] = []
    for field in ("inlets", "outlets"):
        assets = execution.get(field) if isinstance(execution, Mapping) else None
        if not isinstance(assets, Sequence) or isinstance(assets, str | bytes):
            continue
        for asset in assets:
            if not isinstance(asset, Mapping) or "partition" not in asset:
                continue
            parsed.append(_nested_partition(asset["partition"]))
    return tuple(parsed)


def _nested_partition(raw_partition: object) -> tuple[str, str, str, str, str, str]:
    dimensions = raw_partition.get("dimensions") if isinstance(raw_partition, Mapping) else None
    if not isinstance(dimensions, Mapping) or len(dimensions) != 1:
        raise ValueError("DPONE_AIRFLOW_PARTITION_CAPABILITY_INVALID: pack partition requires exactly one dimension")
    dimension, raw = next(iter(dimensions.items()))
    if not isinstance(raw, Mapping):
        raise ValueError("DPONE_AIRFLOW_PARTITION_CAPABILITY_INVALID: pack partition dimension is invalid")
    return (
        _identity_text(dimension, "dimension"),
        _identity_text(raw.get("type") or "temporal", "type"),
        _identity_text(raw.get("granularity"), "granularity"),
        _identity_text(raw.get("timezone") or "UTC", "timezone"),
        _identity_text(raw.get("key_format"), "key_format"),
        _identity_text(raw.get("source") or "dag_schedule", "source"),
    )


def _plan_partition(raw_plan: object) -> tuple[str, str, str, str, str, str] | None:
    if raw_plan is None:
        return None
    if not isinstance(raw_plan, Mapping):
        raise ValueError("DPONE_AIRFLOW_PARTITION_CAPABILITY_INVALID: partition_plan must be a mapping")
    return (
        _identity_text(raw_plan.get("dimension"), "dimension"),
        _identity_text(raw_plan.get("type") or "temporal", "type"),
        _identity_text(raw_plan.get("granularity"), "granularity"),
        _identity_text(raw_plan.get("timezone") or "UTC", "timezone"),
        _identity_text(raw_plan.get("key_format"), "key_format"),
        _identity_text(raw_plan.get("source") or "dag_schedule", "source"),
    )


def _identity_text(raw: object, field: str) -> str:
    value = str(raw or "").strip()
    if not value or len(value) > 128 or "\x00" in value:
        raise ValueError(f"DPONE_AIRFLOW_PARTITION_CAPABILITY_INVALID: partition {field} is invalid")
    return value


def _required_text(payload: Mapping[str, Any], field: str) -> str:
    value = str(payload.get(field) or "").strip()
    if not value:
        raise ValueError(f"DPONE_AIRFLOW_PARTITION_CAPABILITY_INVALID: partition_plan.{field} is required")
    return value


__all__ = [
    "AirflowPartitionCapabilities",
    "PARTITION_DIMENSION_ENV",
    "PARTITION_KEY_ENV",
    "PARTITION_KEY_TEMPLATE",
    "PARTITION_MODE_ENV",
    "PartitionScheduleMaterialization",
    "detect_partition_capabilities",
    "materialize_partition_schedule",
    "partition_env_vars_from_pack",
    "partition_materialization_mode",
]
