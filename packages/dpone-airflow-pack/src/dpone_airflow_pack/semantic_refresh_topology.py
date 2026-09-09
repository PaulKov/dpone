"""Authenticated deployment-neutral topology for semantic-refresh Airflow DAGs."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_FIELDS = {
    "activation",
    "dag_policy",
    "dag_id",
    "dependencies",
    "logical_output_asset_uris",
    "model_unique_ids",
    "profile_sha256",
    "project_config_overlay",
    "schema",
    "topology_sha256",
    "workflow_name",
}
_DAG_POLICY_FIELDS = {
    "catchup",
    "max_active_runs",
    "max_active_tasks",
    "owner",
    "schedule",
    "start_date",
    "tags",
    "timezone",
}


class SemanticRefreshTopologyError(ValueError):
    """Raised when the protected topology document is not exact and canonical."""


@dataclass(frozen=True, slots=True)
class SemanticRefreshDagPolicy:
    """Closed static scheduler policy protected by the topology digest."""

    schedule: str | None
    start_date: str
    timezone: str
    catchup: bool
    max_active_runs: int
    max_active_tasks: int
    owner: str
    tags: tuple[str, ...]

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshDagPolicy:
        if not isinstance(value, Mapping) or set(value) != _DAG_POLICY_FIELDS:
            raise SemanticRefreshTopologyError("semantic-refresh DAG policy fields are not closed")
        schedule = value["schedule"]
        if schedule is not None and (not isinstance(schedule, str) or not schedule.strip()):
            raise SemanticRefreshTopologyError("semantic-refresh DAG schedule must be null or non-empty text")
        start_date = _iso_date(value["start_date"])
        timezone = _text(value["timezone"], "timezone")
        try:
            ZoneInfo(timezone)
        except ZoneInfoNotFoundError as exc:
            raise SemanticRefreshTopologyError("semantic-refresh DAG timezone is unknown") from exc
        catchup = value["catchup"]
        if not isinstance(catchup, bool):
            raise SemanticRefreshTopologyError("semantic-refresh DAG catchup must be boolean")
        max_active_runs = _positive_int(value["max_active_runs"], "max_active_runs")
        max_active_tasks = _positive_int(value["max_active_tasks"], "max_active_tasks")
        owner = _text(value["owner"], "owner")
        tags = _text_sequence(value["tags"], "tags")
        if tags != tuple(sorted(set(tags))):
            raise SemanticRefreshTopologyError("semantic-refresh DAG tags must be sorted and unique")
        return cls(
            schedule=schedule,
            start_date=start_date,
            timezone=timezone,
            catchup=catchup,
            max_active_runs=max_active_runs,
            max_active_tasks=max_active_tasks,
            owner=owner,
            tags=tags,
        )

    def to_mapping(self) -> dict[str, object]:
        return {
            "catchup": self.catchup,
            "max_active_runs": self.max_active_runs,
            "max_active_tasks": self.max_active_tasks,
            "owner": self.owner,
            "schedule": self.schedule,
            "start_date": self.start_date,
            "tags": list(self.tags),
            "timezone": self.timezone,
        }


@dataclass(frozen=True)
class SemanticRefreshTopologyTemplate:
    """Closed topology authenticated before operation identities are projected."""

    workflow_name: str
    dag_id: str
    dag_policy: SemanticRefreshDagPolicy
    model_unique_ids: tuple[str, ...]
    dependencies: Mapping[str, tuple[str, ...]]
    logical_output_asset_uris: Mapping[str, str]
    profile_sha256: str
    project_config_overlay: Mapping[str, object]
    topology_sha256: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> SemanticRefreshTopologyTemplate:
        """Authenticate the exact release template bytes represented by a mapping."""

        if not isinstance(value, Mapping) or set(value) != _FIELDS:
            raise SemanticRefreshTopologyError("semantic-refresh topology fields are not closed")
        if value["schema"] != "dpone.dbt-semantic-refresh-topology-template.v1":
            raise SemanticRefreshTopologyError("semantic-refresh topology schema is invalid")
        if value["activation"] != "POST_DEPLOYMENT_AUTHORITY_REQUIRED":
            raise SemanticRefreshTopologyError("semantic-refresh topology activation is invalid")
        topology_sha256 = _digest(value["topology_sha256"], "topology_sha256")
        unsigned = {key: raw for key, raw in value.items() if key != "topology_sha256"}
        if _canonical_digest(unsigned) != topology_sha256:
            raise SemanticRefreshTopologyError("semantic-refresh topology digest differs")
        workflow_name = _text(value["workflow_name"], "workflow_name")
        dag_id = _text(value["dag_id"], "dag_id")
        dag_policy = SemanticRefreshDagPolicy.from_mapping(value["dag_policy"])
        profile_sha256 = _digest(value["profile_sha256"], "profile_sha256")
        models = _text_sequence(value["model_unique_ids"], "model_unique_ids")
        if len(models) != len(set(models)):
            raise SemanticRefreshTopologyError("semantic-refresh topology model closure is not unique")
        dependencies = _dependency_mapping(value["dependencies"], set(models))
        expected_order = topological_model_order(dependencies)
        if models != expected_order:
            raise SemanticRefreshTopologyError("semantic-refresh topology order is not canonical")
        outputs = _output_mapping(value["logical_output_asset_uris"], set(models))
        overlay = value["project_config_overlay"]
        if not isinstance(overlay, Mapping):
            raise SemanticRefreshTopologyError("semantic-refresh project overlay is invalid")
        return cls(
            workflow_name=workflow_name,
            dag_id=dag_id,
            dag_policy=dag_policy,
            model_unique_ids=models,
            dependencies=dependencies,
            logical_output_asset_uris=outputs,
            profile_sha256=profile_sha256,
            project_config_overlay=dict(overlay),
            topology_sha256=topology_sha256,
        )


def topological_model_order(dependencies: Mapping[str, tuple[str, ...]]) -> tuple[str, ...]:
    """Return deterministic topological order with model-ID tie-breaking."""

    remaining = {model_id: set(parents) for model_id, parents in dependencies.items()}
    ordered: list[str] = []
    while remaining:
        ready = sorted(model_id for model_id, parents in remaining.items() if not parents)
        if not ready:
            raise SemanticRefreshTopologyError("semantic-refresh topology contains a cycle")
        for model_id in ready:
            ordered.append(model_id)
            del remaining[model_id]
        for parents in remaining.values():
            parents.difference_update(ready)
    return tuple(ordered)


def _dependency_mapping(value: object, models: set[str]) -> dict[str, tuple[str, ...]]:
    if not isinstance(value, Mapping) or set(value) != models:
        raise SemanticRefreshTopologyError("dependency graph has missing or extra model nodes")
    result: dict[str, tuple[str, ...]] = {}
    for model_id in models:
        parents = _text_sequence(value[model_id], "dependencies")
        if (
            len(parents) != len(set(parents))
            or any(parent not in models or parent == model_id for parent in parents)
            or parents != tuple(sorted(parents))
        ):
            raise SemanticRefreshTopologyError("semantic-refresh model dependency closure is invalid")
        result[model_id] = parents
    return result


def _output_mapping(value: object, models: set[str]) -> dict[str, str]:
    if not isinstance(value, Mapping) or set(value) != models:
        raise SemanticRefreshTopologyError("logical output closure has missing or extra model nodes")
    result = {model_id: _text(value[model_id], "logical_output_asset_uri") for model_id in models}
    if len(set(result.values())) != len(result):
        raise SemanticRefreshTopologyError("logical output Asset URIs must be unique")
    return result


def _text_sequence(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes) or not value:
        if field == "dependencies" and isinstance(value, Sequence) and not isinstance(value, str | bytes):
            return ()
        raise SemanticRefreshTopologyError(f"semantic-refresh {field} must be a non-empty array")
    return tuple(_text(item, field) for item in value)


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SemanticRefreshTopologyError(f"semantic-refresh {field} must be non-empty text")
    return value


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise SemanticRefreshTopologyError(f"semantic-refresh DAG {field} must be positive")
    return value


def _iso_date(value: object) -> str:
    if not isinstance(value, str):
        raise SemanticRefreshTopologyError("semantic-refresh DAG start_date must be an ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise SemanticRefreshTopologyError("semantic-refresh DAG start_date must be an ISO date") from exc
    if parsed.isoformat() != value:
        raise SemanticRefreshTopologyError("semantic-refresh DAG start_date must be canonical")
    return value


def _digest(value: object, field: str) -> str:
    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None:
        raise SemanticRefreshTopologyError(f"semantic-refresh {field} must be a canonical digest")
    return value


def _canonical_digest(value: Mapping[str, object]) -> str:
    try:
        raw = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    except (TypeError, ValueError) as exc:
        raise SemanticRefreshTopologyError("semantic-refresh topology is not canonical JSON") from exc
    return "sha256:" + hashlib.sha256(raw).hexdigest()


__all__ = [
    "SemanticRefreshTopologyError",
    "SemanticRefreshDagPolicy",
    "SemanticRefreshTopologyTemplate",
    "topological_model_order",
]
