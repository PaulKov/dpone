"""Route bootstrap manifest and next-command planning."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.ops.routes.bootstrap_models import RouteBootstrapReport
from dpone.ops.routes.bootstrap_policy import next_actions_for_blockers, status_from_blockers
from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.ops.routes.models import RouteKey


class RouteBootstrapService:
    """Generate a route manifest draft and onboarding command plan."""

    def __init__(self, *, catalog: RouteProfileCatalog | None = None) -> None:
        self._catalog = catalog or RouteProfileCatalog.default()

    def bootstrap(
        self,
        *,
        output_dir: str | Path,
        source: str,
        sink: str,
        strategy: str,
        dataset: str,
        source_discovery_json: str | Path | None = None,
        manifest_id: str | None = None,
    ) -> RouteBootstrapReport:
        directory = Path(output_dir)
        route = RouteKey.of(source, sink, strategy)
        profile = self._catalog.get(route)
        discovery = _read_discovery(source_discovery_json)
        blockers = _blockers(route=route, profile_exists=profile is not None, discovery=discovery)
        warnings = _warnings(discovery)
        status = status_from_blockers(blockers, warnings)
        manifest = _manifest(
            route=route,
            dataset=dataset,
            manifest_id=manifest_id or _manifest_id(dataset=dataset, route=route),
            discovery=discovery,
        )
        manifest_path = directory / "route_manifest.json"
        directory.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        report = RouteBootstrapReport(
            route=route,
            profile=profile,
            dataset=dataset,
            passed=status != "blocked",
            status=status,
            score=0.0 if blockers else 100.0,
            manifest=manifest,
            manifest_path=str(manifest_path),
            type_risks=_type_risks(discovery),
            next_commands=_next_commands(route=route, dataset=dataset, output_dir=directory),
            blockers=blockers,
            warnings=warnings,
            next_actions=next_actions_for_blockers(
                blockers,
                ready_action="Review route_manifest.json, then run route-readiness.",
            ),
            output_dir=str(directory),
            json_path=str(directory / "route_bootstrap.json"),
            markdown_path=str(directory / "route_bootstrap.md"),
        )
        report.write()
        return report


def _read_discovery(path: str | Path | None) -> Mapping[str, Any]:
    if path is None:
        return {}
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"blockers": ["source_discovery.missing"]}
    except json.JSONDecodeError:
        return {"blockers": ["source_discovery.invalid_json"]}
    return payload if isinstance(payload, Mapping) else {"blockers": ["source_discovery.invalid_shape"]}


def _blockers(*, route: RouteKey, profile_exists: bool, discovery: Mapping[str, Any]) -> tuple[str, ...]:
    blockers: list[str] = []
    if not profile_exists:
        blockers.append(f"route.unsupported:{route.colon_id}")
    blockers.extend(str(item) for item in discovery.get("blockers", ()) if str(item)) if isinstance(
        discovery.get("blockers"), list | tuple
    ) else None
    if discovery and discovery.get("passed") is False:
        blockers.append("source_discovery.not_passed")
    return tuple(dict.fromkeys(blockers))


def _warnings(discovery: Mapping[str, Any]) -> tuple[str, ...]:
    warnings = discovery.get("warnings")
    return tuple(str(item) for item in warnings if str(item)) if isinstance(warnings, list | tuple) else tuple()


def _manifest(*, route: RouteKey, dataset: str, manifest_id: str, discovery: Mapping[str, Any]) -> dict[str, object]:
    columns = _columns(discovery)
    return {
        "version": 1,
        "id": manifest_id,
        "generated_by": "dpone route-bootstrap",
        "processes": [
            {
                "id": manifest_id,
                "route": route.to_dict(),
                "strategy": route.strategy,
                "source": {
                    "type": route.source,
                    "dataset": dataset,
                    "columns": columns,
                },
                "sink": {
                    "type": route.sink,
                    "dataset": dataset,
                },
                "load": {
                    "mode": route.strategy,
                    "state_promotion": "after_sink_commit",
                    "artifacts": "enabled",
                },
            }
        ],
    }


def _columns(discovery: Mapping[str, Any]) -> list[dict[str, object]]:
    tables = discovery.get("tables")
    if not isinstance(tables, list) or not tables:
        return []
    first = tables[0]
    if not isinstance(first, Mapping):
        return []
    columns = first.get("columns")
    if not isinstance(columns, list):
        return []
    return [
        {
            "name": str(column.get("name", "")),
            "type": str(column.get("type", "")),
            "nullable": bool(column.get("nullable", True)),
        }
        for column in columns
        if isinstance(column, Mapping) and column.get("name")
    ]


def _type_risks(discovery: Mapping[str, Any]) -> dict[str, object]:
    warning_columns: list[str] = []
    blocked_columns: list[str] = []
    for table in discovery.get("tables", []) if isinstance(discovery.get("tables"), list) else []:
        if not isinstance(table, Mapping):
            continue
        for column in table.get("columns", []) if isinstance(table.get("columns"), list) else []:
            if not isinstance(column, Mapping):
                continue
            name = str(column.get("name", ""))
            risk = str(column.get("risk_level", "ready"))
            if risk == "blocked":
                blocked_columns.append(name)
            elif risk == "warning":
                warning_columns.append(name)
    return {
        "warning_columns": warning_columns,
        "blocked_columns": blocked_columns,
    }


def _next_commands(*, route: RouteKey, dataset: str, output_dir: Path) -> tuple[str, ...]:
    route_args = f"--source {route.source} --sink {route.sink} --strategy {route.strategy}"
    return (
        f"uv run dpone ops connection-doctor {route_args} --output-dir {output_dir / 'connection-doctor'}",
        f"uv run dpone ops source-discover --source {route.source} --dataset {dataset} "
        f"--schema-json <schema.json> --output-dir {output_dir / 'source-discover'}",
        f"uv run dpone ops route-readiness {route_args} --output-dir {output_dir / 'route-readiness'}",
    )


def _manifest_id(*, dataset: str, route: RouteKey) -> str:
    dataset_id = dataset.replace(".", "_").replace("-", "_").strip("_") or "route"
    return f"{dataset_id}_{route.source}_to_{route.sink}"
