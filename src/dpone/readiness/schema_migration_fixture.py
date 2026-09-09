"""Provider-neutral fixture planning and build evidence for migration rehearsal."""

from __future__ import annotations

import csv
import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from dpone.readiness.migration_control import MigrationPack, stable_fingerprint
from dpone.readiness.schema_migration_row_hash import typed_rows_hash

FIXTURE_PLAN_SCHEMA = "dpone.schema_migration_fixture_plan.v1"
FIXTURE_BUILD_SCHEMA = "dpone.schema_migration_fixture_build.v1"
_MODES = frozenset({"none", "synthetic", "artifact_sample", "masked_sample"})


class TargetFixtureSeeder(Protocol):
    """DI port for writing fixture rows into a sandbox target."""

    def seed(self, *, target: Mapping[str, Any], columns: Sequence[str], rows: Sequence[Mapping[str, Any]]) -> None: ...


@dataclass(frozen=True, slots=True)
class MigrationFixturePlanner:
    """Builds pack-bound fixture requirements from manifest and policy evidence."""

    def plan(
        self,
        *,
        pack: Mapping[str, Any],
        manifest: Mapping[str, Any] | None = None,
        policy: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        migration_pack = MigrationPack.from_mapping(dict(pack))
        data_fixture = _fixture_options(manifest or {}, policy or {})
        quality_profile = _quality_options(manifest or {}, policy or {})
        columns = _columns(manifest or {}, migration_pack)
        key_columns = _key_columns(columns, migration_pack)
        hierarchy = _hierarchy(columns)
        blockers = list(_plan_blockers(migration_pack, data_fixture))
        warnings = list(_plan_warnings(data_fixture, columns))
        payload: dict[str, Any] = {
            "schema_version": FIXTURE_PLAN_SCHEMA,
            "status": "blocked" if blockers else "planned",
            "pack_id": migration_pack.pack_id,
            "target": migration_pack.target.to_dict(),
            "mode": str(data_fixture.get("mode", "none")),
            "artifact_path": data_fixture.get("artifact_path"),
            "requirements": _requirements(data_fixture),
            "masking": _masking_options(data_fixture),
            "columns": [{"name": item["name"], "type": item.get("type", "String")} for item in columns],
            "key_columns": list(key_columns),
            "hierarchy": hierarchy,
            "checks": _checks(quality_profile),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
        }
        payload["fixture_plan_id"] = stable_fingerprint(
            {
                "pack_id": payload["pack_id"],
                "target": payload["target"],
                "mode": payload["mode"],
                "artifact_path": payload["artifact_path"],
                "requirements": payload["requirements"],
                "masking": payload["masking"],
                "columns": payload["columns"],
                "key_columns": payload["key_columns"],
                "hierarchy": payload["hierarchy"],
                "checks": payload["checks"],
                "blockers": payload["blockers"],
            }
        )
        return payload


@dataclass(frozen=True, slots=True)
class SyntheticMigrationFixtureProvider:
    """Deterministic synthetic fixture provider for wide schemas and edge cases."""

    def rows(self, plan: Mapping[str, Any]) -> list[dict[str, Any]]:
        requirements = _mapping(plan.get("requirements"))
        row_count = max(0, int(requirements.get("min_rows", 0)))
        columns = _plan_columns(plan, int(requirements.get("min_columns", 1)))
        seed = str(requirements.get("seed", "dpone-rehearsal-v1"))
        hierarchy = _mapping(plan.get("hierarchy"))
        return [
            _synthetic_row(index=index, columns=columns, seed=seed, hierarchy=hierarchy) for index in range(row_count)
        ]


@dataclass(frozen=True, slots=True)
class DataMaskingPolicy:
    """Deterministic masking rules for local artifact samples."""

    def apply(self, rows: Sequence[Mapping[str, Any]], masking: Mapping[str, Any]) -> list[dict[str, Any]]:
        columns = _mapping(masking.get("columns"))
        if not columns:
            return [dict(row) for row in rows]
        salt = os.getenv(str(masking.get("salt_env", "")))
        if not salt and any(str(action) in {"hash", "token"} for action in columns.values()):
            return []
        return [_masked_row(row, columns=columns, salt=salt or "") for row in rows]


@dataclass(frozen=True, slots=True)
class ArtifactSampleFixtureProvider:
    """Reads JSONL/CSV fixture rows and applies optional masking."""

    masking: DataMaskingPolicy = DataMaskingPolicy()

    def rows(self, plan: Mapping[str, Any]) -> list[dict[str, Any]]:
        artifact_path = str(plan.get("artifact_path") or "")
        if not artifact_path:
            return []
        rows = _read_rows(Path(artifact_path))
        if str(plan.get("mode")) == "masked_sample":
            return self.masking.apply(rows, _mapping(plan.get("masking")))
        return rows


@dataclass(frozen=True, slots=True)
class MigrationFixtureBuilder:
    """Builds fixture evidence and optionally delegates target seeding."""

    def build(
        self,
        *,
        plan: Mapping[str, Any],
        rows: Sequence[Mapping[str, Any]],
        execute: bool,
        seeder: TargetFixtureSeeder | None = None,
    ) -> dict[str, Any]:
        blockers = list(_build_blockers(plan, rows, execute, seeder))
        if execute and not blockers and seeder is not None:
            try:
                seeder.seed(target=_mapping(plan.get("target")), columns=_column_names(plan), rows=rows)
            except Exception as exc:  # pragma: no cover - target adapter surface
                blockers.append(f"schema_migration_fixture.seed_failed:{exc}")
        metrics = {
            "row_count": len(rows),
            "column_count": len(_column_names(plan)),
            "sample_hash": typed_rows_hash(rows, _strings(plan.get("key_columns", []))) if rows else None,
        }
        status = "blocked" if blockers else "built" if execute else "dry_run"
        payload: dict[str, Any] = {
            "schema_version": FIXTURE_BUILD_SCHEMA,
            "status": status,
            "pack_id": plan.get("pack_id"),
            "fixture_plan_id": plan.get("fixture_plan_id"),
            "target": dict(_mapping(plan.get("target"))),
            "mode": plan.get("mode"),
            "columns": list(_column_names(plan)),
            "key_columns": _strings(plan.get("key_columns", [])),
            "hierarchy": dict(_mapping(plan.get("hierarchy"))),
            "checks": dict(_mapping(plan.get("checks"))),
            "metrics": metrics,
            "rows": [dict(row) for row in rows] if len(rows) <= 100 else [],
            "rows_artifact": "embedded" if len(rows) <= 100 else "not_embedded",
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": [] if execute else ["schema_migration_fixture.dry_run"],
        }
        payload["fixture_build_id"] = stable_fingerprint(
            {
                "pack_id": payload["pack_id"],
                "fixture_plan_id": payload["fixture_plan_id"],
                "status": status,
                "metrics": metrics,
                "blockers": payload["blockers"],
            }
        )
        return payload


def _fixture_options(manifest: Mapping[str, Any], policy: Mapping[str, Any]) -> dict[str, Any]:
    raw = _rehearsal_options(manifest).get("data_fixture", {})
    merged = dict(raw) if isinstance(raw, Mapping) else {}
    override = policy.get("data_fixture", policy)
    if isinstance(override, Mapping):
        merged.update(override)
    merged.setdefault("mode", "none")
    merged.setdefault("min_rows", 0)
    merged.setdefault("max_rows", 100_000)
    merged.setdefault("min_columns", 1)
    merged.setdefault("include_edge_cases", True)
    merged.setdefault("preserve_hierarchy", True)
    merged.setdefault("seed", "dpone-rehearsal-v1")
    return merged


def _quality_options(manifest: Mapping[str, Any], policy: Mapping[str, Any]) -> dict[str, Any]:
    raw = _rehearsal_options(manifest).get("quality_profile", {})
    merged = dict(raw) if isinstance(raw, Mapping) else {}
    override = policy.get("quality_profile", {})
    if isinstance(override, Mapping):
        merged.update(override)
    return merged


def _rehearsal_options(manifest: Mapping[str, Any]) -> dict[str, Any]:
    node = manifest.get("sink", {})
    for key in ("options", "physical_design", "migration", "rehearsal"):
        node = node.get(key, {}) if isinstance(node, Mapping) else {}
    return dict(node) if isinstance(node, Mapping) else {}


def _columns(manifest: Mapping[str, Any], pack: MigrationPack) -> list[dict[str, Any]]:
    raw = _mapping(manifest.get("schema")).get("columns", [])
    columns = [dict(item) for item in raw if isinstance(item, Mapping)] if isinstance(raw, list) else []
    if not columns:
        desired = pack.desired.get("columns", [])
        columns = [dict(item) for item in desired if isinstance(item, Mapping)] if isinstance(desired, list) else []
    if not columns:
        columns = [{"name": "id", "type": "Int64", "key": True}]
    return [{"name": str(item.get("name")), "type": str(item.get("type", "String")), **item} for item in columns]


def _key_columns(columns: Sequence[Mapping[str, Any]], pack: MigrationPack) -> tuple[str, ...]:
    keys = tuple(str(item["name"]) for item in columns if bool(item.get("key")))
    if keys:
        return keys
    order_by = pack.desired.get("order_by", [])
    return tuple(str(item) for item in order_by if isinstance(item, str))


def _hierarchy(columns: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    for item in columns:
        parent = str(item.get("parent", "")).strip()
        if parent:
            return {"parent_column": str(item["name"]), "child_column": parent}
    names = {str(item.get("name")) for item in columns}
    if {"id", "parent_id"}.issubset(names):
        return {"parent_column": "parent_id", "child_column": "id"}
    return {}


def _requirements(raw: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "min_rows": int(raw.get("min_rows", 0)),
        "max_rows": int(raw.get("max_rows", 100_000)),
        "min_columns": int(raw.get("min_columns", 1)),
        "include_edge_cases": bool(raw.get("include_edge_cases", True)),
        "preserve_hierarchy": bool(raw.get("preserve_hierarchy", True)),
        "seed": str(raw.get("seed", "dpone-rehearsal-v1")),
    }


def _masking_options(raw: Mapping[str, Any]) -> dict[str, Any]:
    masking = raw.get("masking", {})
    return dict(masking) if isinstance(masking, Mapping) else {}


def _checks(raw: Mapping[str, Any]) -> dict[str, bool]:
    keys = (
        "row_count",
        "typed_hash",
        "null_distribution",
        "distinct_count",
        "min_max",
        "duplicate_key",
        "null_key",
        "nested_parent_child",
    )
    return {key: bool(raw.get(key, True)) for key in keys}


def _plan_blockers(pack: MigrationPack, options: Mapping[str, Any]) -> tuple[str, ...]:
    blockers = list(pack.blockers)
    mode = str(options.get("mode", "none"))
    if mode not in _MODES:
        blockers.append(f"schema_migration_fixture.unsupported_provider:{mode}")
    if mode in {"artifact_sample", "masked_sample"} and not str(options.get("artifact_path", "")).strip():
        blockers.append("schema_migration_fixture.artifact_path_required")
    return tuple(blockers)


def _plan_warnings(options: Mapping[str, Any], columns: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    warnings: list[str] = []
    if str(options.get("mode", "none")) == "none":
        warnings.append("schema_migration_fixture.disabled")
    if len(columns) < int(options.get("min_columns", 1)):
        warnings.append("schema_migration_fixture.column_threshold_requires_synthetic_expansion")
    return tuple(warnings)


def _plan_columns(plan: Mapping[str, Any], min_columns: int) -> list[dict[str, str]]:
    columns = [dict(item) for item in plan.get("columns", []) if isinstance(item, Mapping)]
    while len(columns) < min_columns:
        columns.append({"name": f"extra_{len(columns) + 1}", "type": "String"})
    return [{"name": str(item.get("name")), "type": str(item.get("type", "String"))} for item in columns]


def _synthetic_row(
    *,
    index: int,
    columns: Sequence[Mapping[str, str]],
    seed: str,
    hierarchy: Mapping[str, Any],
) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for column in columns:
        name = str(column.get("name"))
        column_type = str(column.get("type", "String"))
        row[name] = _synthetic_value(index=index, name=name, column_type=column_type, seed=seed)
    parent_column = str(hierarchy.get("parent_column", ""))
    child_column = str(hierarchy.get("child_column", ""))
    if parent_column and child_column and parent_column in row and child_column in row:
        row[parent_column] = None if index < 3 else 1
    return row


def _synthetic_value(*, index: int, name: str, column_type: str, seed: str) -> Any:
    lowered = column_type.lower()
    if name == "id" or "int" in lowered:
        return index + 1
    if "decimal" in lowered or "float" in lowered or "double" in lowered:
        return "-1.00" if index == 1 else f"{index}.{index % 100:02d}"
    if "bool" in lowered:
        return index % 2 == 0
    if "date" in lowered or "time" in lowered:
        return f"2026-06-{(index % 28) + 1:02d}T12:00:00"
    if "json" in lowered or "map" in lowered or "object" in lowered:
        return {"seed": seed, "index": index}
    if "nullable" in lowered and index % 7 == 0:
        return None
    if index % 11 == 0:
        return ""
    return f"{name}_{_short_hash(seed, name, index)}"


def _read_rows(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".csv":
        with path.open(newline="", encoding="utf-8") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    rows: list[dict[str, Any]] = []
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        raw = json.loads(line)
        if not isinstance(raw, Mapping):
            raise ValueError(f"{path}:{line_no} must contain a JSON object")
        rows.append(dict(raw))
    return rows


def _masked_row(row: Mapping[str, Any], *, columns: Mapping[str, Any], salt: str) -> dict[str, Any]:
    masked = dict(row)
    for column, action in columns.items():
        name = str(column)
        if str(action) == "null":
            masked[name] = None
        elif str(action) == "hash":
            masked[name] = "sha256:" + hashlib.sha256(f"{salt}:{masked.get(name)}".encode()).hexdigest()
        elif str(action) == "token":
            masked[name] = "tok_" + _short_hash(salt, name, masked.get(name))
    return masked


def _build_blockers(
    plan: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    execute: bool,
    seeder: TargetFixtureSeeder | None,
) -> tuple[str, ...]:
    requirements = _mapping(plan.get("requirements"))
    blockers = list(_strings(plan.get("blockers", [])))
    if len(rows) < int(requirements.get("min_rows", 0)):
        blockers.append("schema_migration_fixture.insufficient_rows")
    if len(_column_names(plan)) < int(requirements.get("min_columns", 1)):
        blockers.append("schema_migration_fixture.insufficient_columns")
    if execute and seeder is None:
        blockers.append("schema_migration_fixture.target_seeder_required")
    return tuple(blockers)


def _column_names(plan: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(str(item.get("name")) for item in plan.get("columns", []) if isinstance(item, Mapping))


def _mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


def _strings(raw: object) -> list[str]:
    return [str(item) for item in raw if str(item)] if isinstance(raw, list | tuple) else []


def _short_hash(*parts: object) -> str:
    return hashlib.sha256(":".join(map(str, parts)).encode("utf-8")).hexdigest()[:12]
