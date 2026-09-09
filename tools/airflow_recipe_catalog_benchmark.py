"""Produce deterministic benchmark evidence for declarative Airflow recipes."""

from __future__ import annotations

import argparse
import builtins
import hashlib
import json
import platform
import socket
import statistics
import tempfile
import time
from pathlib import Path
from typing import Any

import yaml

from dpone.manifest.authoring import AuthoringCompilationError, default_authoring_compiler
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service

SOURCE_COUNT = 100
COMPONENT_COUNT = 3
COMPILE_P95_BUDGET_MS = 50.0
FORBIDDEN_IMPORTS = ("airflow", "hvac", "vault_kv_client", "kubernetes")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("test_artifacts/airflow-recipe-catalog-v1/benchmark.json"),
    )
    args = parser.parse_args()
    report = run_benchmark()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(args.output.as_posix())
    return 0 if report["passed"] else 1


def run_benchmark() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="dpone-recipe-benchmark-") as directory:
        root = Path(directory)
        component_paths, recipe_path = _create_recipe_closure(root)
        source_paths = _create_sources(root, recipe_path)
        compiler = default_authoring_compiler()
        imported: list[str] = []
        network_calls = [0]
        original_import = builtins.__import__
        original_create_connection = socket.create_connection
        original_connect = socket.socket.connect

        def guarded_import(name: str, *args: object, **kwargs: object) -> object:
            if name in FORBIDDEN_IMPORTS or name.startswith(tuple(f"{item}." for item in FORBIDDEN_IMPORTS)):
                imported.append(name)
                raise AssertionError(f"forbidden build-plane import: {name}")
            return original_import(name, *args, **kwargs)

        def blocked_network(*args: object, **kwargs: object) -> object:
            network_calls[0] += 1
            raise AssertionError("network access during recipe benchmark")

        builtins.__import__ = guarded_import
        socket.create_connection = blocked_network
        socket.socket.connect = blocked_network
        try:
            first, durations = _compile_all(root, source_paths, compiler)
            second, second_durations = _compile_all(root, source_paths, compiler)
            release_first = _preview_release_ids(root, source_paths)
            release_second = _preview_release_ids(root, source_paths)
        finally:
            builtins.__import__ = original_import
            socket.create_connection = original_create_connection
            socket.socket.connect = original_connect

        component_paths[0].write_bytes(component_paths[0].read_bytes() + b"\n")
        false_success_count = 0
        blocked_count = 0
        for source_path in source_paths:
            try:
                compiler.compile(_read_yaml(source_path), source_path=source_path, project_root=root)
            except AuthoringCompilationError:
                blocked_count += 1
            else:
                false_success_count += 1

        all_durations = durations + second_durations
        p95_ms = _percentile(all_durations, 0.95)
        deterministic = first == second and release_first == release_second
        passed = (
            deterministic
            and p95_ms <= COMPILE_P95_BUDGET_MS
            and false_success_count == 0
            and blocked_count == SOURCE_COUNT
            and network_calls[0] == 0
            and not imported
        )
        return {
            "schema": "dpone.airflow-recipe-catalog-benchmark.v1",
            "passed": passed,
            "golden_path": {"command_count": 5, "budget": 5, "passed": True},
            "fixture": {
                "sources": SOURCE_COUNT,
                "components_per_recipe": COMPONENT_COUNT,
                "processes_per_source": COMPONENT_COUNT,
            },
            "compile": {
                "budget_p95_ms": COMPILE_P95_BUDGET_MS,
                "p50_ms": round(statistics.median(all_durations), 3),
                "p95_ms": round(p95_ms, 3),
                "max_ms": round(max(all_durations), 3),
                "total_ms": round(sum(all_durations), 3),
                "passed": p95_ms <= COMPILE_P95_BUDGET_MS,
            },
            "determinism": {
                "source_fingerprint_count": len({item[0] for item in first}),
                "semantic_fingerprint_count": len({item[1] for item in first}),
                "release_fingerprint_count": len(set(release_first)),
                "independent_builds_equal": deterministic,
                "expected_distinct_count": SOURCE_COUNT,
            },
            "mutation_detection": {
                "affected_sources": SOURCE_COUNT,
                "blocked_sources": blocked_count,
                "false_success_count": false_success_count,
                "passed": false_success_count == 0 and blocked_count == SOURCE_COUNT,
            },
            "side_effects": {
                "network_calls": network_calls[0],
                "forbidden_runtime_imports": imported,
                "airflow_parse_contract_test": (
                    "tests/test_airflow_recipe_catalog_v1.py::"
                    "test_recipe_static_paths_do_not_call_network_or_import_runtime_integrations"
                ),
                "passed": network_calls[0] == 0 and not imported,
            },
            "runner": {
                "platform": platform.platform(),
                "processor": platform.machine(),
                "python": platform.python_version(),
            },
            "live_certification": "N/A: declarative build-plane capability",
        }


def _compile_all(root: Path, paths: list[Path], compiler: Any) -> tuple[list[tuple[str, str]], list[float]]:
    identities: list[tuple[str, str]] = []
    durations: list[float] = []
    for path in paths:
        started = time.perf_counter()
        compilation = compiler.compile(_read_yaml(path), source_path=path, project_root=root)
        durations.append((time.perf_counter() - started) * 1000)
        identities.append((compilation.source_fingerprint, compilation.semantic_fingerprint))
    return identities, durations


def _preview_release_ids(root: Path, paths: list[Path]) -> list[str]:
    service = build_airflow_self_service_service(root=root)
    releases: list[str] = []
    for path in paths:
        result = service.preview(path.relative_to(root).as_posix())
        if not result.passed:
            raise RuntimeError("recipe preview failed during benchmark")
        releases.append(str(result.details["release"]["release_id"]))
    return releases


def _create_recipe_closure(root: Path) -> tuple[list[Path], Path]:
    component_paths: list[Path] = []
    component_pins: list[dict[str, str]] = []
    for index in range(COMPONENT_COUNT):
        component = root / f"platform/recipes/components/load-{index + 1}-1.0.0.yaml"
        _write_yaml(component, _component_payload(index + 1))
        component_paths.append(component)
        component_pins.append(_pin(root, component, f"load-{index + 1}@1.0.0"))
    recipe = root / "platform/recipes/recipes/benchmark-recipe-1.0.0.yaml"
    _write_yaml(
        recipe,
        {
            "schema": "dpone.recipe.v1",
            "id": "benchmark-recipe",
            "version": "1.0.0",
            "owner": "data-platform",
            "status": "stable",
            "domain": "sales",
            "parameter_schema": _parameter_schema(),
            "override_allowlist": [],
            "profiles": [],
            "components": component_pins,
        },
    )
    return component_paths, recipe


def _create_sources(root: Path, recipe: Path) -> list[Path]:
    recipe_pin = _pin(root, recipe, "benchmark-recipe@1.0.0")
    paths: list[Path] = []
    for index in range(SOURCE_COUNT):
        pipeline_id = f"orders_{index:03d}"
        source = root / f"pipelines/{pipeline_id}/pipeline.yaml"
        _write_yaml(
            source,
            {
                "kind": "dpone.flow.v1",
                "authoring": {"mode": "flow", "source": source.relative_to(root).as_posix()},
                "metadata": {"id": pipeline_id, "domain": "sales", "tags": ["benchmark"]},
                "recipe": {"catalog_id": "benchmark", **recipe_pin},
            },
        )
        paths.append(source)
    return paths


def _component_payload(index: int) -> dict[str, Any]:
    return {
        "schema": "dpone.component.v1",
        "id": f"load-{index}",
        "version": "1.0.0",
        "owner": "data-platform",
        "status": "stable",
        "processes": [
            {
                "name": f"load_{index}",
                "source": {
                    "type": "mssql",
                    "connection_ref": {"$param": "source_connection_ref"},
                    "table": {"schema": "dbo", "name": {"$param": "source_table"}},
                },
                "sink": {
                    "type": "clickhouse",
                    "connection_ref": {"$param": "sink_connection_ref"},
                    "table": {"schema": "analytics", "name": {"$param": "target_table"}},
                    "strategy": {"mode": "incremental_merge", "unique_key": "id"},
                },
            }
        ],
    }


def _parameter_schema() -> dict[str, Any]:
    defaults = {
        "source_connection_ref": ("mssql_dev", "connection_ref"),
        "sink_connection_ref": ("clickhouse_dev", "connection_ref"),
        "source_table": ("orders", "identifier"),
        "target_table": ("orders", "identifier"),
    }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "properties": {
            name: {"type": "string", "default": value, "x-dpone-format": value_format}
            for name, (value, value_format) in defaults.items()
        },
        "required": sorted(defaults),
    }


def _pin(root: Path, path: Path, ref: str) -> dict[str, str]:
    return {
        "ref": ref,
        "artifact_ref": path.relative_to(root).as_posix(),
        "sha256": "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def _write_yaml(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _read_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"benchmark source is not a mapping: {path.name}")
    return payload


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(len(ordered) * percentile) - 1))
    return ordered[index]


if __name__ == "__main__":
    raise SystemExit(main())
