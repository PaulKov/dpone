"""Produce deterministic Phase 4B signed-catalog benchmark evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import statistics
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

import yaml

from dpone._compat import UTC
from dpone.contracts.blob_signature import BlobSignatureVerification
from dpone.contracts.catalog_bundle import (
    CatalogBundleBuildRequest,
    CatalogBundleVerifyRequest,
    sha256_bytes,
)
from dpone.services.catalog_bundle_builder import CatalogBundleBuilder
from dpone.services.catalog_bundle_verification import CatalogBundleVerificationService

CATALOG_ENTRIES = 1000
VERIFICATION_REPETITIONS = 20
MUTATION_COUNT = 20
VERIFICATION_P95_BUDGET_MS = 2000.0


class _LocalIntegrityVerifier:
    """Exclude external cosign startup while retaining the verifier port."""

    def verify_blob(self, **kwargs: object) -> BlobSignatureVerification:
        del kwargs
        return BlobSignatureVerification.verified("benchmark-port")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("test_artifacts/signed-catalog-extension-conformance-v1/benchmark.json"),
    )
    args = parser.parse_args()
    report = run_benchmark()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(args.output.as_posix())
    return 0 if report["passed"] else 1


def run_benchmark(
    *,
    catalog_entries: int = CATALOG_ENTRIES,
    verification_repetitions: int = VERIFICATION_REPETITIONS,
    mutation_count: int = MUTATION_COUNT,
) -> dict[str, Any]:
    if not 2 <= catalog_entries <= CATALOG_ENTRIES:
        raise ValueError("catalog_entries must be between 2 and 1000")
    if verification_repetitions <= 0 or mutation_count <= 0:
        raise ValueError("benchmark repetitions must be positive")
    mutation_count = min(mutation_count, catalog_entries - 1)
    network_calls = [0]
    subprocess_calls = [0]

    def block_network(*args: object, **kwargs: object) -> object:
        del args, kwargs
        network_calls[0] += 1
        raise AssertionError("network access during signed catalog benchmark")

    def block_subprocess(*args: object, **kwargs: object) -> object:
        del args, kwargs
        subprocess_calls[0] += 1
        raise AssertionError("subprocess access during local integrity benchmark")

    with tempfile.TemporaryDirectory(prefix="dpone-signed-catalog-benchmark-") as directory:
        root = Path(directory)
        first_root, second_root = root / "first", root / "second"
        _create_catalog(first_root, catalog_entries)
        _create_catalog(second_root, catalog_entries)
        with (
            patch("socket.create_connection", block_network),
            patch("socket.socket.connect", block_network),
            patch("subprocess.run", block_subprocess),
        ):
            first, first_ms = _timed_build(first_root)
            second, second_ms = _timed_build(second_root)
            no_op = CatalogBundleBuilder().build(_build_request(first_root))
            verification_ms = _verification_runs(first_root, first.bundle_dir, verification_repetitions)
            detected = _mutation_runs(first_root, first.bundle_dir, mutation_count)

        p95_ms = _percentile(verification_ms, 0.95)
        deterministic = first.bundle_id == second.bundle_id and no_op.status == "no_op"
        side_effect_free = network_calls[0] == 0 and subprocess_calls[0] == 0
        passed = (
            deterministic and detected == mutation_count and p95_ms <= VERIFICATION_P95_BUDGET_MS and side_effect_free
        )
        return {
            "schema": "dpone.signed-catalog-extension-benchmark.v1",
            "passed": passed,
            "fixture": {
                "catalog_entries": catalog_entries,
                "recipe_entries": catalog_entries - 1,
                "shared_components": 1,
                "payload_files": first.artifacts,
            },
            "build": {
                "first_bundle_id": first.bundle_id,
                "second_bundle_id": second.bundle_id,
                "independent_builds_equal": deterministic,
                "idempotent_rebuild_status": no_op.status,
                "first_ms": round(first_ms, 3),
                "second_ms": round(second_ms, 3),
            },
            "verification": {
                "repetitions": verification_repetitions,
                "budget_p95_ms": VERIFICATION_P95_BUDGET_MS,
                "p50_ms": round(statistics.median(verification_ms), 3),
                "p95_ms": round(p95_ms, 3),
                "max_ms": round(max(verification_ms), 3),
                "passed": p95_ms <= VERIFICATION_P95_BUDGET_MS,
                "cosign_startup_excluded": True,
            },
            "mutation_detection": {
                "substitutions": mutation_count,
                "detected": detected,
                "false_success_count": mutation_count - detected,
                "passed": detected == mutation_count,
            },
            "side_effects": {
                "network_calls": network_calls[0],
                "subprocess_calls": subprocess_calls[0],
                "passed": side_effect_free,
            },
            "airflow_parse": {
                "status": "SEPARATE_GATE",
                "command": "uv run pytest tests/test_airflow_provider_parse_slo.py -q",
                "fixture": "100 DAG / 500 workloads",
            },
            "runner": {
                "platform": platform.platform(),
                "processor": platform.machine(),
                "python": platform.python_version(),
            },
            "live_sigstore": "UNVERIFIED: requires an approved CI identity and trusted root",
        }


def _timed_build(root: Path) -> tuple[Any, float]:
    started = time.perf_counter()
    result = CatalogBundleBuilder().build(_build_request(root))
    return result, (time.perf_counter() - started) * 1000


def _build_request(root: Path) -> CatalogBundleBuildRequest:
    return CatalogBundleBuildRequest(
        project_root=root.as_posix(),
        kind="recipe_catalog",
        source="platform/recipes/catalog.yaml",
        bundle_root=".dpone/catalog-bundles",
        publisher_id="benchmark-recipes",
    )


def _verification_runs(root: Path, bundle_dir: str, repetitions: int) -> list[float]:
    service = CatalogBundleVerificationService(
        blob_verifier=_LocalIntegrityVerifier(),
        clock=lambda: datetime(2026, 7, 16, 12, tzinfo=UTC),
    )
    durations: list[float] = []
    for index in range(repetitions):
        started = time.perf_counter()
        result = service.verify(_verify_request(root, bundle_dir, f"verified-{index:03d}.json"))
        durations.append((time.perf_counter() - started) * 1000)
        if not result.is_verified:
            raise RuntimeError(f"benchmark verification failed: {result.code}")
    return durations


def _mutation_runs(root: Path, bundle_dir: str, count: int) -> int:
    manifest = json.loads(Path(bundle_dir, "catalog-bundle.json").read_text(encoding="utf-8"))
    paths = [Path(bundle_dir, item["path"]) for item in manifest["artifacts"] if item["logical_id"] != "catalog"]
    service = CatalogBundleVerificationService(blob_verifier=_LocalIntegrityVerifier())
    detected = 0
    for index, path in enumerate(paths[:count]):
        original = path.read_bytes()
        path.write_bytes(_substitute_one_byte(original))
        try:
            result = service.verify(_verify_request(root, bundle_dir, f"mutation-{index:03d}.json"))
            detected += int(result.code == "DPONE_CATALOG_BUNDLE_INTEGRITY_FAILED")
        finally:
            path.write_bytes(original)
    return detected


def _verify_request(root: Path, bundle_dir: str, output_name: str) -> CatalogBundleVerifyRequest:
    return CatalogBundleVerifyRequest(
        bundle_dir=bundle_dir,
        sigstore_bundle=(root / "trust/catalog.sigstore.json").as_posix(),
        policy=(root / "trust/catalog-policy.yaml").as_posix(),
        trusted_root=(root / "trust/trusted-root.json").as_posix(),
        output=(root / "receipts" / output_name).as_posix(),
    )


def _create_catalog(root: Path, entries: int) -> None:
    component = root / "platform/recipes/shared-component.yaml"
    _write_yaml(component, _component())
    component_pin = _pin(root, component, "shared-component@1.0.0")
    artifacts = [{"kind": "component", **component_pin}]
    for index in range(entries - 1):
        recipe_id = f"recipe-{index:04d}"
        recipe = root / f"platform/recipes/{recipe_id}.yaml"
        _write_yaml(recipe, _recipe(recipe_id, component_pin))
        artifacts.append({"kind": "recipe", **_pin(root, recipe, f"{recipe_id}@1.0.0")})
    catalog = root / "platform/recipes/catalog.yaml"
    _write_yaml(catalog, {"schema": "dpone.recipe-catalog.v1", "catalog_id": "benchmark", "artifacts": artifacts})
    _write_yaml(
        root / "dpone.yaml",
        {
            "schema": "dpone.project.v1",
            "authoring": {
                "primary_source_policy": "one_per_pipeline",
                "recipe_catalog": {"path": "platform/recipes/catalog.yaml", "trusted_catalog_ids": ["benchmark"]},
            },
        },
    )
    trusted_root = root / "trust/trusted-root.json"
    trusted_root.parent.mkdir(parents=True, exist_ok=True)
    trusted_root.write_text('{"trusted":"benchmark"}\n', encoding="utf-8")
    (root / "trust/catalog.sigstore.json").write_text('{"mediaType":"benchmark"}\n', encoding="utf-8")
    _write_yaml(
        root / "trust/catalog-policy.yaml",
        {
            "schema": "dpone.catalog-trust-policy.v1",
            "policy_id": "benchmark",
            "allowed_kinds": ["recipe_catalog"],
            "allowed_publishers": ["benchmark-recipes"],
            "certificate_identity": "https://github.com/dpone/benchmark@refs/heads/master",
            "certificate_oidc_issuer": "https://token.actions.githubusercontent.com",
            "trusted_root_sha256": sha256_bytes(trusted_root.read_bytes()),
            "verifier": {
                "minimum_version": "3.0.4",
                "maximum_version_exclusive": "4.0.0",
                "timeout_seconds": 30,
            },
        },
    )


def _component() -> dict[str, Any]:
    return {
        "schema": "dpone.component.v1",
        "id": "shared-component",
        "version": "1.0.0",
        "owner": "data-platform",
        "status": "stable",
        "processes": [
            {
                "name": "load",
                "source": {
                    "type": "mssql",
                    "connection_ref": "mssql_dev",
                    "table": {"schema": "dbo", "name": "orders"},
                },
                "sink": {
                    "type": "clickhouse",
                    "connection_ref": "clickhouse_dev",
                    "table": {"schema": "analytics", "name": "orders"},
                    "strategy": {"mode": "full_refresh"},
                },
            }
        ],
    }


def _recipe(recipe_id: str, component_pin: dict[str, str]) -> dict[str, Any]:
    return {
        "schema": "dpone.recipe.v1",
        "id": recipe_id,
        "version": "1.0.0",
        "owner": "data-platform",
        "status": "stable",
        "domain": "benchmark",
        "parameter_schema": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "additionalProperties": False,
            "properties": {},
        },
        "override_allowlist": [],
        "profiles": [],
        "components": [component_pin],
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


def _substitute_one_byte(content: bytes) -> bytes:
    index = content.find(b"stable")
    if index < 0:
        raise RuntimeError("benchmark payload does not contain a mutation marker")
    return content[:index] + b"S" + content[index + 1 :]


def _percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, int(len(ordered) * quantile + 0.999999) - 1))]


if __name__ == "__main__":
    raise SystemExit(main())
