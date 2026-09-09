from __future__ import annotations

from pathlib import Path

import pytest
from tools.signed_catalog_extension_benchmark import _create_catalog, run_benchmark

from dpone.manifest.recipe_catalog_operations import RecipeCatalogOperations
from dpone.manifest.recipe_models import RecipeArtifactPin
from dpone.manifest.recipe_resolver import RecipeArtifactLoader


def test_catalog_validation_loads_each_exact_pin_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _create_catalog(tmp_path, 4)
    calls: list[tuple[str, str]] = []
    original = RecipeArtifactLoader.load

    def tracking_load(self: RecipeArtifactLoader, pin: RecipeArtifactPin, *, kind: str):
        calls.append((kind, pin.ref))
        return original(self, pin, kind=kind)

    monkeypatch.setattr(RecipeArtifactLoader, "load", tracking_load)

    result = RecipeCatalogOperations(tmp_path, built_in_recipes={}).validate_catalog()

    assert result["validated_artifacts"] == 4
    assert len(calls) == 4


def test_signed_catalog_benchmark_producer_is_deterministic_and_bounded() -> None:
    report = run_benchmark(catalog_entries=4, verification_repetitions=2, mutation_count=2)

    assert report["passed"] is True
    assert report["fixture"] == {
        "catalog_entries": 4,
        "recipe_entries": 3,
        "shared_components": 1,
        "payload_files": 5,
    }
    assert report["build"]["independent_builds_equal"] is True
    assert report["build"]["idempotent_rebuild_status"] == "no_op"
    assert report["mutation_detection"]["detected"] == 2
    assert report["side_effects"] == {"network_calls": 0, "subprocess_calls": 0, "passed": True}
    assert report["airflow_parse"]["status"] == "SEPARATE_GATE"
