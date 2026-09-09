from __future__ import annotations

from pathlib import Path

import pytest

from dpone.manifest.authoring_migration_io import AuthoringMigrationFileSystem
from dpone.manifest.pipeline_source_reference import (
    PipelineSourceReferenceError,
    resolve_pipeline_reference,
    resolve_pipeline_source_relative,
)
from dpone.readiness.airflow_pipeline_source_reader import resolve_pipeline_path


def test_bare_pipeline_id_has_one_canonical_location(tmp_path: Path) -> None:
    canonical = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    legacy = tmp_path / "orders_daily/pipeline.yaml"
    canonical.parent.mkdir(parents=True)
    legacy.parent.mkdir(parents=True)
    canonical.write_text("kind: dpone.flow.v1\n", encoding="utf-8")
    legacy.write_text("kind: competing\n", encoding="utf-8")

    assert resolve_pipeline_path(tmp_path, "orders_daily") == canonical
    assert AuthoringMigrationFileSystem(tmp_path).read_source("orders_daily").path == canonical
    reference = resolve_pipeline_reference(tmp_path, "orders_daily")
    assert str(reference.requested_id) == "orders_daily"
    assert reference.kind == "id"


def test_explicit_missing_directory_never_falls_back_to_its_basename(tmp_path: Path) -> None:
    unrelated = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    unrelated.parent.mkdir(parents=True)
    unrelated.write_text("kind: dpone.flow.v1\n", encoding="utf-8")

    assert resolve_pipeline_path(tmp_path, "missing/orders_daily") == (tmp_path / "missing/orders_daily/pipeline.yaml")


@pytest.mark.parametrize("target", ("../outside", "pipelines/../outside", ".", "pipelines\\orders"))
def test_pipeline_source_reference_rejects_non_normalized_paths(tmp_path: Path, target: str) -> None:
    with pytest.raises(PipelineSourceReferenceError):
        resolve_pipeline_source_relative(tmp_path, target)


def test_absolute_in_root_source_has_same_relative_identity(tmp_path: Path) -> None:
    source = tmp_path / "pipelines/orders_daily/pipeline.yaml"

    assert resolve_pipeline_source_relative(tmp_path, source) == "pipelines/orders_daily/pipeline.yaml"
    assert resolve_pipeline_source_relative(tmp_path, "pipelines/orders_daily/pipeline.yaml") == (
        "pipelines/orders_daily/pipeline.yaml"
    )
