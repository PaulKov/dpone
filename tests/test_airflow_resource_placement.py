"""Reject misplaced workload resources before authoring or delivery can succeed."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
import yaml

from dpone.manifest.airflow_resources import manifest_airflow_resources
from dpone.manifest.authoring import default_authoring_compiler
from dpone.manifest.errors import ManifestConfigurationError
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.readiness.airflow_authoring_validation import compile_pipeline_source
from tests.test_airflow_recipe_catalog_v1 import (
    _create_external_catalog,
    _refresh_recipe_closure_pins,
    _scaffold,
    _write_yaml,
)
from tests.test_airflow_resources_strict_delivery import PACK_ROOT, RESOURCES, _reconcile, _write_authoring

_CLASSIC_SCOPES = (
    "defaults",
    "schemas.src",
    "schemas.src.defaults",
    "schemas.src.tables[0]",
    "schemas.src.tables[0].overrides",
)
_MODES = ("flow", "folder", "pipeline-alias", "batch-processes", *_CLASSIC_SCOPES)
_RESOURCE_CODE = "DPONE_AIRFLOW_RESOURCES_INVALID"


def _source(
    root: Path, mode: str, *, resources: object = RESOURCES, placement_root: bool = False
) -> tuple[dict[str, Any], str]:
    _write_authoring(root, RESOURCES)
    path = root / "pipeline.yaml"
    process = yaml.safe_load(path.read_text(encoding="utf-8"))
    process.pop("gitops")
    process["execution"] = {"visibility": "task"}
    declaration = {} if placement_root else {"gitops": {"airflow": {"resources": deepcopy(resources)}}}
    source: dict[str, Any] = {
        "kind": "dpone.flow.v1",
        "authoring": {"mode": "flow", "source": "pipeline.yaml"},
        "metadata": {"id": "orders", "domain": "sales"},
    }
    if placement_root:
        source["gitops"] = {"airflow": {"resources": deepcopy(resources)}}
    if mode in _CLASSIC_SCOPES:
        source["kind"] = "dpone.batch.v1"
        source["authoring"]["mode"] = "classic"
        table = {"table": "orders", "id": "orders", "overrides": process}
        schema: dict[str, Any] = {"tables": [table], "defaults": {}}
        source.update({"defaults": {}, "schemas": {"src": schema}})
        targets = {
            "defaults": source["defaults"],
            "schemas.src": schema,
            "schemas.src.defaults": schema["defaults"],
            "schemas.src.tables[0]": table,
            "schemas.src.tables[0].overrides": process,
        }
        targets[mode].update(declaration)
        field = f"{mode}.gitops.airflow.resources"
    else:
        process.update(declaration)
        field = "processes[0].gitops.airflow.resources"
        if mode == "folder":
            source["authoring"]["mode"] = "folder"
            source["fragments"] = ["load.yaml"]
            (root / "load.yaml").write_text(
                yaml.safe_dump({"kind": "dpone.flow-fragment.v1", "processes": [process]}), encoding="utf-8"
            )
        else:
            source["processes"] = [process]
            if mode in {"pipeline-alias", "batch-processes"}:
                source["kind"] = "dpone.pipeline.v1" if mode == "pipeline-alias" else "dpone.batch.v1"
                source.pop("authoring")
    path.write_text(yaml.safe_dump(source, sort_keys=False), encoding="utf-8")
    return source, field


@pytest.mark.parametrize("mode", _MODES)
def test_authoring_rejects_process_resources_with_recovery_path(tmp_path: Path, mode: str) -> None:
    source, field = _source(tmp_path, mode)

    compilation, errors = compile_pipeline_source(
        source, tmp_path / "pipeline.yaml", root=tmp_path, compiler=default_authoring_compiler()
    )

    assert compilation is None
    assert len(errors) == 1
    assert errors[0]["code"] == _RESOURCE_CODE
    assert field in errors[0]["message"]
    assert "manifest root" in errors[0]["message"]


@pytest.mark.parametrize("mode", _CLASSIC_SCOPES)
def test_direct_classic_loader_cannot_bypass_placement_validation(tmp_path: Path, mode: str) -> None:
    _, field = _source(tmp_path, mode)

    with pytest.raises(ManifestConfigurationError) as caught:
        ManifestLoaderRouter().load(tmp_path / "pipeline.yaml", metadata_only=True)

    assert getattr(caught.value, "code", None) == _RESOURCE_CODE
    assert field in str(caught.value)


@pytest.mark.parametrize("mode", _MODES)
def test_reconcile_rejects_process_resources_without_writing_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], mode: str
) -> None:
    monkeypatch.chdir(tmp_path)
    _, field = _source(tmp_path, mode)

    code, payload, _stderr = _reconcile(capsys)

    assert code == 2
    assert payload["packs"] == []
    assert payload["dag_specs"] == []
    matching = [blocker for blocker in payload["blockers"] if blocker["code"] == _RESOURCE_CODE]
    assert len(matching) == 1
    assert field in matching[0]["message"]
    assert "manifest root" in matching[0]["message"]
    assert not (tmp_path / PACK_ROOT).exists()
    assert not (tmp_path / ".dpone-cache").exists()


@pytest.mark.parametrize("resources", [None, {}, {"requests": {"cpu": "secret-value"}}])
def test_placement_is_rejected_even_if_root_resources_exist(tmp_path: Path, resources: object) -> None:
    source, field = _source(tmp_path, "flow", resources=resources)
    source["gitops"] = {"airflow": {"resources": RESOURCES}}

    _, errors = compile_pipeline_source(
        source, tmp_path / "pipeline.yaml", root=tmp_path, compiler=default_authoring_compiler()
    )

    assert len(errors) == 1
    assert errors[0]["code"] == _RESOURCE_CODE
    assert field in errors[0]["message"]
    assert "secret-value" not in str(errors)


def test_connector_options_are_not_interpreted_as_authoring_scopes() -> None:
    options = {"gitops": {"airflow": {"resources": {"requests": {"cpu": "application-data"}}}}}
    source = {"processes": [{"source": {"options": options}}], "gitops": {"airflow": {"resources": RESOURCES}}}

    assert manifest_airflow_resources(source) == RESOURCES


@pytest.mark.parametrize("mode", _MODES)
def test_moving_resources_to_root_restores_authoring(tmp_path: Path, mode: str) -> None:
    source, _ = _source(tmp_path, mode, placement_root=True)

    compilation, errors = compile_pipeline_source(
        source, tmp_path / "pipeline.yaml", root=tmp_path, compiler=default_authoring_compiler()
    )

    assert errors == []
    assert compilation is not None
    assert manifest_airflow_resources(source) == RESOURCES
    assert all("gitops" not in process for process in compilation.processes)


def test_recipe_expansion_cannot_hide_process_resources(tmp_path: Path) -> None:
    paths = _create_external_catalog(tmp_path)
    component = yaml.safe_load(paths["component"].read_text(encoding="utf-8"))
    component["processes"][0]["gitops"] = {"airflow": {"resources": RESOURCES}}
    _write_yaml(paths["component"], component)
    _refresh_recipe_closure_pins(paths)

    assert _scaffold(tmp_path).passed is True
    path = tmp_path / "pipelines/orders_daily/pipeline.yaml"
    source = yaml.safe_load(path.read_text(encoding="utf-8"))

    compilation, errors = compile_pipeline_source(source, path, root=tmp_path, compiler=default_authoring_compiler())

    assert compilation is None
    assert len(errors) == 1
    assert errors[0]["code"] == _RESOURCE_CODE
    assert "processes[0].gitops.airflow.resources" in errors[0]["message"]
