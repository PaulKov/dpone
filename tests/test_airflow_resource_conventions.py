"""Convention layers cannot erase unsupported resource declarations during merge."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from dpone.manifest.authoring import default_authoring_compiler
from dpone.manifest.errors import ManifestConfigurationError
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.readiness.airflow_authoring_validation import compile_pipeline_source
from tests.test_airflow_resource_placement import _source
from tests.test_airflow_resources_strict_delivery import PACK_ROOT, RESOURCES, _reconcile


@pytest.mark.parametrize("route", ["check", "loader", "reconcile"])
@pytest.mark.parametrize("override", ["none", "null-gitops", "null-airflow", "later-preset"])
@pytest.mark.parametrize("scope", ["defaults", "root"])
def test_convention_resource_declarations_fail_before_merge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    route: str,
    override: str,
    scope: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    source, _ = _source(tmp_path, "defaults", placement_root=True)
    declaration = {"gitops": {"airflow": {"resources": RESOURCES}}}
    preset = {"defaults": declaration} if scope == "defaults" else declaration
    (tmp_path / "preset.yaml").write_text(yaml.safe_dump(preset), encoding="utf-8")
    source["convention"] = "preset.yaml"
    if override == "later-preset":
        later = {"defaults": {"gitops": None}} if scope == "defaults" else {"gitops": None}
        (tmp_path / "later.yaml").write_text(yaml.safe_dump(later), encoding="utf-8")
        source["conventions"] = ["later.yaml"]
    elif override != "none":
        target = source["defaults"] if scope == "defaults" else source
        target["gitops"] = None if override == "null-gitops" else {"airflow": None}
    path = tmp_path / "pipeline.yaml"
    path.write_text(yaml.safe_dump(source, sort_keys=False), encoding="utf-8")

    if route == "check":
        _, errors = compile_pipeline_source(source, path, root=tmp_path, compiler=default_authoring_compiler())
        assert len(errors) == 1
        code, message = errors[0]["code"], errors[0]["message"]
    elif route == "loader":
        with pytest.raises(ManifestConfigurationError) as caught:
            ManifestLoaderRouter().load(path, metadata_only=True)
        code, message = getattr(caught.value, "code", None), str(caught.value)
    else:
        exit_code, payload, _stderr = _reconcile(capsys)
        assert exit_code == 2
        assert payload["packs"] == []
        assert payload["dag_specs"] == []
        matching = [item for item in payload["blockers"] if item["code"] == "DPONE_AIRFLOW_RESOURCES_INVALID"]
        assert len(matching) == 1
        code, message = matching[0]["code"], matching[0]["message"]
        assert not (tmp_path / PACK_ROOT).exists()
        assert not (tmp_path / ".dpone-cache").exists()
    assert code == "DPONE_AIRFLOW_RESOURCES_INVALID"
    assert "preset.yaml" in message
    assert "gitops.airflow.resources" in message
    assert "manifest root" in message


def test_ordinary_convention_defaults_and_root_resources_remain_supported(tmp_path: Path) -> None:
    source, _ = _source(tmp_path, "defaults", placement_root=True)
    source["convention"] = "preset.yaml"
    (tmp_path / "preset.yaml").write_text(
        yaml.safe_dump({"defaults": {"description": "Shared process defaults"}, "vars": {"owner": "data"}}),
        encoding="utf-8",
    )
    path = tmp_path / "pipeline.yaml"
    path.write_text(yaml.safe_dump(source), encoding="utf-8")

    _, errors = compile_pipeline_source(source, path, root=tmp_path, compiler=default_authoring_compiler())
    loaded = ManifestLoaderRouter().load(path, metadata_only=True)

    assert errors == []
    assert loaded.raw["gitops"]["airflow"]["resources"] == RESOURCES
    assert loaded.processes[0].raw_config["description"] == "Shared process defaults"
