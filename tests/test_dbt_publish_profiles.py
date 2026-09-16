"""Project-root injection preserves explicit, environment and legacy discovery."""

from pathlib import Path

import pytest

from dpone.app.dbt_publish_composition import DbtPublishProfileLoader
from dpone.manifest.dbt_publish_profiles import DbtPublishProfileRegistry

POLICY = Path(__file__).parents[1] / "examples/dbt-inline-publishing/dpone/dbt-publish-profiles.yml"


@pytest.mark.parametrize("override", ["none", "environment", "explicit"])
def test_profile_loader_uses_selected_root_and_override_precedence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    override: str,
) -> None:
    monkeypatch.delenv("DPONE_DBT_PUBLISH_PROFILES", raising=False)
    root_policy = tmp_path / "dpone/dbt-publish-profiles.yml"
    root_policy.parent.mkdir()
    root_policy.write_bytes(POLICY.read_bytes())
    environment_policy = tmp_path / "environment.yml"
    environment_policy.write_bytes(POLICY.read_bytes())
    explicit_policy = tmp_path / "explicit.yml"
    explicit_policy.write_bytes(POLICY.read_bytes())
    if override != "none":
        monkeypatch.setenv("DPONE_DBT_PUBLISH_PROFILES", str(environment_policy))
    selected = explicit_policy if override == "explicit" else None
    registry, issues = DbtPublishProfileLoader(project_root=tmp_path).load(
        tmp_path / "build/dbt/manifest.json",
        selected,
    )
    expected = selected or (environment_policy if override == "environment" else root_policy)
    assert not issues
    assert registry is not None
    assert registry.source_path == str(expected)


@pytest.mark.parametrize("target", ["target", "custom"])
def test_legacy_registry_discovery_retains_manifest_parent_rule(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target: str,
) -> None:
    monkeypatch.delenv("DPONE_DBT_PUBLISH_PROFILES", raising=False)
    manifest = tmp_path / target / "manifest.json"
    root = tmp_path if target == "target" else manifest.parent
    policy = root / "dpone/dbt-publish-profiles.yml"
    policy.parent.mkdir(parents=True)
    policy.write_bytes(POLICY.read_bytes())
    registry, issues = DbtPublishProfileRegistry.load(manifest)
    assert not issues
    assert registry is not None
    assert registry.source_path == str(policy)
