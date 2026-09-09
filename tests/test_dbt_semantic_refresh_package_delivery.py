from __future__ import annotations

import json
import re
from pathlib import Path
from shutil import copy2

import yaml

from dpone.adapters.dbt_semantic_refresh_project import semantic_refresh_package_sha256
from dpone.ports.semantic_refresh_mssql_admission_authority import compose_admission
from tests.test_semantic_refresh_mssql_authority import _bundle as _canonical_mssql_bundle

ROOT = Path(__file__).parents[1]
PACKAGE = ROOT / "packages" / "dbt-dpone"
PACKAGE_FILES = (
    "dbt_project.yml",
    "macros/dpone_publish.sql",
    "macros/semantic_refresh_restore.sql",
    "macros/semantic_refresh_scope_merge.sql",
)
RUNTIME_IMAGE_PACKAGE_ROOT = "/opt/dpone/runtime/dbt-dpone"


def test_platform_delivery_uses_a_closed_digest_not_a_stale_git_copy_recipe(tmp_path: Path) -> None:
    staged = tmp_path / "dbt_dpone"
    for relative in PACKAGE_FILES:
        target = staged / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        copy2(PACKAGE / relative, target)

    assert semantic_refresh_package_sha256(staged).startswith("sha256:")
    assert not (PACKAGE / "install/packages.yml").exists()
    assert not (PACKAGE / "install/package-lock.yml").exists()
    install = (PACKAGE / "INSTALL.md").read_text(encoding="utf-8")
    assert "macro-paths" in install
    assert "e77ff54434b6b00a58d1fa8aad0b289ad388a659" not in install


def test_runtime_image_delivers_the_closed_platform_package() -> None:
    dockerfile = (ROOT / "docker/runtime/Dockerfile").read_text(encoding="utf-8")
    dockerignore = (ROOT / "docker/runtime/Dockerfile.dockerignore").read_text(encoding="utf-8").splitlines()

    assert "!packages/dbt-dpone/**" not in dockerignore
    for relative in PACKAGE_FILES:
        source = f"packages/dbt-dpone/{relative}"
        assert f"!{source}" in dockerignore
        assert f"COPY --chmod=0444 {source} {RUNTIME_IMAGE_PACKAGE_ROOT}/{relative}" in dockerfile

    assert "from dpone.adapters.dbt_semantic_refresh_project import semantic_refresh_package_sha256" in dockerfile
    assert f'semantic_refresh_package_sha256(Path("{RUNTIME_IMAGE_PACKAGE_ROOT}"))' in dockerfile


def test_pinned_package_contains_platform_scope_merge_lifecycle() -> None:
    project = yaml.safe_load((PACKAGE / "dbt_project.yml").read_text(encoding="utf-8"))
    macro = (PACKAGE / "macros" / "semantic_refresh_scope_merge.sql").read_text(encoding="utf-8")

    assert project["name"] == "dbt_dpone"
    assert project["version"] == "1.0.0"
    assert "macro get_incremental_dpone_scope_merge_sql" in macro


def test_scope_merge_field_closure_matches_the_canonical_mssql_producer() -> None:
    macro = (PACKAGE / "macros" / "semantic_refresh_scope_merge.sql").read_text(encoding="utf-8")
    match = re.search(r"{% set required = \[(.*?)\] %}", macro, flags=re.DOTALL)
    assert match is not None
    macro_fields = set(re.findall(r"'([^']+)'", match.group(1)))
    journal = compose_admission(_canonical_mssql_bundle()).journals[0]
    producer_fields = set(json.loads(journal.strategy_authority_json))

    assert producer_fields == macro_fields
    assert "attempt.keys() | list | sort != required | sort" in macro
    assert "dpone.semantic-refresh-mssql-attempt-strategy-authority.v1" in macro
    assert "COLLATE Latin1_General_100_BIN2" in macro
