from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from tests.agent_policy._release_identity_gate_helpers import (
    codes,
    evaluate,
    repository,
)


def test_rejects_package_version_and_dependency_drift(tmp_path: Path, monkeypatch: Any) -> None:
    root = repository(tmp_path)
    provider = root / "packages/apache-airflow-providers-dpone/pyproject.toml"
    provider.write_text(
        provider.read_text(encoding="utf-8")
        .replace('version = "1.2.3"', 'version = "1.2.4"')
        .replace("dpone-airflow-pack==1.2.3", "dpone-airflow-pack>=1.2"),
        encoding="utf-8",
    )
    report = evaluate(tmp_path, monkeypatch, root=root)

    assert "RELEASE_PACKAGE_VERSION_MISMATCH" in codes(report)
    assert "RELEASE_DEPENDENCY_NOT_EXACT" in codes(report)


@pytest.mark.parametrize(
    ("relative", "requirement"),
    [
        ("pyproject.toml", "dpone-airflow-pack==1.2.3"),
        ("pyproject.toml", "dpone-native-accel==1.2.3"),
        (
            "packages/apache-airflow-providers-dpone/pyproject.toml",
            "dpone-airflow-pack==1.2.3",
        ),
    ],
)
@pytest.mark.parametrize(
    "replacement",
    [
        "PACKAGE==1.2.3 ; python_version < '0'",
        "PACKAGE[unsafe-extra]==1.2.3",
        "PACKAGE @ https://example.invalid/internal.whl",
        "PACKAGE>=1.2.3",
        "PACKAGE==1.2.4",
        'PACKAGE==1.2.3", "PACKAGE==1.2.3',
    ],
    ids=("marker", "extras", "url", "non-exact", "wrong-version", "duplicate"),
)
def test_rejects_nonmandatory_internal_release_requirements(
    tmp_path: Path,
    monkeypatch: Any,
    relative: str,
    requirement: str,
    replacement: str,
) -> None:
    root = repository(tmp_path)
    project = root / relative
    package = requirement.split("==", maxsplit=1)[0]
    source = project.read_text(encoding="utf-8").replace(
        requirement,
        replacement.replace("PACKAGE", package),
    )
    project.write_text(source, encoding="utf-8")

    report = evaluate(tmp_path, monkeypatch, root=root)
    assert "RELEASE_DEPENDENCY_NOT_EXACT" in codes(report)


def test_rejects_missing_changelog_release_section(tmp_path: Path, monkeypatch: Any) -> None:
    root = repository(tmp_path)
    (root / "CHANGELOG.md").write_text("# Changelog\n\n## Unreleased\n", encoding="utf-8")
    report = evaluate(tmp_path, monkeypatch, root=root)

    assert "RELEASE_CHANGELOG_SECTION_MISSING" in codes(report)
