from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest

from dpone.manifest.sparse_paths_discovery import ManifestSparsePathDiscovery
from dpone.manifest.sparse_paths_policy import SparsePathPolicy, SparsePathValidationError


class _Reader:
    def read(self, path: Path) -> object:
        import yaml

        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dedent(text).strip() + "\n", encoding="utf-8")
    return path


def _policy(tmp_path: Path) -> SparsePathPolicy:
    return SparsePathPolicy(repo_root=tmp_path, workload_root=tmp_path / "dpone_workloads")


def test_sparse_path_policy_rejects_unsafe_paths(tmp_path: Path) -> None:
    policy = _policy(tmp_path)

    for raw in ("", "/absolute.yaml", "../escape.yaml", "dpone_workloads/../escape.yaml"):
        with pytest.raises(SparsePathValidationError):
            policy.resolve_user_path(raw, source="test")

    with pytest.raises(SparsePathValidationError):
        policy.resolve_user_path("outside.yaml", source="test")


def test_sparse_path_policy_renders_repo_relative_directory_entries(tmp_path: Path) -> None:
    directory = tmp_path / "dpone_workloads" / "sql" / "orders"
    directory.mkdir(parents=True)
    policy = _policy(tmp_path)

    entry = policy.entry_for_user_path(
        "dpone_workloads/sql/orders/",
        kind="support_path",
        source="--support-path",
        required=False,
        reason="user supplied support path",
    )

    assert entry.path == "dpone_workloads/sql/orders/"
    assert entry.exists is True
    assert entry.is_dir is True


def test_sparse_path_discovery_recurses_manifest_dependencies_and_warns_for_groups(tmp_path: Path) -> None:
    workload = tmp_path / "dpone_workloads"
    root = _write(
        workload / "manifests" / "mssql" / "orders.yaml",
        """
        kind: dpone.batch.v1
        convention: conventions/custom.yaml
        registry: registry/sources.yaml
        defaults:
          depends_on:
            - path: shared/bootstrap.yaml#public.bootstrap
        schemas:
          public:
            tables:
              - table: orders
                depends_on:
                  - "#public.customers"
                  - group: landing_shared
        """,
    )
    _write(workload / "manifests" / "mssql" / "conventions" / "custom.yaml", "vars: {owner: data}")
    _write(workload / "manifests" / "mssql" / "registry" / "sources.yaml", "version: 1\nentries: []")
    _write(
        workload / "manifests" / "mssql" / "shared" / "bootstrap.yaml",
        """
        depends_on:
          - path: missing_seed.yaml
        source: {}
        sink: {}
        """,
    )

    report = ManifestSparsePathDiscovery(reader=_Reader(), policy=_policy(tmp_path)).discover(root)

    assert [entry.path for entry in report.entries] == [
        "dpone_workloads/manifests/mssql/orders.yaml",
        "dpone_workloads/manifests/mssql/conventions/custom.yaml",
        "dpone_workloads/manifests/mssql/registry/sources.yaml",
        "dpone_workloads/manifests/mssql/shared/bootstrap.yaml",
        "dpone_workloads/manifests/mssql/shared/missing_seed.yaml",
    ]
    assert any(warning.code == "group_dependency_unresolved" for warning in report.warnings)
    assert any(
        warning.code == "missing_path" and warning.path.endswith("missing_seed.yaml") for warning in report.warnings
    )


def test_sparse_path_discovery_deduplicates_cycles(tmp_path: Path) -> None:
    workload = tmp_path / "dpone_workloads"
    first = _write(workload / "manifests" / "a.yaml", "depends_on:\n  - path: b.yaml")
    _write(workload / "manifests" / "b.yaml", "depends_on:\n  - path: a.yaml")

    report = ManifestSparsePathDiscovery(reader=_Reader(), policy=_policy(tmp_path)).discover(first)

    assert [entry.path for entry in report.entries] == [
        "dpone_workloads/manifests/a.yaml",
        "dpone_workloads/manifests/b.yaml",
    ]


def test_sparse_path_discovery_blocks_missing_root_manifest(tmp_path: Path) -> None:
    manifest = tmp_path / "dpone_workloads" / "manifests" / "missing.yaml"

    report = ManifestSparsePathDiscovery(reader=_Reader(), policy=_policy(tmp_path)).discover(manifest)

    assert report.entries[0].path == "dpone_workloads/manifests/missing.yaml"
    assert report.blockers[0].code == "manifest_not_found"


def test_sparse_path_discovery_blocks_unparseable_root_manifest(tmp_path: Path) -> None:
    manifest = _write(tmp_path / "dpone_workloads" / "manifests" / "broken.yaml", "source: [")

    report = ManifestSparsePathDiscovery(reader=_Reader(), policy=_policy(tmp_path)).discover(manifest)

    assert report.entries[0].path == "dpone_workloads/manifests/broken.yaml"
    assert report.blockers[0].code == "manifest_read_failed"
