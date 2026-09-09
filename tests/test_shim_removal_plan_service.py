from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.services.docs.check_removal_readiness_service import CheckRemovalReadinessService
from dpone.services.docs.update_shim_removal_plan_service import UpdateShimRemovalPlanService


def _ctx(repo_root: Path) -> AppContext:
    return AppContext(
        settings=Settings(
            repo_root=repo_root,
            project_dir=repo_root,
            manifest_dir=repo_root / "etl-process-manifest",
            sources_registry_paths=(),
        ),
        logger=logging.getLogger("test.shim_removal_plan"),
        fs=LocalFileSystem(),
        yaml=PyYamlCodec(),
    )


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_update_and_check_shim_removal_plan_ready(tmp_path: Path) -> None:
    pkg = tmp_path / "src" / "dpone"
    _write(pkg / "__init__.py", "")
    _write(pkg / "source" / "__init__.py", "")
    _write(pkg / "runtime" / "sources" / "__init__.py", "")

    registry = tmp_path / "docs" / "compatibility_registry.yaml"
    _write(
        registry,
        """
version: 1
entries:
  - deprecated: dpone.source
    canonical: dpone.runtime.sources
    scope: package
    status: deprecated-shim
    removal: after one dedicated deprecation release
    removal_batch: cleanup-release-1
""".strip()
        + "\n",
    )
    doc = tmp_path / "docs" / "shim-removal-plan.md"
    _write(doc, "# Plan\n\n<!-- DPONE_SHIM_REMOVAL_PLAN_START -->\nold\n<!-- DPONE_SHIM_REMOVAL_PLAN_END -->\n")

    update = UpdateShimRemovalPlanService(ctx=_ctx(tmp_path))
    assert (
        update.run(
            argparse.Namespace(
                registry="docs/compatibility_registry.yaml",
                doc="docs/shim-removal-plan.md",
                package="src/dpone",
                batch="cleanup-release-1",
                check=False,
            )
        )
        == 0
    )
    content = doc.read_text(encoding="utf-8")
    assert "ready-for-next-release" in content
    assert "dpone docs check-removal-readiness --batch cleanup-release-1" in content

    check = CheckRemovalReadinessService(ctx=_ctx(tmp_path))
    code, payload = check.run(
        argparse.Namespace(
            format="json",
            registry="docs/compatibility_registry.yaml",
            doc="docs/shim-removal-plan.md",
            package="src/dpone",
            batch="cleanup-release-1",
            write_doc=False,
        )
    )
    assert code == 0
    assert isinstance(payload, dict)
    assert payload["ok"] is True
    assert payload["ready_count"] == 1


def test_check_removal_readiness_detects_blocked_imports(tmp_path: Path) -> None:
    pkg = tmp_path / "src" / "dpone"
    _write(pkg / "__init__.py", "")
    _write(pkg / "source" / "__init__.py", "")
    _write(pkg / "runtime" / "sources" / "__init__.py", "")
    _write(pkg / "services" / "use.py", "import dpone.source\n")

    registry = tmp_path / "docs" / "compatibility_registry.yaml"
    _write(
        registry,
        """
version: 1
entries:
  - deprecated: dpone.source
    canonical: dpone.runtime.sources
    scope: package
    status: deprecated-shim
    removal: after one dedicated deprecation release
    removal_batch: cleanup-release-1
""".strip()
        + "\n",
    )
    doc = tmp_path / "docs" / "shim-removal-plan.md"
    _write(doc, "# Plan\n\n<!-- DPONE_SHIM_REMOVAL_PLAN_START -->\nold\n<!-- DPONE_SHIM_REMOVAL_PLAN_END -->\n")

    check = CheckRemovalReadinessService(ctx=_ctx(tmp_path))
    code, payload = check.run(
        argparse.Namespace(
            format="text",
            registry="docs/compatibility_registry.yaml",
            doc="docs/shim-removal-plan.md",
            package="src/dpone",
            batch="cleanup-release-1",
            write_doc=True,
        )
    )
    assert code == 2
    assert isinstance(payload, str)
    assert "blocked: dpone.source -> dpone.runtime.sources" in payload
    assert "dpone.services.use" in payload
