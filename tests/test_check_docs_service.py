from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.services.docs.check_docs_service import CheckDocsService


def _ctx(repo_root: Path) -> AppContext:
    return AppContext(
        settings=Settings(
            repo_root=repo_root,
            project_dir=repo_root,
            manifest_dir=repo_root / "etl-process-manifest",
            sources_registry_paths=(),
        ),
        logger=logging.getLogger("test.docs_check"),
        fs=LocalFileSystem(),
        yaml=PyYamlCodec(),
    )


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_check_docs_service_ok(tmp_path: Path) -> None:
    _write(tmp_path / "README.md", "# Root\n\nSee [Docs](docs/README.md#child-page).\n")
    _write(
        tmp_path / "docs" / "README.md",
        "# Child page\n\nSee [Example](other.md#details).\n",
    )
    _write(tmp_path / "docs" / "other.md", "# Details\n\nBack to [root](../README.md).\n")

    svc = CheckDocsService(ctx=_ctx(tmp_path))
    code, payload = svc.run(argparse.Namespace(format="json", docs_dir="docs", include_root_readme=True))
    assert code == 0
    assert isinstance(payload, dict)
    assert payload["ok"] is True
    assert payload["issue_count"] == 0
    json.dumps(payload)


def test_check_docs_service_detects_missing_anchor(tmp_path: Path) -> None:
    _write(tmp_path / "README.md", "# Root\n\nSee [Docs](docs/README.md#missing-anchor).\n")
    _write(tmp_path / "docs" / "README.md", "# Child page\n")

    svc = CheckDocsService(ctx=_ctx(tmp_path))
    code, payload = svc.run(argparse.Namespace(format="text", docs_dir="docs", include_root_readme=True))
    assert code == 2
    assert isinstance(payload, str)
    assert "missing-anchor" in payload
