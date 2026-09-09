from __future__ import annotations

import json
from pathlib import Path

from dpone.gitops.models import GitOpsIssue
from dpone.ports.filesystem import FileSystem


def load_gitops_json_artifact(
    *,
    fs: FileSystem,
    repo_root: Path,
    path: Path,
    label: str,
    missing_code: str,
    invalid_code: str,
    source: str,
    enabled: bool = True,
) -> tuple[object, tuple[GitOpsIssue, ...]]:
    if not enabled:
        return None, ()
    full_path = repo_root / path
    if not fs.exists(full_path):
        return None, (
            _issue(code=missing_code, message="Required JSON artifact does not exist", path=label, source=source),
        )
    try:
        return json.loads(fs.read_text(full_path, encoding="utf-8")), ()
    except json.JSONDecodeError as exc:
        return None, (
            _issue(
                code=invalid_code,
                message=f"JSON artifact could not be parsed: {exc.msg}",
                path=label,
                source=source,
            ),
        )


def _issue(*, code: str, message: str, path: str, source: str) -> GitOpsIssue:
    return GitOpsIssue(code=code, message=message, path=path, source=source)


__all__ = ["load_gitops_json_artifact"]
