from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.ports.filesystem import FileSystem


from pathlib import Path
from typing import Protocol

from dpone.gitops.paths import GitOpsPathValidationError, confined_repo_file_path


class GitOpsOutputContext(Protocol):
    fs: FileSystem
    settings: object


def write_optional_output(
    ctx: GitOpsOutputContext,
    raw_output: object,
    content: str,
    *,
    suppress_unsafe: bool = False,
) -> None:
    if not raw_output:
        return
    repo_root = getattr(ctx.settings, "repo_root")
    try:
        _rel_path, destination = confined_repo_file_path(Path(repo_root), str(raw_output), source="--output")
    except GitOpsPathValidationError:
        if suppress_unsafe:
            return
        raise
    ctx.fs.write_text(destination, content, encoding="utf-8")


__all__ = ["GitOpsOutputContext", "write_optional_output"]
