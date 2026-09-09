from __future__ import annotations

import subprocess
from collections.abc import Callable, Sequence
from pathlib import Path
from subprocess import CompletedProcess
from typing import Protocol

from dpone.gitops.models import GitOpsIssue
from dpone.gitops.paths import GitOpsPathValidationError, confined_repo_path, safe_relative_path

GitCommandRunner = Callable[..., CompletedProcess[str]]


class ChangedFilesFileReader(Protocol):
    def read_text(self, path: Path, *, encoding: str = "utf-8") -> str: ...


class GitChangedFilesResolver:
    """Resolves GitOps changed-file inputs before impact analysis."""

    def __init__(self, *, fs: ChangedFilesFileReader, run: GitCommandRunner = subprocess.run) -> None:
        self._fs = fs
        self._run = run

    def resolve(
        self,
        *,
        args: object,
        repo_root: Path,
        require_input: bool = True,
    ) -> tuple[tuple[str, ...], tuple[GitOpsIssue, ...], tuple[GitOpsIssue, ...]]:
        values: list[str] = []
        warnings: list[GitOpsIssue] = []
        blockers: list[GitOpsIssue] = []
        values.extend(str(item) for item in (getattr(args, "changed_files", []) or ()))
        file_values, file_blocker = self._from_changed_files_file(
            raw_path=getattr(args, "changed_files_file", None),
            repo_root=repo_root,
        )
        values.extend(file_values)
        if file_blocker is not None:
            blockers.append(file_blocker)
        diff_values, diff_blocker = self._from_git_diff(
            from_ref=getattr(args, "from_ref", None),
            to_ref=getattr(args, "to_ref", None),
            repo_root=repo_root,
        )
        values.extend(diff_values)
        if diff_blocker is not None:
            blockers.append(diff_blocker)
        normalized, path_blocker = _normalize_paths(values)
        if path_blocker is not None:
            blockers.append(path_blocker)
        if not normalized and not blockers and require_input:
            blockers.append(
                GitOpsIssue(
                    code="changed_files_missing",
                    message="Provide --changed-files, --changed-files-file, or --from-ref with --to-ref",
                    path="",
                    source="changed-files",
                )
            )
        return tuple(normalized), tuple(warnings), tuple(blockers)

    def _from_changed_files_file(
        self,
        *,
        raw_path: object,
        repo_root: Path,
    ) -> tuple[tuple[str, ...], GitOpsIssue | None]:
        if not raw_path:
            return (), None
        try:
            rel_path, destination = confined_repo_path(
                repo_root,
                str(raw_path),
                source="--changed-files-file",
            )
        except GitOpsPathValidationError as exc:
            return (), GitOpsIssue(
                code="invalid_path",
                message=str(exc),
                path=str(raw_path or ""),
                source="--changed-files-file",
            )
        label = rel_path.as_posix()
        try:
            content = self._fs.read_text(destination, encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            return (), GitOpsIssue(
                code="changed_files_file_read_failed",
                message=_sanitize_message(f"Could not read changed-files file: {exc}", repo_root=repo_root),
                path=label,
                source="--changed-files-file",
            )
        return tuple(line.strip() for line in content.splitlines() if line.strip()), None

    def _from_git_diff(
        self,
        *,
        from_ref: object,
        to_ref: object,
        repo_root: Path,
    ) -> tuple[tuple[str, ...], GitOpsIssue | None]:
        if not from_ref and not to_ref:
            return (), None
        from_text = str(from_ref or "").strip()
        to_text = str(to_ref or "").strip()
        if not from_text or not to_text:
            return (), GitOpsIssue(
                code="git_diff_refs_incomplete",
                message="Both --from-ref and --to-ref are required for Git diff input",
                path=f"{from_text}..{to_text}",
                source="git diff",
            )
        ref_blocker = _validate_ref_pair(from_text, to_text)
        if ref_blocker is not None:
            return (), ref_blocker
        result = self._run(
            ["git", "diff", "--name-only", from_text, to_text, "--"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            message = result.stderr.strip() or result.stdout.strip() or "git diff failed"
            return (), GitOpsIssue(
                code="git_diff_failed",
                message=_sanitize_message(message, repo_root=repo_root),
                path=f"{from_text}..{to_text}",
                source="git diff",
            )
        return tuple(line.strip() for line in result.stdout.splitlines() if line.strip()), None


def resolve_changed_files(
    *,
    fs: ChangedFilesFileReader,
    repo_root: Path,
    args: object,
    require_input: bool = False,
) -> tuple[tuple[str, ...], tuple[GitOpsIssue, ...], tuple[GitOpsIssue, ...]]:
    """Resolve changed-file CLI inputs without applying a caller-specific producer."""

    return GitChangedFilesResolver(fs=fs).resolve(
        args=args,
        repo_root=repo_root,
        require_input=require_input,
    )


def _validate_ref_pair(from_ref: str, to_ref: str) -> GitOpsIssue | None:
    for source, ref in (("--from-ref", from_ref), ("--to-ref", to_ref)):
        if ref.startswith("-") or "\0" in ref or "\n" in ref:
            return GitOpsIssue(
                code="invalid_git_ref",
                message=f"{source} is not a safe Git ref",
                path=ref,
                source=source,
            )
    return None


def _dedupe(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _normalize_paths(values: Sequence[str]) -> tuple[list[str], GitOpsIssue | None]:
    normalized: list[str] = []
    for value in values:
        try:
            normalized.append(safe_relative_path(value, source="--changed-files").as_posix())
        except GitOpsPathValidationError as exc:
            return [], GitOpsIssue(
                code="invalid_changed_path",
                message=str(exc),
                path=str(value),
                source="--changed-files",
            )
    return _dedupe(normalized), None


def _sanitize_message(message: str, *, repo_root: Path) -> str:
    return message.replace(str(repo_root), "<repo_root>")


__all__ = ["ChangedFilesFileReader", "GitChangedFilesResolver", "resolve_changed_files"]
