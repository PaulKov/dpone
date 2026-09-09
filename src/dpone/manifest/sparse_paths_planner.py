from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from dpone.manifest.sparse_paths_discovery import (
    ManifestSparsePathDiscovery,
    ManifestSparsePathReader,
    ResolvedSparsePath,
    SparsePathEntry,
    SparsePathIssue,
    SparsePathPolicy,
    SparsePathReport,
    SparsePathValidationError,
    infer_workload_root,
)


class ManifestSparsePathPlanner:
    """Builds sparse path reports from raw CLI/service inputs."""

    def __init__(
        self,
        *,
        reader: ManifestSparsePathReader,
        repo_root: Path,
        workload_root: Path,
        initial_blockers: Sequence[SparsePathIssue] = (),
    ) -> None:
        self._policy = SparsePathPolicy(repo_root=repo_root, workload_root=workload_root)
        self._discovery = ManifestSparsePathDiscovery(reader=reader, policy=self._policy)
        self._initial_blockers = tuple(initial_blockers)

    @classmethod
    def from_inputs(
        cls,
        *,
        reader: ManifestSparsePathReader,
        repo_root: Path,
        raw_manifest: str,
        raw_workload_root: object,
    ) -> ManifestSparsePathPlanner:
        try:
            workload_root = infer_workload_root(raw_manifest, repo_root, raw_workload_root)
            initial_blockers: tuple[SparsePathIssue, ...] = ()
        except SparsePathValidationError as exc:
            workload_root = repo_root
            initial_blockers = (
                SparsePathIssue(
                    code="invalid_path",
                    message=str(exc),
                    path=str(raw_workload_root or ""),
                    source="--workload-root",
                ),
            )
        return cls(
            reader=reader,
            repo_root=repo_root,
            workload_root=workload_root,
            initial_blockers=initial_blockers,
        )

    @property
    def policy(self) -> SparsePathPolicy:
        return self._policy

    def build_report(
        self,
        *,
        raw_manifest: str,
        include_global_overrides: bool,
        include_env_overrides: Sequence[object],
        include_registry: bool,
        registry_paths: Sequence[object],
        support_paths: Sequence[object],
    ) -> SparsePathReport:
        try:
            manifest_path = self._policy.resolve_user_path(raw_manifest, source="path")
        except SparsePathValidationError as exc:
            return SparsePathReport(
                manifest=raw_manifest,
                workload_root=self._policy.repo_relative(self._policy.workload_root),
                entries=(),
                blockers=(
                    *self._initial_blockers,
                    SparsePathIssue(code="invalid_path", message=str(exc), path=raw_manifest, source="path"),
                ),
            )

        report = self._discovery.discover(manifest_path)
        report = self._append_requested_paths(
            report,
            include_global_overrides=include_global_overrides,
            include_env_overrides=include_env_overrides,
            include_registry=include_registry,
            registry_paths=registry_paths,
            support_paths=support_paths,
        )
        if not self._initial_blockers:
            return report
        return SparsePathReport(
            manifest=report.manifest,
            workload_root=report.workload_root,
            entries=report.entries,
            warnings=report.warnings,
            blockers=(*self._initial_blockers, *report.blockers),
        )

    def _append_requested_paths(
        self,
        report: SparsePathReport,
        *,
        include_global_overrides: bool,
        include_env_overrides: Sequence[object],
        include_registry: bool,
        registry_paths: Sequence[object],
        support_paths: Sequence[object],
    ) -> SparsePathReport:
        entries = list(report.entries)
        seen = {entry.path for entry in entries}
        warnings = list(report.warnings)
        blockers = list(report.blockers)

        def add(entry: SparsePathEntry) -> None:
            if entry.path in seen:
                return
            seen.add(entry.path)
            entries.append(entry)
            if not entry.exists:
                warnings.append(
                    SparsePathIssue(
                        code="missing_path",
                        message="Requested optional sparse path does not exist",
                        path=entry.path,
                        source=entry.source,
                    )
                )

        def add_blocker(exc: Exception, *, raw: object, source: str) -> None:
            blockers.append(SparsePathIssue(code="invalid_path", message=str(exc), path=str(raw), source=source))

        workload_root = self._policy.workload_root
        if include_global_overrides:
            add(
                _entry(
                    self._policy.entry_for_resolved_path(
                        workload_root / "overrides" / "global.yaml",
                        kind="global_override",
                        source="--include-global-overrides",
                        required=False,
                        reason="standard workload global override",
                    ),
                    kind="global_override",
                    source="--include-global-overrides",
                    required=False,
                    reason="standard workload global override",
                )
            )

        for env in include_env_overrides:
            try:
                env_segment = _validate_env_segment(env)
                add(
                    _entry(
                        self._policy.entry_for_resolved_path(
                            workload_root / "overrides" / f"{env_segment}.yaml",
                            kind="env_override",
                            source="--include-env-overrides",
                            required=False,
                            reason=f"standard workload environment override for {env_segment}",
                        ),
                        kind="env_override",
                        source="--include-env-overrides",
                        required=False,
                        reason=f"standard workload environment override for {env_segment}",
                    )
                )
            except SparsePathValidationError as exc:
                add_blocker(exc, raw=env, source="--include-env-overrides")

        if include_registry:
            add(
                _entry(
                    self._policy.entry_for_resolved_path(
                        workload_root / "registry",
                        kind="registry_dir",
                        source="--include-registry",
                        required=False,
                        reason="standard workload registry directory",
                        is_dir=True,
                    ),
                    kind="registry_dir",
                    source="--include-registry",
                    required=False,
                    reason="standard workload registry directory",
                )
            )

        for registry in registry_paths:
            try:
                add(
                    _entry(
                        self._policy.entry_for_user_path(
                            registry,
                            kind="registry",
                            source="--registry",
                            required=False,
                            reason="explicit registry path",
                        ),
                        kind="registry",
                        source="--registry",
                        required=False,
                        reason="explicit registry path",
                    )
                )
            except SparsePathValidationError as exc:
                add_blocker(exc, raw=registry, source="--registry")

        for support_path in support_paths:
            try:
                add(
                    _entry(
                        self._policy.entry_for_user_path(
                            support_path,
                            kind="support_path",
                            source="--support-path",
                            required=False,
                            reason="user supplied support path",
                        ),
                        kind="support_path",
                        source="--support-path",
                        required=False,
                        reason="user supplied support path",
                    )
                )
            except SparsePathValidationError as exc:
                add_blocker(exc, raw=support_path, source="--support-path")

        return SparsePathReport(
            manifest=report.manifest,
            workload_root=report.workload_root,
            entries=tuple(entries),
            warnings=tuple(warnings),
            blockers=tuple(blockers),
        )


def _validate_env_segment(raw: object) -> str:
    text = str(raw or "").strip()
    if not text:
        raise SparsePathValidationError("--include-env-overrides must not be empty")
    if text in {".", ".."} or "/" in text or "\\" in text or ".." in text:
        raise SparsePathValidationError("--include-env-overrides must be a single safe path segment")
    return text


def _entry(
    resolved: ResolvedSparsePath,
    *,
    kind: str,
    source: str,
    required: bool,
    reason: str,
) -> SparsePathEntry:
    return SparsePathEntry(
        path=resolved.path,
        kind=kind,
        source=source,
        required=required,
        exists=resolved.exists,
        is_dir=resolved.is_dir,
        reason=reason,
    )


__all__ = ["ManifestSparsePathPlanner"]
