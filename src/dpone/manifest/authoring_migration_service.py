"""Plan-first application service for semantically checked authoring migration."""

from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import cast

from dpone.manifest.authoring import (
    AuthoringCompilation,
    AuthoringCompilationError,
    AuthoringCompiler,
    default_authoring_compiler,
)
from dpone.manifest.authoring_migration_io import (
    AuthoringMigrationFileSystem,
    AuthoringMigrationIoError,
    AuthoringSourceSnapshot,
)
from dpone.manifest.authoring_migration_models import (
    AuthoringMigrationIdentity,
    AuthoringMigrationResult,
    AuthoringMode,
)
from dpone.manifest.authoring_migration_rendering import (
    AuthoringMigrationCandidate,
    AuthoringModeRenderer,
    CandidateFolderLoader,
)
from dpone.manifest.authoring_migration_reporting import (
    blocked_from_compilation_result,
    blocked_result,
    blocked_without_source_result,
    migration_changes,
    migration_plan_id,
    no_op_result,
    with_apply_error,
    yaml_bytes,
)

_MODES = frozenset({"classic", "flow", "folder"})
_RETAINED_WARNING = "DPONE_AUTHORING_MIGRATION_OLD_FRAGMENTS_RETAINED"


class AuthoringMigrationService:
    """Coordinate pure rendering, canonical verification and confined apply."""

    def __init__(
        self,
        *,
        root: str | Path = ".",
        filesystem: AuthoringMigrationFileSystem | None = None,
        renderer: AuthoringModeRenderer | None = None,
    ) -> None:
        self._filesystem = filesystem or AuthoringMigrationFileSystem(root)
        self._root = self._filesystem.root
        self._renderer = renderer or AuthoringModeRenderer()

    def migrate(
        self,
        *,
        target: str | Path,
        target_mode: str,
        expected_source_mode: str | None = None,
        apply: bool,
    ) -> AuthoringMigrationResult:
        normalized_target = _mode(target_mode)
        normalized_expected = _mode(expected_source_mode) if expected_source_mode else None
        try:
            snapshot = self._filesystem.read_source(target)
        except AuthoringMigrationIoError as exc:
            return blocked_without_source_result(
                mode="apply" if apply else "plan",
                target_mode=normalized_target,
                code=exc.code,
                message=str(exc),
                exit_code=4 if exc.code.endswith("PATH_INVALID") else 1,
            )
        if "recipe" in snapshot.payload:
            return blocked_result(
                snapshot=snapshot,
                source_mode=normalized_expected or normalized_target,
                target_mode=normalized_target,
                mode="apply" if apply else "plan",
                code="DPONE_AUTHORING_MIGRATION_RECIPE_UNSUPPORTED",
                message="Recipe-authored pipelines cannot be materialized implicitly during authoring migration.",
                exit_code=1,
            )
        try:
            before = default_authoring_compiler().compile(
                snapshot.payload,
                source_path=snapshot.path,
                project_root=self._root,
            )
        except Exception as exc:  # noqa: BLE001 - normalize the canonical compiler boundary.
            code = (
                exc.code if isinstance(exc, AuthoringCompilationError) else "DPONE_AUTHORING_MIGRATION_SOURCE_INVALID"
            )
            return blocked_result(
                snapshot=snapshot,
                source_mode=normalized_expected or normalized_target,
                target_mode=normalized_target,
                mode="apply" if apply else "plan",
                code=code,
                message=str(exc),
                exit_code=1,
            )
        source_mode = _mode(before.authoring_mode)
        if normalized_expected is not None and normalized_expected != source_mode:
            return blocked_result(
                snapshot=snapshot,
                source_mode=source_mode,
                target_mode=normalized_target,
                mode="apply" if apply else "plan",
                code="DPONE_AUTHORING_MIGRATION_SOURCE_MODE_MISMATCH",
                message=f"Detected authoring mode is {source_mode}, not {normalized_expected}.",
                exit_code=2,
                semantic_fingerprint=before.semantic_fingerprint,
            )
        if source_mode == normalized_target:
            return no_op_result(snapshot, before, apply=apply)

        candidate = self._renderer.render(
            source_payload=snapshot.payload,
            compilation=before,
            source_ref=snapshot.relative_path,
            target_mode=normalized_target,
        )
        desired_root = yaml_bytes(candidate.root_payload)
        desired_fragment = yaml_bytes(candidate.fragment_payload) if candidate.fragment_payload else None
        fragment_relative = _fragment_relative(snapshot, candidate)
        try:
            fragment_existing = self._filesystem.read_optional(fragment_relative) if fragment_relative else None
        except AuthoringMigrationIoError as exc:
            return blocked_from_compilation_result(
                snapshot,
                before,
                target_mode=normalized_target,
                mode="apply" if apply else "plan",
                code=exc.code,
                message=str(exc),
                exit_code=4,
            )
        if desired_fragment is not None and fragment_existing not in {None, desired_fragment}:
            return blocked_from_compilation_result(
                snapshot,
                before,
                target_mode=normalized_target,
                mode="apply" if apply else "plan",
                code="DPONE_AUTHORING_MIGRATION_FILE_CONFLICT",
                message="processes.yaml exists with different user-owned content.",
                exit_code=4,
            )
        try:
            after = _compile_candidate(
                candidate,
                source_path=snapshot.path,
                project_root=self._root,
            )
        except Exception as exc:  # noqa: BLE001 - candidate failure is a stable migration blocker.
            return blocked_from_compilation_result(
                snapshot,
                before,
                target_mode=normalized_target,
                mode="apply" if apply else "plan",
                code="DPONE_AUTHORING_MIGRATION_TARGET_INVALID",
                message=str(exc),
                exit_code=1,
            )
        if before.semantic_fingerprint != after.semantic_fingerprint:
            return blocked_from_compilation_result(
                snapshot,
                before,
                target_mode=normalized_target,
                mode="apply" if apply else "plan",
                code="DPONE_AUTHORING_MIGRATION_SEMANTIC_DRIFT",
                message="Target authoring source does not preserve the canonical semantic fingerprint.",
                exit_code=4,
                target_fingerprint=after.semantic_fingerprint,
            )

        changes = migration_changes(
            snapshot=snapshot,
            candidate=candidate,
            desired_root=desired_root,
            fragment_relative=fragment_relative,
            desired_fragment=desired_fragment,
            fragment_existing=fragment_existing,
        )
        warnings = (_RETAINED_WARNING,) if candidate.retained_files else ()
        plan_id = migration_plan_id(
            snapshot=snapshot,
            source_mode=source_mode,
            target_mode=normalized_target,
            semantic_fingerprint=before.semantic_fingerprint,
            changes=changes,
            retained_files=candidate.retained_files,
        )
        result = AuthoringMigrationResult(
            mode="apply" if apply else "plan",
            status="ready",
            plan_id=plan_id,
            source=AuthoringMigrationIdentity(
                mode=source_mode,
                path=snapshot.relative_path,
                sha256=snapshot.sha256,
                semantic_fingerprint=before.semantic_fingerprint,
            ),
            target=AuthoringMigrationIdentity(
                mode=normalized_target,
                semantic_fingerprint=after.semantic_fingerprint,
            ),
            changes=changes,
            retained_files=candidate.retained_files,
            warnings=warnings,
        )
        if not apply:
            return result
        try:
            outcome = self._filesystem.apply(
                source=snapshot,
                desired_source=desired_root,
                fragment_relative=fragment_relative,
                desired_fragment=desired_fragment,
            )
            applied_snapshot = self._filesystem.read_source(snapshot.relative_path)
            applied = default_authoring_compiler().compile(
                applied_snapshot.payload,
                source_path=applied_snapshot.path,
                project_root=self._root,
            )
        except AuthoringMigrationIoError as exc:
            return with_apply_error(result, code=exc.code, message=str(exc), exit_code=4)
        except OSError:
            return with_apply_error(
                result,
                code="DPONE_AUTHORING_MIGRATION_APPLY_FAILED",
                message="Authoring migration could not be committed safely.",
                exit_code=4,
            )
        except Exception as exc:  # noqa: BLE001 - post-write verification must fail closed.
            return with_apply_error(
                result,
                code="DPONE_AUTHORING_MIGRATION_POST_VERIFY_FAILED",
                message=str(exc),
                exit_code=4,
            )
        if applied.semantic_fingerprint != before.semantic_fingerprint or applied.authoring_mode != normalized_target:
            return with_apply_error(
                result,
                code="DPONE_AUTHORING_MIGRATION_POST_VERIFY_FAILED",
                message="Applied source failed semantic or authority verification; restore it from source control.",
                exit_code=4,
            )
        return AuthoringMigrationResult(
            mode="apply",
            status="applied",
            plan_id=plan_id,
            source=result.source,
            target=AuthoringMigrationIdentity(
                mode=normalized_target,
                path=snapshot.relative_path,
                sha256=outcome.source_sha256,
                semantic_fingerprint=applied.semantic_fingerprint,
            ),
            changes=changes,
            retained_files=candidate.retained_files,
            warnings=warnings,
        )


def _compile_candidate(
    candidate: AuthoringMigrationCandidate,
    *,
    source_path: Path,
    project_root: Path,
) -> AuthoringCompilation:
    if candidate.fragment_payload is None:
        compiler = AuthoringCompiler()
    else:
        compiler = AuthoringCompiler(
            folder_loader=CandidateFolderLoader(
                candidate.fragment_payload,
                fragment_name=candidate.fragment_name or "processes.yaml",
            )
        )
    return compiler.compile(
        candidate.root_payload,
        source_path=source_path,
        project_root=project_root,
        include_content_dependencies=False,
    )


def _fragment_relative(snapshot: AuthoringSourceSnapshot, candidate: AuthoringMigrationCandidate) -> str | None:
    if not candidate.fragment_name:
        return None
    return (PurePosixPath(snapshot.relative_path).parent / candidate.fragment_name).as_posix()


def _mode(value: str | None) -> AuthoringMode:
    normalized = str(value or "").strip().lower()
    if normalized not in _MODES:
        raise ValueError("Authoring mode must be classic, flow, or folder.")
    return cast(AuthoringMode, normalized)


__all__ = ["AuthoringMigrationService"]
