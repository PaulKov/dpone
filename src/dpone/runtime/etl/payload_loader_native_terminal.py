"""Native-transfer terminal transitions used by the payload loader."""

from __future__ import annotations

from typing import Any, NoReturn

from dpone.runtime.commit_unknown import classify_runtime_commit_unknown
from dpone.runtime.etl.payload_loader_invocation import accepts_keyword
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome


def record_target_success(runtime_service: Any, context: Any, load_result: Any) -> Any:
    """Record that the target returned before durable checkpoint persistence."""

    record = getattr(runtime_service, "record_target_success", None)
    return record(context, load_result) if callable(record) else load_result


def commit_checkpoints(runtime_service: Any, context: Any, load_result: Any) -> tuple[Any, bool]:
    """Commit native checkpoints and report whether that boundary was present."""

    commit = getattr(runtime_service, "commit_checkpoints", None)
    if callable(commit):
        return commit(context, load_result), bool(getattr(context, "enabled", True))
    mark_committed = getattr(runtime_service, "mark_committed", None)
    if callable(mark_committed):
        return mark_committed(context, load_result), bool(getattr(context, "enabled", True))
    return load_result, False


def publish_success_report(runtime_service: Any, context: Any, load_result: Any) -> Any:
    """Publish optional native-transfer success evidence."""

    publish = getattr(runtime_service, "publish_success_report", None)
    return publish(context, load_result) if callable(publish) else load_result


def require_committed_success_handling(error: Exception) -> None:
    """Mark a post-checkpoint reporting failure as retry-visible.

    The original exception identity is preserved because native resume must retry
    only the missing report, while the processor must not downgrade it to a
    generic committed-secondary warning.
    """

    error.blocks_committed_success = True  # type: ignore[attr-defined]
    error.artifact_terminal_outcome = ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN  # type: ignore[attr-defined]


def native_quality_scope(runtime_service: Any, context: Any) -> Any | None:
    """Resolve the bounded quality scope owned by the native transfer."""

    quality_scope = getattr(runtime_service, "quality_scope", None)
    if callable(quality_scope) and (scope := quality_scope(context)) is not None:
        return scope
    from dpone.runtime.native_transfer_quality_scope import (
        NativeTransferQualityScope,
        has_slice_evidence,
    )

    artifact = getattr(getattr(context, "payload", None), "artifact", None)
    if has_slice_evidence(artifact):
        return NativeTransferQualityScope.from_slice_evidence_artifact(artifact)
    return None


def before_target_mutation(runtime_service: Any, context: Any) -> None:
    """Invoke the optional native-transfer target mutation guard."""

    before_target = getattr(runtime_service, "before_target_mutation", None)
    if callable(before_target):
        before_target(context)


def mark_failed_preserving_primary(runtime_service: Any, context: Any, exc: Exception) -> None:
    """Persist a safe native failure without replacing its primary exception."""

    try:
        _mark_failed(runtime_service, context, exc)
    except Exception:
        return


def mark_native_failed(runtime_service: Any, context: Any, exc: Exception) -> None:
    """Invoke the native failure adapter once and expose implementation errors.

    This strict compatibility entrypoint is intentionally separate from the
    best-effort primary-error preservation used by the load lifecycle.
    """

    _mark_failed(runtime_service, context, exc)


def _mark_failed(runtime_service: Any, context: Any, exc: Exception) -> None:
    mark_failed = getattr(runtime_service, "mark_failed", None)
    if not callable(mark_failed):
        return
    code = getattr(exc, "code", None)
    safe_code = code if isinstance(code, str) else "native_transfer_failed"
    if accepts_keyword(mark_failed, "safe_error_code"):
        mark_failed(context, exc, safe_error_code=safe_code)
        return
    mark_failed(context, exc)


def raise_commit_unknown_if_needed(
    runtime_service: Any,
    context: Any,
    primary_error: Exception,
) -> None:
    """Persist a terminal receipt and stop retry when target state is unknown."""

    failure = classify_runtime_commit_unknown(runtime_service, context, primary_error)
    if failure is None:
        return
    publish = getattr(runtime_service, "publish_commit_unknown_report", None)
    if callable(publish):
        try:
            publish(context, failure)
        except Exception:
            add_note = getattr(failure, "add_note", None)
            if callable(add_note):
                add_note("terminal COMMIT_UNKNOWN evidence could not be persisted")
    _raise_commit_unknown(failure, primary_error)


def _raise_commit_unknown(failure: Exception, primary_error: Exception) -> NoReturn:
    raise failure from primary_error


__all__ = [
    "before_target_mutation",
    "commit_checkpoints",
    "mark_native_failed",
    "mark_failed_preserving_primary",
    "native_quality_scope",
    "publish_success_report",
    "require_committed_success_handling",
    "raise_commit_unknown_if_needed",
    "record_target_success",
]
