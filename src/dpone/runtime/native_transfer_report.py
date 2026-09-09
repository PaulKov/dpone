"""Native transfer runtime report rendering."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dpone.runtime.commit_unknown import CommitUnknownError
from dpone.runtime.native_transfer_quality_evidence import (
    normalize_native_quality_scope_summary,
)

_REPORT_SCHEMA_VERSION = "dpone.native_transfer.runtime_report.v1"


def build_runtime_report_payload(context: Any, load_result: Any) -> dict[str, Any]:
    """Build the bounded native-transfer acceptance report payload."""

    payload = {
        "schema_version": _REPORT_SCHEMA_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),  # noqa: UP017
        "run_id": context.run_id,
        "source_type": (context.load_config.options or {}).get("source_type", "unknown"),
        "sink_type": (context.load_config.options or {}).get("sink_type", "unknown"),
        "strategy": context.load_config.load_strategy.value,
        "success_scope": "native_transfer_acceptance",
        "checkpoint_summary": context.checkpoint_store.summary() if context.checkpoint_store else {},
        "resume_plan": context.resume_plan.to_dict(),
        "load_result": {
            "inserted_rows": load_result.inserted_rows,
            "updated_rows": load_result.updated_rows,
            "total_rows": load_result.total_rows,
            "staging_rows": load_result.staging_rows,
        },
        "passed": True,
    }
    quality_scope = _quality_scope_summary(load_result)
    if quality_scope is not None:
        payload["quality_scope"] = quality_scope
    return payload


def render_runtime_report_markdown(payload: dict[str, Any]) -> str:
    """Render a compact Markdown runtime report."""

    if payload.get("passed") is False:
        return "\n".join(
            [
                f"# Native transfer terminal report: {payload['run_id']}",
                "",
                f"- status: `{payload.get('status', 'failed')}`",
                f"- code: `{payload.get('code', 'native_transfer_failed')}`",
                "- automatic retry: `blocked`",
                "- recovery: `operator verification required`",
                "",
            ]
        )
    resume = payload["resume_plan"]["summary"]
    return "\n".join(
        [
            f"# Native transfer runtime report: {payload['run_id']}",
            "",
            f"- schema_version: `{payload['schema_version']}`",
            f"- strategy: `{payload['strategy']}`",
            f"- resume skip: `{resume.get('skip', 0)}`",
            f"- resume retry: `{resume.get('retry', 0)}`",
            f"- inserted_rows: `{payload['load_result']['inserted_rows']}`",
            f"- total_rows: `{payload['load_result']['total_rows']}`",
            "",
        ]
    )


def publish_runtime_report_bundle(
    artifact_dir: Path,
    *,
    base_name: str,
    payload: dict[str, Any],
) -> tuple[Path, Path]:
    """Publish Markdown first and the authoritative success JSON last."""

    artifact_dir.mkdir(parents=True, exist_ok=True)
    json_path = artifact_dir / f"{base_name}.json"
    markdown_path = artifact_dir / f"{base_name}.md"
    json_path.unlink(missing_ok=True)
    json_temp: Path | None = None
    markdown_temp: Path | None = None
    try:
        markdown_temp = _write_temp(
            artifact_dir,
            suffix=".md.tmp",
            content=render_runtime_report_markdown(payload),
        )
        json_temp = _write_temp(
            artifact_dir,
            suffix=".json.tmp",
            content=json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        )
        os.replace(markdown_temp, markdown_path)
        markdown_temp = None
        os.replace(json_temp, json_path)
        json_temp = None
        _fsync_directory(artifact_dir)
        return json_path, markdown_path
    finally:
        if markdown_temp is not None:
            markdown_temp.unlink(missing_ok=True)
        if json_temp is not None:
            json_temp.unlink(missing_ok=True)


def publish_native_transfer_runtime_report(
    artifact_dir: Path,
    *,
    run_id: str,
    context: Any,
    load_result: Any,
) -> tuple[Path, Path]:
    """Publish one bounded report using a filesystem-safe run identity."""

    base_name = _runtime_report_base_name(run_id)
    return publish_runtime_report_bundle(
        artifact_dir,
        base_name=base_name,
        payload=build_runtime_report_payload(context, load_result),
    )


def publish_native_transfer_commit_unknown_report(
    artifact_dir: Path,
    *,
    run_id: str,
    error: CommitUnknownError,
) -> tuple[Path, Path]:
    """Replace any optimistic report with the authoritative terminal outcome."""

    payload: dict[str, Any] = {
        "schema_version": _REPORT_SCHEMA_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),  # noqa: UP017
        "run_id": run_id,
        "status": error.code,
        "code": error.code,
        "passed": False,
        **error.outcome.to_jsonable(),
    }
    return publish_runtime_report_bundle(
        artifact_dir,
        base_name=_runtime_report_base_name(run_id),
        payload=payload,
    )


def _runtime_report_base_name(run_id: str) -> str:
    return "native_transfer_runtime_" + "".join(
        char if char.isalnum() or char in {"-", "_"} else "_" for char in run_id
    )


def _write_temp(directory: Path, *, suffix: str, content: str) -> Path:
    descriptor, raw_path = tempfile.mkstemp(
        prefix=".native-transfer-report-",
        suffix=suffix,
        dir=directory,
        text=True,
    )
    path = Path(raw_path)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        path.unlink(missing_ok=True)
        raise
    return path


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _quality_scope_summary(load_result: Any) -> dict[str, object] | None:
    metrics = getattr(load_result, "reconciliation_metrics", None)
    if not isinstance(metrics, Mapping) or "native_transfer_quality_scope" not in metrics:
        return None
    return normalize_native_quality_scope_summary(metrics["native_transfer_quality_scope"])


__all__ = [
    "build_runtime_report_payload",
    "publish_native_transfer_commit_unknown_report",
    "publish_native_transfer_runtime_report",
    "publish_runtime_report_bundle",
    "render_runtime_report_markdown",
]
