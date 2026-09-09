"""Run observability artifact writer."""

from __future__ import annotations

import html
import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from dpone._compat import UTC
from dpone.readiness.managed_models import RunArtifactPaths
from dpone.readiness.managed_utils import _redact, _safe_filename


class RunArtifactWriter:
    """Writes local run observability artifacts."""

    def __init__(self, base_dir: str | Path = ".dpone/runs") -> None:
        self.base_dir = Path(base_dir)

    def write(
        self,
        *,
        run_id: str,
        pipeline: str,
        timeline: Sequence[Mapping[str, Any]],
        state: Mapping[str, Any] | None = None,
        quality: Mapping[str, Any] | None = None,
        schema_evolution: Mapping[str, Any] | None = None,
        reconciliation: Mapping[str, Any] | None = None,
    ) -> RunArtifactPaths:
        payload = _redact(
            {
                "run_id": run_id,
                "pipeline": pipeline,
                "created_at": datetime.now(UTC).isoformat(),
                "timeline": [dict(item) for item in timeline],
                "state": dict(state or {}),
                "quality": dict(quality or {}),
                "schema_evolution": dict(schema_evolution or {}),
                "reconciliation": dict(reconciliation or {}),
            }
        )
        directory = self.base_dir / _safe_filename(run_id)
        directory.mkdir(parents=True, exist_ok=True)
        json_path = directory / "run.json"
        md_path = directory / "run.md"
        html_path = directory / "run.html"
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
        md_path.write_text(_render_run_markdown(payload), encoding="utf-8")
        html_path.write_text(_render_run_html(payload), encoding="utf-8")
        return RunArtifactPaths(run_id=run_id, json_path=json_path, markdown_path=md_path, html_path=html_path)


def _render_run_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# dpone run report",
        "",
        f"- run_id: `{payload['run_id']}`",
        f"- pipeline: `{payload['pipeline']}`",
        "",
        "## Timeline",
        "",
    ]
    for item in payload.get("timeline", []):
        lines.append(f"- {item}")
    lines.extend(
        [
            "",
            "## State",
            "",
            f"```json\n{json.dumps(payload.get('state', {}), indent=2, ensure_ascii=False, default=str)}\n```",
            "",
        ]
    )
    return "\n".join(lines)


def _render_run_html(payload: Mapping[str, Any]) -> str:
    body = html.escape(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return f"<!doctype html><html><head><meta charset='utf-8'><title>dpone run {html.escape(str(payload['run_id']))}</title></head><body><h1>dpone run report</h1><pre>{body}</pre></body></html>\n"


__all__ = ["RunArtifactWriter"]
