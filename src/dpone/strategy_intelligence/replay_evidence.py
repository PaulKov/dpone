from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dpone.strategy_intelligence.replay_adapters import ReplayAdapterResult

SCHEMA_VERSION = "dpone.replay.evidence.v1"
RUNBOOK_PATH = "docs/testing/replay-integration.md"
RUNBOOK_MD_LINK = "../../docs/testing/replay-integration.md"


class ReplayEvidenceWriter:
    """Write audit-friendly replay evidence artifacts."""

    def __init__(self, artifact_dir: str | Path) -> None:
        self._artifact_dir = Path(artifact_dir)

    def write(
        self,
        *,
        action: str,
        run_id: str,
        service: str,
        source_type: str,
        strategy_mode: str,
        mode: str,
        executed: bool,
        status: str,
        artifact_path: Path,
        commands: tuple[str, ...],
        operations: tuple[str, ...],
        state_committed: bool,
        adapter_result: ReplayAdapterResult | None,
    ) -> tuple[Path, Path]:
        self._artifact_dir.mkdir(parents=True, exist_ok=True)
        evidence = self._payload(
            action=action,
            run_id=run_id,
            service=service,
            source_type=source_type,
            strategy_mode=strategy_mode,
            mode=mode,
            executed=executed,
            status=status,
            artifact_path=artifact_path,
            commands=commands,
            operations=operations,
            state_committed=state_committed,
            adapter_result=adapter_result,
        )
        base = f"{action}_{_safe_name(run_id)}_evidence"
        json_path = self._artifact_dir / f"{base}.json"
        md_path = self._artifact_dir / f"{base}.md"
        json_path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        md_path.write_text(_render_markdown(evidence), encoding="utf-8")
        return json_path, md_path

    def _payload(
        self,
        *,
        action: str,
        run_id: str,
        service: str,
        source_type: str,
        strategy_mode: str,
        mode: str,
        executed: bool,
        status: str,
        artifact_path: Path,
        commands: tuple[str, ...],
        operations: tuple[str, ...],
        state_committed: bool,
        adapter_result: ReplayAdapterResult | None,
    ) -> dict[str, Any]:
        diagnostics = adapter_result.to_dict() if adapter_result is not None else {}
        return {
            "schema_version": SCHEMA_VERSION,
            "created_at": datetime.now(UTC).isoformat(),
            "action": action,
            "run_id": run_id,
            "service": service,
            "source_type": source_type,
            "strategy_mode": strategy_mode,
            "mode": mode,
            "executed": executed,
            "status": status,
            "state_committed": state_committed,
            "artifact_path": str(artifact_path),
            "runbook": RUNBOOK_PATH,
            "commands": list(commands),
            "operations": list(operations),
            "row_count_checks": _extract_checks(diagnostics, key="row_count_check", plural_key="row_count_checks"),
            "status_checks": _extract_checks(diagnostics, key="status_check", plural_key="status_checks"),
            "diagnostics": diagnostics,
        }


def _extract_checks(diagnostics: dict[str, Any], *, key: str, plural_key: str) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    for item in diagnostics.get("diagnostics", ()):
        details = item.get("details") or {}
        single = details.get(key)
        if isinstance(single, dict):
            checks.append(dict(single))
        many = details.get(plural_key)
        if isinstance(many, list):
            checks.extend(dict(check) for check in many if isinstance(check, dict))
    return checks


def _render_markdown(evidence: dict[str, Any]) -> str:
    lines = [
        f"# Replay evidence: {evidence['action']} {evidence['run_id']}",
        "",
        f"- schema_version: `{evidence['schema_version']}`",
        f"- service: `{evidence['service']}`",
        f"- strategy_mode: `{evidence['strategy_mode']}`",
        f"- mode: `{evidence['mode']}`",
        f"- executed: `{evidence['executed']}`",
        f"- status: `{evidence['status']}`",
        f"- state_committed: `{evidence['state_committed']}`",
        f"- artifact_path: `{evidence['artifact_path']}`",
        f"- runbook: [Replay integration runbook]({RUNBOOK_MD_LINK})",
        "",
        "## Commands",
        "",
        *[f"- `{command}`" for command in evidence["commands"]],
        "",
        "## Operations",
        "",
        *[f"- `{operation}`" for operation in evidence["operations"]],
        "",
        "## Row count checks",
        "",
        "| Name | Value |",
        "|---|---|",
        *_table_rows(evidence["row_count_checks"], value_key="value"),
        "",
        "## Status checks",
        "",
        "| Name | Status |",
        "|---|---|",
        *_table_rows(evidence["status_checks"], value_key="status"),
        "",
    ]
    return "\n".join(lines)


def _table_rows(checks: list[dict[str, Any]], *, value_key: str) -> list[str]:
    if not checks:
        return ["| none | n/a |"]
    return [f"| {check.get('name', 'unnamed')} | {check.get(value_key, 'n/a')} |" for check in checks]


def _safe_name(value: str) -> str:
    return "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in value)
