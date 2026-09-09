"""Schema evolution notification artifacts."""

from __future__ import annotations

import json
from pathlib import Path


class SchemaNotificationService:
    def __init__(self, output_dir: str | Path) -> None:
        self.output_dir = Path(output_dir)

    def write(self, *, ledger_path: str | Path, channel: str = "artifact") -> dict[str, object]:
        ledger = Path(ledger_path)
        payload = json.loads(ledger.read_text(encoding="utf-8"))
        self.output_dir.mkdir(parents=True, exist_ok=True)
        stem = ledger.stem
        json_path = self.output_dir / f"{stem}.notification.json"
        markdown_path = self.output_dir / f"{stem}.notification.md"
        report = {
            "channel": channel,
            "status": payload.get("status", "unknown"),
            "ledger_path": str(ledger),
            "blockers": payload.get("blockers", []),
            "json_path": str(json_path),
            "markdown_path": str(markdown_path),
        }
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        markdown_path.write_text(_markdown(report, payload), encoding="utf-8")
        return report


def _markdown(report: dict[str, object], payload: dict[str, object]) -> str:
    lines = [
        "# Schema evolution notification",
        "",
        f"- Channel: `{report['channel']}`",
        f"- Status: `{report['status']}`",
        f"- Ledger: `{report['ledger_path']}`",
        "",
        "## Blockers",
        "",
    ]
    blockers = payload.get("blockers", [])
    if isinstance(blockers, list) and blockers:
        lines.extend(f"- `{item}`" for item in blockers)
    else:
        lines.append("- none")
    lines.extend(["", "## Actions", ""])
    actions = payload.get("actions", [])
    if isinstance(actions, list):
        for action in actions:
            if isinstance(action, dict):
                lines.append(
                    f"- `{action.get('column', '-')}` risk `{action.get('risk_level', '-')}` decision `{action.get('decision', '-')}`"
                )
    return "\n".join(lines) + "\n"
