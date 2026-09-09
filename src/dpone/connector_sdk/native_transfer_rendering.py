from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dpone.connector_sdk.certification_artifacts import CertificationArtifactPublisher


class TransportEvidenceRenderer:
    """Render connector native-transfer capability evidence for CLI and CI."""

    def render_json(self, report: Any) -> str:
        return json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n"

    def render_markdown(self, report: Any) -> str:
        payload = report.to_dict()
        lines = [
            "# dpone connector capability certification",
            "",
            f"- connector: `{payload['connector']}`",
            f"- connector_type: `{payload['connector_type']}`",
            f"- profile: `{payload['profile']}`",
            f"- status: `{payload['status']}`",
        ]
        blockers = payload.get("blockers") or []
        if blockers:
            lines.extend(["", "## Blockers", ""])
            lines.extend(f"- `{blocker}`" for blocker in blockers)
        lines.extend(["", "## Capabilities", ""])
        for name, result in payload["capabilities"].items():
            lines.append(f"- `{name}`: `{result['status']}`")
            for case in result.get("cases") or []:
                lines.append(f"  - `{case['name']}`: `{case['status']}`")
        return "\n".join(lines) + "\n"

    def write(self, report: Any, artifact_dir: str | Path) -> tuple[Path, Path]:
        return CertificationArtifactPublisher().publish(
            artifact_dir,
            base_name="connector-capability-certification",
            json_content=self.render_json(report),
            markdown_content=self.render_markdown(report),
        )


__all__ = ["TransportEvidenceRenderer"]
