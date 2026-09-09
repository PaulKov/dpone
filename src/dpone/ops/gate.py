"""Go-live gate decisions based on operational evidence."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass

from dpone.ops.evidence import OpsEvidenceBundle, evidence_item_passed


@dataclass(frozen=True, slots=True)
class GoLiveDecision:
    passed: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone ops go-live gate",
            "",
            f"- Passed: `{self.passed}`",
            f"- Blockers: `{len(self.blockers)}`",
            f"- Warnings: `{len(self.warnings)}`",
            "",
        ]
        if self.blockers:
            lines.extend(
                [
                    "## Blockers",
                    "",
                    "Fix failed evidence before go-live:",
                    "",
                ]
            )
            lines.extend(f"- `{blocker}`" for blocker in self.blockers)
            lines.append("")
        if self.warnings:
            lines.extend(["## Warnings", ""])
            lines.extend(f"- `{warning}`" for warning in self.warnings)
            lines.append("")
        if not self.blockers and not self.warnings:
            lines.append("All required evidence passed.")
            lines.append("")
        return "\n".join(lines)


class GoLiveGateService:
    """Evaluates whether a bundle is safe enough for a controlled go-live."""

    def evaluate(self, bundle: OpsEvidenceBundle) -> GoLiveDecision:
        blockers = tuple(
            dict.fromkeys(
                (
                    *(("bundle.empty",) if not bundle.items else ()),
                    *(item.name for item in bundle.items if item.required and not evidence_item_passed(item)),
                )
            )
        )
        warnings = tuple(item.name for item in bundle.items if not item.required and not evidence_item_passed(item))
        return GoLiveDecision(passed=not blockers and bundle.passed, blockers=blockers, warnings=warnings)
