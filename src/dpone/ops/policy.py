"""Policy-as-code evaluation for ops evidence bundles."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dpone.ops.evidence import OpsEvidenceBundle, evidence_item_passed


@dataclass(frozen=True, slots=True)
class OpsPolicyDecision:
    passed: bool
    violations: tuple[str, ...]
    actions: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone ops policy decision",
            "",
            f"- Passed: `{self.passed}`",
            f"- Violations: `{len(self.violations)}`",
            "",
        ]
        if self.violations:
            lines.extend(["| violation | action |", "|---|---|"])
            for violation, action in zip(self.violations, self.actions, strict=True):
                lines.append(f"| `{violation}` | {action} |")
        else:
            lines.append("All policy rules passed.")
        return "\n".join(lines) + "\n"


class OpsPolicyService:
    """Evaluates local JSON policies against immutable ops evidence."""

    def evaluate(self, bundle: OpsEvidenceBundle, policy: Mapping[str, Any]) -> OpsPolicyDecision:
        violations: list[str] = []
        actions: list[str] = []
        self._check_bundle_shape(bundle, violations, actions)
        self._check_bundle_status(bundle, policy, violations, actions)
        self._check_required_evidence(bundle, policy, violations, actions)
        self._check_required_items_passed(bundle, violations, actions)
        self._check_artifact_files(bundle, policy, violations, actions)
        return OpsPolicyDecision(
            passed=not violations,
            violations=tuple(violations),
            actions=tuple(actions),
        )

    @staticmethod
    def _check_bundle_shape(
        bundle: OpsEvidenceBundle,
        violations: list[str],
        actions: list[str],
    ) -> None:
        if not bundle.items:
            violations.append("bundle.empty")
            actions.append("Build a non-empty evidence bundle before go-live.")
            return
        names = [item.name for item in bundle.items if item.required]
        duplicates = tuple(dict.fromkeys(name for name in names if names.count(name) > 1))
        for name in duplicates:
            violations.append(f"duplicate_required_evidence.{name}")
            actions.append(f"Keep exactly one required evidence item named `{name}`.")

    def _check_bundle_status(
        self,
        bundle: OpsEvidenceBundle,
        policy: Mapping[str, Any],
        violations: list[str],
        actions: list[str],
    ) -> None:
        if bool(policy.get("require_bundle_passed", True)) and not bundle.passed:
            violations.append("bundle.failed")
            actions.append("Fix failed evidence and rebuild the ops evidence bundle.")

    def _check_required_evidence(
        self,
        bundle: OpsEvidenceBundle,
        policy: Mapping[str, Any],
        violations: list[str],
        actions: list[str],
    ) -> None:
        present = {item.name for item in bundle.items}
        for name in [str(item) for item in policy.get("required_evidence", [])]:
            if name not in present:
                violations.append(f"missing_required_evidence.{name}")
                actions.append(f"Add `{name}` to the evidence bundle or relax the policy.")

    def _check_required_items_passed(
        self,
        bundle: OpsEvidenceBundle,
        violations: list[str],
        actions: list[str],
    ) -> None:
        for item in bundle.items:
            if item.required and not evidence_item_passed(item):
                violations.append(f"required_evidence_failed.{item.name}")
                actions.append(f"Fix `{item.name}` before go-live.")

    def _check_artifact_files(
        self,
        bundle: OpsEvidenceBundle,
        policy: Mapping[str, Any],
        violations: list[str],
        actions: list[str],
    ) -> None:
        for item in bundle.items:
            if not Path(item.path).exists():
                violations.append(f"artifact_missing.{item.name}")
                actions.append(f"Restore or regenerate artifact `{item.path}`.")
            elif not evidence_item_passed(item):
                violations.append(f"artifact_untrusted.{item.name}")
                actions.append(f"Regenerate `{item.path}` and update its recorded checksum.")
