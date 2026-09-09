"""Provider-neutral diff contract for schema migration evidence bundles."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint

BUNDLE_DIFF_SCHEMA = "dpone.schema_migration_bundle_diff.v1"
BUNDLE_GATE_SCHEMA = "dpone.schema_migration_bundle_gate.v1"
_BREAKING_SEVERITIES = {"high", "critical"}


@dataclass(frozen=True, slots=True)
class MigrationBundleDiffOptions:
    require_attestation: bool = False
    block_target_mismatch: bool = True

    def to_dict(self) -> dict[str, bool]:
        return {
            "require_attestation": self.require_attestation,
            "block_target_mismatch": self.block_target_mismatch,
        }


@dataclass(frozen=True, slots=True)
class MigrationBundleDiffInputs:
    base_bundle: Mapping[str, Any]
    head_bundle: Mapping[str, Any]
    base_artifacts: Mapping[str, Mapping[str, Any]]
    head_artifacts: Mapping[str, Mapping[str, Any]]
    base_verification: Mapping[str, Any]
    head_verification: Mapping[str, Any]
    base_gate: Mapping[str, Any] | None = None
    head_gate: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class MigrationBundleDiffChange:
    kind: str
    path: str
    classification: str
    severity: str
    risk_tags: tuple[str, ...]
    base: Any
    head: Any
    reviewer_action: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "path": self.path,
            "classification": self.classification,
            "severity": self.severity,
            "risk_tags": list(self.risk_tags),
            "base": self.base,
            "head": self.head,
            "reviewer_action": self.reviewer_action,
        }


class MigrationBundleDiffBuilder:
    """Builds deterministic bundle diff evidence from normalized inputs."""

    def __init__(self, classifier: MigrationBundleDiffClassifier | None = None) -> None:
        self._classifier = classifier or MigrationBundleDiffClassifier()

    def build(self, *, inputs: MigrationBundleDiffInputs, options: MigrationBundleDiffOptions) -> dict[str, Any]:
        blockers = [
            *_verification_blockers("base", inputs.base_verification),
            *_verification_blockers("head", inputs.head_verification),
            *_attestation_blockers(inputs, options),
            *_target_blockers(inputs, options),
            *_gate_blockers("base", inputs.base_bundle, inputs.base_gate),
            *_gate_blockers("head", inputs.head_bundle, inputs.head_gate),
        ]
        changes = [change.to_dict() for change in self._changes(inputs)]
        status = _status(blockers, changes)
        payload: dict[str, Any] = {
            "schema_version": BUNDLE_DIFF_SCHEMA,
            "status": status,
            "base_bundle_id": inputs.base_bundle.get("bundle_id"),
            "head_bundle_id": inputs.head_bundle.get("bundle_id"),
            "base_pack_id": inputs.base_bundle.get("pack_id"),
            "head_pack_id": inputs.head_bundle.get("pack_id"),
            "target": dict(inputs.head_bundle.get("target", {}))
            if isinstance(inputs.head_bundle.get("target"), Mapping)
            else {},
            "summary": _summary(changes),
            "changes": changes,
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": _warnings(inputs),
            "recommendations": _recommendations(status, changes),
        }
        payload["diff_id"] = stable_fingerprint(
            {
                "base_bundle_id": payload["base_bundle_id"],
                "head_bundle_id": payload["head_bundle_id"],
                "options": options.to_dict(),
                "changes": changes,
                "blockers": payload["blockers"],
                "warnings": payload["warnings"],
            }
        )
        return payload

    def _changes(self, inputs: MigrationBundleDiffInputs) -> tuple[MigrationBundleDiffChange, ...]:
        changes: list[MigrationBundleDiffChange] = []
        changes.extend(_artifact_changes(self._classifier, "migration_pack", inputs, _PACK_FIELDS))
        changes.extend(_artifact_changes(self._classifier, "impact_plan", inputs, _IMPACT_FIELDS))
        changes.extend(_artifact_changes(self._classifier, "approval", inputs, _APPROVAL_FIELDS))
        changes.extend(_artifact_changes(self._classifier, "environment_contract", inputs, _ENV_FIELDS))
        changes.extend(_artifact_changes(self._classifier, "certification", inputs, _CERT_FIELDS))
        changes.extend(_artifact_changes(self._classifier, "promotion", inputs, _PROMOTION_FIELDS))
        changes.extend(_gate_changes(self._classifier, inputs))
        changes.extend(_bundle_metadata_changes(self._classifier, inputs))
        return tuple(changes)


class MigrationBundleDiffClassifier:
    """Classifies raw bundle deltas into reviewer-facing risk semantics."""

    def classify(self, *, kind: str, path: str, base: Any, head: Any) -> MigrationBundleDiffChange:
        risk_tags = _risk_tags(kind, path, base, head)
        severity = _severity(kind, path, base, head, risk_tags)
        return MigrationBundleDiffChange(
            kind=kind,
            path=path,
            classification=_classification(kind, path),
            severity=severity,
            risk_tags=risk_tags,
            base=base,
            head=head,
            reviewer_action=_reviewer_action(kind, path, risk_tags),
        )


_PACK_FIELDS = ("strategy", "changes", "ddl", "phases", "rollback", "desired_fingerprint", "actual_fingerprint")
_IMPACT_FIELDS = ("required_approvals", "risk_tags", "blockers", "warnings")
_APPROVAL_FIELDS = ("approved_risks", "approved_by", "expires_at")
_ENV_FIELDS = ("environments", "policies")
_CERT_FIELDS = ("status", "environment", "certification_id")
_PROMOTION_FIELDS = ("status", "from_environment", "to_environment", "certification_id", "promotion_id")


def _artifact_changes(
    classifier: MigrationBundleDiffClassifier,
    kind: str,
    inputs: MigrationBundleDiffInputs,
    fields: tuple[str, ...],
) -> tuple[MigrationBundleDiffChange, ...]:
    base = inputs.base_artifacts.get(kind, {})
    head = inputs.head_artifacts.get(kind, {})
    changes = []
    for field in fields:
        if base.get(field) != head.get(field):
            changes.append(
                classifier.classify(kind=kind, path=f"{kind}.{field}", base=base.get(field), head=head.get(field))
            )
    return tuple(changes)


def _gate_changes(
    classifier: MigrationBundleDiffClassifier, inputs: MigrationBundleDiffInputs
) -> tuple[MigrationBundleDiffChange, ...]:
    if inputs.base_gate is None and inputs.head_gate is None:
        return ()
    base = inputs.base_gate or {}
    head = inputs.head_gate or {}
    return tuple(
        classifier.classify(kind="gate", path=f"gate.{field}", base=base.get(field), head=head.get(field))
        for field in ("status", "profile", "blockers", "warnings")
        if base.get(field) != head.get(field)
    )


def _bundle_metadata_changes(
    classifier: MigrationBundleDiffClassifier, inputs: MigrationBundleDiffInputs
) -> tuple[MigrationBundleDiffChange, ...]:
    return tuple(
        classifier.classify(
            kind="bundle_metadata",
            path=f"bundle_metadata.{field}",
            base=inputs.base_bundle.get(field),
            head=inputs.head_bundle.get(field),
        )
        for field in ("bundle_id", "artifacts")
        if inputs.base_bundle.get(field) != inputs.head_bundle.get(field)
    )


def _verification_blockers(side: str, verification: Mapping[str, Any]) -> tuple[str, ...]:
    if verification.get("status") != "blocked":
        return ()
    return (
        f"migration_bundle_diff.{side}_verification_blocked",
        *(str(item) for item in verification.get("blockers", []) if str(item)),
    )


def _attestation_blockers(inputs: MigrationBundleDiffInputs, options: MigrationBundleDiffOptions) -> tuple[str, ...]:
    if not options.require_attestation:
        return ()
    blockers = []
    if not isinstance(inputs.base_bundle.get("attestation"), Mapping):
        blockers.append("migration_bundle_diff.base_attestation_missing")
    if not isinstance(inputs.head_bundle.get("attestation"), Mapping):
        blockers.append("migration_bundle_diff.head_attestation_missing")
    return tuple(blockers)


def _target_blockers(inputs: MigrationBundleDiffInputs, options: MigrationBundleDiffOptions) -> tuple[str, ...]:
    if not options.block_target_mismatch:
        return ()
    return (
        ("migration_bundle_diff.target_mismatch",)
        if inputs.base_bundle.get("target") != inputs.head_bundle.get("target")
        else ()
    )


def _gate_blockers(side: str, bundle: Mapping[str, Any], gate: Mapping[str, Any] | None) -> tuple[str, ...]:
    if gate is None:
        return ()
    blockers = []
    if gate.get("schema_version") != BUNDLE_GATE_SCHEMA:
        blockers.append(f"migration_bundle_diff.{side}_gate_invalid_schema")
    if gate.get("pack_id") != bundle.get("pack_id"):
        blockers.append(f"migration_bundle_diff.{side}_gate_pack_id_mismatch")
    if gate.get("bundle_id") not in {None, bundle.get("bundle_id")}:
        blockers.append(f"migration_bundle_diff.{side}_gate_bundle_id_mismatch")
    return tuple(blockers)


def _status(blockers: list[str], changes: list[dict[str, Any]]) -> str:
    if blockers:
        return "blocked"
    if any(str(change.get("severity")) in _BREAKING_SEVERITIES for change in changes):
        return "breaking"
    return "changed" if changes else "same"


def _summary(changes: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "changes_count": len(changes),
        "breaking_count": sum(1 for change in changes if str(change.get("severity")) in _BREAKING_SEVERITIES),
        "approval_delta_count": sum(1 for change in changes if change.get("kind") == "approval"),
        "impact_delta_count": sum(1 for change in changes if change.get("kind") == "impact_plan"),
        "gate_delta_count": sum(1 for change in changes if change.get("kind") == "gate"),
    }


def _warnings(inputs: MigrationBundleDiffInputs) -> list[str]:
    return list(
        dict.fromkeys(
            [
                *(str(item) for item in inputs.base_verification.get("warnings", []) if str(item)),
                *(str(item) for item in inputs.head_verification.get("warnings", []) if str(item)),
            ]
        )
    )


def _recommendations(status: str, changes: list[dict[str, Any]]) -> list[str]:
    if status == "blocked":
        return ["Resolve bundle diff blockers before using this delta as review evidence."]
    if status == "breaking":
        return ["Review high and critical changes before approving this migration bundle."]
    if changes:
        return ["Review changed evidence before merging or promoting this migration bundle."]
    return ["No material bundle delta detected."]


def _risk_tags(kind: str, path: str, base: Any, head: Any) -> tuple[str, ...]:
    if kind == "impact_plan" and path.endswith(("required_approvals", "risk_tags")):
        return tuple(sorted(set(_string_values(head)) - set(_string_values(base))))
    if kind == "approval" and path.endswith("approved_risks"):
        return tuple(sorted(set(_string_values(base)) - set(_string_values(head))))
    return ()


def _severity(kind: str, path: str, base: Any, head: Any, risk_tags: tuple[str, ...]) -> str:
    if kind == "gate" and path == "gate.status" and head == "blocked":
        return "critical"
    if "data_destructive" in risk_tags or "shadow_cutover" in risk_tags:
        return "critical"
    if kind == "migration_pack" and path.endswith(("ddl", "phases")):
        return "high"
    if kind == "impact_plan" and path.endswith("required_approvals") and risk_tags:
        return "high"
    if kind == "approval" and path.endswith("approved_risks") and risk_tags:
        return "high"
    if kind in {"impact_plan", "approval", "promotion", "certification"}:
        return "medium"
    return "low" if kind == "gate" else "info"


def _classification(kind: str, path: str) -> str:
    if kind == "migration_pack" and path.endswith(("ddl", "phases", "strategy", "rollback")):
        return "physical"
    return {
        "migration_pack": "schema",
        "impact_plan": "impact",
        "approval": "approval",
        "environment_contract": "evidence",
        "certification": "evidence",
        "promotion": "promotion",
        "gate": "policy",
        "bundle_metadata": "metadata",
    }.get(kind, "metadata")


def _reviewer_action(kind: str, path: str, risk_tags: tuple[str, ...]) -> str:
    if kind == "migration_pack" and path.endswith("ddl"):
        return "Review changed migration DDL before applying this bundle."
    if kind == "migration_pack" and path.endswith("phases"):
        return "Review changed migration phase graph before applying this bundle."
    if kind == "impact_plan" and path.endswith("required_approvals") and risk_tags:
        return f"Approve {risk_tags[0]} risk or update approval artifact."
    if kind == "approval" and path.endswith("approved_risks"):
        return "Restore missing approval risks or re-run impact approval."
    if kind == "gate":
        return "Re-run bundle gate and resolve policy blockers before deploy."
    return "Review this evidence delta before approving the migration."


def _string_values(raw: Any) -> tuple[str, ...]:
    return tuple(str(item) for item in raw if str(item)) if isinstance(raw, list) else ()


__all__ = [
    "BUNDLE_DIFF_SCHEMA",
    "MigrationBundleDiffBuilder",
    "MigrationBundleDiffChange",
    "MigrationBundleDiffClassifier",
    "MigrationBundleDiffInputs",
    "MigrationBundleDiffOptions",
]
