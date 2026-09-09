"""Provider-neutral audit archive, retention and legal hold contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import import_module
from pathlib import Path
from typing import Any

from dpone.readiness import data_product_audit_retention_support as support
from dpone.readiness.data_product_audit_retention_store import AuditArchiveStore
from dpone.readiness.migration_control import stable_fingerprint

AUDIT_ARCHIVE_PLAN_SCHEMA = "dpone.data_product_audit_archive_plan.v1"
AUDIT_ARCHIVE_RUN_SCHEMA = "dpone.data_product_audit_archive_run.v1"
AUDIT_ARCHIVE_VERIFICATION_SCHEMA = "dpone.data_product_audit_archive_verification.v1"
AUDIT_RETENTION_PLAN_SCHEMA = "dpone.data_product_audit_retention_plan.v1"
_PASSING = {"allowed", "archived", "held", "passed", "ready", "signed", "verified", "waived", "warning"}


@dataclass(frozen=True, slots=True)
class AuditRetentionOptions:
    enabled: bool
    mode: str
    profile: str
    store_backend: str
    store_uri: str
    layout: str
    include_artifacts: tuple[str, ...]
    manifest_hash_chain: bool
    merkle_root: bool
    min_days: int
    delete_after_days: int
    expired_policy: str
    legal_hold_enabled: bool
    require_reason: bool
    require_authority_gate: bool

    @classmethod
    def from_product(cls, product: Mapping[str, Any]) -> AuditRetentionOptions:
        raw = product.get("audit_retention") if isinstance(product.get("audit_retention"), Mapping) else {}
        archive = raw.get("archive") if isinstance(raw.get("archive"), Mapping) else {}
        retention = raw.get("retention") if isinstance(raw.get("retention"), Mapping) else {}
        legal = raw.get("legal_hold") if isinstance(raw.get("legal_hold"), Mapping) else {}
        return cls(
            enabled=support.bool_value(raw.get("enabled"), False),
            mode=str(raw.get("mode") or "gate"),
            profile=str(raw.get("profile") or "prod_strict"),
            store_backend=str(archive.get("store_backend") or "local_fs"),
            store_uri=str(archive.get("store_uri") or ".dpone/audit-archive"),
            layout=str(archive.get("layout") or "{product_id}/{yyyy}/{mm}/{audit_archive_id}"),
            include_artifacts=tuple(support.strings(archive.get("include_artifacts"))),
            manifest_hash_chain=support.bool_value(archive.get("manifest_hash_chain"), True),
            merkle_root=support.bool_value(archive.get("merkle_root"), True),
            min_days=support.int_value(retention.get("min_days"), 0),
            delete_after_days=support.int_value(retention.get("delete_after_days"), 0),
            expired_policy=str(retention.get("expired_policy") or "block"),
            legal_hold_enabled=support.bool_value(legal.get("enabled"), False),
            require_reason=support.bool_value(legal.get("require_reason"), False),
            require_authority_gate=support.bool_value(legal.get("require_authority_gate"), False),
        )


class AuditEvidenceIndex:
    """Normalizes evidence payloads into canonical refs and bytes."""

    def build(self, evidence: Mapping[str, Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
        refs: list[dict[str, Any]] = []
        for kind, payload in sorted(evidence.items()):
            content = support.canonical_bytes(payload)
            refs.append(
                {
                    "kind": str(kind),
                    "artifact_id": support.evidence_id(payload),
                    "status": str(payload.get("status") or ""),
                    "sha256": support.sha256(content),
                    "recorded_at": payload.get("recorded_at"),
                    "relative_path": f"artifacts/{kind}.json",
                    "payload": dict(payload),
                }
            )
        return tuple(refs)


class AuditEvidenceArchivePlanner:
    """Builds deterministic audit archive plans."""

    def plan(
        self,
        *,
        manifest: Mapping[str, Any],
        evidence: Mapping[str, Mapping[str, Any]],
        observed_at: str | None = None,
    ) -> dict[str, Any]:
        product = support.product(manifest)
        options = AuditRetentionOptions.from_product(product)
        observed = support.parse_time(observed_at) or datetime(1970, 1, 1, tzinfo=UTC)
        if not options.enabled:
            return _plan_payload("disabled", product, options, (), (), (), observed)
        refs = AuditEvidenceIndex().build(evidence)
        blockers = _plan_blockers(options, refs)
        warnings: list[str] = []
        if options.profile == "advisory":
            warnings.extend(blockers)
            blockers = []
        status = "blocked" if blockers else "ready"
        return _plan_payload(status, product, options, refs, blockers, warnings, observed)


class AuditEvidenceArchiver:
    """Writes audit archives through an injected store."""

    def __init__(self, store: AuditArchiveStore) -> None:
        self._store = store

    def run(self, *, plan: Mapping[str, Any], execute: bool) -> dict[str, Any]:
        archive_uri = _archive_uri(plan)
        manifest = _archive_manifest(plan, archive_uri)
        status = "archived" if execute and plan.get("status") != "blocked" else "dry_run"
        blockers = [] if plan.get("status") != "blocked" else list(plan.get("blockers", []))
        if execute and not blockers:
            evidence = plan.get("evidence") if isinstance(plan.get("evidence"), Mapping) else {}
            artifacts = {
                str(ref["relative_path"]): support.canonical_bytes(evidence.get(str(ref.get("kind")), {}))
                for ref in support.mappings(plan.get("artifact_refs"))
            }
            self._store.write_archive(archive_uri=archive_uri, manifest=manifest, artifacts=artifacts)
        payload: dict[str, Any] = {
            "schema_version": AUDIT_ARCHIVE_RUN_SCHEMA,
            "status": "blocked" if blockers else status,
            "dry_run": not execute,
            "store_backend": plan.get("archive", {}).get("store_backend"),
            "archive_uri": archive_uri,
            "product": dict(plan.get("product", {})) if isinstance(plan.get("product"), Mapping) else {},
            "product_id": support.product_id(plan),
            "audit_archive_plan_id": plan.get("audit_archive_plan_id"),
            "audit_archive_id": manifest["audit_archive_id"],
            "artifact_count": len(manifest["artifacts"]),
            "artifacts": manifest["artifacts"],
            "hash_chain": manifest["hash_chain"],
            "merkle_root": manifest["merkle_root"],
            "retention": dict(plan.get("retention", {})) if isinstance(plan.get("retention"), Mapping) else {},
            "blockers": blockers,
            "warnings": list(plan.get("warnings", [])),
        }
        payload["audit_archive_run_id"] = stable_fingerprint(payload)
        return payload


class AuditArchiveVerifier:
    """Verifies archive manifest and archived evidence bytes."""

    def __init__(self, store: AuditArchiveStore) -> None:
        self._store = store

    def verify(self, *, archive_run: Mapping[str, Any]) -> dict[str, Any]:
        blockers: list[str] = []
        warnings: list[str] = []
        manifest: dict[str, Any] = {}
        try:
            manifest = self._store.read_manifest(str(archive_run.get("archive_uri")))
        except (OSError, ValueError, TypeError) as exc:
            blockers.append(f"data_product_audit_retention.archive_manifest_unreadable:{type(exc).__name__}")
        if manifest:
            blockers.extend(_verify_manifest(archive_run, manifest))
            blockers.extend(_verify_artifacts(self._store, str(archive_run.get("archive_uri")), manifest))
        status = "blocked" if blockers else "warning" if warnings else "verified"
        payload: dict[str, Any] = {
            "schema_version": AUDIT_ARCHIVE_VERIFICATION_SCHEMA,
            "status": status,
            "store_backend": archive_run.get("store_backend"),
            "archive_uri": archive_run.get("archive_uri"),
            "product": dict(archive_run.get("product", {})) if isinstance(archive_run.get("product"), Mapping) else {},
            "product_id": support.product_id(archive_run),
            "audit_archive_run_id": archive_run.get("audit_archive_run_id"),
            "audit_archive_id": archive_run.get("audit_archive_id"),
            "artifact_count": len(manifest.get("artifacts", [])) if manifest else 0,
            "artifacts": list(manifest.get("artifacts", [])) if manifest else [],
            "hash_chain": list(manifest.get("hash_chain", [])) if manifest else [],
            "merkle_root": manifest.get("merkle_root") if manifest else None,
            "retention": dict(archive_run.get("retention", {}))
            if isinstance(archive_run.get("retention"), Mapping)
            else {},
            "blockers": blockers,
            "warnings": warnings,
        }
        payload["audit_archive_verification_id"] = stable_fingerprint(payload)
        return payload


class AuditRetentionPlanner:
    """Creates non-destructive retention decisions."""

    def plan(
        self,
        *,
        manifest: Mapping[str, Any],
        archive_verification: Mapping[str, Any],
        legal_hold: Mapping[str, Any] | None = None,
        observed_at: str | None = None,
    ) -> dict[str, Any]:
        options = AuditRetentionOptions.from_product(support.product(manifest))
        observed = support.parse_time(observed_at) or datetime.now(UTC)
        retention = (
            archive_verification.get("retention") if isinstance(archive_verification.get("retention"), Mapping) else {}
        )
        delete_after = support.parse_time(retention.get("delete_after"))
        blockers: list[str] = []
        warnings: list[str] = []
        held = bool(legal_hold and legal_hold.get("status") == "held")
        expired = bool(delete_after and observed > delete_after)
        decision = "held" if held else "expired_blocked" if expired and options.expired_policy == "block" else "retain"
        if held:
            blockers.append("data_product_audit_retention.archive_under_legal_hold")
        if expired and options.expired_policy == "block":
            blockers.append("data_product_audit_retention.archive_retention_expired")
        elif expired:
            warnings.append("data_product_audit_retention.archive_retention_expired")
        status = "blocked" if blockers else "warning" if warnings else "ready"
        payload: dict[str, Any] = {
            "schema_version": AUDIT_RETENTION_PLAN_SCHEMA,
            "status": status,
            "decision": decision,
            "product": dict(archive_verification.get("product", {}))
            if isinstance(archive_verification.get("product"), Mapping)
            else {},
            "product_id": support.product_id(archive_verification),
            "audit_archive_verification_id": archive_verification.get("audit_archive_verification_id"),
            "audit_archive_run_id": archive_verification.get("audit_archive_run_id"),
            "archive_uri": archive_verification.get("archive_uri"),
            "retention_items": [_retention_item(archive_verification, decision, held, delete_after)],
            "blockers": blockers,
            "warnings": warnings,
        }
        payload["audit_retention_plan_id"] = stable_fingerprint(payload)
        return payload


def _plan_payload(
    status: str,
    product: Mapping[str, Any],
    options: AuditRetentionOptions,
    refs: Sequence[Mapping[str, Any]],
    blockers: Sequence[str],
    warnings: Sequence[str],
    observed: datetime,
) -> dict[str, Any]:
    hash_chain = support.hash_chain(refs) if options.manifest_hash_chain else []
    merkle_root = support.merkle_root([str(ref["sha256"]) for ref in refs]) if options.merkle_root else None
    payload: dict[str, Any] = {
        "schema_version": AUDIT_ARCHIVE_PLAN_SCHEMA,
        "status": status,
        "mode": options.mode,
        "profile": options.profile,
        "product": support.product_ref(product),
        "archive": {
            "store_backend": options.store_backend,
            "store_uri": options.store_uri,
            "layout": options.layout,
            "include_artifacts": list(options.include_artifacts),
        },
        "retention": support.retention(options.min_days, options.delete_after_days, options.expired_policy, observed),
        "legal_hold": {
            "enabled": options.legal_hold_enabled,
            "require_reason": options.require_reason,
            "require_authority_gate": options.require_authority_gate,
        },
        "artifact_refs": [{k: v for k, v in ref.items() if k != "payload"} for ref in refs],
        "evidence": {str(ref["kind"]): ref["payload"] for ref in refs},
        "hash_chain": hash_chain,
        "merkle_root": merkle_root,
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
    }
    payload["audit_archive_plan_id"] = stable_fingerprint(payload)
    return payload


def _plan_blockers(options: AuditRetentionOptions, refs: Sequence[Mapping[str, Any]]) -> list[str]:
    present = {str(ref["kind"]) for ref in refs}
    blockers = [
        f"data_product_audit_retention.required_artifact_missing:{kind}"
        for kind in options.include_artifacts
        if kind not in present
    ]
    if options.store_backend != "local_fs":
        blockers.append(f"data_product_audit_retention.unsupported_store:{options.store_backend}")
    for ref in refs:
        if ref.get("kind") in options.include_artifacts and ref.get("status") not in _PASSING:
            blockers.append(f"data_product_audit_retention.artifact_status_blocked:{ref.get('kind')}")
    return blockers


def _archive_uri(plan: Mapping[str, Any]) -> str:
    archive = plan.get("archive") if isinstance(plan.get("archive"), Mapping) else {}
    product_id = str(support.product_id(plan) or "unknown").replace(".", "_")
    now = datetime.now(UTC)
    archive_id = str(plan.get("audit_archive_plan_id") or stable_fingerprint(plan))
    layout = str(archive.get("layout") or "{product_id}/{yyyy}/{mm}/{audit_archive_id}")
    relative = layout.format(
        product_id=product_id, yyyy=f"{now.year:04d}", mm=f"{now.month:02d}", audit_archive_id=archive_id
    )
    return str(Path(str(archive.get("store_uri") or ".dpone/audit-archive")) / relative)


def _archive_manifest(plan: Mapping[str, Any], archive_uri: str) -> dict[str, Any]:
    artifacts = [dict(ref) for ref in support.mappings(plan.get("artifact_refs"))]
    archive_id = stable_fingerprint({"plan": plan.get("audit_archive_plan_id"), "artifacts": artifacts})
    return {
        "schema_version": "dpone.data_product_audit_archive_manifest.v1",
        "audit_archive_id": archive_id,
        "audit_archive_plan_id": plan.get("audit_archive_plan_id"),
        "product_id": support.product_id(plan),
        "archive_uri": archive_uri,
        "artifacts": artifacts,
        "hash_chain": list(plan.get("hash_chain", [])),
        "merkle_root": plan.get("merkle_root"),
    }


def _verify_manifest(archive_run: Mapping[str, Any], manifest: Mapping[str, Any]) -> list[str]:
    blockers = []
    if manifest.get("audit_archive_id") != archive_run.get("audit_archive_id"):
        blockers.append("data_product_audit_retention.archive_manifest_id_mismatch")
    if manifest.get("merkle_root") != archive_run.get("merkle_root"):
        blockers.append("data_product_audit_retention.archive_manifest_merkle_mismatch")
    return blockers


def _verify_artifacts(store: AuditArchiveStore, archive_uri: str, manifest: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    digests: list[str] = []
    for artifact in support.mappings(manifest.get("artifacts")):
        kind = str(artifact.get("kind"))
        try:
            content = store.read_artifact(archive_uri, str(artifact.get("relative_path")))
        except OSError:
            blockers.append(f"data_product_audit_retention.artifact_missing:{kind}")
            continue
        digest = support.sha256(content)
        digests.append(digest)
        if digest != artifact.get("sha256"):
            blockers.append(f"data_product_audit_retention.artifact_hash_mismatch:{kind}")
    if digests and support.merkle_root(digests) != manifest.get("merkle_root"):
        blockers.append("data_product_audit_retention.merkle_root_mismatch")
    return blockers


def _retention_item(
    verification: Mapping[str, Any], decision: str, held: bool, delete_after: datetime | None
) -> dict[str, Any]:
    return {
        "archive_uri": verification.get("archive_uri"),
        "audit_archive_verification_id": verification.get("audit_archive_verification_id"),
        "decision": decision,
        "held": held,
        "delete_after": delete_after.isoformat().replace("+00:00", "Z") if delete_after else None,
    }


canonical_bytes = support.canonical_bytes


def __getattr__(name: str) -> Any:
    if name in {"LEGAL_HOLD_SCHEMA", "LegalHoldService"}:
        value = getattr(import_module("dpone.readiness.data_product_legal_hold"), name)
        globals()[name] = value
        return value
    raise AttributeError(f"{__name__!s} has no attribute {name!r}")
