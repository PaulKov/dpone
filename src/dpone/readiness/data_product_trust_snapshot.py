"""Trust snapshot and gate evaluation services."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.readiness import data_product_trust_support as support


class TrustSnapshotBuilder:
    """Build a product-level trust snapshot from indexed evidence refs."""

    def snapshot(self, *, index: Mapping[str, Any], product_id: str, profile: str = "prod_strict") -> dict[str, Any]:
        if index.get("status") == "disabled":
            return _snapshot_payload(index, product_id, profile, {}, 0.0, "disabled", (), ())
        options = _options(index)
        refs = [ref for ref in support.mappings(index.get("evidence_refs")) if ref.get("product_id") == product_id]
        domains = _domain_matrix(refs, options["required_domains"])
        score = _trust_score(domains, options["score_weights"])
        blockers = _snapshot_blockers(domains, score, float(options["score_minimums"].get(profile, 0.0)))
        warnings: list[str] = []
        blockers, warnings = support.apply_profile(
            blockers=blockers,
            warnings=warnings,
            profile=profile,
            mode=str(index.get("mode") or "gate"),
        )
        return _snapshot_payload(
            index,
            product_id,
            profile,
            domains,
            score,
            support.status(blockers, warnings, "allowed"),
            blockers,
            warnings,
        )


class TrustGate:
    """Profile-aware go/no-go decision over a trust snapshot."""

    def evaluate(self, *, snapshot: Mapping[str, Any], profile: str = "prod_strict") -> dict[str, Any]:
        if snapshot.get("status") == "disabled":
            blockers: list[str] = []
            warnings: list[str] = []
        else:
            blockers = list(support.strings(snapshot.get("blockers")))
            warnings = list(support.strings(snapshot.get("warnings")))
            if profile == "regulated" and not _product(snapshot).get("owner"):
                blockers.append("data_product_trust.owner_missing")
        blockers, warnings = support.apply_profile(blockers=blockers, warnings=warnings, profile=profile)
        payload = {
            "schema_version": support.GATE_SCHEMA,
            "status": "allowed"
            if snapshot.get("status") == "disabled"
            else support.status(blockers, warnings, "allowed"),
            "profile": profile,
            "product": _product(snapshot),
            "product_id": snapshot.get("product_id"),
            "trust_snapshot_id": snapshot.get("trust_snapshot_id"),
            "evidence_lake_index_id": snapshot.get("evidence_lake_index_id"),
            "trust_score": snapshot.get("trust_score"),
            "domains": dict(_mapping(snapshot.get("domains"))),
            "blockers": support.dedupe(blockers),
            "warnings": support.dedupe(warnings),
            "recommendations": _recommendations("blocked" if blockers else "warning" if warnings else "allowed"),
        }
        return support.payload_id(payload, "trust_gate_id")


def _domain_matrix(refs: Sequence[Mapping[str, Any]], required_domains: Sequence[str]) -> dict[str, dict[str, Any]]:
    by_domain: dict[str, list[Mapping[str, Any]]] = {}
    for ref in refs:
        by_domain.setdefault(str(ref.get("domain") or "unknown"), []).append(ref)
    matrix: dict[str, dict[str, Any]] = {}
    for domain in required_domains:
        domain_refs = by_domain.get(domain, [])
        blockers = [item for ref in domain_refs for item in support.strings(ref.get("blockers"))]
        warnings = [item for ref in domain_refs for item in support.strings(ref.get("warnings"))]
        if not domain_refs:
            status = "missing"
        elif blockers or any(ref.get("status") == "blocked" for ref in domain_refs):
            status = "blocked"
        elif warnings or any(ref.get("status") in {"warning", "waived"} for ref in domain_refs):
            status = "degraded"
        else:
            status = "healthy"
        matrix[domain] = {
            "status": status,
            "evidence_refs": [dict(ref) for ref in domain_refs],
            "blockers": support.dedupe(blockers),
            "warnings": support.dedupe(warnings),
        }
    return matrix


def _trust_score(domains: Mapping[str, Mapping[str, Any]], weights: Mapping[str, float]) -> float:
    if not domains:
        return 0.0
    total = sum(float(weights.get(domain, 1.0)) for domain in domains) or float(len(domains))
    points = 0.0
    for domain, state in domains.items():
        weight = float(weights.get(domain, 1.0))
        points += weight * _domain_points(str(state.get("status")))
    return round(points / total, 6)


def _domain_points(status: str) -> float:
    return {"healthy": 1.0, "degraded": 0.5, "stale": 0.25}.get(status, 0.0)


def _snapshot_blockers(domains: Mapping[str, Mapping[str, Any]], score: float, threshold: float) -> list[str]:
    blockers: list[str] = []
    for domain, state in domains.items():
        if state.get("status") == "missing":
            blockers.append(f"data_product_trust.required_domain_missing:{domain}")
        if state.get("status") == "blocked":
            blockers.append(f"data_product_trust.domain_blocked:{domain}")
    if score < threshold:
        blockers.append("data_product_trust.score_below_threshold")
    return blockers


def _snapshot_payload(
    index: Mapping[str, Any],
    product_id: str,
    profile: str,
    domains: Mapping[str, Mapping[str, Any]],
    score: float,
    status: str,
    blockers: Sequence[str],
    warnings: Sequence[str],
) -> dict[str, Any]:
    payload = {
        "schema_version": support.SNAPSHOT_SCHEMA,
        "status": status,
        "profile": profile,
        "product": _mapping(index.get("product")),
        "product_id": product_id,
        "evidence_lake_index_id": index.get("evidence_lake_index_id"),
        "trust_score": score,
        "domains": {key: dict(value) for key, value in domains.items()},
        "blockers": support.dedupe(blockers),
        "warnings": support.dedupe(warnings),
        "recommendations": _recommendations(status),
    }
    return support.payload_id(payload, "trust_snapshot_id")


def _options(index: Mapping[str, Any]) -> dict[str, Any]:
    raw = _mapping(index.get("options"))
    return {
        "required_domains": tuple(support.strings(raw.get("required_domains"))) or support.DEFAULT_REQUIRED_DOMAINS,
        "score_minimums": _mapping(raw.get("score_minimums")),
        "score_weights": _mapping(raw.get("score_weights")),
    }


def _recommendations(status: str) -> list[str]:
    if status == "blocked":
        return ["Resolve missing, stale, or blocked trust evidence before release closeout."]
    if status == "warning":
        return ["Review degraded evidence domains and record accepted risk if needed."]
    return ["Record trust gate and report in the evidence registry."]


def _product(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    return _mapping(payload.get("product"))


def _mapping(raw: Any) -> Mapping[str, Any]:
    return raw if isinstance(raw, Mapping) else {}


__all__ = ["TrustGate", "TrustSnapshotBuilder"]
