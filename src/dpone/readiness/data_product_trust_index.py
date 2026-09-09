"""Evidence Lake indexing and query services for Data Product Trust Center."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.readiness import data_product_trust_support as support


class TrustEvidenceLakeIndexer:
    """Build a normalized provider-neutral evidence index from local artifacts."""

    def index(
        self,
        *,
        manifests: Sequence[Mapping[str, Any]],
        evidence_payloads: Sequence[Mapping[str, Any]] = (),
        registry_records: Sequence[Mapping[str, Any]] = (),
    ) -> dict[str, Any]:
        options = _options(manifests)
        if not options.enabled:
            return _index_payload(options, (), "disabled", (), ())
        refs = [
            *_refs_from_payloads(evidence_payloads, options),
            *_refs_from_records(registry_records, options),
        ]
        blockers = _index_blockers(options, refs)
        warnings: list[str] = []
        blockers, warnings = support.apply_profile(
            blockers=blockers,
            warnings=warnings,
            profile=options.profile,
            mode=options.mode,
        )
        return _index_payload(options, refs, support.status(blockers, warnings, "ready"), blockers, warnings)


class TrustQueryEngine:
    """Query normalized evidence refs with deterministic ordering and ids."""

    def query(
        self,
        *,
        index: Mapping[str, Any],
        product_id: str | None = None,
        domain: str | None = None,
        artifact_kind: str | None = None,
        status: str | None = None,
        owner: str | None = None,
        max_results: int = 500,
    ) -> dict[str, Any]:
        refs = [
            ref
            for ref in support.mappings(index.get("evidence_refs"))
            if _matches(ref, product_id, domain, artifact_kind, status, owner)
        ]
        refs = sorted(refs, key=_sort_key)
        selected = refs[: max(0, max_results)]
        warnings = ["data_product_trust.query_truncated"] if len(refs) > len(selected) else []
        payload = {
            "schema_version": support.QUERY_SCHEMA,
            "status": "warning" if warnings else "ready",
            "filters": {
                "product_id": product_id,
                "domain": domain,
                "artifact_kind": artifact_kind,
                "status": status,
                "owner": owner,
                "max_results": max_results,
            },
            "evidence_lake_index_id": index.get("evidence_lake_index_id"),
            "summary": {"matched": len(refs), "returned": len(selected)},
            "evidence_refs": [dict(ref) for ref in selected],
            "blockers": [],
            "warnings": warnings,
        }
        return support.payload_id(payload, "trust_query_result_id")


def _options(manifests: Sequence[Mapping[str, Any]]) -> support.TrustCenterOptions:
    for manifest in manifests:
        options = support.TrustCenterOptions.from_manifest(manifest)
        if options.enabled:
            return options
    return support.TrustCenterOptions.from_manifest(manifests[0] if manifests else {})


def _refs_from_payloads(
    payloads: Sequence[Mapping[str, Any]], options: support.TrustCenterOptions
) -> list[dict[str, Any]]:
    refs = [_evidence_ref(payload, options) for payload in payloads if isinstance(payload, Mapping)]
    return _dedupe_refs(refs)


def _refs_from_records(
    records: Sequence[Mapping[str, Any]], options: support.TrustCenterOptions
) -> list[dict[str, Any]]:
    payloads: list[Mapping[str, Any]] = []
    for record in records:
        for ref in support.mappings(record.get("artifact_refs")):
            payloads.append({**ref, "status": record.get("status") or ref.get("status") or "unknown"})
    return _refs_from_payloads(payloads, options)


def _evidence_ref(payload: Mapping[str, Any], options: support.TrustCenterOptions) -> dict[str, Any]:
    kind = support.artifact_kind(payload)
    product_id = support.product_id(payload, fallback=str(options.product.get("id") or ""))
    product = payload.get("product") if isinstance(payload.get("product"), Mapping) else options.product
    ref = {
        "product_id": product_id,
        "owner": product.get("owner") if isinstance(product, Mapping) else options.product.get("owner"),
        "domain": support.domain_for_kind(kind),
        "artifact_kind": kind,
        "schema_version": payload.get("schema_version"),
        "status": payload.get("status") or "unknown",
        "evidence_id": support.evidence_id(payload),
        "blockers": list(support.strings(payload.get("blockers"))),
        "warnings": list(support.strings(payload.get("warnings"))),
    }
    return support.payload_id(ref, "evidence_ref_id")


def _dedupe_refs(refs: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[Any, Any, Any], dict[str, Any]] = {}
    for ref in refs:
        key = (ref.get("product_id"), ref.get("artifact_kind"), ref.get("evidence_id") or ref.get("evidence_ref_id"))
        deduped[key] = dict(ref)
    return sorted(deduped.values(), key=_sort_key)


def _index_blockers(options: support.TrustCenterOptions, refs: Sequence[Mapping[str, Any]]) -> list[str]:
    blockers: list[str] = []
    del refs
    if not options.product.get("id"):
        blockers.append("data_product_trust.product_id_missing")
    if not options.product.get("owner") and options.profile == "regulated":
        blockers.append("data_product_trust.owner_missing")
    return blockers


def _index_payload(
    options: support.TrustCenterOptions,
    refs: Sequence[Mapping[str, Any]],
    status: str,
    blockers: Sequence[str],
    warnings: Sequence[str],
) -> dict[str, Any]:
    payload = {
        "schema_version": support.INDEX_SCHEMA,
        "status": status,
        "mode": options.mode,
        "profile": options.profile,
        "product": dict(options.product),
        "product_id": options.product.get("id"),
        "options": {
            "required_domains": list(options.required_domains),
            "include_artifacts": list(options.include_artifacts),
            "score_minimums": dict(options.score_minimums),
            "score_weights": dict(options.score_weights),
        },
        "summary": {"evidence_refs": len(refs), "products": 1 if options.product.get("id") else 0},
        "evidence_refs": [dict(ref) for ref in refs],
        "blockers": support.dedupe(blockers),
        "warnings": support.dedupe(warnings),
    }
    return support.payload_id(payload, "evidence_lake_index_id")


def _matches(
    ref: Mapping[str, Any],
    product_id: str | None,
    domain: str | None,
    artifact_kind: str | None,
    status: str | None,
    owner: str | None,
) -> bool:
    filters = {
        "product_id": product_id,
        "domain": domain,
        "artifact_kind": artifact_kind,
        "status": status,
        "owner": owner,
    }
    return all(value is None or str(ref.get(key)) == value for key, value in filters.items())


def _sort_key(ref: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(ref.get("product_id") or ""),
        str(ref.get("domain") or ""),
        str(ref.get("artifact_kind") or ""),
        str(ref.get("evidence_ref_id") or ""),
    )


__all__ = ["TrustEvidenceLakeIndexer", "TrustQueryEngine"]
