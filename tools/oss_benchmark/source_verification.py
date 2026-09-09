"""Source registry and citation verification for public benchmark claims."""

from __future__ import annotations

import hashlib
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from tools.oss_benchmark.config import ROOT

VERIFIED = "verified"
STALE = "stale"
UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class SourceCheckResult:
    """Verification outcome returned by a source verifier implementation."""

    status: str
    last_checked_at: str
    verification_mode: str
    content_hash: str | None = None
    last_error: str | None = None


class SourceVerifier(Protocol):
    """Port for source verification; tests can inject a fake implementation."""

    def verify(
        self,
        source: dict[str, Any],
        *,
        checked_at: str,
        verify_urls: bool,
    ) -> SourceCheckResult:
        """Verify one source and return status plus immutable evidence."""


class DefaultSourceVerifier:
    """Verify local files by content hash and URLs by metadata or optional live fetch."""

    def __init__(self, *, root: Path = ROOT, timeout_seconds: int = 12) -> None:
        self._root = root
        self._timeout_seconds = timeout_seconds

    def verify(
        self,
        source: dict[str, Any],
        *,
        checked_at: str,
        verify_urls: bool,
    ) -> SourceCheckResult:
        value = str(source.get("value") or "")
        if _is_url(value):
            return self._verify_url(value, checked_at=checked_at, live=verify_urls)
        return self._verify_local(value, checked_at=checked_at)

    def _verify_url(self, value: str, *, checked_at: str, live: bool) -> SourceCheckResult:
        parsed = urllib.parse.urlparse(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return SourceCheckResult(
                status=UNAVAILABLE,
                last_checked_at=checked_at,
                verification_mode="metadata-url",
                last_error="invalid URL",
            )
        if not live:
            return SourceCheckResult(
                status=VERIFIED,
                last_checked_at=checked_at,
                verification_mode="metadata-url",
                content_hash=_hash_text(value),
            )
        request = urllib.request.Request(
            value,
            headers={"User-Agent": "dpone-oss-benchmark-source-verifier/1.0"},
            method="GET",
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout_seconds) as response:
                status_code = getattr(response, "status", 200)
                data = response.read(65536)
        except (OSError, urllib.error.URLError, TimeoutError) as exc:
            return SourceCheckResult(
                status=UNAVAILABLE,
                last_checked_at=checked_at,
                verification_mode="live-url",
                last_error=str(exc),
            )
        if int(status_code) >= 400:
            return SourceCheckResult(
                status=UNAVAILABLE,
                last_checked_at=checked_at,
                verification_mode="live-url",
                last_error=f"HTTP {status_code}",
            )
        return SourceCheckResult(
            status=VERIFIED,
            last_checked_at=checked_at,
            verification_mode="live-url",
            content_hash=_hash_bytes(data or value.encode("utf-8")),
        )

    def _verify_local(self, value: str, *, checked_at: str) -> SourceCheckResult:
        path = self._resolve_local_path(value)
        if not path.exists() or not path.is_file():
            return SourceCheckResult(
                status=UNAVAILABLE,
                last_checked_at=checked_at,
                verification_mode="local-file",
                last_error=f"{value} does not exist",
            )
        return SourceCheckResult(
            status=VERIFIED,
            last_checked_at=checked_at,
            verification_mode="local-file",
            content_hash=_hash_bytes(path.read_bytes()),
        )

    def _resolve_local_path(self, value: str) -> Path:
        normalized = value.removeprefix("$WORKSPACE/")
        primary = self._root / normalized
        if primary.exists():
            return primary
        return self._root / "docs" / "benchmarks" / normalized


def source_id_for(value: str) -> str:
    """Return a deterministic compact source id."""

    return f"src_{hashlib.sha1(value.encode('utf-8')).hexdigest()[:12]}"


def build_source_registry(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Discover public source references and link them to benchmark claims."""

    claims = _claim_ids_by_source_ref(payload)
    registry: dict[str, dict[str, Any]] = {}
    for entry in (payload.get("feature_parity") or {}).get("entries") or []:
        claim_ids = [*claims.get("feature_parity.entries[].sources", []), _feature_claim_id(entry)]
        for value in _as_list(entry.get("sources")):
            _upsert_source(registry, value, claim_ids, "feature_parity.entries[].sources")
    for note in payload.get("closed_core_notes") or []:
        _upsert_source(
            registry,
            str(note.get("url") or ""),
            claims.get("closed_core_notes[].url", ["closed_core_comparator_scope"]),
            "closed_core_notes[].url",
        )
    for link in (payload.get("trust_center") or {}).get("evidence_links") or []:
        _upsert_source(
            registry,
            str(link.get("href") or ""),
            claims.get("trust_center.evidence_links[].href", ["customer_trust_center_verified"]),
            "trust_center.evidence_links[].href",
        )
    return sorted(registry.values(), key=lambda item: (item["source_type"], item["value"]))


def build_source_verification(
    payload: dict[str, Any],
    *,
    previous_payload: dict[str, Any] | None = None,
    verifier: SourceVerifier | None = None,
    verify_urls: bool = False,
) -> dict[str, Any]:
    """Verify source registry entries and retain previous values when a refresh fails."""

    checked_at = str(payload.get("generated_at") or (payload.get("run_context") or {}).get("generated_at") or "")
    previous = _previous_sources_by_id(previous_payload)
    checker = verifier or DefaultSourceVerifier()
    sources = [
        _merge_result(source, checker.verify(source, checked_at=checked_at, verify_urls=verify_urls), previous)
        for source in build_source_registry(payload)
    ]
    claim_matrix = _build_claim_matrix(sources)
    return {
        "schema_version": 1,
        "status": _overall_status(sources),
        "checked_at": checked_at,
        "verify_urls": verify_urls,
        "summary": _summary(sources, claim_matrix),
        "claim_matrix": claim_matrix,
        "sources": sources,
        "policy": {
            "stale_behavior": "Failed source refreshes keep previous values when prior evidence exists.",
            "url_mode": "live-url" if verify_urls else "metadata-url",
        },
    }


def _upsert_source(
    registry: dict[str, dict[str, Any]],
    value: str,
    claim_ids: list[str],
    source_ref: str,
) -> None:
    value = value.strip()
    if not value:
        return
    source_id = source_id_for(value)
    item = registry.setdefault(
        source_id,
        {
            "source_id": source_id,
            "value": value,
            "source_type": _source_type(value),
            "source_ref": source_ref,
            "linked_claim_ids": [],
        },
    )
    for claim_id in claim_ids:
        if claim_id and claim_id not in item["linked_claim_ids"]:
            item["linked_claim_ids"].append(claim_id)


def _merge_result(
    source: dict[str, Any],
    result: SourceCheckResult,
    previous: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if result.status == VERIFIED:
        return {
            **source,
            "status": VERIFIED,
            "last_checked_at": result.last_checked_at,
            "last_updated_at": result.last_checked_at,
            "refresh_attempted_at": result.last_checked_at,
            "verification_mode": result.verification_mode,
            "content_hash": result.content_hash,
            "last_error": None,
        }
    previous_source = previous.get(str(source.get("source_id")))
    if previous_source and previous_source.get("status") in {VERIFIED, STALE}:
        return {
            **previous_source,
            **source,
            "status": STALE,
            "last_checked_at": previous_source.get("last_checked_at") or previous_source.get("last_updated_at"),
            "last_updated_at": previous_source.get("last_updated_at") or previous_source.get("last_checked_at"),
            "refresh_attempted_at": result.last_checked_at,
            "verification_mode": result.verification_mode,
            "last_error": result.last_error or "source refresh failed",
        }
    return {
        **source,
        "status": UNAVAILABLE,
        "last_checked_at": result.last_checked_at,
        "last_updated_at": None,
        "refresh_attempted_at": result.last_checked_at,
        "verification_mode": result.verification_mode,
        "content_hash": None,
        "last_error": result.last_error or "source unavailable",
    }


def _build_claim_matrix(sources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    claim_map: dict[str, list[dict[str, Any]]] = {}
    for source in sources:
        for claim_id in source.get("linked_claim_ids") or []:
            claim_map.setdefault(str(claim_id), []).append(source)
    rows: list[dict[str, Any]] = []
    for claim_id, claim_sources in sorted(claim_map.items()):
        statuses = {str(source.get("status")) for source in claim_sources}
        status = UNAVAILABLE if UNAVAILABLE in statuses else STALE if STALE in statuses else VERIFIED
        rows.append(
            {
                "claim_id": claim_id,
                "source_count": len(claim_sources),
                "source_ids": [str(source.get("source_id")) for source in claim_sources],
                "status": status,
            }
        )
    return rows


def _summary(sources: list[dict[str, Any]], claim_matrix: list[dict[str, Any]]) -> dict[str, Any]:
    counts = {
        status: sum(1 for source in sources if source.get("status") == status)
        for status in (VERIFIED, STALE, UNAVAILABLE)
    }
    total = len(sources)
    traceable = sum(1 for claim in claim_matrix if claim.get("source_count", 0) > 0)
    source_health = int(round(((counts[VERIFIED] + (0.5 * counts[STALE])) / total) * 100)) if total else 100
    traceability = int(round((traceable / len(claim_matrix)) * 100)) if claim_matrix else 100
    return {
        "source_count": total,
        "verified_count": counts[VERIFIED],
        "stale_count": counts[STALE],
        "unavailable_count": counts[UNAVAILABLE],
        "claim_count": len(claim_matrix),
        "claim_traceability_percent": traceability,
        "source_health_score": source_health,
    }


def _overall_status(sources: list[dict[str, Any]]) -> str:
    statuses = {str(source.get("status")) for source in sources}
    if UNAVAILABLE in statuses:
        return UNAVAILABLE
    if STALE in statuses:
        return STALE
    return VERIFIED


def _previous_sources_by_id(previous_payload: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    previous_sources = ((previous_payload or {}).get("source_verification") or {}).get("sources") or []
    return {str(source.get("source_id")): dict(source) for source in previous_sources if source.get("source_id")}


def _claim_ids_by_source_ref(payload: dict[str, Any]) -> dict[str, list[str]]:
    claims: dict[str, list[str]] = {}
    for claim in (payload.get("public_evidence_integrity") or {}).get("claim_evidence") or []:
        if claim.get("status") == "missing":
            continue
        source_ref = str(claim.get("source") or "")
        claim_id = str(claim.get("claim_id") or "")
        if source_ref and claim_id:
            claims.setdefault(source_ref, []).append(claim_id)
    return claims


def _feature_claim_id(entry: dict[str, Any]) -> str:
    return f"feature:{entry.get('tool', 'unknown')}:{entry.get('dimension', 'unknown')}"


def _source_type(value: str) -> str:
    if _is_url(value):
        return "url"
    if value.startswith("docs/") or value.endswith(".md"):
        return "local-doc"
    return "local-file"


def _is_url(value: str) -> bool:
    return value.startswith(("http://", "https://"))


def _as_list(value: Any) -> list[str]:
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if str(item)]
    if value:
        return [str(value)]
    return []


def _hash_text(value: str) -> str:
    return _hash_bytes(value.encode("utf-8"))


def _hash_bytes(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"
