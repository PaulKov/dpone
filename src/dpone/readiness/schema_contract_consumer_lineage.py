"""Provider-neutral schema contract consumer lineage evidence."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from dpone.readiness.migration_control import stable_fingerprint

CONSUMER_LINEAGE_SCHEMA = "dpone.schema_contract_consumer_lineage.v1"
CONFIDENCE_ORDER = {"table_only": 0, "inferred": 1, "parsed": 2, "explicit": 3}


class SchemaConsumerLineageProvider(Protocol):
    source: str

    def lineage(
        self, *, contract_id: str, target_table: str, target_columns: Sequence[str]
    ) -> tuple[dict[str, Any], ...]: ...

    def blockers(self) -> tuple[str, ...]: ...

    def warnings(self) -> tuple[str, ...]: ...


@dataclass(frozen=True, slots=True)
class SchemaConsumerLineageInventoryBuilder:
    """Builds a stable offline lineage artifact from local evidence providers."""

    def build(
        self,
        *,
        contract_version: Mapping[str, Any],
        options: Any,
        providers: Sequence[SchemaConsumerLineageProvider],
    ) -> dict[str, Any]:
        evidence: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
        blockers: list[str] = []
        warnings: list[str] = []
        required = set(getattr(options, "required_sources", ()) or ())
        for provider in providers:
            provider_blockers = list(provider.blockers())
            (blockers if provider.source in required else warnings).extend(provider_blockers)
            warnings.extend(provider.warnings())
            for item in provider.lineage(
                contract_id=str(contract_version.get("contract_id") or ""),
                target_table=str(_mapping(contract_version.get("target")).get("table") or ""),
                target_columns=_contract_columns(contract_version),
            ):
                normalized = _lineage_evidence(item, source=provider.source)
                key = (
                    normalized["consumer_id"],
                    normalized["dataset"],
                    str(normalized["column"]),
                    normalized["source"],
                    normalized["confidence"],
                )
                evidence.setdefault(key, normalized)
        if getattr(options, "mode", "gate") == "observe":
            warnings.extend(blockers)
            blockers = []
        values = sorted(evidence.values(), key=lambda item: _evidence_sort_key(item))
        payload: dict[str, Any] = {
            "schema_version": CONSUMER_LINEAGE_SCHEMA,
            "status": "blocked" if blockers else "discovered",
            "contract_id": contract_version.get("contract_id"),
            "contract_version_id": contract_version.get("contract_version_id"),
            "version": contract_version.get("version"),
            "evidence": values,
            "summary": {
                "evidence_count": len(values),
                "consumers_count": len({item["consumer_id"] for item in values}),
                "by_confidence": dict(sorted(Counter(item["confidence"] for item in values).items())),
            },
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
        }
        payload["consumer_lineage_id"] = stable_fingerprint(payload)
        return payload


@dataclass(frozen=True, slots=True)
class ConsumerEvidenceConfidenceClassifier:
    """Normalizes external provider confidence labels into the public taxonomy."""

    def classify(self, value: object) -> str:
        confidence = str(value or "table_only")
        return confidence if confidence in CONFIDENCE_ORDER else "inferred"

    def is_low_confidence(self, value: object) -> bool:
        return is_low_confidence(self.classify(value))


def merge_inventory_with_lineage(inventory: Mapping[str, Any], lineage: Mapping[str, Any] | None) -> dict[str, Any]:
    if not lineage:
        return dict(inventory)
    consumers = _consumer_index(inventory.get("consumers", []))
    for consumer in consumers_from_lineage(lineage):
        existing = consumers.get(str(consumer["id"]))
        consumers[str(consumer["id"])] = _merge_consumer(existing, consumer) if existing else consumer
    payload = dict(inventory)
    payload["consumers"] = sorted(consumers.values(), key=lambda item: str(item.get("id")))
    summary = dict(_mapping(payload.get("summary")))
    summary["consumers_count"] = len(consumers)
    summary["lineage_evidence_count"] = len(_lineage_items(lineage))
    summary["lineage_confidence"] = lineage_confidence(lineage)
    payload["summary"] = summary
    payload["warnings"] = list(
        dict.fromkeys([*list(inventory.get("warnings", [])), *list(lineage.get("warnings", []))])
    )
    payload["blockers"] = list(
        dict.fromkeys([*list(inventory.get("blockers", [])), *list(lineage.get("blockers", []))])
    )
    payload["consumer_inventory_id"] = stable_fingerprint(payload)
    return payload


def consumers_from_lineage(lineage: Mapping[str, Any] | None) -> tuple[dict[str, Any], ...]:
    grouped: dict[str, dict[str, Any]] = {}
    for item in _lineage_items(lineage):
        consumer_id = str(item.get("consumer_id") or "unknown")
        current = grouped.setdefault(
            consumer_id,
            {
                "id": consumer_id,
                "type": str(item.get("consumer_type") or "consumer"),
                "owner": item.get("owner"),
                "source": "lineage",
                "version_constraint": item.get("version_constraint"),
                "reads": {"columns": []},
                "confidence": str(item.get("confidence") or "table_only"),
                "lineage_sources": [],
            },
        )
        if item.get("column"):
            current["reads"]["columns"].append(str(item["column"]))
        current["confidence"] = _best_confidence(str(current["confidence"]), str(item.get("confidence")))
        current["lineage_sources"].append(str(item.get("source") or "lineage"))
    result: list[dict[str, Any]] = []
    for consumer in grouped.values():
        consumer["reads"]["columns"] = sorted(set(consumer["reads"]["columns"]))
        consumer["lineage_sources"] = sorted(set(consumer["lineage_sources"]))
        result.append(consumer)
    return tuple(sorted(result, key=lambda item: str(item.get("id"))))


def lineage_confidence(lineage: Mapping[str, Any] | None) -> dict[str, int]:
    return dict(
        sorted(Counter(str(item.get("confidence") or "table_only") for item in _lineage_items(lineage)).items())
    )


def is_low_confidence(value: str | None) -> bool:
    return str(value or "table_only") in {"inferred", "table_only"}


def _contract_columns(contract_version: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(str(item.get("name")) for item in contract_version.get("columns", []) if isinstance(item, Mapping))


def _lineage_evidence(raw: Mapping[str, Any], *, source: str) -> dict[str, Any]:
    confidence = ConsumerEvidenceConfidenceClassifier().classify(raw.get("confidence"))
    return {
        "consumer_id": str(raw.get("consumer_id") or raw.get("id") or "unknown"),
        "consumer_type": str(raw.get("consumer_type") or raw.get("type") or "consumer"),
        "owner": raw.get("owner"),
        "dataset": str(raw.get("dataset") or ""),
        "column": str(raw["column"]) if raw.get("column") is not None else None,
        "source": str(raw.get("source") or source),
        "confidence": confidence,
        "path": raw.get("path"),
        "version_constraint": raw.get("version_constraint"),
    }


def _consumer_index(raw: object) -> dict[str, dict[str, Any]]:
    return {str(item.get("id")): dict(item) for item in raw if isinstance(item, Mapping)}


def _merge_consumer(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, Any]:
    columns = sorted(
        set(_mapping(left.get("reads")).get("columns", [])) | set(_mapping(right.get("reads")).get("columns", []))
    )
    sources = sorted(set(left.get("lineage_sources", [])) | set(right.get("lineage_sources", [])))
    return {
        **dict(left),
        "reads": {"columns": [str(item) for item in columns]},
        "confidence": _best_confidence(str(left.get("confidence") or "explicit"), str(right.get("confidence"))),
        "lineage_sources": sources,
    }


def _best_confidence(left: str, right: str) -> str:
    return left if CONFIDENCE_ORDER.get(left, 0) >= CONFIDENCE_ORDER.get(right, 0) else right


def _lineage_items(lineage: Mapping[str, Any] | None) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(lineage, Mapping):
        return ()
    return tuple(item for item in lineage.get("evidence", []) if isinstance(item, Mapping))


def _evidence_sort_key(item: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (str(item.get("consumer_id")), str(item.get("dataset")), str(item.get("column")), str(item.get("source")))


def _mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


__all__ = [
    "CONSUMER_LINEAGE_SCHEMA",
    "ConsumerEvidenceConfidenceClassifier",
    "SchemaConsumerLineageInventoryBuilder",
    "SchemaConsumerLineageProvider",
    "consumers_from_lineage",
    "is_low_confidence",
    "lineage_confidence",
    "merge_inventory_with_lineage",
]
