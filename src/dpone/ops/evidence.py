"""Auditable operational evidence bundles."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dpone.ops.certification import CertificationHarnessService
from dpone.ops.certification_artifacts import artifact_payload_passed, certification_trust
from dpone.ops.checksums import sha256_file
from dpone.ops.contracts import DataContractService
from dpone.ops.marketplace import ConnectorMarketplaceService


@dataclass(frozen=True, slots=True)
class EvidenceItem:
    name: str
    path: str
    sha256: str
    required: bool
    passed: bool
    evidence_status: str | None = None

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        if self.evidence_status is None:
            payload.pop("evidence_status")
        return payload


@dataclass(frozen=True, slots=True)
class OpsEvidenceBundle:
    run_id: str
    passed: bool
    items: tuple[EvidenceItem, ...]
    artifact_dir: str | None = None

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "run_id": self.run_id,
            "passed": self.passed,
            "items": [item.to_dict() for item in self.items],
        }
        if self.artifact_dir is not None:
            payload["artifact_dir"] = self.artifact_dir
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone ops evidence bundle",
            "",
            f"- Run ID: `{self.run_id}`",
            f"- Passed: `{self.passed}`",
            "",
            "| evidence | required | status | sha256 | artifact |",
            "|---|---:|---|---|---|",
        ]
        for item in self.items:
            status = "pass" if item.passed else "fail"
            lines.append(f"| `{item.name}` | `{item.required}` | {status} | `{item.sha256}` | `{item.path}` |")
        return "\n".join(lines) + "\n"

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> OpsEvidenceBundle:
        items = tuple(_evidence_item_from_dict(item) for item in payload.get("items", []) if isinstance(item, Mapping))
        return cls(
            run_id=str(payload["run_id"]),
            passed=bool(items)
            and payload.get("passed") is True
            and all(evidence_item_passed(item) for item in items if item.required),
            items=items,
            artifact_dir=str(payload["artifact_dir"]) if payload.get("artifact_dir") is not None else None,
        )


class EvidenceBundleService:
    """Collects focused ops evidence into one immutable bundle."""

    def __init__(
        self,
        *,
        certification: CertificationHarnessService | None = None,
        contracts: DataContractService | None = None,
        marketplace: ConnectorMarketplaceService | None = None,
    ) -> None:
        self._certification = certification or CertificationHarnessService()
        self._contracts = contracts or DataContractService()
        self._marketplace = marketplace or ConnectorMarketplaceService.default()

    def build(
        self,
        *,
        artifact_dir: str | Path,
        run_id: str,
        source: str,
        sink: str,
        strategy: str,
        rows: Sequence[Mapping[str, Any]],
        contract: Mapping[str, Any],
        row_count: int | None = None,
    ) -> OpsEvidenceBundle:
        directory = Path(artifact_dir)
        directory.mkdir(parents=True, exist_ok=True)

        certification_report = self._certification.run_mock_contract(
            artifact_dir=directory,
            source=source,
            sink=sink,
            strategy=strategy,
            row_count=row_count,
        )
        certification_path = directory / "certification_report.json"

        contract_report = self._contracts.evaluate(rows, contract)
        contract_path = directory / "data_contract_report.json"
        contract_md_path = directory / "data_contract_report.md"
        contract_path.write_text(contract_report.to_json(), encoding="utf-8")
        contract_md_path.write_text(contract_report.to_markdown(), encoding="utf-8")

        catalog = self._marketplace.catalog()
        marketplace_path = directory / "connector_marketplace.json"
        marketplace_md_path = directory / "connector_marketplace.md"
        marketplace_path.write_text(catalog.to_json(), encoding="utf-8")
        marketplace_md_path.write_text(catalog.to_markdown(), encoding="utf-8")

        certification_decision = certification_trust(certification_report.to_dict())
        items = (
            self._item(
                "certification",
                certification_path,
                passed=certification_decision.passed,
                evidence_status=certification_decision.evidence_status,
            ),
            self._item("data_contract", contract_path, passed=contract_report.passed),
            self._item("marketplace", marketplace_path, passed=catalog.passed),
        )
        bundle = OpsEvidenceBundle(
            run_id=run_id,
            passed=all(item.passed for item in items if item.required),
            items=items,
            artifact_dir=str(directory),
        )
        (directory / "ops_evidence_bundle.json").write_text(bundle.to_json(), encoding="utf-8")
        (directory / "ops_evidence_bundle.md").write_text(bundle.to_markdown(), encoding="utf-8")
        return bundle

    @staticmethod
    def _item(
        name: str,
        path: Path,
        *,
        passed: bool,
        required: bool = True,
        evidence_status: str | None = None,
    ) -> EvidenceItem:
        return EvidenceItem(
            name=name,
            path=str(path),
            sha256=sha256_file(path),
            required=required,
            passed=passed,
            evidence_status=evidence_status,
        )


def evidence_item_passed(item: EvidenceItem) -> bool:
    """Revalidate one evidence item at a deserialization or policy boundary."""

    path = Path(item.path)
    if not path.is_file() or path.is_symlink() or sha256_file(path) != item.sha256:
        return False
    payload = _json_mapping(path)
    if payload is None:
        return False
    projected = artifact_payload_passed(item.to_dict(), name=item.name)
    actual = artifact_payload_passed(payload, name=item.name)
    return projected and actual


def _evidence_item_from_dict(item: Mapping[str, Any]) -> EvidenceItem:
    raw_status = item.get("evidence_status")
    evidence_status = raw_status if isinstance(raw_status, str) else None
    name = str(item.get("name") or "")
    return EvidenceItem(
        name=name,
        path=str(item.get("path") or ""),
        sha256=str(item.get("sha256") or ""),
        required=item.get("required") is True,
        passed=artifact_payload_passed(item, name=name),
        evidence_status=evidence_status,
    )


def _json_mapping(path: Path) -> Mapping[str, Any] | None:
    if path.suffix.lower() != ".json":
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, Mapping) else None
