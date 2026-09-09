from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dpone.strategy_intelligence.native_transfer_evidence_artifacts import NativeTransferEvidenceArtifactWriter


class NativeTransferEvidenceBundleService:
    def write_from_files(
        self,
        *,
        run_id: str,
        plan_json: str | Path | None,
        contract_json: str | Path | None,
        payload_specs: tuple[str, ...],
        output_dir: str | Path,
    ) -> dict[str, Any]:
        contract = self._load_contract(plan_json=plan_json, contract_json=contract_json)
        payloads = self._load_payloads(payload_specs)
        artifact = NativeTransferEvidenceArtifactWriter(output_dir).write(
            run_id=run_id,
            evidence_contract=contract,
            payloads=payloads,
        )
        return {
            "run_id": run_id,
            "directory": str(artifact.directory),
            "index_path": str(artifact.index_path),
            "markdown_path": str(artifact.markdown_path),
            "artifact_paths": [str(path) for path in artifact.artifact_paths],
        }

    def _load_contract(
        self,
        *,
        plan_json: str | Path | None,
        contract_json: str | Path | None,
    ) -> dict[str, Any]:
        if bool(plan_json) == bool(contract_json):
            raise ValueError("provide exactly one of --plan-json or --contract-json")
        payload = _read_json(Path(plan_json or contract_json or ""))
        if contract_json:
            return _expect_mapping(payload, "contract_json")
        return self._extract_contract(_expect_mapping(payload, "plan_json"))

    def _extract_contract(self, payload: dict[str, Any]) -> dict[str, Any]:
        candidates = [
            payload.get("evidence_contract"),
            _deep_get(payload, ("native_transfer_plan", "evidence_contract")),
            _deep_get(payload, ("strategy_intelligence", "decision", "native_transfer_plan", "evidence_contract")),
        ]
        for candidate in candidates:
            if isinstance(candidate, dict) and candidate:
                return dict(candidate)
        raise ValueError("native transfer evidence_contract was not found in plan JSON")

    def _load_payloads(self, payload_specs: tuple[str, ...]) -> dict[str, dict[str, Any]]:
        payloads: dict[str, dict[str, Any]] = {}
        for spec in payload_specs:
            name, path = _parse_payload_spec(spec)
            payloads[name] = _expect_mapping(_read_json(path), name)
        return payloads


def _parse_payload_spec(spec: str) -> tuple[str, Path]:
    if "=" not in spec:
        raise ValueError("--payload must use artifact_name=path syntax")
    name, raw_path = spec.split("=", 1)
    if not name or not raw_path:
        raise ValueError("--payload must use artifact_name=path syntax")
    return name, Path(raw_path)


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _expect_mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must contain a JSON object")
    return dict(value)


def _deep_get(payload: dict[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current
