from __future__ import annotations

import json
from pathlib import Path

from dpone.adapters.dbt_manifest_schema import OfficialDbtManifestValidator

ROOT = Path(__file__).resolve().parents[1]


def test_real_sqlserver_manifest_v12_passes_with_only_bounded_adapter_extension() -> None:
    payload = json.loads(
        (ROOT / "examples" / "dbt-inline-publishing" / "fixtures" / "manifest.v12.json").read_text(encoding="utf-8")
    )

    violations = OfficialDbtManifestValidator().validate(payload, version=12)

    assert violations
    assert {item.severity for item in violations} == {"warning"}
    assert {item.rule for item in violations} == {"additionalProperties"}
    assert all(item.path.startswith("$.macros.") for item in violations)


def test_official_schema_blocks_model_contract_shape_drift() -> None:
    payload = json.loads(
        (ROOT / "examples" / "dbt-inline-publishing" / "fixtures" / "manifest.v12.json").read_text(encoding="utf-8")
    )
    model = payload["nodes"]["model.dpone_dbt_demo.competitive_pricing"]
    model["config"]["contract"]["enforced"] = "false"

    violations = OfficialDbtManifestValidator().validate(payload, version=12)

    assert any(
        item.severity == "error" and "model.dpone_dbt_demo.competitive_pricing" in item.path for item in violations
    )
