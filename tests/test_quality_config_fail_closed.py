from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft7Validator
from jsonschema.exceptions import ValidationError

from dpone.governance.quality import QualityGatePolicy
from dpone.governance.quality_normalize import normalize_quality_config


@pytest.mark.parametrize(
    "quality",
    (
        {"mode": "fail", "checks": [{"type": "min_rows", "threshold": 1, "sied": "source"}]},
        {"mode": "fail", "checks": [{"type": "min_rows", "threshold": 1, "enabled": False}]},
        {"gates": [{"type": "min_rows", "threshold": 1, "sied": "source"}]},
        {"gates": [{"type": "min_rows", "threshold": 1, "enabled": False}]},
        {"mode": "fail", "checks": [{"type": "min_rows", "threshold": 1}], "enabeld": True},
    ),
)
def test_quality_runtime_rejects_unknown_reserved_fields(quality: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="unknown field"):
        QualityGatePolicy.from_config(quality)


def test_min_rows_rejects_conflicting_threshold_aliases() -> None:
    with pytest.raises(ValueError, match="threshold and value must match"):
        normalize_quality_config(
            {
                "mode": "fail",
                "checks": [
                    {
                        "type": "min_rows",
                        "threshold": 0,
                        "value": 100,
                    }
                ],
            }
        )


def test_min_rows_accepts_equal_threshold_aliases_deterministically() -> None:
    normalized = normalize_quality_config(
        {
            "mode": "fail",
            "checks": [
                {
                    "type": "min_rows",
                    "threshold": 5,
                    "value": 5,
                }
            ],
        }
    )

    assert normalized["gates"][0]["threshold"] == 5


def test_unknown_warning_check_keeps_legacy_provider_fields_explicitly_non_blocking() -> None:
    policy = QualityGatePolicy.from_config(
        {
            "mode": "warn",
            "checks": [
                {
                    "id": "provider_check",
                    "type": "provider_check",
                    "provider_options": {"enabled": True},
                }
            ],
        }
    )

    assert policy.gates[0].severity == "warning"
    assert policy.gates[0].raw["_compatibility_unknown_check"] is True


@pytest.mark.parametrize(
    "gate",
    (
        {"id": "selected_key_not_null", "type": "not_null", "side": "target", "column": "event_id"},
        {"id": "selected_key_unique", "type": "unique", "side": "target", "columns": ["event_id"]},
    ),
)
def test_runtime_rejects_studio_sample_check_fields_on_canonical_gates(gate: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="unknown field"):
        QualityGatePolicy.from_config({"gates": [gate]})


@pytest.mark.parametrize(
    "quality",
    (
        {"mode": "fail", "checks": [{"type": "min_rows", "threshold": 1, "sied": "source"}]},
        {"mode": "fail", "checks": [{"type": "min_rows", "threshold": 1, "enabled": False}]},
        {"gates": [{"type": "min_rows", "threshold": 1, "sied": "source"}]},
        {"gates": [{"type": "min_rows", "threshold": 1, "enabled": False}]},
        {"mode": "fail", "checks": [{"type": "min_rows", "threshold": 1}], "enabeld": True},
    ),
)
def test_classic_and_flow_schemas_reject_unknown_reserved_fields(
    quality: dict[str, object],
) -> None:
    for schema_path in (
        Path("src/dpone/schema/etl-batch-manifest.schema.json"),
        Path("src/dpone/schema/etl-flow-manifest.schema.json"),
    ):
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        quality_schema = {
            "$ref": "#/definitions/managed_quality",
            "definitions": schema["definitions"],
        }
        with pytest.raises(ValidationError):
            Draft7Validator(quality_schema).validate(quality)
