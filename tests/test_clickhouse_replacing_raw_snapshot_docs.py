"""Public manifest validation for the opt-in raw source snapshot selector."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft7Validator

ROOT = Path(__file__).resolve().parents[1]


def _native_validator() -> Draft7Validator:
    schema = json.loads((ROOT / "src/dpone/schema/etl-batch-manifest.schema.json").read_text())
    return Draft7Validator({"$ref": "#/definitions/native_transfer", "definitions": schema["definitions"]})


@pytest.mark.parametrize(
    "selector",
    [
        {"mode": "query_visible"},
        {"mode": "exact_raw_rows", "replica_scope": "single_server"},
        {"mode": "exact_raw_rows", "replica_scope": "connected_replica"},
    ],
)
def test_raw_snapshot_schema_accepts_closed_explicit_modes(selector: dict[str, str]) -> None:
    native: dict[str, object] = {"source_snapshot": selector}
    if selector["mode"] == "exact_raw_rows":
        native["execution"] = {"verification_backend": "target_local"}
    _native_validator().validate(native)


def test_raw_snapshot_schema_preserves_omission() -> None:
    _native_validator().validate({})


@pytest.mark.parametrize(
    "selector",
    [
        None,
        {},
        {"mode": "final"},
        {"mode": "exact_raw_rows"},
        {"mode": "exact_raw_rows", "replica_scope": "all_replicas"},
        {"mode": "query_visible", "replica_scope": "single_server"},
        {"mode": "exact_raw_rows", "replica_scope": "single_server", "final": False},
    ],
)
def test_raw_snapshot_schema_rejects_ambiguous_or_unapproved_modes(selector: object) -> None:
    assert list(_native_validator().iter_errors({"source_snapshot": selector}))


@pytest.mark.parametrize("execution", [{}, {"verification_backend": "python_readback"}])
def test_raw_snapshot_schema_requires_target_local_authority(execution: dict[str, str]) -> None:
    assert list(
        _native_validator().iter_errors(
            {
                "source_snapshot": {"mode": "exact_raw_rows", "replica_scope": "single_server"},
                "execution": execution,
            }
        )
    )


def test_complete_raw_snapshot_example_matches_published_schema() -> None:
    schema = json.loads((ROOT / "src/dpone/schema/etl-batch-manifest.schema.json").read_text())
    example = yaml.safe_load((ROOT / "examples/native/clickhouse-replacing-to-mssql-native.yaml").read_text())
    Draft7Validator(schema).validate(example)
