from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
from importlib.resources import files

import pytest

from dpone.adapters.dbt_run_results_schema import OfficialDbtRunResultsValidator

OFFICIAL_SCHEMA_SHA256 = "1783bda55656bde624ed67640375640a2919a8608e2505e3a02c04cd139e2cdf"


def _valid_payload() -> dict[str, object]:
    return {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v6.json",
            "dbt_version": "1.12.3",
            "generated_at": "2026-07-28T12:00:00Z",
            "invocation_id": "invocation-1",
            "invocation_started_at": None,
            "env": {"DBT_ENV": "test"},
        },
        "results": [
            {
                "status": "success",
                "timing": [
                    {
                        "name": "execute",
                        "started_at": "2026-07-28T12:00:00Z",
                        "completed_at": "2026-07-28T12:00:01Z",
                    }
                ],
                "thread_id": "Thread-1",
                "execution_time": 1.0,
                "adapter_response": {"rows_affected": 1},
                "message": None,
                "failures": None,
                "unique_id": "model.analytics.orders",
                "compiled": True,
                "compiled_code": "select 1",
                "relation_name": "analytics.orders",
                "batch_results": {
                    "successful": [["2026-07-28", "analytics.orders"]],
                    "failed": [],
                },
            }
        ],
        "elapsed_time": 1.0,
        "args": {"which": "build"},
    }


def test_vendored_run_results_v6_schema_matches_official_artifact() -> None:
    schema_bytes = files("dpone.schema.dbt").joinpath("run-results-v6.schema.json").read_bytes()

    assert sha256(schema_bytes).hexdigest() == OFFICIAL_SCHEMA_SHA256


def test_official_run_results_v6_schema_accepts_valid_payload_and_optional_fields() -> None:
    violations = OfficialDbtRunResultsValidator().validate(_valid_payload(), version=6)

    assert violations == ()


def test_official_run_results_v6_schema_returns_bounded_deterministic_violations() -> None:
    payload = deepcopy(_valid_payload())
    payload["unexpected_root"] = True
    payload["results"] = [
        {"unique_id": f"model.analytics.invalid_{index}", "unexpected": True} for index in reversed(range(25))
    ]

    first = OfficialDbtRunResultsValidator().validate(payload, version=6)
    second = OfficialDbtRunResultsValidator().validate(payload, version=6)

    assert first == second
    assert len(first) == 20
    assert all(item.severity == "error" for item in first)
    assert [item.path for item in first] == sorted(item.path for item in first)
    assert {item.rule for item in first} <= {"additionalProperties", "required"}


def test_official_run_results_v6_schema_rejects_invalid_payload() -> None:
    payload = _valid_payload()
    payload["elapsed_time"] = "one second"

    violations = OfficialDbtRunResultsValidator().validate(payload, version=6)

    assert len(violations) == 1
    assert violations[0].path == "$.elapsed_time"
    assert violations[0].rule == "type"
    assert violations[0].severity == "error"


def test_official_run_results_schema_rejects_unsupported_version() -> None:
    with pytest.raises(ValueError, match="unsupported dbt run-results schema version"):
        OfficialDbtRunResultsValidator().validate(_valid_payload(), version=7)
