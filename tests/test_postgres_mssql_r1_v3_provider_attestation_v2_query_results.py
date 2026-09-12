"""Query-arm partition for Provider Attestation V2."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_red_probe import run_case

_CASES = tuple(
    item["case_id"]
    for item in json.loads(
        Path("docs/schemas/evidence/postgres-mssql-r1-v3-provider-attestation-v2-cases.json").read_text(
            encoding="utf-8"
        )
    )["ordered_cases"]
    if item["partition"] == "query_arms"
)


@pytest.mark.parametrize("case_id", _CASES, ids=lambda value: value)
def test_case(case_id: str, record_property) -> None:
    run_case(case_id, record_property)
