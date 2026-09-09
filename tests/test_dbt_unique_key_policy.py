from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from dpone.adapters.dbt_publish_artifact_reader import DbtArtifactReader
from dpone.contracts.dbt_publish_models import (
    DbtColumnArtifact,
    DbtModelArtifact,
    DbtPublishIntent,
    DbtPublishStrategyPolicy,
)
from dpone.contracts.dbt_unique_key_policy import (
    evaluate_dbt_unique_key,
)
from dpone.services.dbt_publish_planning import DbtPublishPlanner

_ROOT = Path(__file__).parents[1]
_MANIFEST = _ROOT / "examples" / "dbt-inline-publishing" / "fixtures" / "manifest.v12.json"


@pytest.mark.parametrize(
    ("value", "enforced", "columns", "expected_code"),
    [
        (None, True, {"id": True}, "DPONE_DBT_UNIQUE_KEY_MISSING"),
        ([], True, {"id": True}, "DPONE_DBT_UNIQUE_KEY_MISSING"),
        (["id", 1], True, {"id": True}, "DPONE_DBT_UNIQUE_KEY_INVALID"),
        ("lower(id)", True, {"id": True}, "DPONE_DBT_UNIQUE_KEY_EXPRESSION_UNSUPPORTED"),
        (["id", "ID"], True, {"id": True, "ID": True}, "DPONE_DBT_UNIQUE_KEY_INVALID"),
        ("id", False, {"id": True}, "DPONE_DBT_UNIQUE_KEY_NOT_IN_CONTRACT"),
        ("missing", True, {"id": True}, "DPONE_DBT_UNIQUE_KEY_NOT_IN_CONTRACT"),
        ("id", True, {"id": False}, "DPONE_DBT_UNIQUE_KEY_NULLABLE"),
    ],
)
def test_unique_key_policy_fails_closed_with_stable_codes(
    value: object,
    enforced: bool,
    columns: dict[str, bool],
    expected_code: str,
) -> None:
    report = evaluate_dbt_unique_key(
        value,
        contract_enforced=enforced,
        not_null_by_column=columns,
    )

    assert not report.passed
    assert [issue.code for issue in report.issues] == [expected_code]


def test_unique_key_policy_preserves_ordered_composite_identifiers() -> None:
    report = evaluate_dbt_unique_key(
        ["account_id", "valid_at"],
        contract_enforced=True,
        not_null_by_column={"account_id": True, "valid_at": True},
    )

    assert report.passed
    assert report.keys == ("account_id", "valid_at")


def test_artifact_reader_preserves_expression_string_as_one_key(
    tmp_path: Path,
) -> None:
    class AcceptOfficialSchema:
        def validate(self, payload, *, version):
            del payload, version
            return ()

    payload = json.loads(_MANIFEST.read_text(encoding="utf-8"))
    model = payload["nodes"]["model.dpone_dbt_demo.competitive_pricing_history"]
    model["config"]["unique_key"] = "concat(product_id, ',', date_id)"
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    artifact, issues = DbtArtifactReader(validator=AcceptOfficialSchema()).read(manifest)

    assert issues == ()
    assert artifact is not None
    history = next(item for item in artifact.models if item.name == "competitive_pricing_history")
    assert history.unique_key == ("concat(product_id, ',', date_id)",)


def test_auto_strategy_does_not_emit_an_unadmitted_key() -> None:
    model = replace(_model(), unique_key=("lower(id)",))
    planner = DbtPublishPlanner()

    strategy, issues = planner.strategy(
        model,
        DbtPublishIntent(
            enabled=True,
            profile="profile",
            workflow="workflow",
        ),
        _policy("incremental_merge"),
    )

    assert strategy == {
        "mode": "unresolved",
        "decision_reason": "no_policy_and_capability_authorized_safe_strategy",
    }
    assert {issue.code for issue in issues} == {
        "DPONE_DBT_STRATEGY_UNRESOLVED",
        "DPONE_DBT_UNIQUE_KEY_EXPRESSION_UNSUPPORTED",
    }


def test_publish_and_dbt_keys_must_match_even_for_non_merge_strategy() -> None:
    planner = DbtPublishPlanner()

    strategy, issues = planner.strategy(
        _model(),
        DbtPublishIntent(
            enabled=True,
            profile="profile",
            workflow="workflow",
            strategy_mode="full_refresh",
            unique_key=("other_id",),
        ),
        DbtPublishStrategyPolicy(
            allowed_strategies=("full_refresh",),
            full_refresh_authorized=True,
            full_refresh_max_source_bytes=1024,
        ),
    )

    assert strategy["mode"] == "full_refresh"
    assert [issue.code for issue in issues] == ["DPONE_DBT_UNIQUE_KEY_INVALID"]


def test_auto_strategy_emits_only_the_admitted_exact_key() -> None:
    strategy, issues = DbtPublishPlanner().strategy(
        _model(),
        DbtPublishIntent(
            enabled=True,
            profile="profile",
            workflow="workflow",
        ),
        _policy("incremental_merge"),
    )

    assert issues == ()
    assert strategy == {
        "mode": "incremental_merge",
        "decision_reason": "dbt_incremental_with_unique_key",
        "unique_key": ["id"],
    }


def _model() -> DbtModelArtifact:
    return DbtModelArtifact(
        unique_id="model.example.orders",
        name="orders",
        original_file_path="models/orders.sql",
        database="DWH",
        schema="mart",
        alias="orders",
        materialized="incremental",
        contract_enforced=True,
        columns=("id", "value"),
        column_contracts=(
            DbtColumnArtifact(
                name="id",
                data_type="bigint",
                nullable=False,
                constraints=("not_null",),
            ),
            DbtColumnArtifact(
                name="value",
                data_type="varchar",
                nullable=True,
            ),
        ),
        group=None,
        tags=(),
        meta={},
        unique_key=("id",),
        depends_on=(),
    )


def _policy(*strategies: str) -> DbtPublishStrategyPolicy:
    return DbtPublishStrategyPolicy(allowed_strategies=tuple(strategies))
