"""Contract tests for the `gitops.airflow_dag_spec` artifact (Phase 1).

Covers the frozen models, authoring-block validation, fingerprint discipline
and the published JSON Schema (docs/schemas/gitops/airflow-dag-spec.schema.json).
"""

from __future__ import annotations

import json
from pathlib import Path

from dpone.gitops.airflow_dag_spec import (
    DAG_SPEC_KIND,
    DAG_SPEC_SCHEMA_VERSION,
    DagSpecEdge,
    DagSpecNode,
    compute_spec_fingerprint,
    parse_dag_declaration,
)

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "docs" / "schemas" / "gitops" / "airflow-dag-spec.schema.json"


def _declaration(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "description": "sync",
        "schedule": "0 6 * * *",
        "start_date": "2026-07-07",
        "timezone": "Europe/Moscow",
        "catchup": False,
        "max_active_runs": 1,
        "tags": ["dpone"],
        "default_args": {"retries": 0},
        "operator_overrides": {"in_cluster": True},
        "workloads": ["app", "web"],
        "wiring": {"mode": "waves", "max_parallel_workloads": 2},
    }
    payload.update(overrides)
    return payload


def test_parse_dag_declaration_accepts_a_valid_catalog_entry() -> None:
    declaration, issues = parse_dag_declaration("DAG__m__sync", _declaration())

    assert not issues
    assert declaration is not None
    assert declaration.dag_id == "DAG__m__sync"
    assert declaration.schedule == "0 6 * * *"
    assert declaration.start_date == "2026-07-07"
    assert declaration.workloads == ("app", "web")
    assert declaration.wiring.mode == "waves"
    assert declaration.wiring.max_parallel_workloads == 2


def test_parse_dag_declaration_normalizes_null_schedule_and_asset_schedule() -> None:
    null_decl, null_issues = parse_dag_declaration("d1", _declaration(schedule=None))
    asset_decl, asset_issues = parse_dag_declaration(
        "d2",
        _declaration(schedule={"assets": ["postgres://dst/app", {"uri": "ext://x", "external": True}]}),
    )

    assert not null_issues and null_decl is not None
    assert null_decl.schedule is None
    assert not asset_issues and asset_decl is not None
    assert asset_decl.schedule is not None
    assert [asset.uri for asset in asset_decl.schedule.assets] == ["postgres://dst/app", "ext://x"]
    assert [asset.external for asset in asset_decl.schedule.assets] == [False, True]


def test_parse_dag_declaration_requires_exactly_one_workload_source() -> None:
    both, both_issues = parse_dag_declaration("d", _declaration(group="wa"))
    neither, neither_issues = parse_dag_declaration("d", _declaration(workloads=None))

    assert both is None
    assert any(issue.code == "dag_spec_workload_source_ambiguous" for issue in both_issues)
    assert neither is None
    assert any(issue.code == "dag_spec_workload_source_missing" for issue in neither_issues)


def test_parse_dag_declaration_rejects_bad_wiring_mode_and_schedule_types() -> None:
    _, mode_issues = parse_dag_declaration("d", _declaration(wiring={"mode": "magic"}))
    _, schedule_issues = parse_dag_declaration("d", _declaration(schedule=42))
    _, start_issues = parse_dag_declaration("d", _declaration(start_date=None))

    assert any(issue.code == "dag_spec_wiring_mode_invalid" for issue in mode_issues)
    assert any(issue.code == "dag_spec_schedule_invalid" for issue in schedule_issues)
    assert any(issue.code == "dag_spec_start_date_missing" for issue in start_issues)


def test_parse_dag_declaration_rejects_unknown_keys_with_a_hint() -> None:
    _, issues = parse_dag_declaration("d", _declaration(shedule="typo"))

    assert any(issue.code == "dag_spec_unknown_key" and "shedule" in issue.message for issue in issues)


def test_spec_fingerprint_is_deterministic_and_ignores_advisory_fields() -> None:
    payload = {
        "kind": DAG_SPEC_KIND,
        "schema_version": DAG_SPEC_SCHEMA_VERSION,
        "dag_id": "d",
        "nodes": [{"node_id": "a"}],
        "edges": [],
    }
    reordered = json.loads(json.dumps(dict(reversed(list(payload.items())))))
    with_advisory = {**payload, "warnings": [{"code": "x"}], "spec_fingerprint": "sha256:stale"}

    fingerprint = compute_spec_fingerprint(payload)

    assert fingerprint.startswith("sha256:")
    assert compute_spec_fingerprint(reordered) == fingerprint
    assert compute_spec_fingerprint(with_advisory) == fingerprint


def test_dag_spec_models_are_frozen_and_jsonable() -> None:
    edge = DagSpecEdge(upstream="a", downstream="b", reason="curated", origin="wiring.waves")
    node = DagSpecNode(node_id="a", workload_id="a")

    assert edge.to_jsonable() == {
        "upstream": "a",
        "downstream": "b",
        "reason": "curated",
        "origin": "wiring.waves",
    }
    assert node.to_jsonable()["pack_ref"] == "cached://workloads/a"
    assert node.to_jsonable()["pack_path"] == ".dpone/gitops/airflow/a/airflow-pack.json"


def test_published_json_schema_accepts_a_full_spec_document() -> None:
    import jsonschema

    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    document = {
        "kind": DAG_SPEC_KIND,
        "schema_version": DAG_SPEC_SCHEMA_VERSION,
        "producer": "dpone gitops airflow dag-spec",
        "dag_id": "DAG__m__sync",
        "domain": "marketing",
        "description": "sync",
        "schedule": {"assets": [{"uri": "postgres://dst/app", "external": False}]},
        "start_date": "2026-07-07",
        "timezone": "Europe/Moscow",
        "catchup": False,
        "max_active_runs": 1,
        "tags": ["dpone"],
        "default_args": {"retries": 0, "retry_delay_minutes": 5},
        "operator_overrides": {"in_cluster": True},
        "source": {"type": "catalog", "path": "dpone_workloads/gitops/domains/marketing.yaml"},
        "wiring": {
            "mode": "waves",
            "max_parallel_workloads": 2,
            "dependencies": {},
            "visible_task_budget": {"warn": 100, "max": 250},
        },
        "visible_task_plan": {
            "estimated_total": 1,
            "warn_threshold": 100,
            "max_tasks": 250,
            "status": "within_budget",
        },
        "nodes": [
            {
                "node_id": "app",
                "workload_id": "app",
                "selector": None,
                "task_group": None,
                "visibility": "inline",
                "estimated_visible_tasks": 1,
                "pack_ref": "cached://app",
                "pack_path": ".dpone/gitops/airflow/app/airflow-pack.json",
            }
        ],
        "edges": [{"upstream": "app", "downstream": "web", "reason": "declared", "origin": "depends_on"}],
        "topological_order": ["app"],
        "warnings": [],
        "spec_fingerprint": "sha256:" + "0" * 64,
    }

    jsonschema.validate(document, schema)


def test_published_json_schema_rejects_wrong_kind_and_bad_edge_reason() -> None:
    import jsonschema

    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    good = {
        "kind": DAG_SPEC_KIND,
        "schema_version": "1",
        "producer": "p",
        "dag_id": "d",
        "schedule": None,
        "start_date": "2026-01-01",
        "nodes": [{"node_id": "a", "workload_id": "a", "pack_ref": "cached://a", "pack_path": "x"}],
        "edges": [],
        "topological_order": ["a"],
        "spec_fingerprint": "sha256:" + "0" * 64,
    }

    jsonschema.validate(good, schema)
    for broken in (
        {**good, "kind": "gitops.airflow_pack"},
        {**good, "edges": [{"upstream": "a", "downstream": "b", "reason": "guessed", "origin": "x"}]},
        {**good, "schedule": 42},
    ):
        try:
            jsonschema.validate(broken, schema)
        except jsonschema.ValidationError:
            continue
        raise AssertionError(f"schema accepted invalid document: {broken}")
