"""Protected record doubles plus actual vendored dbt schemas/parser."""

import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.contracts.composition_dbt_outcome import (
    EVIDENCE_SUBJECT_FIELDS,
    DbtArtifactOriginal,
    DbtCaptureRecord,
    DbtChildExit,
    DbtExitRecord,
    DbtOutcomeExpectation,
)
from dpone.contracts.dbt_execution_evidence import DbtNodeOutcome
from dpone.contracts.dbt_graph_contract import dbt_graph_contract_sha256
from dpone.contracts.strict_json import canonical_json_bytes
from dpone.runtime.dbt_run_results import parse_dbt_run_results
from dpone.services.composition_dbt_outcome import CompositionDbtOutcomeObserver
from tests.test_composition_activation_contract import digest
from tests.test_composition_dbt_capture import intent
from tests.test_dbt_dev_evidence_runtime_writer import _evidence
from tests.test_dbt_run_results_schema_validation import _valid_payload


@pytest.fixture
def scenario(tmp_path):
    manifest = json.loads(
        (Path(__file__).parents[1] / "examples/dbt-inline-publishing/fixtures/manifest.v12.json").read_bytes()
    )
    node = "model.dpone_dbt_demo.competitive_pricing"
    results = _valid_payload()
    results["results"][0]["unique_id"] = node
    value = intent(tmp_path)
    evidence = replace(
        _evidence(),
        toolchain_sha256=value.toolchain_sha256,
        invocation_id="invocation-1",
        dbt_schema_version="https://schemas.getdbt.com/dbt/run-results/v6.json",
        airflow={
            "dag_id": "DAG",
            "run_id": value.attempt.dag_run_id,
            "task_id": value.attempt.task_id,
            "try_number": value.attempt.try_number,
            "map_index": value.attempt.map_index,
        },
        nodes=(DbtNodeOutcome(node, "success", 1.0),),
    ).to_dict()
    build_manifest = json.loads(json.dumps(manifest))
    build_manifest["metadata"]["invocation_id"] = "invocation-1"
    payloads = (manifest, build_manifest, results, evidence)
    originals = tuple(
        DbtArtifactOriginal(role, path, canonical_json_bytes(payload))
        for (role, path), payload in zip(value.artifact_paths, payloads, strict=True)
    )
    value = replace(value, preflight_manifest_sha256=originals[0].sha256)
    expected = DbtOutcomeExpectation(
        dbt_graph_contract_sha256(manifest, (node,)),
        (node,),
        (node,),
        "1.12.3",
        "v12",
        "v6",
        tuple((key, evidence[key]) for key in sorted(EVIDENCE_SUBJECT_FIELDS)),
        ((node, "table", digest("schema")),),
    )
    closure = canonical_json_bytes(
        {
            "schema": "dpone.composition-dbt-quiescence.v1",
            "intent_sha256": value.intent_sha256,
            "pid": 123,
            "start_ticks": 100,
            "child_uid": value.child_uid,
            "boot_id": "offline-boot",
            "pid_namespace": "offline-ns",
        }
    )
    exited = DbtExitRecord(value, DbtChildExit(123, 100, 0), closure, originals[0])
    state = SimpleNamespace(record=DbtCaptureRecord(value, "CAPTURED", exited, originals), db_calls=[])
    store = SimpleNamespace(
        load_intent=lambda attempt: value,
        read_capture=lambda attempt: state.record,
        load_expectation=lambda attempt: expected,
    )

    def observe(attempt, intent, expectation, invocation):
        state.db_calls.append(attempt)
        return canonical_json_bytes(
            {
                "schema": "dpone.composition-dbt-materialization-observation.v1",
                "attempt_sha256": attempt.attempt_sha256,
                "intent_sha256": intent.intent_sha256,
                "invocation_id": invocation,
                "materializations": [list(row) for row in expectation.materializations],
                "catalog": [
                    {
                        "unique_id": unique_id,
                        "schema_sha256": schema_sha256,
                    }
                    for unique_id, _kind, schema_sha256 in expectation.materializations
                ],
            }
        )

    observer = CompositionDbtOutcomeObserver(
        store, result_parser=parse_dbt_run_results, materialization_observer=observe
    )
    return value, state, observer


def test_exact_run_results_and_fresh_materialization_required(scenario):
    value, state, observer = scenario
    result = observer.observe(value.attempt)
    assert result.state == "SUCCEEDED" and result.materialization_original
    assert state.db_calls == [value.attempt]


@pytest.mark.parametrize("fault", ["missing", "exit", "result", "graph", "evidence", "duplicate"])
def test_after_dispatch_uncertainty_never_becomes_failed(scenario, fault):
    value, state, observer = scenario
    record = state.record
    if fault == "missing":
        state.record = None
    elif fault == "exit":
        state.record = replace(record, exit_record=replace(record.exit_record, child=DbtChildExit(123, 100, 1)))
    else:
        originals = list(record.originals)
        index = {"result": 2, "graph": 1, "evidence": 3, "duplicate": 2}[fault]
        document = json.loads(originals[index].content)
        if fault == "result":
            document["results"][0]["status"] = "error"
        elif fault == "graph":
            document["nodes"]["model.dpone_dbt_demo.competitive_pricing"]["alias"] = "other"
        elif fault == "evidence":
            document["invocation_id"] = "other"
        else:
            document["results"].append(document["results"][0])
        originals[index] = replace(originals[index], content=canonical_json_bytes(document))
        state.record = replace(record, originals=tuple(originals))
    assert observer.observe(value.attempt).state == "COMMIT_UNKNOWN"
    assert state.db_calls == []


def test_database_mismatch_stays_unknown(scenario):
    value, state, observer = scenario
    observer._observe_materializations = lambda *args: b"{}"
    assert observer.observe(value.attempt).state == "COMMIT_UNKNOWN"


def test_only_protected_undispatched_closure_can_fail(scenario):
    value, state, observer = scenario
    state.record = DbtCaptureRecord(
        value,
        "UNDISPATCHED",
        undispatched_closure_original=canonical_json_bytes(
            {
                "attempt_sha256": value.attempt.attempt_sha256,
                "intent_sha256": value.intent_sha256,
                "build_dispatched": False,
                "closed": True,
            }
        ),
    )
    assert observer.observe(value.attempt).state == "FAILED"
    assert state.db_calls == []
    state.record = replace(state.record, undispatched_closure_original=b"{}")
    assert observer.observe(value.attempt).state == "COMMIT_UNKNOWN"


def _retarget_catalog(observer, rewrite):
    """Reshape only the observed catalog of an otherwise complete observation."""
    delegate = observer._observe_materializations

    def observe(attempt, intent, expectation, invocation):
        document = json.loads(delegate(attempt, intent, expectation, invocation))
        document["catalog"] = rewrite(document["catalog"])
        return canonical_json_bytes(document)

    observer._observe_materializations = observe


@pytest.mark.parametrize(
    "rewrite",
    [
        pytest.param(lambda catalog: [], id="missing"),
        pytest.param(lambda catalog: [{**catalog[0], "unique_id": "model.other.thing"}], id="foreign"),
        pytest.param(lambda catalog: [{**catalog[0], "schema_sha256": digest("other")}], id="digest"),
        pytest.param(lambda catalog: [[catalog[0]["unique_id"], catalog[0]["schema_sha256"]]], id="shape"),
        pytest.param(lambda catalog: catalog[0], id="not_a_list"),
    ],
)
def test_catalog_must_bind_every_declared_materialization(scenario, rewrite):
    value, _state, observer = scenario
    _retarget_catalog(observer, rewrite)
    assert observer.observe(value.attempt).state == "COMMIT_UNKNOWN"


def test_catalog_may_carry_additional_observed_metadata(scenario):
    """Extra observed columns are retained evidence, not an undeclared obligation."""
    value, _state, observer = scenario
    _retarget_catalog(
        observer,
        lambda catalog: [{**row, "declared_columns": ["date_id"], "kind": "table"} for row in catalog],
    )
    assert observer.observe(value.attempt).state == "SUCCEEDED"
