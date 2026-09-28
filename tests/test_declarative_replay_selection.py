"""Public authoring and source-free identity for declarative durable replay."""

from copy import deepcopy
from dataclasses import replace

import pytest

from dpone.contracts.quality_replay_selection import (
    replay_selection,
    validate_replay_configuration,
)
from dpone.dag.process_config_parser import ETLProcessConfigParser
from dpone.runtime.governance.quality_replay_identity import admission_digest
from tests.test_quality_replay_identity import config


def manifest():
    return {
        "name": "refresh",
        "source": {"type": "postgres", "connection_id": "source", "table": {"schema": "public", "name": "items"}},
        "sink": {
            "type": "clickhouse",
            "connection_id": "target",
            "table": {"schema": "analytics", "name": "items"},
            "staging": {"schema": "analytics"},
            "strategy": {"mode": "full_refresh", "max_source_bytes": 1024},
            "options": {
                "durable_quality_replay": True,
                "physical_design": {
                    "storage": {
                        "clickhouse": {
                            "engine": "ReplicatedMergeTree",
                            "order_by": ["id"],
                            "cluster": {"name": "replicas", "ddl_scope": "cluster", "replication_mode": "internal"},
                        }
                    }
                },
            },
        },
    }


@pytest.mark.parametrize("value", [None, 0, 1, "true", "false", {}, []])
def test_selector_rejects_non_boolean_before_hydration(value, monkeypatch):
    raw = manifest()
    raw["sink"]["options"]["durable_quality_replay"] = value
    monkeypatch.setattr(
        "dpone.dag.config_models.ETLProcessConfig.ensure_runtime_bindings", lambda *_: pytest.fail("hydration")
    )
    with pytest.raises(ValueError, match="DPONE_REPLAY_QUALITY"):
        ETLProcessConfigParser().parse(raw)


def test_selector_internal_route_and_legacy_default():
    raw = manifest()
    assert validate_replay_configuration(raw) is True
    assert replay_selection({}, sink_type="postgres") is False
    raw["sink"]["options"]["durable_quality_replay"] = False
    raw["sink"]["strategy"]["mode"] = "append"
    assert validate_replay_configuration(raw) is False


@pytest.mark.parametrize("edit", ["sink", "strategy", "external", "budget", "source", "root", "typo"])
def test_unsafe_or_misplaced_selector_fails_before_runtime(edit):
    raw = manifest()
    if edit == "sink":
        raw["sink"]["type"] = "postgres"
    if edit == "strategy":
        raw["sink"]["strategy"]["mode"] = "append"
    if edit == "budget":
        raw["sink"]["strategy"].pop("max_source_bytes")
    if edit == "external":
        raw["sink"]["options"]["physical_design"]["storage"]["clickhouse"]["cluster"]["replication_mode"] = "external"
    if edit == "source":
        raw["source"]["options"] = {"durable_quality_replay": False}
    if edit == "root":
        raw["durable_quality_replay"] = True
    if edit == "typo":
        raw["sink"]["options"]["durable_quality_replayy"] = True
    with pytest.raises(ValueError, match="DPONE_REPLAY_QUALITY"):
        validate_replay_configuration(raw)


@pytest.mark.parametrize("selected", [False, True])
def test_composition_selector_preserves_existing_identity(selected):
    original = config(sink_options={"lineage": False}, lineage=False)
    options = deepcopy(original.options)
    options["durable_quality_replay"] = selected
    options["sink_options"]["durable_quality_replay"] = selected
    assert admission_digest(replace(original, options=options)) == admission_digest(original)


def test_conflicting_flat_nested_or_source_selector_cannot_be_excluded():
    for options in [
        {"durable_quality_replay": True, "sink_options": {"durable_quality_replay": False}},
        {"source_options": {"durable_quality_replay": True}},
        {"durable_quality_replay": "true"},
        {"source_options": {"sink_options": {"durable_quality_replay": True}}},
        {"sink_options": {"sink_options": {"durable_quality_replay": object()}}},
    ]:
        with pytest.raises(ValueError):
            admission_digest(config(**options))


def test_airflow_attempt_changes_do_not_change_bound_interval_semantics():
    interval = {
        "interval_start": "2026-09-27T00:00:00+00:00",
        "interval_end": "2026-09-28T00:00:00+00:00",
        "logical_date": "2026-09-27T00:00:00+00:00",
        "dag_id": "replay_demo",
        "dag_run_id": "run_001",
        "try_number": 1,
        "partition_key": None,
        "partition_dimension": None,
        "partition_mode": None,
    }
    original = config(interval=interval)
    assert admission_digest(config(interval={**interval, "try_number": 2})) == admission_digest(original)
    assert admission_digest(config(interval={**interval, "dag_run_id": "run_002"})) != admission_digest(original)
    assert admission_digest(
        config(interval={**interval, "interval_end": "2026-09-29T00:00:00+00:00"})
    ) != admission_digest(original)
    with pytest.raises(ValueError):
        admission_digest(config(interval={**interval, "unknown": True}))
    with pytest.raises(ValueError):
        admission_digest(config(interval={**interval, "try_number": True}))
