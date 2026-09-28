"""Synthetic declarative replay through real manifest and processor boundaries.

A runtime-only hydrator injects an in-memory authority and catalog. The seed is
an original publication produced by the real services, not a forged receipt.
These tests do not certify ClickHouse, KeeperMap, or an Airflow deployment.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from dpone.commands import run_cmd
from dpone.ports.runtime_hydrator import RuntimeBindings
from dpone.runtime.bootstrap_runner import DefaultProcessRunner
from dpone.runtime.governance.ports import StagedLoadHandle
from dpone.runtime.sinks.clickhouse_full_refresh_publication import (
    REPLAY_OPTION,
    ClickHouseFullRefreshPublicationService,
)
from tests.test_quality_replay_runtime import SCHEMA, Rig
from tests.test_runtime_etl_processor_split import StubLogger, StubSink, StubSource

ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = (
    ROOT / "examples/batch/durable-replay-full-refresh.batch.yaml",
    ROOT / "examples/flow/durable-replay-full-refresh.flow.yaml",
)


class ReplayHydrator:
    """Inject external dependencies only; retain the production execution path."""

    def __init__(self, *, fail_quality=False, target=False, initial_complete=True):
        if target:
            from tests.test_quality_replay_target import TargetRig

            self.rig = TargetRig()
        else:
            self.rig = Rig()
        self.initial_complete = initial_complete
        self.seeded = False
        self.fail_quality = fail_quality
        self.identities = []
        self.configurations = []
        self.allow_source_boundary = False
        self.source_boundaries = 0

    def build(self, *, config, load_config):
        assert config["sink"]["options"]["durable_quality_replay"] is True
        assert load_config.options["durable_quality_replay"] is True
        assert load_config.options["sink_options"]["durable_quality_replay"] is True
        self.configurations.append(load_config)
        owner = self

        class Source(StubSource):
            def extract(self, *_args):
                if owner.allow_source_boundary:
                    owner.source_boundaries += 1
                    raise RuntimeError("synthetic new operation source boundary")
                pytest.fail("committed retry must not read the source")

            def get_incremental_state(self, *_args):
                pytest.fail("committed retry must not read source state")

        class Sink(StubSink):
            quality_replay_store = owner.rig.fresh_store()

            def prepare_runtime_admission(self, config, *, run_context, dag_id, **kwargs):
                identified = ClickHouseFullRefreshPublicationService.bind_runtime_identity(
                    config, scheduler_run_id=run_context.run_id, process_id=dag_id or ""
                )
                owner.identities.append(run_context.run_id)
                if not owner.seeded:
                    rig = owner.rig
                    rig.config = identified
                    rig.candidate = replace(identified, target_table="candidate")
                    rig.handle = StagedLoadHandle(rig.candidate, SCHEMA, 2)
                    rig.session = rig.fresh_session(store=rig.store)
                    rig.publish(complete=owner.initial_complete)
                    owner.seeded = True
                    if owner.fail_quality:
                        rig.overwrite_record(quality_evidence=None)
                return owner.rig.service.prepare_admission(identified)

            def replay_result(self, config):
                return config.options.get(REPLAY_OPTION)

            def load(self, *_args):
                pytest.fail("committed retry must not redispatch a load")

        return RuntimeBindings(source_obj=Source(None), sink_obj=Sink(), etl_logger=StubLogger())


def install_replay_runtime(monkeypatch, *, fail_quality=False, target=False, initial_complete=True):
    from dpone.ports import process_runner, runtime_hydrator

    hydrator = ReplayHydrator(fail_quality=fail_quality, target=target, initial_complete=initial_complete)
    monkeypatch.setattr(runtime_hydrator, "_RUNTIME_HYDRATOR", hydrator)
    monkeypatch.setattr(process_runner, "_PROCESS_RUNNER", DefaultProcessRunner())
    return hydrator


def replay_manifest(tmp_path, example=EXAMPLES[0], *, target=False):
    """Keep declared gates and adapt acceptance columns to the synthetic schema."""
    payload = yaml.safe_load(example.read_text())
    quality = payload.get("quality") or payload["processes"][0]["quality"]
    quality["acceptance"]["capture"]["target"] = target
    quality["acceptance"]["checks"]["null_counts"] = "off"
    quality["acceptance"]["checks"]["distinct_counts"] = "off"
    path = tmp_path / example.name
    path.write_text(yaml.safe_dump(payload, sort_keys=False))
    return path


def run_args(path, *, run_id="operation-001"):
    parser = argparse.ArgumentParser()
    run_cmd.register_parser(parser.add_subparsers(dest="command"))
    argv = ["run", str(path), "--dag-id", "replay_demo", "--format", "json"]
    if run_id is not None:
        argv += ["--run-id", run_id]
    return parser.parse_args(argv)


def invoke(args):
    return run_cmd.cmd_run(args, ctx=object(), logger=logging.getLogger("replay-test"))


@pytest.mark.parametrize("example", EXAMPLES, ids=["batch", "flow"])
def test_manifest_cli_retries_preserve_original_proof_without_redispatch(monkeypatch, tmp_path, capsys, example):
    runtime = install_replay_runtime(monkeypatch)
    args = run_args(replay_manifest(tmp_path, example))
    for _ in range(2):
        assert invoke(args) == 0, capsys.readouterr()
        captured = capsys.readouterr()
        payload = json.loads(captured.out)
        assert payload["run_id"] == "operation-001"
        assert payload["passed"] is True
        replay = payload["result"]["details"]["reconciliation_metrics"]["quality_replay"]
        assert replay["replayed_from"]["run_id"] == "synthetic-run"
        assert replay["quality_gates"]["passed"] is True
    assert runtime.identities == ["operation-001", "operation-001"]
    assert runtime.rig.ddl.dispatches == runtime.rig.ddl.cleanup_dispatches == 1


def test_cli_quality_failure_is_one_json_error_and_never_success(monkeypatch, tmp_path, capsys):
    runtime = install_replay_runtime(monkeypatch, fail_quality=True)
    args = run_args(replay_manifest(tmp_path))
    assert invoke(args) == 1
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["passed"] is False
    assert payload["result"]["status"] == "error"
    assert payload["result"]["error_code"].startswith("DPONE_REPLAY_QUALITY_EVIDENCE_")
    assert "DPONE_REPLAY_QUALITY_EVIDENCE_" in captured.err
    assert runtime.rig.ddl.dispatches == 1


@pytest.mark.parametrize("example", EXAMPLES, ids=["batch", "flow"])
def test_documented_target_manifests_validate_schema_and_parse_without_hydration(monkeypatch, example):
    import jsonschema

    from dpone.manifest.loader import ManifestLoaderRouter
    from dpone.ports import runtime_hydrator

    class NoRuntime:
        def build(self, **kwargs):
            pytest.fail("metadata parsing must not hydrate credentials or clients")

    monkeypatch.setattr(runtime_hydrator, "_RUNTIME_HYDRATOR", NoRuntime())
    schema_name = "etl-batch-manifest" if "batch" in example.name else "etl-flow-manifest"
    schema = json.loads((ROOT / f"src/dpone/schema/{schema_name}.schema.json").read_text())
    jsonschema.validate(yaml.safe_load(example.read_text()), schema)
    manifest = ManifestLoaderRouter().load(example, metadata_only=True)
    options = manifest.processes[0].config.load_config.options
    assert options["sink_options"]["durable_quality_replay"] is True
    assert options["quality"]["acceptance"]["capture"]["target"] is True


@pytest.mark.parametrize("explicit", [True, False], ids=["new-explicit-run", "fresh-manual-uuid"])
def test_new_invocation_does_not_replay_previous_operation(monkeypatch, tmp_path, capsys, explicit):
    runtime = install_replay_runtime(monkeypatch)
    for name in ("DPONE_DAG_RUN_ID", "DPONE_DAG_ID", "DPONE_TRY_NUMBER"):
        monkeypatch.delenv(name, raising=False)
    path = replay_manifest(tmp_path)
    first = run_args(path, run_id="operation-001" if explicit else None)
    assert invoke(first) == 0, capsys.readouterr()
    capsys.readouterr()
    runtime.allow_source_boundary = True
    second = run_args(path, run_id="operation-002" if explicit else None)
    assert invoke(second) == 1
    payload = json.loads(capsys.readouterr().out)
    assert "synthetic new operation source boundary" in payload["result"]["errors"][0]
    assert runtime.source_boundaries == 1
    assert runtime.identities[0] != runtime.identities[1]
    if not explicit:
        assert all(value.startswith("manual-") for value in runtime.identities)
    assert runtime.rig.ddl.dispatches == 1


@pytest.mark.parametrize("initial_complete", [False, True], ids=["pending-target", "complete-target"])
def test_cli_target_completion_and_replay_never_rescan_or_redispatch(monkeypatch, tmp_path, capsys, initial_complete):
    runtime = install_replay_runtime(monkeypatch, target=True, initial_complete=initial_complete)
    args = run_args(replay_manifest(tmp_path, target=True))
    for _ in range(2):
        assert invoke(args) == 0, capsys.readouterr()
        payload = json.loads(capsys.readouterr().out)
        replay = payload["result"]["details"]["reconciliation_metrics"]["quality_replay"]
        assert replay["kind"] == "dpone.quality.replay.result.v2"
        assert replay["acceptance"]["target"]["row_count"] == 2
    assert runtime.rig.reader.scans == 1
    assert runtime.rig.capsule().state == "COMPLETE"
    assert runtime.rig.ddl.dispatches == 1


def test_cli_target_timeout_preserves_commit_truth_and_retry_finishes_without_source(monkeypatch, tmp_path, capsys):
    from dpone.contracts.target_acceptance import TargetAcceptanceError

    runtime = install_replay_runtime(monkeypatch, target=True, initial_complete=False)
    runtime.rig.reader.failure = TargetAcceptanceError("INCOMPLETE", quiescent=True)
    args = run_args(replay_manifest(tmp_path, target=True))
    assert invoke(args) == 1
    captured = capsys.readouterr()
    failure = json.loads(captured.out)
    assert failure["result"]["replay_details"]["target_commit"] == "proven"
    assert failure["result"]["replay_details"]["governance"] == "blocked"
    assert "target_commit=proven" in captured.err
    assert runtime.rig.capsule().state == "TARGET_PENDING"
    runtime.rig.reader.failure = None
    assert invoke(args) == 0, capsys.readouterr()
    assert json.loads(capsys.readouterr().out)["passed"] is True
    assert runtime.rig.reader.scans == 2
    assert runtime.rig.ddl.dispatches == 1


@pytest.mark.parametrize("value", ["true", None, 1])
def test_cli_rejects_invalid_selector_before_runtime_hydration(monkeypatch, tmp_path, capsys, value):
    runtime = install_replay_runtime(monkeypatch)
    path = replay_manifest(tmp_path)
    payload = yaml.safe_load(path.read_text())
    payload["defaults"]["sink"]["options"]["durable_quality_replay"] = value
    path.write_text(yaml.safe_dump(payload))
    assert invoke(run_args(path)) == 2
    failure = json.loads(capsys.readouterr().out)
    assert failure["passed"] is False
    assert "DPONE_REPLAY_QUALITY_EVIDENCE_UNSUPPORTED" in failure["result"]["errors"][0]
    assert runtime.configurations == []
    assert runtime.rig.ddl.dispatches == 0
