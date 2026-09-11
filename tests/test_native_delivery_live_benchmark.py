"""Hermetic producer contracts; these tests confer no live certification."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest
from tools.native_delivery_live_benchmark import APPROVAL_FLAGS, main
from tools.native_delivery_live_support.artifacts import ArtifactStore, canonical_json, read_artifact
from tools.native_delivery_live_support.correctness import failure_recovery
from tools.native_delivery_live_support.execution import (
    ExecutionAdapter,
    environment_record,
    measured,
    unavailable,
)
from tools.native_delivery_live_support.hermetic import LIMITS
from tools.native_delivery_live_support.hermetic import HermeticRouteFactory as FakeFactory
from tools.native_delivery_live_support.hermetic import HermeticRouteSession as FakeSession
from tools.native_delivery_live_support.hermetic import produce_fixture as produce
from tools.native_delivery_live_support.profiles import PROFILES, Dataset, exact_multiset
from tools.native_delivery_live_support.runner import configuration
from tools.native_delivery_live_support.validation import require_comparable, validate_metric, validate_run


@pytest.fixture
def fake_rss(monkeypatch):
    class NoProcessSampler:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def metric(self):
            return unavailable("bytes", "hermetic_no_process_sampling")

    monkeypatch.setattr("tools.native_delivery_live_support.runner.ProcessTreeRss", NoProcessSampler)


@pytest.mark.parametrize("profile", PROFILES)
def test_profiles_are_deterministic_and_bind_the_seed(profile):
    first = Dataset(profile, rows=16, seed=7)
    assert list(first.generate()) == list(Dataset(profile, rows=16, seed=7).generate())
    assert first.description() != Dataset(profile, rows=16, seed=8).description()
    assert sum(exact_multiset(first.generate()).values()) == 16


def test_exact_multiset_preserves_types_null_empty_binary_and_duplicates():
    values = [{"v": v} for v in [None, "", b"", Decimal("1.00"), "1.00", 1, True, "é", "é", None]]
    counts = exact_multiset(iter(values))
    assert len(counts) == 9
    assert counts != exact_multiset(iter(values[:-1]))


@pytest.mark.parametrize("rows", [-1, True, 1.2])
def test_dataset_rejects_invalid_rows(rows):
    with pytest.raises(ValueError):
        Dataset("narrow", rows=rows, seed=0)


def test_atomic_artifacts_refuse_overwrite_and_detect_tampering(tmp_path):
    store = ArtifactStore(tmp_path / "run.json")
    ref = store.write("receipt", {"status": "PASS"})
    assert read_artifact(tmp_path, ref) == {"status": "PASS"}
    store.publish({"receipt": ref})
    with pytest.raises(FileExistsError):
        ArtifactStore(tmp_path / "run.json").publish({})
    (tmp_path / ref["path"]).write_text("{}")
    with pytest.raises(ValueError):
        read_artifact(tmp_path, ref)


@pytest.mark.parametrize("path", ["../escape", "/tmp/escape"])
def test_artifact_paths_cannot_escape(tmp_path, path):
    with pytest.raises(ValueError):
        read_artifact(tmp_path, {"path": path, "sha256": "a" * 64, "status": "PASS"})


def test_serialization_failure_preserves_existing_output(tmp_path):
    output = tmp_path / "run.json"
    output.write_text("original")
    with pytest.raises(ValueError):
        ArtifactStore(output, overwrite=True).publish({"bad": float("nan")})
    assert output.read_text() == "original"
    assert json.loads(canonical_json({"unicode": "世界"}))["unicode"] == "世界"


def test_hermetic_envelope_matches_frozen_fields_without_live_authority(tmp_path, fake_rss):
    factory = FakeFactory()
    envelope = produce(tmp_path, factory)
    assert envelope["status"] == "UNVERIFIED"
    assert len(envelope["samples"]) == 4
    assert [s["is_warmup"] for s in envelope["samples"]] == [True, False, False, False]
    assert all(s["status"] == "PASS" for s in envelope["samples"])
    assert validate_run(envelope, tmp_path) == []
    assert envelope["fidelity_receipt"]["status"] == envelope["recovery_receipt"]["status"] == "PASS"
    unknown = next(s for s in factory.sessions if s.case == "unknown_commit")
    assert unknown.closed and not unknown.cleaned and unknown.publications == 1
    assert all(s.closed for s in factory.sessions)
    assert "p95" not in envelope["samples"][0]
    assert "secret" not in "".join(p.read_text() for p in tmp_path.rglob("*.json"))


@pytest.mark.parametrize("field", ["workload", "configuration", "environment", "route"])
def test_comparison_rejects_identity_and_layout_drift(field):
    original = {key: {"sha256": "a" * 64} for key in ("workload", "configuration", "environment", "route")}
    changed = {**original, field: {"sha256": "b" * 64}}
    with pytest.raises(ValueError):
        require_comparable(original, changed)


@pytest.mark.parametrize("change", ["version", "receipt_identity", "missing_check", "duplicate_sample"])
def test_parser_rejects_contract_and_binding_corruption(tmp_path, fake_rss, change):
    envelope = produce(tmp_path)
    if change == "version":
        envelope["schema_version"] = True
    elif change == "duplicate_sample":
        envelope["samples"].append(envelope["samples"][0])
    else:
        ref = envelope["samples"][0]["correctness"]
        value = read_artifact(tmp_path, ref)
        if change == "receipt_identity":
            value["subject_commit"] = "f" * 40
        else:
            value["checks"].pop()
        replacement = ArtifactStore(tmp_path / "unused.json").write("corrupt", value)
        envelope["samples"][0]["correctness"] = replacement
    with pytest.raises(ValueError):
        validate_run(envelope, tmp_path)


@pytest.mark.parametrize("metric", [measured(0, "bytes", "test"), unavailable("bytes", "no_sql_permission")])
def test_metrics_keep_zero_separate_from_absence(metric):
    validate_metric(metric)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), True, -1])
def test_metrics_reject_nonfinite_boolean_negative_values(value):
    with pytest.raises(ValueError):
        measured(value, "seconds", "test")


@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d.pop("parallelism"),
        lambda d: d.update(password="secret"),
        lambda d: d.update(max_row_bytes=2**30),
        lambda d: d.update(parallelism=True),
    ],
)
def test_configuration_uses_exact_canonical_limits(mutate):
    limits = dict(LIMITS)
    mutate(limits)
    with pytest.raises(ValueError):
        configuration(limits)


@pytest.mark.parametrize("approved_env", [False, True])
def test_absence_writes_skip_without_factory_import(tmp_path, monkeypatch, capsys, approved_env):
    for name in APPROVAL_FLAGS:
        monkeypatch.setenv(name, "1" if approved_env else "0")

    def forbidden(*args, **kwargs):
        raise AssertionError("factory import before approval")

    monkeypatch.setattr("tools.native_delivery_live_benchmark.load_factory", forbidden)
    limits = tmp_path / "limits.json"
    limits.write_text(json.dumps(LIMITS))
    args = ["run", "--limits", str(limits), "--output", str(tmp_path / "run.json")]
    if not approved_env:
        args += ["--factory", "must.never:load"]
    assert main(args) == 0
    envelope = json.loads((tmp_path / "run.json").read_text())
    assert envelope["status"] == "SKIP"
    assert validate_run(envelope, tmp_path) == []
    assert main(["inspect", str(tmp_path / "run.json")]) == 0
    assert "eligible_trials=0" in capsys.readouterr().out


def test_output_failure_happens_before_live_factory(tmp_path, monkeypatch):
    for name in APPROVAL_FLAGS:
        monkeypatch.setenv(name, "1")
    monkeypatch.setattr(
        "tools.native_delivery_live_benchmark.load_factory", lambda *a, **k: pytest.fail("unexpected factory")
    )
    limits, output = tmp_path / "limits.json", tmp_path / "run.json"
    limits.write_text(json.dumps(LIMITS))
    output.write_text("old")
    assert main(["run", "--limits", str(limits), "--output", str(output), "--factory", "real:factory"]) == 2
    assert output.read_text() == "old"


def test_symlinked_artifact_is_rejected(tmp_path):
    target = tmp_path / "target.json"
    target.write_text('{"status":"PASS"}')
    (tmp_path / "link.json").symlink_to(target)
    with pytest.raises(ValueError):
        read_artifact(tmp_path, {"path": "link.json", "sha256": "a" * 64, "status": "PASS"})


def test_unsafe_environment_is_rejected_and_not_echoed(tmp_path, monkeypatch, capsys):
    for name in APPROVAL_FLAGS:
        monkeypatch.setenv(name, "1")
    factory = FakeFactory()
    monkeypatch.setattr(factory, "describe", lambda: {"versions": {"password": "very-secret"}})
    with pytest.raises(ValueError):
        environment_record(factory)
    monkeypatch.setattr("tools.native_delivery_live_benchmark.load_factory", lambda *a, **k: factory)
    limits = tmp_path / "limits.json"
    limits.write_text(json.dumps(LIMITS))
    assert main(["run", "--limits", str(limits), "--output", str(tmp_path / "run.json"), "--factory", "x:y"]) == 2
    assert "very-secret" not in capsys.readouterr().err


def test_source_reopening_and_duplicate_publication_fail_recovery(monkeypatch):
    original = FakeSession.recover

    def broken(self, *, source_allowed):
        original(self, source_allowed=source_allowed)
        self.queries += 1
        self.publications += 1

    monkeypatch.setattr(FakeSession, "recover", broken)
    checks = failure_recovery(FakeFactory(), Dataset("binary", 16), "partition_replace")
    assert {c["id"] for c in checks if c["status"] == "FAIL"} == {"receipt_first_recovery", "source_free_resume"}


def test_failed_fidelity_prevents_timed_trials(tmp_path, fake_rss, monkeypatch):
    original = FakeSession._publish

    def corrupt(self):
        original(self)
        self.rows.pop()

    monkeypatch.setattr(FakeSession, "_publish", corrupt)
    envelope = produce(tmp_path)
    assert envelope["status"] == "FAIL" and envelope["samples"] == []


def test_clock_measures_visibility_before_pipeline():
    from tools.native_delivery_live_support.execution import DeliveryClock

    ticks = iter([1000000000, 3000000000, 5000000000])
    clock = DeliveryClock(now=lambda: next(ticks))
    clock.source_acquired()
    clock.committed_visible()
    visible, pipeline = clock.finish()
    assert visible["value"] == 2 and pipeline["value"] == 4


def test_wide_profile_has_200_columns():
    assert len(Dataset("wide", 1).schema()) == 200
    assert len(next(Dataset("wide", 1).generate())) == 200


@pytest.mark.parametrize(
    "action,known,expected",
    [
        ("cleanup", False, "UNVERIFIED"),
        ("cleanup", True, "PASS"),
        ("recover", False, "UNVERIFIED"),
        ("recover", True, "PASS"),
    ],
)
def test_maintenance_retains_unknown_resources(tmp_path, action, known, expected):
    from tools.native_delivery_live_support.execution import DeliveryClock
    from tools.native_delivery_live_support.maintenance import maintain

    session = FakeSession(Dataset("narrow", 0), "existing", DeliveryClock())
    session.known = known
    factory = FakeFactory()
    factory.attach = lambda owner: session
    result = maintain(
        factory, action=action, owner=session.invocation_id, store=ArtifactStore(tmp_path / "operation.json")
    )
    assert result["status"] == expected
    assert session.cleaned == (known and action == "cleanup")
    assert session.closed


def test_failing_atomic_replace_keeps_previous_envelope_and_receipts(tmp_path, monkeypatch):
    import os

    output = tmp_path / "run.json"
    store = ArtifactStore(output)
    ref = store.write("old", {"status": "PASS"})
    store.publish({"ref": ref})
    original = output.read_bytes()

    def fail(*args):
        raise OSError("failure")

    monkeypatch.setattr(os, "replace", fail)
    with pytest.raises(OSError):
        ArtifactStore(output, overwrite=True).publish({})
    assert output.read_bytes() == original
    assert read_artifact(tmp_path, ref) == {"status": "PASS"}
    assert not list(tmp_path.glob(".delivery-*"))


def test_wrong_loaded_baseline_is_rejected():
    with pytest.raises(ValueError, match="baseline_commit_mismatch"):
        ExecutionAdapter(FakeFactory(), "baseline").subject()


def test_help_and_import_do_not_discover_services(monkeypatch):
    import importlib
    import socket
    import subprocess

    def forbidden(*args, **kwargs):
        pytest.fail("unexpected service/process discovery")

    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    import tools.native_delivery_live_benchmark as module

    importlib.reload(module)
    with pytest.raises(SystemExit) as exc:
        module.main(["--help"])
    assert exc.value.code == 0


@pytest.mark.parametrize("missing", ["event", "probe"])
def test_lost_ack_must_really_fire_and_probe_receipt(monkeypatch, missing):
    original = FakeSession.run

    def omit(self):
        original(self)
        if self.fault == "lost_ack":
            if missing == "event":
                self.events = ()
            else:
                self.probes = 0

    monkeypatch.setattr(FakeSession, "run", omit)
    checks = failure_recovery(FakeFactory(), Dataset("narrow", 16), "partition_replace")
    assert next(c for c in checks if c["id"] == "receipt_first_recovery")["status"] == "FAIL"


def test_noop_recovery_of_known_rollback_cannot_pass(tmp_path):
    from tools.native_delivery_live_support.execution import DeliveryClock
    from tools.native_delivery_live_support.maintenance import maintain

    session = FakeSession(Dataset("narrow", 16), "existing", DeliveryClock())
    session.recover = lambda **kwargs: None
    factory = FakeFactory()
    factory.attach = lambda owner: session
    result = maintain(factory, action="recover", owner=session.invocation_id, store=ArtifactStore(tmp_path / "op.json"))
    assert result["status"] == "UNVERIFIED" and not session.cleaned


@pytest.mark.parametrize("case", ["limits", "versions", "rows"])
def test_inspector_rejects_description_tampering(tmp_path, fake_rss, case):
    envelope = produce(tmp_path)
    if case == "limits":
        envelope["configuration"]["limits"]["parallelism"] = 2
    elif case == "versions":
        envelope["environment"]["versions"]["mssql"] = "different"
    else:
        envelope["workload"]["rows"] += 1
    with pytest.raises(ValueError):
        validate_run(envelope, tmp_path)


def test_failed_trial_records_observed_target_count(tmp_path, fake_rss, monkeypatch):
    original = FakeSession._publish

    def corrupt_trial(self):
        original(self)
        if self.case.startswith("trial-"):
            self.rows.pop()

    monkeypatch.setattr(FakeSession, "_publish", corrupt_trial)
    envelope = produce(tmp_path)
    assert envelope["status"] == "FAIL"
    assert envelope["samples"][0]["metrics"]["rows"]["value"] == 15


def test_fixed_v1_contract_fixture_remains_valid_and_unverified():
    path = Path(__file__).resolve().parents[1] / "test_artifacts/delivery-acceleration/dda-05/contract-fixture.json"
    envelope = json.loads(path.read_text())
    assert envelope["status"] == "SKIP" and validate_run(envelope, path.parent) == []


def test_unknown_commit_replay_must_preserve_target(monkeypatch):
    original = FakeSession.recover

    def corrupt_unknown(self, *, source_allowed):
        if not self.known:
            self.rows, self.outside = [], []
        original(self, source_allowed=source_allowed)

    monkeypatch.setattr(FakeSession, "recover", corrupt_unknown)
    checks = failure_recovery(FakeFactory(), Dataset("narrow", 16), "partition_replace")
    assert next(c for c in checks if c["id"] == "receipt_first_recovery")["status"] == "FAIL"


def test_maintenance_rejects_repeat_publication(tmp_path):
    from tools.native_delivery_live_support.execution import DeliveryClock
    from tools.native_delivery_live_support.maintenance import maintain

    session = FakeSession(Dataset("narrow", 16), "existing", DeliveryClock())
    session.run()
    session.recover = lambda **kwargs: session._publish()
    factory = FakeFactory()
    factory.attach = lambda owner: session
    result = maintain(factory, action="recover", owner=session.invocation_id, store=ArtifactStore(tmp_path / "op.json"))
    assert result["status"] == "UNVERIFIED"


def test_producer_drift_fails_even_when_subject_is_unchanged(tmp_path, fake_rss, monkeypatch):
    state = {"commit": "a" * 40, "dirty": False}
    monkeypatch.setattr("tools.native_delivery_live_support.runner.git_identity", lambda path: dict(state))
    factory = FakeFactory()
    original = factory.open

    def changing(*args, **kwargs):
        state["dirty"] = True
        return original(*args, **kwargs)

    monkeypatch.setattr(factory, "open", changing)
    envelope = produce(tmp_path, factory)
    assert envelope["status"] == "FAIL"
    assert any("identity changed" in note for note in envelope["limitations"])


def test_live_fixture_exceptions_are_redacted_without_chaining():
    from tests.integration.mssql.clickhouse_mssql_delivery_support import redacted_live

    @redacted_live
    def unsafe():
        raise RuntimeError("secret://user:password@host")

    with pytest.raises(pytest.fail.Exception) as error:
        unsafe()
    assert "secret" not in str(error.value)
    assert error.value.__context__ is None


@pytest.mark.parametrize("expected,observed", [(True, 1), (False, 0), ({"rows": True}, {"rows": 1})])
def test_json_assertions_do_not_coerce_booleans(expected, observed):
    from tools.native_delivery_live_support.correctness import check

    assert check("typed_content", expected, observed)["status"] == "FAIL"


def test_live_factory_setup_failure_has_no_driver_exception_chain(tmp_path, monkeypatch):
    from tests.integration.mssql.clickhouse_mssql_delivery_support import live_factory

    for name in APPROVAL_FLAGS:
        monkeypatch.setenv(name, "1")
    limits = tmp_path / "limits.json"
    limits.write_text(json.dumps(LIMITS))
    monkeypatch.setenv("DPONE_DDA_LIMITS_FILE", str(limits))
    monkeypatch.setenv("DPONE_DDA_ROUTE_FACTORY", "fake:factory")

    def unsafe(*args, **kwargs):
        raise RuntimeError("secret driver failure")

    monkeypatch.setattr("tests.integration.mssql.clickhouse_mssql_delivery_support.load_factory", unsafe)
    with pytest.raises(pytest.fail.Exception) as error:
        live_factory("full_refresh", "bounded_native")
    assert error.value.__context__ is None and "secret" not in str(error.value)


def test_capture_freezes_reused_row_dictionaries():
    from tools.native_delivery_live_support.profiles import capture_rows

    row = {"value": 0}

    def reused():
        for value in range(3):
            row["value"] = value
            yield row

    captured = capture_rows(reused())
    row["value"] = 99
    assert captured == ({"value": 0}, {"value": 1}, {"value": 2})


@pytest.mark.parametrize("phase", ["publish", "unknown_recovery"])
def test_outside_window_snapshots_cannot_be_rewritten_in_place(tmp_path, fake_rss, monkeypatch, phase):
    original_publish, original_recover = FakeSession._publish, FakeSession.recover

    def corrupt_publish(self):
        original_publish(self)
        if phase == "publish":
            self.outside[0]["outside"] = "changed"

    def corrupt_recovery(self, *, source_allowed):
        if phase == "unknown_recovery" and not self.known:
            self.outside[0]["outside"] = "changed"
        original_recover(self, source_allowed=source_allowed)

    monkeypatch.setattr(FakeSession, "_publish", corrupt_publish)
    monkeypatch.setattr(FakeSession, "recover", corrupt_recovery)
    report = produce(tmp_path)
    assert report["status"] == "FAIL"
    assert report["recovery_receipt"]["status"] == "FAIL"


def test_full_refresh_unknown_commit_may_replace_all_old_rows(monkeypatch):
    original = FakeSession._publish

    def publish(self):
        original(self)
        self.outside.clear()

    monkeypatch.setattr(FakeSession, "_publish", publish)
    assert all(c["status"] == "PASS" for c in failure_recovery(FakeFactory(), Dataset("narrow", 16), "full_refresh"))
