"""Real private files/context/service/journal; SQL boundary is scripted here."""

import json
from contextlib import contextmanager
from dataclasses import replace
from importlib import import_module

import pytest

from dpone.adapters.publication_retirement_attempts import PrivateRetirementAttempts
from dpone.app.publication_operator_context import resolve_authority, verified_context
from dpone.contracts.publication_authority_binding import publication_binding_digest
from dpone.contracts.publication_retirement_operator_plan import decode_retirement_operator_plan
from tests.test_publication_retirement_service import Observer, Store
from tests.test_publication_schema_application import operator_context, projected_credentials  # noqa: F401


def setup(tmp_path, monkeypatch):
    module = import_module("dpone.app.publication_retirement_application")
    environ, _ = operator_context(tmp_path)
    _, binding, pin = resolve_authority(verified_context(environ, "prod"), "source-main")
    observer = Observer()
    digest = publication_binding_digest(binding, endpoint_identity=pin)
    observer.value = replace(
        observer.value, binding_digest=digest, freeze=replace(observer.value.freeze, binding_digest=digest)
    )

    class CapturingStore(Store):
        def retire_if_absent(self, plan):
            self.retired_plans.append(plan)
            return super().retire_if_absent(plan)

    store = CapturingStore(observer)
    store.retired_plans = []
    builds = []

    def build(**kwargs):
        builds.append(kwargs)
        return store

    monkeypatch.setattr(module, "build_publication_retirement", build)
    directory = tmp_path / "journal"
    directory.mkdir(mode=0o700)
    journal = PrivateRetirementAttempts(directory)
    app = module.PublicationRetirementApplication(
        environ=environ, observer=observer, journal=journal, clock=lambda: 220
    )
    return app, observer, journal, store, builds


def planned(tmp_path, monkeypatch):
    app, observer, journal, store, builds = setup(tmp_path, monkeypatch)
    path = tmp_path / "retirement.json"
    result = app.plan(connection_ref="source-main", environment="prod", path=path)
    assert result["status"] == "ready"
    return app, observer, journal, store, builds, path, result["plan_digest"]


def test_plan_apply_verify_preserve_original_plan_and_claim_once(tmp_path, monkeypatch):
    app, observer, journal, store, builds, path, digest = planned(tmp_path, monkeypatch)
    plan = decode_retirement_operator_plan(path.read_bytes())
    assert plan.journal_identity == journal.require_ready()
    assert store.calls == [] and len(builds) == 1
    assert not tuple((tmp_path / "journal").iterdir())
    applied = app.apply(path=path, environment="prod", confirmation_digest=digest)
    assert applied["status"] == "retired_unpublished"
    assert store.retired_plans == [plan.retirement]
    verified = app.verify(path=path, environment="prod", confirmation_digest=digest)
    assert verified["status"] == "retired_unpublished"
    assert store.retired_plans == [plan.retirement] and len(tuple((tmp_path / "journal").iterdir())) == 1
    assert "synthetic-private-password" not in path.read_text() + json.dumps(applied)
    assert not observer.held


@pytest.mark.parametrize("action", ["apply", "verify"])
@pytest.mark.parametrize("digest", ["0" * 64, "é" * 64, None])
def test_bad_confirmation_precedes_credentials_and_sql(tmp_path, monkeypatch, action, digest):
    app, _, _, _, builds, path, _ = planned(tmp_path, monkeypatch)
    monkeypatch.delenv("AIRFLOW_CONN_SYNTHETIC_METADATA")
    result = getattr(app, action)(path=path, environment="prod", confirmation_digest=digest)
    assert result["reason_code"] == "plan_confirmation_required"
    assert result["status"] == "blocked" and len(builds) == 1


def test_changed_journal_stops_before_credentials_or_sql(tmp_path, monkeypatch):
    app, _, _, store, builds, path, digest = planned(tmp_path, monkeypatch)
    (tmp_path / "journal").rename(tmp_path / "previous")
    (tmp_path / "journal").mkdir(mode=0o700)
    monkeypatch.delenv("AIRFLOW_CONN_SYNTHETIC_METADATA")
    result = app.apply(path=path, environment="prod", confirmation_digest=digest)
    assert result["status"] == "blocked" and result["reason_code"] == "retirement_journal_unavailable"
    assert len(builds) == 1 and store.calls == []


@pytest.mark.parametrize("dependency", ["observer", "journal"])
def test_missing_trusted_dependency_stops_before_context(tmp_path, dependency):
    cls = import_module("dpone.app.publication_retirement_application").PublicationRetirementApplication
    kwargs = {"observer": Observer(), "journal": PrivateRetirementAttempts(tmp_path)}
    kwargs[dependency] = None
    result = cls(environ={}, **kwargs).plan(connection_ref="source-main", environment="prod", path=tmp_path / "plan")
    assert result["status"] == "blocked"
    assert not (tmp_path / "plan").exists()


def test_unknown_write_keeps_marker_and_disallows_reapply(tmp_path, monkeypatch):
    app, _, _, store, _, path, digest = planned(tmp_path, monkeypatch)
    store.after = "unknown"
    first = app.apply(path=path, environment="prod", confirmation_digest=digest)
    assert first["status"] == "outcome_unknown" and len(store.retired_plans) == 1
    store.before = store.after = "absent"
    second = app.apply(path=path, environment="prod", confirmation_digest=digest)
    assert second["status"] == "outcome_unknown" and len(store.retired_plans) == 1


def test_observation_binding_drift_does_not_write_plan(tmp_path, monkeypatch):
    (
        app,
        observer,
        _,
        store,
        _,
    ) = setup(tmp_path, monkeypatch)
    observer.value = replace(observer.value, binding_digest="d" * 64)
    result = app.plan(connection_ref="source-main", environment="prod", path=tmp_path / "plan")
    assert result["status"] != "ready" and store.calls == []
    assert not (tmp_path / "plan").exists()


def test_changed_verified_context_blocks_saved_plan_before_sql(tmp_path, monkeypatch):
    from tests.test_runtime_connection_context_loader import _replace_plan_descriptor

    app, _, _, store, builds, path, digest = planned(tmp_path, monkeypatch)
    registry_path = next(tmp_path.glob("payload/runtime-connection-contexts/*/connection-registry.json"))
    registry = json.loads(registry_path.read_bytes())
    registry["connections"]["source-registry"]["connection"]["host"] = "other-synthetic-sql"
    content = json.dumps(registry, sort_keys=True, separators=(",", ":")).encode()
    registry_path.write_bytes(content)
    app._environ = _replace_plan_descriptor(app._environ, name="connection_registry", content=content)
    monkeypatch.delenv("AIRFLOW_CONN_SYNTHETIC_METADATA")
    result = app.apply(path=path, environment="prod", confirmation_digest=digest)
    assert result["status"] == "blocked" and result["reason_code"] == "plan_context_differs"
    assert len(builds) == 1 and store.calls == []


def test_expired_saved_plan_verification_retains_original_provenance(tmp_path, monkeypatch):
    app, observer, _, store, _, path, digest = planned(tmp_path, monkeypatch)
    original = decode_retirement_operator_plan(path.read_bytes())
    observer.value = replace(
        observer.value,
        freeze=replace(
            observer.value.freeze, receipt_digest="e" * 64, established_at=300, drained_at=310, expires_at=400
        ),
        replicas=tuple(
            replace(item, history_through=310, history_receipt_digest="f" * 64) for item in observer.value.replicas
        ),
    )
    app._clock = lambda: 320
    store.before = "exact"
    result = app.verify(path=path, environment="prod", confirmation_digest=digest)
    assert result["status"] == "retired_unpublished"
    assert store.calls == [("inspect", original.retirement.digest)] and not store.retired_plans
    assert decode_retirement_operator_plan(path.read_bytes()) == original
    assert not tuple((tmp_path / "journal").iterdir())


def test_claim_failure_cannot_reach_sql_mutation(tmp_path, monkeypatch):
    app, _, journal, store, _, path, digest = planned(tmp_path, monkeypatch)

    def unavailable(_):
        raise OSError("synthetic-private journal failure")

    monkeypatch.setattr(journal, "claim", unavailable)
    result = app.apply(path=path, environment="prod", confirmation_digest=digest)
    assert result["status"] == "outcome_unknown"
    assert not store.retired_plans
    assert "synthetic-private" not in json.dumps(result)


@pytest.mark.parametrize("action", ["apply", "verify"])
def test_observer_close_failure_cannot_report_success(tmp_path, monkeypatch, action):
    app, observer, _, store, _, path, digest = planned(tmp_path, monkeypatch)
    original_hold = observer.hold

    @contextmanager
    def broken_close():
        with original_hold() as held:
            yield held
        raise OSError("synthetic-private observer close")

    monkeypatch.setattr(observer, "hold", broken_close)
    if action == "verify":
        store.before = "exact"
    result = getattr(app, action)(path=path, environment="prod", confirmation_digest=digest)
    assert result["status"] == "outcome_unknown"
    assert len(store.retired_plans) == (1 if action == "apply" else 0)
    assert path.is_file() and "synthetic-private" not in json.dumps(result)
