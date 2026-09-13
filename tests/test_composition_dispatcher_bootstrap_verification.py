"""Preparation ordering with explicit boundary doubles, not deployment proof."""

from types import SimpleNamespace

import pytest

from dpone.app import composition_dispatcher_bootstrap_verification as module
from dpone.contracts.composition_identity import CompositionAdmissionError
from tests.test_composition_dispatcher_service_policy import bootstrap_document, decode_bootstrap


@pytest.fixture
def boundary(monkeypatch):
    config = decode_bootstrap(bootstrap_document())
    events, now = [], [10.0]
    facts = {
        "linux": {
            "containers": {
                name: {"mounts": {"mountinfo_sha256": "sha256:" + digit * 64}}
                for name, digit in (("dispatcher", "a"), ("clickhouse", "b"))
            }
        }
    }
    enrolled = SimpleNamespace(
        enrollment_sha256=config.supervisor_enrollment_sha256,
        document=b"enrollment-original",
        body={"facts": facts},
        role_id=lambda name: name,
    )
    loader = object()
    policy_path = config.service_policy.tls.certificate_file.parent / "policy.json"

    def read(actual_config, actual_loader, deadline):
        assert actual_config is config and actual_loader is loader and deadline == 130.0
        events.append("sql-closed")
        return enrolled

    class Client:
        def __init__(self, path, **kwargs):
            assert path == config.host_probe_socket
            assert kwargs["dispatcher_gid"] == config.dispatcher_gid

        def capture(self, digest, *, deadline):
            assert digest == enrolled.enrollment_sha256 and deadline == 130.0
            events.append("host")
            return facts

        def capture_mounts(self, digest, *, expected_mountinfo_sha256, deadline):
            assert digest == enrolled.enrollment_sha256 and deadline == 130.0
            assert expected_mountinfo_sha256 == {
                name: "sha256:" + digit * 64 for name, digit in (("dispatcher", "a"), ("clickhouse", "b"))
            }
            events.append("mounts")
            return {"actual": "tables"}

    def process(enrollment, policy, path, deadline):
        assert enrollment is enrolled and policy == config.service_policy and path == policy_path and deadline == 130.0
        events.append("process")

    def isolation(enrollment, policy, tables, deadline):
        assert (
            enrollment is enrolled
            and policy == config.service_policy
            and tables == {"actual": "tables"}
            and deadline == 130.0
        )
        events.append("isolation")

    monkeypatch.setattr(module.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(module, "read_bootstrap_enrollment", read)
    monkeypatch.setattr(module, "CaptureSupervisorFactsClient", Client)
    monkeypatch.setattr(module, "require_dispatcher_process", process)
    monkeypatch.setattr(module, "require_dispatcher_artifact_isolation", isolation)
    return SimpleNamespace(
        config=config, loader=loader, path=policy_path, events=events, enrolled=enrolled, now=now, client=Client
    )


def run(case, deadline=130.0):
    return module.require_dispatcher_bootstrap(case.config, case.loader, case.path, deadline)


def test_repeats_sql_host_process_and_isolation_before_returning(boundary):
    assert run(boundary) is None
    assert boundary.events == ["sql-closed", "host", "mounts", "process", "isolation", "host"] * 2


@pytest.mark.parametrize(
    "stage", ["read_bootstrap_enrollment", "require_dispatcher_process", "require_dispatcher_artifact_isolation"]
)
def test_any_failed_authority_prevents_preparation_success(boundary, monkeypatch, stage):
    def fail(*args, **kwargs):
        raise RuntimeError("private-driver-information")

    monkeypatch.setattr(module, stage, fail)
    with pytest.raises(CompositionAdmissionError, match="dispatcher_bootstrap_unverified") as caught:
        run(boundary)
    assert "private" not in str(caught.value)


def test_changed_host_facts_fail_before_process_or_isolation(boundary, monkeypatch):
    monkeypatch.setattr(boundary.client, "capture", lambda *a, **k: {"changed": True})
    with pytest.raises(CompositionAdmissionError):
        run(boundary)
    assert boundary.events == ["sql-closed"]


def test_late_final_host_observation_cannot_activate(boundary, monkeypatch):
    original = boundary.client.capture

    def late(*args, **kwargs):
        facts = original(*args, **kwargs)
        if boundary.events.count("host") == 4:
            boundary.now[0] = 130.0
        return facts

    monkeypatch.setattr(boundary.client, "capture", late)
    with pytest.raises(CompositionAdmissionError):
        run(boundary)


@pytest.mark.parametrize("deadline", [10.0, float("nan"), float("inf"), True])
def test_invalid_or_expired_deadline_fails_before_any_observation(boundary, deadline):
    with pytest.raises(CompositionAdmissionError):
        run(boundary, deadline)
    assert boundary.events == []
