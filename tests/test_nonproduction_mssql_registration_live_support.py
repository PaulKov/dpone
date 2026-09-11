"""Offline fixture safety checks; no SQL, signatures or permission proof."""

import json
import os
import subprocess
import sys
from contextlib import nullcontext
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock
from xml.etree import ElementTree

import pytest

from dpone.contracts.nonproduction_registration import require_execution_memberships
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError
from tests.integration.composition import nonproduction_mssql_registration_live_support as support
from tests.integration.composition import test_nonproduction_mssql_registration_live as live
from tests.integration.composition import test_nonproduction_mssql_registration_recovery_live as recovery
from tests.integration.composition.mssql_gate_live_provisioning import SqlFailure
from tests.integration.composition.test_nonproduction_mssql_registration_live import expanded
from tests.nonproduction_authority_helpers import NOW, execution, limits
from tests.nonproduction_signature_helpers import github_policy, trust


def environment():
    token = "a" * 24
    return {
        "DPONE_RUN_COMPOSITION_MSSQL_REGISTRATION_LIVE": "1",
        "DPONE_RUN_COMPOSITION_MSSQL_LIVE": "1",
        "DPONE_COMPOSITION_RUN_TOKEN": token,
        "DPONE_COMPOSITION_SQL_PORT": "49152",
        "DPONE_COMPOSITION_SQL_DATABASE": "dpone_composition_" + token,
        "MSSQL_SA_PASSWORD": "Dp1!" + "x" * 32,
    }


def test_optout_and_wrong_platform_cannot_open_sql():
    for env in (dict[str, str](), environment()):
        with pytest.raises(pytest.skip.Exception):
            support.require_owned_database(env, system="Darwin", machine="arm64")


@pytest.mark.parametrize("field", tuple(environment()))
def test_wrong_runner_identity_is_not_accepted(field):
    env = environment() | {field: "private-canary"}
    error = pytest.skip.Exception if field.endswith("REGISTRATION_LIVE") else RuntimeError
    with pytest.raises(error) as caught:
        support.require_owned_database(env, system="Linux", machine="x86_64")
    assert "private-canary" not in str(caught.value)


def test_exact_owned_scope_without_driver_import():
    value = support.require_owned_database(environment(), system="Linux", machine="x86_64")
    assert value.port == 49152 and "Dp1!" not in repr(value)


@pytest.mark.parametrize("count", [3, 4, 64])
def test_complete_fixture_request_binds_the_unmodified_full_grant(count):
    policy = github_policy()
    grant = expanded(execution(policy), count)
    originals = live.originals_for(grant, policy)
    assert originals.grant == grant and len(originals.grant.workloads) == count


@pytest.mark.parametrize("lower", ["policy", "grant", "workload"])
def test_live_ceiling_candidates_are_valid_originals_but_exceed_retained_union(lower):
    policy = github_policy(limits=limits(max_workloads=3 if lower == "policy" else 4))
    grant = execution(policy)
    workloads = (
        replace(
            grant.workloads[0], workload_id="aa_native", limits=limits(max_workloads=3 if lower == "workload" else 64)
        ),
        *grant.workloads[1:],
    )
    grant = replace(grant, workloads=workloads, limits=limits(max_workloads=3 if lower == "grant" else 64))
    live.originals_for(grant, policy)
    with pytest.raises(NonproductionAuthorityError, match="membership_budget"):
        require_execution_memberships(grant, policy, tuple(row.workload_id for row in execution(policy).workloads))


def test_initial_live_pool_candidate_reaches_pool_check_after_current_policy_validation():
    """Execute only the live case's initial pure-input phase; no database double."""

    class StopAfterClaims(Exception):
        pass

    class ClaimPhase:
        policy = github_policy()

        def append_policy(self, value):
            self.policy = value

        def grant(self):
            return execution(self.policy)

        def register(self, grant):
            self.members = tuple(row.workload_id for row in grant.workloads)

        def snapshot(self):
            return ()

        def reject(self, grant, reason):
            originals = live.originals_for(grant, self.policy)
            originals.require_policy(
                self.policy.to_bytes(), self.policy.policy_sha256, self.policy.revocation_epoch, NOW
            )
            assert reason == "membership_budget"
            with pytest.raises(NonproductionAuthorityError, match=reason):
                require_execution_memberships(grant, self.policy, self.members)
            raise StopAfterClaims

    with pytest.raises(StopAfterClaims):
        live.test_complete_campaign_membership_obeys_current_ceilings(ClaimPhase())


def test_history_cycle_refuses_downgrade_then_reopens_exact_newer_epoch_policy():
    original = github_policy()
    newer = replace(
        original, revocation_epoch=original.revocation_epoch + 1, revoked_grant_ids=(execution(original).grant_id,)
    )
    case = SimpleNamespace(policy=newer, expected=SimpleNamespace(revision=2, snapshot=trust(newer)))
    changes, stale = [], []

    def append(value):
        changes.append(value)
        if value.revocation_epoch < case.policy.revocation_epoch:
            raise SqlFailure(51000)
        case.policy = value
        case.expected = SimpleNamespace(revision=case.expected.revision + 1, snapshot=trust(value))

    case.append_policy = append
    case.trust = SimpleNamespace(provider=lambda: SimpleNamespace(read_revision=lambda: case.expected))
    case.reject_read = lambda grant, reason, expected: stale.append((reason, expected))
    live.require_history_cycle(case, original, execution(original))
    assert changes[0] == original and changes[-1] == newer and len(changes) == 3
    assert changes[1].revocation_epoch == newer.revocation_epoch and changes[1] != newer
    assert case.expected.revision == 4 and case.expected.snapshot == trust(newer)
    assert stale[0][0] == "trust_revision_changed" and stale[0][1].revision == 2


@pytest.mark.parametrize("permission,matches", [(0, True), (1, True), (None, True), (0, False)])
def test_principal_probe_preserves_raw_permissions_before_exact_assertion(monkeypatch, permission, matches):
    evidence = []
    rows = (("private-login", "private-login", 0, 0, permission, permission),)
    probe = Mock(return_value=rows)
    monkeypatch.setattr(recovery, "execute", probe)
    case = SimpleNamespace(
        record=lambda name, payload: evidence.append((name, json.loads(support.observation_document(payload))))
    )
    login = "private-login" if matches else "different-private-login"
    with nullcontext() if permission == 0 and matches else pytest.raises(AssertionError):
        recovery.require_restricted_identity(case, object(), login)
    assert "HAS_PERMS_BY_NAME(NULL,NULL," in probe.call_args.args[1]
    assert evidence[0][1]["control_server"] is permission and "private-login" not in repr(evidence)
    assert evidence[0][1]["login_matches"] is matches and evidence[0][1]["user_matches"] is matches


@pytest.mark.parametrize("field", ["PATH", "BYTES", "SHA256"])
def test_bulk_fixture_rejects_any_foreign_pin(field):
    values = {
        "DPONE_COMPOSITION_BULK_FIXTURE_PATH": support.BULK_PATH,
        "DPONE_COMPOSITION_BULK_FIXTURE_BYTES": "2",
        "DPONE_COMPOSITION_BULK_FIXTURE_SHA256": "sha256:" + sha256(b"1\n").hexdigest(),
    }
    assert support.require_bulk_fixture(values) == support.BULK_PATH
    values["DPONE_COMPOSITION_BULK_FIXTURE_" + field] = "private-canary"
    with pytest.raises(RuntimeError, match="owned_bulk_fixture_required"):
        support.require_bulk_fixture(values)


@pytest.mark.parametrize("code", [229, 102, 4861, 4860, 4902, None])
@pytest.mark.parametrize("index,label", enumerate(support.DENIAL_OPERATIONS, 1))
def test_denial_diagnostics_precede_exact_assertion_for_every_operation(code, index, label):
    plan = live.storage_denials(SimpleNamespace(table=str, trigger=str))
    assert tuple(label for label, _ in plan) == support.DENIAL_OPERATIONS
    case = object.__new__(support.RegistrationCase)
    case.record_property = Mock()
    operation = Mock(side_effect=None if code is None else SqlFailure(code))
    with nullcontext() if code == 229 else pytest.raises(AssertionError):
        live.observe_denial(case, index, label, operation, {229, 4834})
    name, raw = case.record_property.call_args.args
    assert name == f"dpone.registration.denial_{index}"
    expected = {"unexpected_success": True} if code is None else {"sql_error": code}
    assert json.loads(raw) == {"operation_index": index, "operation": label} | expected
    operation.assert_called_once_with()


@pytest.mark.parametrize("mode,completed", [("before_commit", 0), ("after_commit", 1)])
def test_commit_faults_count_only_completed_underlying_commit(mode, completed):
    events = []
    raw = SimpleNamespace(commit=lambda: events.append("commit"))
    boundary = recovery.BoundaryConnection(SimpleNamespace(connect=lambda: raw), mode)
    with pytest.raises(ConnectionError):
        boundary.commit()
    assert events == ["commit"] * completed and boundary.commits == completed and boundary.commit_calls == 1


def test_underlying_commit_failure_never_becomes_lost_ack():
    def fail():
        raise RuntimeError("underlying_failure")

    boundary = recovery.BoundaryConnection(
        SimpleNamespace(connect=lambda: SimpleNamespace(commit=fail)), "after_commit"
    )
    with pytest.raises(RuntimeError, match="underlying_failure"):
        boundary.commit()
    assert boundary.commits == 0


def test_partial_write_fault_occurs_after_exact_real_statement_and_readback(monkeypatch):
    calls = []
    raw = SimpleNamespace(execute=lambda *args: calls.append(args), nextset=lambda: False)
    boundary = SimpleNamespace(
        mode="first_member", raw=object(), partial=(), case=SimpleNamespace(table=lambda kind: kind)
    )
    monkeypatch.setattr(recovery, "execute", lambda *args: ((1, 1),))
    cursor = recovery.BoundaryCursor(raw, boundary)
    with pytest.raises(ConnectionError, match="injected_after_real_membership"):
        cursor.execute("INSERT INTO memberships VALUES (?);", b"private-original")
    assert calls == [("INSERT INTO memberships VALUES (?);", b"private-original")] and boundary.partial == ((1, 1),)


def test_failed_setup_still_runs_owned_cleanup(monkeypatch):
    events = []

    def fail():
        raise RuntimeError("installation_failed")

    case = SimpleNamespace(install=fail, cleanup=lambda: events.append("cleanup"))
    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    monkeypatch.setattr(support, "require_owned_database", lambda *args, **kwargs: object())
    monkeypatch.setattr(support, "RegistrationCase", lambda *args: case)
    fixture = cast(Any, support.registration_case).__wrapped__(lambda *args: None)
    with pytest.raises(RuntimeError, match="installation_failed"):
        next(fixture)
    assert events == ["cleanup"]


def test_owned_bulk_probe_is_cleaned_up_after_actual_setup_refusal(monkeypatch):
    statements = []
    monkeypatch.setattr(support, "require_bulk_fixture", lambda env: support.BULK_PATH)

    def sql(statement, *args):
        statements.append(statement)
        if "OPENROWSET" in statement:
            raise SqlFailure(4861)

    case = SimpleNamespace(schema="fixture", sql=sql)
    with pytest.raises(SqlFailure):
        recovery.test_restricted_login_cannot_bypass_registration_storage(case)
    assert statements[-1] == "DROP TABLE [fixture].[registration_bulk_probe];"


@pytest.mark.parametrize(
    "payload",
    [
        {"password": "private-canary"},
        {"rows": True},
        {"rows": -1},
        {"rows": 2**63},
        {"rows": "private-canary"},
        {"digest": "private-canary"},
        {"sql_error": 0},
        {"xact_state": 2},
        {"lock_mode": "private-canary"},
        {"bulk_path": "PASS"},
        {"operation_index": 7},
        {"operation": "private-canary"},
        {"operation": []},
        {"unexpected_success": 1},
    ],
)
def test_evidence_rejects_raw_or_invented_authority(payload):
    with pytest.raises(ValueError):
        support.observation_document(payload)


def test_evidence_preserves_bounded_actual_observations():
    values = {"rows": 2, "members": 3, "digest": "sha256:" + "a" * 64}
    assert json.loads(support.observation_document(values)) == values


def report():
    return SimpleNamespace(
        failed=True,
        skipped=False,
        longrepr="private-canary",
        sections=["private-canary"],
        user_properties=[("dpone.registration.state", '{"rows":1}'), ("unsafe", "private-canary")],
    )


@pytest.mark.parametrize("error", [SqlFailure(229), RuntimeError("private-canary")])
def test_sanitizer_preserves_failure_and_removes_originals(error):
    value = report()
    item = SimpleNamespace(nodeid=support.LIVE_MODULES[0] + "::test_case", stash=pytest.Stash())
    support.sanitize_report(item, SimpleNamespace(excinfo=SimpleNamespace(value=error)), value)
    assert value.failed and not value.skipped and "private-canary" not in repr(value)
    assert value.sections == []
    teardown = report()
    teardown.failed = False
    support.sanitize_report(item, SimpleNamespace(excinfo=None), teardown)
    assert any(name == "dpone.registration.failure" for name, _ in teardown.user_properties)


def test_unrelated_reports_and_duplicate_or_malformed_properties():
    value = report()
    before = repr(value)
    support.sanitize_report(SimpleNamespace(nodeid="tests/other.py::case"), SimpleNamespace(excinfo=None), value)
    assert repr(value) == before
    value.failed = False
    value.user_properties = [([], "private-canary"), ("dpone.registration.state", '{"rows":"private-canary"}')] + [
        ("dpone.registration.state", '{"rows":1}')
    ] * 100
    denials = [
        (
            f"dpone.registration.denial_{index}",
            support.observation_document({"operation_index": index, "operation": label, "sql_error": 229}),
        )
        for index, label in enumerate(support.DENIAL_OPERATIONS, 1)
    ]
    value.user_properties += denials
    support.sanitize_report(
        SimpleNamespace(nodeid=support.LIVE_MODULES[1] + "::case"), SimpleNamespace(excinfo=None), value
    )
    assert value.user_properties == [("dpone.registration.state", '{"rows":1}'), *denials]


def test_fault_restoration_runs_and_cleanup_errors_remain_failures(monkeypatch):
    case = object.__new__(support.RegistrationCase)
    events = []
    monkeypatch.setattr(case, "require_schema", lambda: events.append("audit"))
    with pytest.raises(AssertionError):
        with case.fault(lambda: events.append("change"), lambda: events.append("restore")):
            raise AssertionError("original failure")
    assert events == ["change", "restore", "audit"]
    with pytest.raises(RuntimeError, match="restore_failed"):
        with case.fault(lambda: None, lambda: (_ for _ in ()).throw(RuntimeError("restore_failed"))):
            pass


def child(tmp_path, script=None):
    root = Path(__file__).resolve().parents[1]
    junit = tmp_path / "junit.xml"
    result = subprocess.run(
        [
            sys.executable,
            *(["-c", script] if script else ["-m", "pytest"]),
            *support.LIVE_MODULES,
            "-p",
            "tests.integration.composition.nonproduction_mssql_registration_live_support",
            "-p",
            "no:cacheprovider",
            "-o",
            "addopts=",
            "-o",
            "junit_family=xunit1",
            "--tb=no",
            "--junitxml",
            str(junit),
        ],
        cwd=root,
        env=os.environ
        | {
            "DPONE_RUN_COMPOSITION_MSSQL_REGISTRATION_LIVE": "0",
            "DPONE_RUN_COMPOSITION_MSSQL_LIVE": "0",
            "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
            "PYTEST_ADDOPTS": "",
            "PYTEST_PLUGINS": "",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": os.pathsep.join((str(root / "src"), str(root))),
        },
        capture_output=True,
        timeout=60,
        check=False,
    )
    return result, junit


def test_exact_eighteen_cases_opt_out_without_opening_sql(tmp_path):
    result, junit = child(tmp_path)
    assert result.returncode == 0
    cases = ElementTree.parse(junit).getroot().findall(".//testcase")
    assert len(cases) == 18
    assert {case.attrib["classname"] + "::" + case.attrib["name"] for case in cases} == {
        module[:-3].replace("/", ".") + "::test_" + name
        for module, names in zip(support.LIVE_MODULES, live.CASE_NAMES, strict=True)
        for name in names
    }
    assert all(case.find("skipped") is not None for case in cases)
    assert all(case.find("failure") is None and case.find("error") is None for case in cases)


def test_real_pytest_setup_failure_stays_sanitized_through_teardown(tmp_path):
    script = """
import sys, pytest
class Fault:
    @pytest.hookimpl(tryfirst=True)
    def pytest_runtest_setup(self, item):
        raise RuntimeError('private-canary')
raise SystemExit(pytest.main(sys.argv[1:], plugins=[Fault()]))
"""
    result, junit = child(tmp_path, script)
    assert result.returncode == 1 and "private-canary" not in junit.read_text()
    cases = ElementTree.parse(junit).getroot().findall(".//testcase")
    assert len(cases) == 18 and all(case.find("error") is not None for case in cases)
    assert all(
        any(p.attrib["name"] == "dpone.registration.failure" for p in case.findall("properties/property"))
        for case in cases
    )
