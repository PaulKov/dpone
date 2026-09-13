"""Offline runner safety and evidence contracts; mocks are never live passes."""

import json
import os
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path
from types import SimpleNamespace

import pytest
from tools import composition_mssql_synthetic as runner

from tests.integration.composition.mssql_store_live_support import OwnedDatabase


def junit(path, names=None, *, outcome=None, declared=None):
    names = runner.expected_cases("store") if names is None else names
    suite = ET.Element(
        "testsuite",
        tests=str(len(names) if declared is None else declared),
        failures=str(len(names) if outcome == "failure" else 0),
        errors=str(len(names) if outcome == "error" else 0),
        skipped=str(len(names) if outcome == "skipped" else 0),
    )
    for name in names:
        module, _, case_name = name.rpartition("::")
        case = ET.SubElement(suite, "testcase", classname=module or runner.TEST_CLASS, name=case_name)
        if outcome:
            ET.SubElement(case, outcome)
    ET.ElementTree(suite).write(path)


@pytest.mark.parametrize("outcome", [None, "skipped", "error", "failure"])
def test_junit_requires_every_expected_case_and_zero_nonpasses(tmp_path, outcome):
    path = tmp_path / "junit.xml"
    junit(path, outcome=outcome)
    if outcome:
        with pytest.raises(runner.RunFailure, match="junit"):
            runner.validate_results(path)
    else:
        result = runner.validate_results(path)
        assert result["totals"]["passed"] == len(runner.expected_cases("store")) > 0


@pytest.mark.parametrize("variant", ["empty", "missing", "duplicate", "inflated", "foreign", "malformed"])
def test_junit_rejects_incomplete_or_inconsistent_evidence(tmp_path, variant):
    path = tmp_path / "junit.xml"
    names = list(runner.expected_cases("store"))
    if variant == "empty":
        names = []
    elif variant == "missing":
        names.pop()
    elif variant == "duplicate":
        names[-1] = names[0]
    elif variant == "foreign":
        names[-1] = "test_unrelated"
    junit(path, names, declared=999 if variant == "inflated" else None)
    if variant == "malformed":
        path.write_text("<broken")
    with pytest.raises(runner.RunFailure, match="junit"):
        runner.validate_results(path)


@pytest.fixture
def harness(monkeypatch, tmp_path):
    calls = []
    control = {"dirty": False, "pytest_code": 0, "cleanup_code": 0, "changed": False}
    git_reads = 0

    def command(args, *, env=None, timeout=120):
        nonlocal git_reads
        calls.append((args, env))
        code, stdout = 0, ""
        if args[:3] == ["git", "rev-parse", "HEAD"]:
            git_reads += 1
            stdout = ("b" if control["changed"] and git_reads > 1 else "a") * 40
        elif args[:2] == ["git", "status"]:
            stdout = " M test.py" if control["dirty"] else ""
        elif args[:2] == ["docker", "version"]:
            stdout = "linux/amd64" if "Server.Os" in args[-1] else "27.0.0"
        elif args[:2] == ["docker", "port"]:
            stdout = "127.0.0.1:49152"
        elif args[:2] == ["docker", "inspect"]:
            stdout = "sha256:" + "c" * 64
        elif args[:3] == ["docker", "image", "inspect"]:
            stdout = "linux/amd64"
        elif args[:3] == ["docker", "rm", "-f"]:
            code = control["cleanup_code"]
        elif "pytest" in args:
            path = Path(args[args.index("--junitxml") + 1])
            junit(path)
            code = control["pytest_code"]
            stdout = "sensitive child output must never persist"
        return subprocess.CompletedProcess(args, code, stdout, "sensitive driver error")

    monkeypatch.setattr(runner, "command", command)
    monkeypatch.setattr(runner.platform, "system", lambda: "Linux")
    monkeypatch.setattr(runner.platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(
        runner, "provision_database", lambda env: {"sql_server_version": "16.0", "odbc_driver_version": "18.5"}
    )
    return calls, control, tmp_path / "evidence"


@pytest.mark.parametrize("system,machine", [("Darwin", "arm64"), ("Linux", "aarch64"), ("Windows", "AMD64")])
def test_refuses_unsupported_host_before_any_container(monkeypatch, harness, system, machine):
    calls, _, output = harness
    monkeypatch.setattr(runner.platform, "system", lambda: system)
    monkeypatch.setattr(runner.platform, "machine", lambda: machine)
    assert runner.run(output) == 1
    report = json.loads((output / "summary.json").read_text())
    assert report["status"] == "UNVERIFIED"
    assert report["reason"] == "linux_x86_64_required"
    assert not any(args[0] == "docker" for args, _ in calls)


def test_refuses_dirty_source_without_container(harness):
    calls, control, output = harness
    control["dirty"] = True
    assert runner.run(output) == 1
    assert not any(args[:2] == ["docker", "run"] for args, _ in calls)
    assert json.loads((output / "summary.json").read_text())["reason"] == "source_not_clean"


@pytest.mark.parametrize("failure", [None, "pytest_code", "cleanup_code", "changed"])
def test_owned_container_cleanup_and_exact_component_scope(harness, failure):
    calls, control, output = harness
    if failure:
        control[failure] = 1
    assert runner.run(output) == (1 if failure else 0)
    report = json.loads((output / "summary.json").read_text())
    starts = [(args, env) for args, env in calls if args[:2] == ["docker", "run"]]
    assert len(starts) == 1
    args, env = starts[0]
    name = args[args.index("--name") + 1]
    assert args[-1] == runner.SQL_IMAGE
    assert args[args.index("-p") + 1] == "127.0.0.1::1433"
    assert "MSSQL_SA_PASSWORD" in args
    secret = env["MSSQL_SA_PASSWORD"]
    assert all(secret not in arg for call, _ in calls for arg in call)
    assert any(call == ["docker", "rm", "-f", name] for call, _ in calls)
    assert report["component_scope"]["clickhouse"] == "synthetic_enrollment_metadata_only"
    assert report["component_scope"]["route_certification"] == "UNVERIFIED"
    assert report["component_scope"]["native_v2_and_logon_guarantees"] == "UNVERIFIED"
    assert report["status"] == ("FAIL" if failure else "PASS")
    assert report["cleanup"] == ("FAIL" if failure == "cleanup_code" else "PASS")
    assert report["source_commit"] == "a" * 40
    for artifact in output.iterdir():
        assert secret not in artifact.read_text()
        assert "sensitive" not in artifact.read_text()


def test_start_timeout_still_attempts_owned_cleanup(monkeypatch, harness):
    calls, _, output = harness
    original = runner.command

    def fail_start(args, **kwargs):
        if args[:2] == ["docker", "run"]:
            calls.append((args, kwargs.get("env")))
            raise subprocess.TimeoutExpired(args, 1, stderr="sensitive")
        return original(args, **kwargs)

    monkeypatch.setattr(runner, "command", fail_start)
    assert runner.run(output) == 1
    assert any(args[:3] == ["docker", "rm", "-f"] for args, _ in calls)
    assert "sensitive" not in (output / "summary.json").read_text()


def test_existing_output_cannot_reuse_stale_pass(harness):
    calls, _, output = harness
    output.mkdir()
    (output / "summary.json").write_text("retained evidence")
    assert runner.run(output) == 1
    assert not calls
    assert (output / "summary.json").read_text() == "retained evidence"


@pytest.mark.parametrize("invalid", ["enable", "database", "port", "token", "password"])
def test_fixture_refuses_nonrunner_scope_without_connecting(invalid):
    env = {
        "DPONE_RUN_COMPOSITION_MSSQL_LIVE": "1",
        "DPONE_COMPOSITION_RUN_TOKEN": "a" * 24,
        "DPONE_COMPOSITION_SQL_DATABASE": "dpone_composition_" + "a" * 24,
        "DPONE_COMPOSITION_SQL_PORT": "49152",
        "MSSQL_SA_PASSWORD": "Dp1!" + "b" * 43,
    }
    keys = {
        "enable": "DPONE_RUN_COMPOSITION_MSSQL_LIVE",
        "database": "DPONE_COMPOSITION_SQL_DATABASE",
        "port": "DPONE_COMPOSITION_SQL_PORT",
        "token": "DPONE_COMPOSITION_RUN_TOKEN",
        "password": "MSSQL_SA_PASSWORD",
    }
    env[keys[invalid]] = "existing_business_service"
    with pytest.raises(RuntimeError, match="owned_synthetic_database_required"):
        OwnedDatabase.from_environment(env)


@pytest.mark.parametrize("profile", ["store", "registration"])
def test_actual_pytest_child_skips_without_optin_and_cannot_pass(tmp_path, profile):
    """Exercise collection/plugin/JUnit wiring without any SQL or Docker I/O."""
    from tools.ci.assert_junit_executed import junit_cases

    env = dict(os.environ)
    for flag in ("DPONE_RUN_COMPOSITION_MSSQL_LIVE", *runner.profiles.PROFILE_FLAGS):
        env.pop(flag, None)
    with pytest.raises(runner.RunFailure, match="junit_incomplete_or_not_green"):
        runner.execute_component(tmp_path, env, profile)
    cases = junit_cases(tmp_path / "junit.xml")
    assert len(cases) == len(runner.expected_cases(profile))
    assert {case.node_id for case in cases} == set(runner.expected_cases(profile))
    assert all(case.status == "skipped" for case in cases)


def test_output_under_checkout_is_refused_without_side_effects(monkeypatch, harness):
    calls, _, output = harness
    monkeypatch.setattr(runner, "ROOT", output.parent)
    assert runner.run(output) == 1
    assert not calls and not output.exists()


@pytest.mark.parametrize("profile,count", [("store", 11), ("gate", 25), ("trust", 9), ("registration", 18)])
def test_profiles_have_closed_disjoint_case_inventories(tmp_path, profile, count):
    cases = runner.expected_cases(profile)
    assert len(cases) == len(set(cases)) == count
    path = tmp_path / "junit.xml"
    suite = ET.Element("testsuite", tests=str(count), failures="0", errors="0", skipped="0")
    for node in cases:
        module, name = node.split("::")
        ET.SubElement(suite, "testcase", classname=module, name=name)
    ET.ElementTree(suite).write(path)
    assert runner.validate_results(path, profile)["totals"]["passed"] == count
    other = "gate" if profile == "store" else "store"
    with pytest.raises(runner.RunFailure, match="junit_incomplete"):
        runner.validate_results(path, other)


@pytest.mark.parametrize("profile,count", [("trust", 9), ("registration", 18)])
def test_np_profile_selects_only_its_plugin_and_private_optin(monkeypatch, harness, profile, count):
    calls, _, output = harness
    original = runner.command
    monkeypatch.setenv("DPONE_RUN_COMPOSITION_MSSQL_GATE_LIVE", "1")
    monkeypatch.setenv("DPONE_RUN_COMPOSITION_MSSQL_TRUST_LIVE", "untrusted_ambient")
    monkeypatch.setenv("DPONE_RUN_COMPOSITION_MSSQL_REGISTRATION_LIVE", "untrusted_ambient")
    monkeypatch.setenv("DPONE_COMPOSITION_BULK_FIXTURE_PATH", "untrusted_ambient")

    def command(args, **kwargs):
        result = original(args, **kwargs)
        if "pytest" in args:
            path = Path(args[args.index("--junitxml") + 1])
            cases = runner.expected_cases(profile)
            suite = ET.Element("testsuite", tests=str(len(cases)), failures="0", errors="0", skipped="0")
            for node in cases:
                module, name = node.split("::")
                ET.SubElement(suite, "testcase", classname=module, name=name)
            ET.ElementTree(suite).write(path)
        return result

    monkeypatch.setattr(runner, "command", command)
    assert runner.run(output, profile) == 0
    args, env = next((args, env) for args, env in calls if "pytest" in args)
    assert f"tests.integration.composition.nonproduction_mssql_{profile}_live_support" in args
    assert f"tests/integration/composition/test_nonproduction_mssql_{profile}_live.py" in args
    assert env[f"DPONE_RUN_COMPOSITION_MSSQL_{profile.upper()}_LIVE"] == "1"
    for other in {"gate", "trust", "registration"} - {profile}:
        assert f"DPONE_RUN_COMPOSITION_MSSQL_{other.upper()}_LIVE" not in env
    if profile == "registration":
        import hashlib

        assert env["DPONE_COMPOSITION_BULK_FIXTURE_PATH"] == "/tmp/dpone-composition-registration-bulk.txt"
        assert env["DPONE_COMPOSITION_BULK_FIXTURE_BYTES"] == "2"
        assert env["DPONE_COMPOSITION_BULK_FIXTURE_SHA256"] == "sha256:" + hashlib.sha256(b"1\n").hexdigest()
        assert sum(call[:2] == ["docker", "exec"] for call, _ in calls) == 1
    else:
        assert not any(name.startswith("DPONE_COMPOSITION_BULK_FIXTURE_") for name in env)
        assert not any(call[:2] == ["docker", "exec"] for call, _ in calls)
    report = json.loads((output / "summary.json").read_text())
    assert report["profile"] == profile and report["totals"]["passed"] == count
    assert report["component_scope"]["sql_server"] == "real_append_only_nonproduction_" + profile
    assert report["component_scope"]["worker_execution"] == "UNVERIFIED"
    assert report["component_scope"]["route_certification"] == "UNVERIFIED"
    assert report["cleanup"] == "PASS"


def test_actual_trust_child_collects_exact_cases_and_cannot_pass_without_optin(tmp_path):
    from tools.ci.assert_junit_executed import junit_cases

    env = dict(os.environ)
    env.pop("DPONE_RUN_COMPOSITION_MSSQL_TRUST_LIVE", None)
    env.pop("DPONE_RUN_COMPOSITION_MSSQL_LIVE", None)
    with pytest.raises(runner.RunFailure, match="junit_incomplete_or_not_green"):
        runner.execute_component(tmp_path, env, "trust")
    cases = junit_cases(tmp_path / "junit.xml")
    assert len(cases) == 9 and {case.node_id for case in cases} == set(runner.expected_cases("trust"))
    assert all(case.status == "skipped" for case in cases)


def test_bulk_fixture_failure_stops_registration_and_cleans_container(monkeypatch, harness):
    calls, _, output = harness
    original = runner.command

    def fail_fixture(args, **kwargs):
        result = original(args, **kwargs)
        if args[:2] == ["docker", "exec"]:
            return subprocess.CompletedProcess(args, 1, "", "private fixture failure")
        return result

    monkeypatch.setattr(runner, "command", fail_fixture)
    assert runner.run(output, "registration") == 1
    report = json.loads((output / "summary.json").read_text())
    assert (report["status"], report["stage"], report["cleanup"]) == ("FAIL", "owned_bulk_fixture", "PASS")
    assert not any("pytest" in args for args, _ in calls)
    assert "private" not in (output / "summary.json").read_text()


def test_gate_child_collects_only_exact_gate_cases_and_skips_without_optin(tmp_path):
    from tools.ci.assert_junit_executed import junit_cases

    env = dict(os.environ)
    env.pop("DPONE_RUN_COMPOSITION_MSSQL_GATE_LIVE", None)
    env.pop("DPONE_RUN_COMPOSITION_MSSQL_LIVE", None)
    with pytest.raises(runner.RunFailure, match="junit_incomplete_or_not_green"):
        runner.execute_component(tmp_path, env, "gate")
    cases = junit_cases(tmp_path / "junit.xml")
    assert len(cases) == 25
    assert {case.node_id for case in cases} == set(runner.expected_cases("gate"))
    assert all(case.status == "skipped" for case in cases)


def test_unknown_profile_is_rejected_before_side_effects(harness):
    calls, _, output = harness
    with pytest.raises(runner.RunFailure, match="unknown_component_profile"):
        runner.run(output, "all")
    assert not calls and not output.exists()


@pytest.mark.parametrize("own_case", [True, False])
def test_store_plugin_sanitizes_only_its_live_cases(own_case):
    from tests.integration.composition.mssql_store_live_support import pytest_runtest_makereport

    report = SimpleNamespace(failed=True, longrepr="original diagnostic", sections=[("capture", "output")])
    node = runner.TEST_FILE + "::test_example" if own_case else "tests/test_other.py::test_example"
    hook = pytest_runtest_makereport(SimpleNamespace(nodeid=node), None)
    next(hook)
    with pytest.raises(StopIteration):
        hook.send(SimpleNamespace(get_result=lambda: report))
    if own_case:
        assert report.longrepr != "original diagnostic" and report.sections == []
    else:
        assert report.longrepr == "original diagnostic" and report.sections == [("capture", "output")]
