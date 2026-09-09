from __future__ import annotations

import json
from dataclasses import asdict, fields
from pathlib import Path

import pytest

from dpone.ops.connection_doctor import ConnectionDoctorService
from dpone.ops.routes.bootstrap_models import OnboardingCheck
from dpone.readiness.doctor import DoctorCheck, DoctorService
from dpone.readiness.python_import_health import PythonImportHealth


def test_mssql_profile_requires_loadable_pyodbc_and_bcp() -> None:
    def import_probe(name: str) -> PythonImportHealth:
        if name == "pyodbc":
            return PythonImportHealth(False, "python_import_failed", "module is installed but cannot be loaded")
        return PythonImportHealth(True, None, "module import succeeded")

    service = DoctorService(
        import_probe=import_probe,
        binary_resolver=lambda name: None if name == "bcp" else f"/bin/{name}",
    )

    payload = service.run(profile="mssql")
    checks = {check["name"]: check for check in payload["checks"]}

    assert payload["passed"] is False
    assert checks["pyodbc"] == {
        "name": "pyodbc",
        "status": "fail",
        "message": "module is installed but cannot be loaded",
        "required": True,
    }
    assert checks["bcp"] == {
        "name": "bcp",
        "status": "fail",
        "message": "executable was not found on PATH",
        "required": True,
    }
    assert any("unixODBC" in fix for fix in payload["fixes"])
    assert any("mssql-tools18" in fix for fix in payload["fixes"])


def test_public_doctor_dataclass_signatures_remain_v1_compatible() -> None:
    doctor = DoctorCheck("python", "pass", "ready", True)
    onboarding = OnboardingCheck(
        name="python_import.pyodbc",
        kind="python_import",
        required=True,
        passed=True,
        status="ready",
        summary="ready",
        remediation="",
    )

    assert tuple(field.name for field in fields(DoctorCheck)) == (
        "name",
        "status",
        "message",
        "required",
    )
    assert asdict(doctor) == doctor.to_dict()
    assert tuple(field.name for field in fields(OnboardingCheck)) == (
        "name",
        "kind",
        "required",
        "passed",
        "status",
        "summary",
        "remediation",
    )
    assert asdict(onboarding) == onboarding.to_dict()


@pytest.mark.parametrize(
    ("reason_code", "expected_fix"),
    (
        ("python_import_not_installed", "Install MSSQL extra"),
        ("python_import_failed", "loadable unixODBC"),
        ("python_import_path_invalid", "effective Python import path"),
        ("python_import_policy_invalid", "effective Python startup policy"),
        ("python_import_startup_unsupported", "standard reproducible Python startup environment"),
        ("python_import_timed_out", "timed-out Python import"),
        ("python_import_probe_unavailable", "child Python interpreter can start"),
    ),
)
def test_mssql_profile_returns_reason_specific_redacted_import_remediation(
    reason_code: str,
    expected_fix: str,
) -> None:
    service = DoctorService(
        import_probe=lambda name: (
            PythonImportHealth(False, reason_code, "redacted import failure")
            if name == "pyodbc"
            else PythonImportHealth(True, None, "module import succeeded")
        ),
        binary_resolver=lambda _name: "/bin/tool",
    )

    payload = service.run(profile="mssql")

    assert any(expected_fix in fix for fix in payload["fixes"])
    assert all("/private" not in fix and "secret" not in fix for fix in payload["fixes"])


@pytest.mark.parametrize(
    ("reason_code", "expected_fix"),
    (
        ("python_import_not_installed", "requested Python runtime dependency"),
        ("python_import_failed", "native runtime dependencies"),
        ("python_import_path_invalid", "effective Python import path"),
        ("python_import_policy_invalid", "effective Python startup policy"),
        ("python_import_startup_unsupported", "standard reproducible Python startup environment"),
        ("python_import_timed_out", "timed-out Python import"),
        ("python_import_probe_unavailable", "child Python interpreter can start"),
    ),
)
def test_route_connection_doctor_uses_shared_redacted_import_remediation(
    reason_code: str,
    expected_fix: str,
) -> None:
    check = ConnectionDoctorService(
        import_probe=lambda _name: PythonImportHealth(False, reason_code, "redacted import failure")
    )._import_checks(("pyodbc",))[0]

    assert expected_fix in check.remediation
    assert "/private" not in check.remediation
    assert "secret" not in check.remediation


def test_local_profile_does_not_claim_a_broken_optional_driver_passed() -> None:
    broken = PythonImportHealth(False, "python_import_failed", "module is installed but cannot be loaded")
    service = DoctorService(
        import_probe=lambda name: (
            broken if name == "pyodbc" else PythonImportHealth(True, None, "module import succeeded")
        ),
        binary_resolver=lambda _name: "/bin/tool",
    )

    payload = service.run(profile="local")
    pyodbc = next(check for check in payload["checks"] if check["name"] == "pyodbc")

    assert payload["passed"] is True
    assert pyodbc["status"] == "warn"
    assert set(pyodbc) == {"name", "status", "message", "required"}


def test_route_connection_doctor_keeps_the_public_v1_check_shape(tmp_path: Path) -> None:
    report = ConnectionDoctorService(
        tool_resolver=lambda _name: "/bin/tool",
        import_probe=lambda _name: PythonImportHealth(
            False,
            "python_import_failed",
            "module is installed but cannot be loaded",
        ),
    ).check(
        output_dir=tmp_path,
        source="clickhouse",
        sink="mssql",
        strategy="full_refresh",
        python_imports=("pyodbc",),
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    check = next(item for item in payload["checks"] if item["name"] == "python_import.pyodbc")

    assert payload["status"] == "blocked"
    assert check["passed"] is False
    assert set(check) == {
        "name",
        "kind",
        "required",
        "passed",
        "status",
        "summary",
        "remediation",
    }
    assert "python_import.pyodbc.missing" in payload["blockers"]


def test_mssql_doctor_docs_bound_process_budget_and_containment() -> None:
    root = Path(__file__).resolve().parents[1]
    guide = " ".join((root / "docs/mssql.md").read_text(encoding="utf-8").split())

    assert "five-second process-communication budget" in guide
    assert "up to two additional seconds per started child" in guide
    assert "original inherited process group" in guide
    assert "process-group reassignment or session detachment is outside" in guide
    assert "Windows, the runner bounds the stdin writer together with the direct child" in guide
    assert "makes no descendant-containment claim" in guide


def test_windows_doctor_ci_proves_stdin_backpressure_is_bounded() -> None:
    root = Path(__file__).resolve().parents[1]
    workflow = (root / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    regression = "tests/test_doctor_import_runner.py::test_windows_runner_bounds_a_child_that_never_reads_stdin"

    assert workflow.count(regression) == 1
