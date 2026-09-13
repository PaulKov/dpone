"""Reproduce baseline diagnostics and interval behavior through public APIs.

This is an analysis producer, not a live dbt execution or a regression fix.
It uses injected in-memory process doubles, synthetic redaction material and
temporary files confined to this artifact directory. No globals or private
methods are replaced. Defect observations deliberately retain status FAIL.

Run from the repository root with the locked Python environment::

    uv run --locked --no-sync python -B \
        test_artifacts/dbt-programme/stage-05/synthetic_probe.py
"""

from __future__ import annotations

import hashlib
import io
import json
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

from dpone_airflow_pack.dag_schedule import parse_spec_start_date

from dpone.adapters.dbt_process_supervisor import DbtProcessSupervisor, ProcessSupervisionError
from dpone.adapters.dbt_subprocess import SubprocessDbtCommandRunner
from dpone.contracts.dbt_invocation import DbtInvocationContext
from dpone.contracts.dbt_runtime import DbtExecutionInterval, DbtPublishingError
from dpone.contracts.run_interval import run_interval_from_env

BASELINE = "46830976b214262c7772800523e832a5a6f6d78f"
SOURCE_PATHS = (
    "src/dpone/adapters/dbt_subprocess.py",
    "src/dpone/adapters/dbt_process_supervisor.py",
    "src/dpone/contracts/dbt_invocation.py",
    "src/dpone/contracts/dbt_runtime.py",
    "src/dpone/contracts/run_interval.py",
    "packages/dpone-airflow-pack/src/dpone_airflow_pack/dag_schedule.py",
)


class CompletedProcess:
    """Finite pipe output supplied through the runner's public process factory."""

    def __init__(self, payload: bytes) -> None:
        self.stdout = io.BytesIO(payload)
        self.stderr = io.BytesIO()

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        return 7


class TimedOutProcess(CompletedProcess):
    """A timeout with no real child process to terminate."""

    def wait(self, timeout: float | None = None) -> int:
        raise subprocess.TimeoutExpired(("synthetic",), timeout)


class FailedCleanup(DbtProcessSupervisor):
    """Explicitly injected public supervisor reporting an independent failure."""

    def terminate(self, process: Any) -> None:
        del process
        raise ProcessSupervisionError("synthetic cleanup failed")


def output_observations(root: Path) -> list[dict[str, Any]]:
    """Observe retained-boundary shrinkage and an ordinary 64 KiB boundary."""

    args = ("dbt", "build", "--target-path", str(root / "attempt" / "target"))
    secret = "SYNTHETIC_" + "s" * 54
    payload = (secret + "x" * 16 + secret).encode()
    result = SubprocessDbtCommandRunner(
        max_output_bytes=64,
        popen_factory=lambda *args, **kwargs: CompletedProcess(payload),
        dbt_executable="synthetic",
    ).run(args, cwd=root, timeout_seconds=1, redactions=(secret,))
    assert result.stdout == "[REDACTED]" + "x" * 16 + secret[:38]
    assert result.exit_code == 7 and result.stdout_truncated
    observations: list[dict[str, Any]] = [
        {
            "case": "retained_boundary_redaction_shrinkage",
            "status": "FAIL",
            "property": "No fragment of a known secret is disclosed by truncation.",
            "synthetic_secret_bytes": 64,
            "filler_bytes": 16,
            "output_limit_bytes": 64,
            "visible_secret_prefix_bytes": 38,
            "exit_code_preserved": True,
            "truncation_reported": True,
        }
    ]
    boundary_secret = "SYNTHETIC_BOUNDARY_VALUE"
    boundary_payload = b"x" * (65536 - 10) + boundary_secret.encode() + b" ok"
    boundary_result = SubprocessDbtCommandRunner(
        max_output_bytes=70000,
        popen_factory=lambda *args, **kwargs: CompletedProcess(boundary_payload),
        dbt_executable="synthetic",
    ).run(args, cwd=root, timeout_seconds=1, redactions=(boundary_secret,))
    assert boundary_secret not in boundary_result.stdout
    assert boundary_result.stdout.endswith("[REDACTED] ok")
    observations.append({"case": "secret_spans_64KiB_read_boundary", "status": "PASS"})
    try:
        SubprocessDbtCommandRunner(
            popen_factory=lambda *args, **kwargs: TimedOutProcess(b"synthetic failure"),
            process_supervisor=FailedCleanup(posix=False),
            dbt_executable="synthetic",
        ).run(args, cwd=root, timeout_seconds=1, redactions=())
    except DbtPublishingError as error:
        assert error.code == "DPONE_DBT_EXECUTION_FAILED"
        assert str(error) == "dbt process cleanup did not complete safely"
        assert isinstance(error.__cause__, ProcessSupervisionError)
        assert isinstance(error.__context__, subprocess.TimeoutExpired)
        observations.append(
            {
                "case": "timeout_with_cleanup_failure",
                "status": "FAIL",
                "property": "Cleanup failure does not replace the primary timeout diagnostic.",
                "public_message": str(error),
                "direct_cause": type(error.__cause__).__name__,
                "timeout_preserved_in_context": True,
            }
        )
    else:
        raise AssertionError("Expected the injected timeout and cleanup failure")
    return observations


def interval_observations() -> list[dict[str, Any]]:
    """Check fixed-offset comparisons without claiming regional zone validation."""

    cases = (
        ("missing", "", "", False),
        ("partial", "2026-09-01T00:00:00Z", "", False),
        ("zero_width", "2026-09-01T00:00:00Z", "2026-09-01T00:00:00Z", False),
        ("naive", "2026-09-01T00:00:00", "2026-09-02T00:00:00", False),
        ("reverse_instant", "2026-09-01T00:00:00Z", "2026-09-01T00:30:00+01:00", False),
        ("equal_instant", "2026-09-01T00:00:00Z", "2026-09-01T01:00:00+01:00", False),
        ("spring_23h", "2026-03-08T00:00:00-05:00", "2026-03-09T00:00:00-04:00", True),
        ("autumn_25h", "2026-11-01T00:00:00-04:00", "2026-11-02T00:00:00-05:00", True),
        ("autumn_fold", "2026-11-01T01:30:00-04:00", "2026-11-01T01:15:00-05:00", True),
    )
    observations: list[dict[str, Any]] = []
    for name, start, end, expected in cases:
        try:
            DbtExecutionInterval(start=start, end=end)
            accepted = True
        except DbtPublishingError:
            accepted = False
        assert accepted is expected, name
        item: dict[str, Any] = {"case": name, "status": "PASS", "accepted": accepted}
        if accepted:
            item["elapsed_hours"] = (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds() / 3600
        observations.append(item)
    interval = run_interval_from_env({"DPONE_DAG_ID": "synthetic_dag", "DPONE_DAG_RUN_ID": "synthetic_manual"})
    assert not interval.is_empty and interval.execution_datetime() is None
    observations.append(
        {"case": "manual_identity_without_bounds", "status": "PASS", "is_empty": False, "execution_datetime": None}
    )
    authored = "2026-09-13T15:45:30+00:00"
    parsed = parse_spec_start_date(authored, timezone_name="UTC")
    assert parsed.isoformat() == "2026-09-13T00:00:00+00:00"
    observations.append(
        {
            "case": "aware_start_date_with_timezone",
            "status": "FAIL",
            "property": "Authored timestamp precision is preserved.",
            "authored": authored,
            "observed": parsed.isoformat(),
        }
    )
    return observations


def main() -> None:
    """Write reproducible findings only after asserting exact production identity."""

    artifact_root = Path(__file__).resolve().parent
    repo_root = artifact_root.parents[2]
    subprocess.run(
        ("git", "diff", "--exit-code", BASELINE, "--", "src", "packages", "pyproject.toml", "uv.lock"),
        cwd=repo_root,
        check=True,
        capture_output=True,
    )
    source_hashes = {name: hashlib.sha256((repo_root / name).read_bytes()).hexdigest() for name in SOURCE_PATHS}
    with tempfile.TemporaryDirectory(prefix=".synthetic-probe-", dir=artifact_root) as temporary:
        observations = output_observations(Path(temporary))
    observations.extend(interval_observations())
    environment = DbtInvocationContext.canonical().environment(home="/synthetic-home")
    assert "DBT_DEBUG" not in environment and "DBT_LOG_LEVEL" not in environment
    observations.append({"case": "canonical_environment_excludes_debug_overrides", "status": "PASS"})
    report = {
        "assessed_source_commit": BASELINE,
        "source_sha256": source_hashes,
        "scope": "Public APIs, injected finite processes and synthetic values; no live execution.",
        "producer_status": "PASS",
        "status_meaning": "PASS producer means baseline reproduced; FAIL observations are unresolved defects.",
        "observations": observations,
        "unverified": [
            "real Airflow scheduling",
            "live dbt logs",
            "peak memory",
            "dbt-owned log-file redaction and size",
        ],
    }
    output = artifact_root / "synthetic-observations.json"
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"producer_status": "PASS", "observations": len(observations), "failed_properties": 3}))


if __name__ == "__main__":
    main()
