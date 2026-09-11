"""Execute the current harness with the real frozen baseline, without services."""

from __future__ import annotations

import json
import os
import subprocess
import textwrap
import venv
from pathlib import Path

import pytest
from tools.native_delivery_live_benchmark import APPROVAL_FLAGS
from tools.native_delivery_live_support.execution import BASELINE_COMMIT
from tools.native_delivery_live_support.hermetic import LIMITS

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "tools/native_delivery_live_benchmark.py"


@pytest.fixture(scope="module")
def baseline_python(tmp_path_factory):
    """A real local clone and dependency-free venv; candidate src is never added."""
    directory = tmp_path_factory.mktemp("dda-baseline")
    checkout = directory / "subject"
    subprocess.run(
        ["git", "clone", "--shared", "--no-checkout", str(ROOT), str(checkout)], check=True, capture_output=True
    )
    subprocess.run(["git", "-C", str(checkout), "sparse-checkout", "set", "src/dpone"], check=True, capture_output=True)
    subprocess.run(
        ["git", "-C", str(checkout), "checkout", "--detach", BASELINE_COMMIT], check=True, capture_output=True
    )
    environment = directory / "venv"
    venv.EnvBuilder(with_pip=False, symlinks=os.name != "nt").create(environment)
    python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    site = subprocess.check_output(
        [str(python), "-I", "-c", "import sysconfig; print(sysconfig.get_path('purelib'))"], text=True
    ).strip()
    Path(site, "baseline.pth").write_text(str(checkout / "src") + "\n", encoding="utf-8")
    return python, checkout


def invoke_baseline(baseline_python, tmp_path, limits, *, approved):
    python, checkout = baseline_python
    factory_path = tmp_path / "baseline_factory.py"
    factory_path.write_text(
        textwrap.dedent("""\
            import importlib.util
            import inspect
            import json
            import sys
            from pathlib import Path
            import dpone
            from dpone.contracts.mssql_native_chunks import NativeChunkLimits
            from tools.native_delivery_live_support.hermetic import HermeticRouteFactory

            def create(*, configuration, route):
                identity = {
                    "dpone_file": dpone.__file__,
                    "model_file": inspect.getfile(NativeChunkLimits),
                    "candidate_helper_available": importlib.util.find_spec(
                        "dpone.contracts.native_delivery_observations") is not None,
                    "configuration": configuration,
                    "prefix": sys.prefix,
                    "base_prefix": sys.base_prefix,
                }
                Path(__file__).with_suffix(".json").write_text(json.dumps(identity))
                factory = HermeticRouteFactory()
                factory.subject_checkout = Path(dpone.__file__).resolve().parents[2]
                return factory
            """),
        encoding="utf-8",
    )
    # -I ignores ambient PYTHONPATH; only the baseline and this hermetic factory
    # are exposed by the isolated interpreter's own site directory.
    site = subprocess.check_output(
        [str(python), "-I", "-c", "import sysconfig; print(sysconfig.get_path('purelib'))"], text=True
    ).strip()
    Path(site, "factory.pth").write_text(str(tmp_path) + "\n", encoding="utf-8")
    limits_path = tmp_path / "limits.json"
    limits_path.write_text(json.dumps(limits), encoding="utf-8")
    output = tmp_path / "run.json"
    environment = {key: os.environ[key] for key in ("PATH", "SYSTEMROOT", "WINDIR", "TEMP", "TMP") if key in os.environ}
    if approved:
        # Synthetic branch coverage only: the child can load only our hermetic
        # factory and receives no host service configuration or credentials.
        environment.update(dict.fromkeys(APPROVAL_FLAGS, "1"))
    result = subprocess.run(
        [
            str(python),
            "-I",
            str(HARNESS),
            "run",
            "--adapter",
            "baseline",
            "--factory",
            "baseline_factory:create",
            "--rows",
            "8",
            "--limits",
            str(limits_path),
            "--output",
            str(output),
        ],
        cwd=checkout,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert subprocess.check_output(["git", "-C", str(checkout), "status", "--porcelain"], text=True) == ""
    return result, output, factory_path.with_suffix(".json")


def test_absolute_current_harness_executes_real_baseline_and_retains_honest_identity(baseline_python, tmp_path):
    result, output, marker = invoke_baseline(baseline_python, tmp_path, LIMITS, approved=True)
    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    assert result.stdout.strip() == f"{output}: UNVERIFIED"
    report = json.loads(output.read_text())
    identity = json.loads(marker.read_text())
    _, checkout = baseline_python
    assert Path(identity["dpone_file"]).resolve() == checkout / "src/dpone/__init__.py"
    assert Path(identity["model_file"]).resolve() == checkout / "src/dpone/contracts/mssql_native_chunks.py"
    assert identity["candidate_helper_available"] is False
    assert identity["prefix"] != identity["base_prefix"]
    assert report["subject"] == {"commit": BASELINE_COMMIT, "dirty": False}
    assert report["status"] == "UNVERIFIED"
    assert identity["configuration"] == report["configuration"]
    assert report["configuration"]["sha256"] == "4ceac4f9c46a4280916ce9df6d7038e55a1ea0f0b0540edb1a116c62b61c4054"
    assert report["fidelity_receipt"]["status"] == report["recovery_receipt"]["status"] == "PASS"
    assert len(report["samples"]) == 4
    assert all(sample["status"] == "PASS" for sample in report["samples"])


def test_baseline_without_approval_writes_skip_without_loading_factory(baseline_python, tmp_path):
    result, output, marker = invoke_baseline(baseline_python, tmp_path, LIMITS, approved=False)
    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    assert result.stdout.strip() == f"{output}: SKIP"
    assert json.loads(output.read_text())["status"] == "SKIP"
    assert not marker.exists()


@pytest.mark.parametrize(
    "limits",
    [
        {key: value for key, value in LIMITS.items() if key != "parallelism"},
        {**LIMITS, "extra": 1},
        {**LIMITS, "parallelism": True},
        {**LIMITS, "max_row_bytes": LIMITS["max_bytes"] + 1},
    ],
)
def test_baseline_invalid_limits_fail_before_factory_or_artifacts(baseline_python, tmp_path, limits):
    result, output, marker = invoke_baseline(baseline_python, tmp_path, limits, approved=True)
    assert result.returncode == 2
    assert result.stdout == ""
    assert (
        result.stderr
        == "native_delivery.invalid_input_or_execution; inspect configuration, factory and retained evidence\n"
    )
    assert not output.exists()
    assert not marker.exists()
