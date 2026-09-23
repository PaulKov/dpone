"""Offline producer trust boundaries; fixtures do not certify a real deployment."""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def platform_xattr_fixture(monkeypatch):
    """Host-only unit fixture; actual Linux builds use real capability checks."""
    if not hasattr(os, "getxattr"):

        def unavailable(*args):
            raise OSError(95, "unit fixture")

        monkeypatch.setattr(os, "getxattr", unavailable, raising=False)


TOOLS = Path(__file__).parents[1] / "packages/dpone-mssql-sqlclient/tools"


def load(name):
    spec = importlib.util.spec_from_file_location(name, TOOLS / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_missing_inventory_never_trusted(tmp_path):
    producer = load("produce_admission")
    (tmp_path / "a.dll").write_bytes(b"candidate")
    with pytest.raises(ValueError):
        producer.verify_tree(tmp_path, {})


def test_inventory_detects_tamper_extra_and_missing(tmp_path):
    producer = load("produce_admission")
    import hashlib

    expected = {"a.dll": hashlib.sha256(b"approved").hexdigest()}
    target = tmp_path / "a.dll"
    target.write_bytes(b"approved")
    assert producer.verify_tree(tmp_path, expected) == expected
    target.write_bytes(b"tampered")
    with pytest.raises(ValueError):
        producer.verify_tree(tmp_path, expected)
    target.write_bytes(b"approved")
    (tmp_path / "extra").touch()
    with pytest.raises(ValueError):
        producer.verify_tree(tmp_path, expected)
    (tmp_path / "extra").unlink()
    target.unlink()
    with pytest.raises(ValueError):
        producer.verify_tree(tmp_path, expected)


def test_symlink_is_not_a_file_pin(tmp_path):
    producer = load("produce_admission")
    (tmp_path / "alias").symlink_to("/etc/hosts")
    with pytest.raises(ValueError):
        producer.verify_tree(tmp_path, {"alias": "0" * 64})


@pytest.mark.parametrize("name", ["../x", "/x", "a/../x", "a//x", "x\\y"])
def test_noncanonical_inventory_path_rejected(tmp_path, name):
    with pytest.raises(ValueError):
        load("produce_admission").verify_tree(tmp_path, {name: "0" * 64})


def test_only_exact_two_diagnostic_outputs_excluded():
    build = load("build_companion")
    assert build.DIAGNOSTICS == frozenset({"Dpone.Mssql.SqlClient.Worker.pdb", "Dpone.Mssql.SqlClient.Worker.xml"})


def fixture_deployment(tmp_path):
    producer = load("produce_admission")
    import hashlib

    from dpone.contracts.strict_json import canonical_json_bytes

    companion = tmp_path / "companion"
    runtime = tmp_path / "runtime"
    companion.mkdir()
    runtime.mkdir()
    for name in producer.COMPANION_ROLES:
        path = companion / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"fixture-only")
    profile = {
        "runtimeOptions": {
            "framework": {"name": "Microsoft.NETCore.App", "version": "8.0.31"},
            "rollForward": "Disable",
            "configProperties": {"System.GC.HeapHardLimit": 536870912, "System.GC.Server": False},
        }
    }
    (companion / (producer.STEM + ".runtimeconfig.json")).write_bytes(canonical_json_bytes(profile))
    deps = {
        "runtimeTarget": {"name": "fixture"},
        "libraries": {"Microsoft.Data.SqlClient/7.0.2": {}, "Apache.Arrow/23.0.0": {}},
        "targets": {
            "fixture": {
                "fixture": {"runtime": {name: {} for name in producer.COMPANION_ROLES if name.endswith(".dll")}}
            }
        },
    }
    (companion / (producer.STEM + ".deps.json")).write_bytes(canonical_json_bytes(deps))
    runtime_names = [
        "dotnet",
        "host/fxr/8.0.31/libhostfxr.so",
        "shared/Microsoft.NETCore.App/8.0.31/libhostpolicy.so",
        "shared/Microsoft.NETCore.App/8.0.31/libcoreclr.so",
    ]
    for name in runtime_names:
        path = runtime / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"fixture-runtime")
    cp = {name: hashlib.sha256((companion / name).read_bytes()).hexdigest() for name in producer.COMPANION_ROLES}
    rp = {name: hashlib.sha256((runtime / name).read_bytes()).hexdigest() for name in runtime_names}
    receipt = tmp_path / "build.json"
    receipt.write_bytes(canonical_json_bytes({"status": "PASS", "companion": cp}))
    return producer, dict(
        companion=companion,
        runtime=runtime,
        expected_companion=cp,
        expected_runtime=rp,
        build_receipt=receipt,
        expected_receipt_sha256=hashlib.sha256(receipt.read_bytes()).hexdigest(),
        output=tmp_path / "admission",
    )


def test_manifest_uses_existing_domain_and_exact_profile(tmp_path):
    from dpone.adapters.mssql_sqlclient_installation import validate_manifest

    producer, args = fixture_deployment(tmp_path)
    result = producer.produce(**args)
    body = (args["output"] / "deployment.json").read_bytes()
    manifest = validate_manifest(body, result["build_sha256"])
    assert len(manifest["files"]) == 44
    assert sum(row["role"] == "managed_dependency" for row in manifest["files"]) == 37
    assert result["companion_inventory"] == args["expected_companion"]


@pytest.mark.parametrize(
    "mutation",
    [
        "companion_tamper",
        "runtime_tamper",
        "extra",
        "missing",
        "receipt",
        "profile",
        "deps",
        "self_output",
        "existing_output",
        "runtime_extra_fxr",
    ],
)
def test_admission_negative_has_no_success_receipt(tmp_path, mutation):
    producer, args = fixture_deployment(tmp_path)
    companion, runtime = args["companion"], args["runtime"]
    if mutation == "companion_tamper":
        (companion / (producer.STEM + ".dll")).write_bytes(b"drift")
    elif mutation == "runtime_tamper":
        (runtime / "dotnet").write_bytes(b"drift")
    elif mutation == "extra":
        (companion / "test.dll").touch()
    elif mutation == "missing":
        (companion / "Apache.Arrow.dll").unlink()
    elif mutation == "receipt":
        args["expected_receipt_sha256"] = "0" * 64
    elif mutation in {"profile", "deps"}:
        import hashlib

        from dpone.contracts.strict_json import canonical_json_bytes

        name = producer.STEM + (".runtimeconfig.json" if mutation == "profile" else ".deps.json")
        (companion / name).write_bytes(b"{}")
        args["expected_companion"][name] = hashlib.sha256(b"{}").hexdigest()
        args["build_receipt"].write_bytes(
            canonical_json_bytes({"status": "PASS", "companion": args["expected_companion"]})
        )
        args["expected_receipt_sha256"] = hashlib.sha256(args["build_receipt"].read_bytes()).hexdigest()
    elif mutation == "self_output":
        args["output"] = companion / "admission"
    elif mutation == "existing_output":
        args["output"].mkdir()
    elif mutation == "runtime_extra_fxr":
        (runtime / "host/fxr/8.0.99").mkdir()
    with pytest.raises((ValueError, OSError)):
        producer.produce(**args)
    assert not (args["output"] / "admission-receipt.json").exists()


def test_duplicate_pin_json_rejected(tmp_path):
    path = tmp_path / "pins.json"
    path.write_text('{"same":"a","same":"b"}')
    with pytest.raises(ValueError):
        load("produce_admission").read_object(path)


def test_build_rejects_unapproved_sdk_before_creating_output(tmp_path):
    load("produce_admission")
    producer = load("build_companion")
    with pytest.raises(ValueError):
        producer.build(
            source=tmp_path, sdk=tmp_path, feed=tmp_path, pins={"sdk_image": "candidate"}, output=tmp_path / "new"
        )
    assert not (tmp_path / "new").exists()


def test_child_deadline_reaps_process(tmp_path):
    import subprocess
    import time

    load("produce_admission")
    producer = load("build_companion")
    log = tmp_path / "child.log"
    with pytest.raises(subprocess.TimeoutExpired):
        producer.run_child(
            [sys.executable, "-c", "import os,time;print(os.getpid(),flush=True);time.sleep(20)"],
            cwd=tmp_path,
            environment={"PATH": "/usr/bin:/bin"},
            log=log,
            deadline=time.monotonic() + 1,
        )
    pid = int(log.read_text().strip())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


@pytest.mark.parametrize("tool", ["build_companion", "produce_admission"])
def test_cli_help_is_usable(tool):
    import subprocess

    result = subprocess.run(
        [sys.executable, str(TOOLS / (tool + ".py")), "--help"], capture_output=True, text=True, timeout=10
    )
    assert result.returncode == 0
    assert "--pins" in result.stdout
    assert result.stderr == ""


def test_admission_cli_failure_is_generic_and_has_no_side_effect(tmp_path):
    import subprocess

    pins = tmp_path / "pins.json"
    pins.write_text("{}")
    output = tmp_path / "new"
    result = subprocess.run(
        [
            sys.executable,
            str(TOOLS / "produce_admission.py"),
            "--companion",
            str(tmp_path),
            "--runtime",
            str(tmp_path),
            "--pins",
            str(pins),
            "--build-receipt",
            str(pins),
            "--output",
            str(output),
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 1
    assert "producer.inputs_or_output_invalid" in result.stdout
    assert str(tmp_path) not in result.stdout + result.stderr
    assert not output.exists()


def test_timeout_kills_descendant_in_same_process_group(tmp_path):
    import subprocess
    import time

    load("produce_admission")
    producer = load("build_companion")
    started = tmp_path / "started"
    delayed = tmp_path / "delayed"
    grandchild = (
        "from pathlib import Path;import time;"
        f"Path({str(started)!r}).touch();time.sleep(2);Path({str(delayed)!r}).touch()"
    )
    child = f'import subprocess,sys,time;subprocess.Popen([sys.executable,"-c",{grandchild!r}]);time.sleep(20)'
    with pytest.raises(subprocess.TimeoutExpired):
        producer.run_child(
            [sys.executable, "-c", child],
            cwd=tmp_path,
            environment={"PATH": "/usr/bin:/bin"},
            log=tmp_path / "tree.log",
            deadline=time.monotonic() + 1,
        )
    assert started.exists(), "descendant actually started before timeout"
    time.sleep(1.5)
    assert not delayed.exists(), "descendant must not execute after group termination"
