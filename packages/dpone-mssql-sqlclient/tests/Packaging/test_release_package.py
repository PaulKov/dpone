from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import pytest

from dpone.contracts.strict_json import canonical_json_bytes

TOOLS = Path(__file__).parents[2] / "tools"
sys.path.insert(0, str(TOOLS))

import package_release  # noqa: E402
from package_release import assemble  # noqa: E402
from qualify_reproducible_build import qualify  # noqa: E402
from release_bundle import runtime_inventory, validate_release_tree  # noqa: E402


def _write(path: Path, value: bytes | dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(value) if isinstance(value, dict) else value)


def _build_fixture(root: Path) -> tuple[Path, Path, Path, Path, Path]:
    build, reproducibility, admission, runtime = (
        root / "build",
        root / "repro.json",
        root / "admission",
        root / "runtime",
    )
    names = {
        "Dpone.Mssql.SqlClient.Worker.dll": "worker_assembly",
        "Dpone.Mssql.SqlClient.Worker.runtimeconfig.json": "worker_runtimeconfig",
        "Dpone.Mssql.SqlClient.Worker.deps.json": "worker_deps",
        "Microsoft.Data.SqlClient.dll": "managed_dependency",
        "Apache.Arrow.dll": "managed_dependency",
    }
    for index, name in enumerate(names):
        _write(build / "companion" / name, f"file-{index}".encode())
    inventory = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in (build / "companion").iterdir()}
    dependencies = [
        {"name": "Apache.Arrow", "version": "23.0.0", "package_sha256": "1" * 64},
        {"name": "Microsoft.Data.SqlClient", "version": "7.0.2", "package_sha256": "2" * 64},
    ]
    build_receipt = {
        "status": "PASS",
        "sdk_image_external_controller_pin": "sha256:" + "a" * 64,
        "sdk_version": "8.0.425",
        "companion": inventory,
        "dependencies": dependencies,
    }
    _write(build / "build-receipt.json", build_receipt)
    receipt_hash = hashlib.sha256((build / "build-receipt.json").read_bytes()).hexdigest()
    runtime_files = {
        "dotnet": b"host",
        "host/fxr/8.0.31/libhostfxr.so": b"fxr",
        "shared/Microsoft.NETCore.App/8.0.31/libcoreclr.so": b"coreclr",
        "shared/Microsoft.NETCore.App/8.0.31/libhostpolicy.so": b"hostpolicy",
    }
    for name, body in runtime_files.items():
        _write(runtime / name, body)
    (runtime / "dotnet").chmod(0o755)
    runtime_inventory = {name: hashlib.sha256(body).hexdigest() for name, body in runtime_files.items()}
    deployment_files = [
        {"origin": "companion", "path": name, "role": role, "sha256": inventory[name]} for name, role in names.items()
    ] + [
        {
            "origin": "dotnet",
            "path": name,
            "role": "dotnet_host" if name == "dotnet" else "runtime_library",
            "sha256": digest,
        }
        for name, digest in runtime_inventory.items()
    ]
    deployment = {
        "schema_version": 1,
        "backend": "mssql_sqlclient",
        "platform": "linux_arm64",
        "runtime_version": "8.0.31",
        "sqlclient_version": "7.0.2",
        "arrow_version": "23.0.0",
        "files": sorted(deployment_files, key=lambda row: (row["origin"], row["path"])),
    }
    build_sha = hashlib.sha256(b"dpone.sqlclient.deployment.v1\0" + canonical_json_bytes(deployment)).hexdigest()
    _write(admission / "deployment.json", deployment)
    _write(
        admission / "admission-receipt.json",
        {
            "status": "PASS",
            "build_sha256": build_sha,
            "companion_inventory": inventory,
            "runtime_inventory": runtime_inventory,
            "build_receipt_sha256": receipt_hash,
        },
    )
    _write(
        reproducibility,
        {
            "schema_version": "dpone.mssql-sqlclient.reproducible-build.v1",
            "status": "PASS",
            "sdk_image": "sha256:" + "a" * 64,
            "sdk_version": "8.0.425",
            "input_pins_sha256": "5" * 64,
            "companion_inventory": inventory,
            "build_receipt_sha256": [receipt_hash, "6" * 64],
        },
    )
    licenses = root / "licenses.json"
    _write(
        licenses,
        {
            "schema_version": "dpone.licenses.v1",
            "packages": [
                {
                    "name": row["name"],
                    "version": row["version"],
                    "spdx_expression": "MIT",
                    "license_text": "Reviewed license text.",
                }
                for row in [
                    *dependencies,
                    {"name": "Microsoft.NETCore.App", "version": "8.0.31"},
                ]
            ],
        },
    )
    return build, reproducibility, admission, licenses, runtime


def test_release_is_closed_read_only_compatible_and_deterministic(tmp_path: Path) -> None:
    build, reproducibility, admission, licenses, runtime = _build_fixture(tmp_path)
    first = assemble(
        version="0.83.0",
        build=build,
        reproducibility=reproducibility,
        admission=admission,
        licenses=licenses,
        runtime=runtime,
        output=tmp_path / "one",
    )
    second = assemble(
        version="0.83.0",
        build=build,
        reproducibility=reproducibility,
        admission=admission,
        licenses=licenses,
        runtime=runtime,
        output=tmp_path / "two",
    )
    assert first["archive_sha256"] == second["archive_sha256"]
    root = next((tmp_path / "one/install/dpone-mssql-sqlclient/0.83.0").iterdir())
    assert (root.stat().st_mode & 0o222) == 0
    assert all((path.stat().st_mode & 0o222) == 0 for path in root.rglob("*"))
    assert (root / "runtime/dotnet").stat().st_mode & 0o111
    assert (root / "runtime/shared/Microsoft.NETCore.App/8.0.31/libcoreclr.so").read_bytes() == b"coreclr"
    compatibility = json.loads((root / "metadata/compatibility.json").read_text())
    assert compatibility["dpone"] == {"minimum": "0.83.0", "maximum_exclusive": "0.84.0"}
    assert json.loads((root / "signature-input.json").read_text())["signature"] is None
    assert "previously admitted complete root" in " ".join((root / "OPERATIONS.md").read_text().split())
    assert len(list((root / "licenses").glob("*.txt"))) == 3
    manifest = json.loads((root / "release-manifest.json").read_text())
    assert manifest["control_files"] == ["release-manifest.json", "signature-input.json"]
    assert {row["path"] for row in manifest["files"]} == {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() and path.name not in manifest["control_files"]
    }
    assert validate_release_tree(root) == manifest
    root.chmod(0o755)
    (root / "unexpected.bin").write_bytes(b"extra")
    with pytest.raises(ValueError, match="inventory_mismatch"):
        validate_release_tree(root)


def test_release_rejects_inventory_drift_and_incomplete_license_review(tmp_path: Path) -> None:
    build, reproducibility, admission, licenses, runtime = _build_fixture(tmp_path)
    (build / "companion/Microsoft.Data.SqlClient.dll").write_bytes(b"drift")
    with pytest.raises(ValueError, match="evidence_mismatch"):
        assemble(
            version="0.83.0",
            build=build,
            reproducibility=reproducibility,
            admission=admission,
            licenses=licenses,
            runtime=runtime,
            output=tmp_path / "drift",
        )
    build, reproducibility, admission, licenses, runtime = _build_fixture(tmp_path / "fresh")
    value = json.loads(licenses.read_text())
    value["packages"].pop()
    _write(licenses, value)
    with pytest.raises(ValueError, match="licenses_incomplete"):
        assemble(
            version="0.83.0",
            build=build,
            reproducibility=reproducibility,
            admission=admission,
            licenses=licenses,
            runtime=runtime,
            output=tmp_path / "licenses-missing",
        )


@pytest.mark.parametrize("mutation", ["missing", "drift", "extra"])
def test_release_rejects_runtime_inventory_changes(tmp_path: Path, mutation: str) -> None:
    build, reproducibility, admission, licenses, runtime = _build_fixture(tmp_path)
    if mutation == "missing":
        (runtime / "shared/Microsoft.NETCore.App/8.0.31/libcoreclr.so").unlink()
    elif mutation == "drift":
        (runtime / "dotnet").write_bytes(b"changed")
    else:
        (runtime / "unexpected.so").write_bytes(b"extra")
    with pytest.raises(ValueError, match="runtime_inventory_mismatch"):
        assemble(
            version="0.83.0",
            build=build,
            reproducibility=reproducibility,
            admission=admission,
            licenses=licenses,
            runtime=runtime,
            output=tmp_path / "invalid-runtime",
        )


def test_release_rejects_unsafe_paths_and_runtime_objects(tmp_path: Path) -> None:
    build, reproducibility, admission, licenses, runtime = _build_fixture(tmp_path)

    def package(runtime_arg: Path, output: Path) -> None:
        assemble(
            version="0.83.0",
            build=build,
            reproducibility=reproducibility,
            admission=admission,
            licenses=licenses,
            runtime=runtime_arg,
            output=output,
        )

    with pytest.raises(ValueError, match="output_exists"):
        package(runtime, runtime / "recursive-output")
    with pytest.raises(ValueError, match="input_path_invalid"):
        package(Path("relative-runtime"), tmp_path / "relative")
    (runtime / "linked").symlink_to(runtime / "dotnet")
    with pytest.raises(ValueError, match="runtime_inventory_invalid"):
        package(runtime, tmp_path / "linked")


def test_runtime_budget_rejects_file_over_512_mib_before_hashing(tmp_path: Path) -> None:
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    oversized = runtime / "oversized"
    with oversized.open("wb") as stream:
        stream.truncate(512 * 1024**2 + 1)
    with pytest.raises(ValueError, match="runtime_inventory_invalid"):
        runtime_inventory(runtime)


def test_evidence_is_packaged_from_one_stable_snapshot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    build, reproducibility, admission, licenses, runtime = _build_fixture(tmp_path)
    originals = {
        "deployment": (admission / "deployment.json").read_bytes(),
        "admission": (admission / "admission-receipt.json").read_bytes(),
        "reproducibility": reproducibility.read_bytes(),
    }
    copier = package_release.secure_copy_tree
    mutated = False

    def mutating_copy(*args: Any, **kwargs: Any) -> None:
        nonlocal mutated
        copier(*args, **kwargs)
        if not mutated:
            mutated = True
            (admission / "deployment.json").write_bytes(b"replaced")
            (admission / "admission-receipt.json").write_bytes(b"replaced")
            reproducibility.write_bytes(b"replaced")

    monkeypatch.setattr(package_release, "secure_copy_tree", mutating_copy)
    assemble(
        version="0.83.0",
        build=build,
        reproducibility=reproducibility,
        admission=admission,
        licenses=licenses,
        runtime=runtime,
        output=tmp_path / "release",
    )
    root = next((tmp_path / "release/install/dpone-mssql-sqlclient/0.83.0").iterdir())
    assert (root / "deployment.json").read_bytes() == originals["deployment"]
    assert (root / "metadata/admission-receipt.json").read_bytes() == originals["admission"]
    provenance = json.loads((root / "metadata/provenance.json").read_text())
    assert provenance["reproducibility_receipt_sha256"] == hashlib.sha256(originals["reproducibility"]).hexdigest()


def test_release_rejects_admission_for_another_build_receipt(tmp_path: Path) -> None:
    build, reproducibility, admission, licenses, runtime = _build_fixture(tmp_path)
    receipt = json.loads((admission / "admission-receipt.json").read_text())
    receipt["build_receipt_sha256"] = "f" * 64
    _write(admission / "admission-receipt.json", receipt)
    with pytest.raises(ValueError, match="evidence_mismatch"):
        assemble(
            version="0.83.0",
            build=build,
            reproducibility=reproducibility,
            admission=admission,
            licenses=licenses,
            runtime=runtime,
            output=tmp_path / "release",
        )


def test_double_build_requires_exact_companion_bytes(tmp_path: Path) -> None:
    calls = 0

    def builder(**kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        output = kwargs["output"]
        output.mkdir()
        receipt = {"call": calls}
        _write(output / "build-receipt.json", receipt)
        return {
            "status": "PASS",
            "sdk_image_external_controller_pin": "sha256:" + "a" * 64,
            "sdk_version": "8.0.425",
            "companion": {"worker.dll": "1" * 64 if calls == 1 else "2" * 64},
        }

    source, sdk, feed = tmp_path / "source", tmp_path / "sdk", tmp_path / "feed"
    for path in (source, sdk, feed):
        path.mkdir()
    with pytest.raises(ValueError, match="reproducibility_mismatch"):
        qualify(source=source, sdk=sdk, feed=feed, pins={}, output=tmp_path / "output", builder=builder)
    assert (tmp_path / "output/reproducibility-failure.json").is_file()
    assert not (tmp_path / "output/reproducibility-receipt.json").exists()
