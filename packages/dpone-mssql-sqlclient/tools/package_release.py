"""Assemble an immutable companion release from approved double-build evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path
from typing import Any

from release_bundle import (
    dependency_rows,
    deterministic_tar,
    digest,
    inventory,
    license_rows,
    make_read_only,
    read_json,
    runtime_inventory,
    secure_copy_tree,
    secure_read,
    validate_release_tree,
    validate_version,
    write_json,
)

from dpone.adapters.mssql_sqlclient_installation import validate_manifest
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object


def _verify_inputs(
    build: Path, reproducibility: Path, admission: Path, runtime: Path
) -> tuple[dict[str, Any], dict[str, Any], dict[str, str], dict[str, bytes]]:
    snapshots = {
        "build": secure_read(build / "build-receipt.json", 2 * 1024**2),
        "reproducibility": secure_read(reproducibility, 2 * 1024**2),
        "admission": secure_read(admission / "admission-receipt.json", 2 * 1024**2),
        "deployment": secure_read(admission / "deployment.json", 2 * 1024**2),
    }
    build_receipt = strict_json_object(snapshots["build"])
    repro = strict_json_object(snapshots["reproducibility"])
    admitted = strict_json_object(snapshots["admission"])
    build_receipt_sha256 = hashlib.sha256(snapshots["build"]).hexdigest()
    companion = build / "companion"
    actual = inventory(companion)
    actual_runtime = runtime_inventory(runtime)
    if (
        repro.get("schema_version") != "dpone.mssql-sqlclient.reproducible-build.v1"
        or repro.get("status") != "PASS"
        or repro.get("companion_inventory") != actual
        or build_receipt.get("companion") != actual
        or build_receipt_sha256 not in repro.get("build_receipt_sha256", [])
        or admitted.get("status") != "PASS"
        or admitted.get("companion_inventory") != actual
        or admitted.get("build_receipt_sha256") != build_receipt_sha256
    ):
        raise ValueError("release.evidence_mismatch")
    if admitted.get("runtime_inventory") != actual_runtime:
        raise ValueError("release.runtime_inventory_mismatch")
    manifest = validate_manifest(snapshots["deployment"], admitted["build_sha256"])
    expected_runtime = {row["path"]: row["sha256"] for row in manifest["files"] if row["origin"] == "dotnet"}
    if expected_runtime != actual_runtime:
        raise ValueError("release.runtime_inventory_mismatch")
    return build_receipt, admitted, actual_runtime, snapshots


def assemble(
    *,
    version: str,
    build: Path,
    reproducibility: Path,
    admission: Path,
    licenses: Path,
    runtime: Path,
    output: Path,
) -> dict[str, Any]:
    """Create one closed release tree, unsigned subject and deterministic tar."""
    validate_version(version)
    _validate_paths(build, reproducibility, admission, licenses, runtime, output)
    receipt, admitted, admitted_runtime, snapshots = _verify_inputs(build, reproducibility, admission, runtime)
    dependencies = dependency_rows(receipt)
    dependencies.append(
        {
            "name": "Microsoft.NETCore.App",
            "version": "8.0.31",
            "package_sha256": hashlib.sha256(canonical_json_bytes(admitted_runtime)).hexdigest(),
        }
    )
    dependencies.sort(key=lambda row: (row["name"].lower(), row["version"]))
    reviewed_licenses = license_rows(read_json(licenses), dependencies)
    staging = output.with_name(output.name + ".pending")
    if staging.exists() or staging.is_symlink():
        raise ValueError("release.output_exists")
    staging.mkdir(mode=0o700)
    try:
        root = staging / "install" / "dpone-mssql-sqlclient" / version / admitted["build_sha256"]
        companion = root / "companion"
        secure_copy_tree(
            build / "companion",
            companion,
            admitted["companion_inventory"],
            maximum_files=4096,
            maximum_file_bytes=256 * 1024**2,
            maximum_total_bytes=2 * 1024**3,
        )
        packaged_runtime = root / "runtime"
        secure_copy_tree(
            runtime,
            packaged_runtime,
            admitted_runtime,
            maximum_files=8192,
            maximum_file_bytes=512 * 1024**2,
            maximum_total_bytes=2 * 1024**3,
            executable=frozenset({"dotnet"}),
        )
        (root / "deployment.json").write_bytes(snapshots["deployment"])
        metadata = root / "metadata"
        metadata.mkdir()
        (metadata / "admission-receipt.json").write_bytes(snapshots["admission"])
        write_json(
            metadata / "dependencies.json",
            {"schema_version": "dpone.dependency-inventory.v1", "packages": dependencies},
        )
        write_json(metadata / "licenses.json", {"schema_version": "dpone.licenses.v1", "packages": reviewed_licenses})
        license_directory = root / "licenses"
        license_directory.mkdir()
        for index, row in enumerate(reviewed_licenses, 1):
            (license_directory / f"{index:04d}.txt").write_text(
                f"Package: {row['name']} {row['version']}\nSPDX: {row['spdx_expression']}\n\n{row['license_text'].strip()}\n",
                encoding="utf-8",
            )
        write_json(
            metadata / "sbom.spdx.json", _sbom(version, admitted["build_sha256"], dependencies, reviewed_licenses)
        )
        write_json(metadata / "compatibility.json", _compatibility(version, admitted["build_sha256"]))
        write_json(
            metadata / "provenance.json",
            _provenance(
                receipt, snapshots["reproducibility"], admitted, hashlib.sha256(snapshots["build"]).hexdigest()
            ),
        )
        (root / "OPERATIONS.md").write_text(_operations(version, admitted["build_sha256"]), encoding="utf-8")
        payload = inventory(root)
        (root / "checksums.sha256").write_text(
            "".join(f"{value}  {name}\n" for name, value in sorted(payload.items())), encoding="ascii"
        )
        payload = inventory(root)
        manifest = {
            "schema_version": "dpone.mssql-sqlclient.release-manifest.v1",
            "artifact": "dpone-mssql-sqlclient",
            "version": version,
            "platform": "linux_arm64",
            "dpone_compatibility": ">=0.83.0,<0.84.0",
            "deployment_build_sha256": admitted["build_sha256"],
            "files": [{"path": name, "sha256": value} for name, value in sorted(payload.items())],
            "control_files": ["release-manifest.json", "signature-input.json"],
        }
        write_json(root / "release-manifest.json", manifest)
        manifest_digest = digest(root / "release-manifest.json")
        write_json(
            root / "signature-input.json",
            {
                "schema_version": "dpone.mssql-sqlclient.signature-input.v1",
                "subject": "release-manifest",
                "subject_sha256": manifest_digest,
                "signature": None,
            },
        )
        validate_release_tree(root)
        make_read_only(staging / "install")
        archive = staging / f"dpone-mssql-sqlclient-{version}-linux-arm64.tar"
        deterministic_tar(root, archive)
        result = {
            "schema_version": "dpone.mssql-sqlclient.release-receipt.v1",
            "status": "PASS",
            "scope": "unsigned_package_only; no publication, installation, startup, SQL, or route certification",
            "version": version,
            "deployment_build_sha256": admitted["build_sha256"],
            "release_manifest_sha256": manifest_digest,
            "archive_sha256": digest(archive),
        }
        write_json(staging / "release-receipt.json", result)
        staging.rename(output)
        return result
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def _validate_paths(
    build: Path, reproducibility: Path, admission: Path, licenses: Path, runtime: Path, output: Path
) -> None:
    directories = (build, admission, runtime)
    files = (reproducibility, licenses)
    if any(not path.is_absolute() or path.resolve(strict=True) != path or not path.is_dir() for path in directories):
        raise ValueError("release.input_path_invalid")
    if any(not path.is_absolute() or path.resolve(strict=True) != path or not path.is_file() for path in files):
        raise ValueError("release.input_path_invalid")
    if (
        not output.is_absolute()
        or output.exists()
        or output.is_symlink()
        or output.parent.resolve(strict=True) != output.parent
        or any(root == output or root in output.parents for root in directories)
    ):
        raise ValueError("release.output_exists")


def _compatibility(version: str, build_sha256: str) -> dict[str, Any]:
    return {
        "schema_version": "dpone.mssql-sqlclient.compatibility.v1",
        "companion_version": version,
        "dpone": {"minimum": "0.83.0", "maximum_exclusive": "0.84.0"},
        "platform": {"os": "linux", "architecture": "arm64"},
        "dotnet_runtime": "8.0.31",
        "deployment_build_sha256": build_sha256,
    }


def _provenance(
    receipt: dict[str, Any], reproducibility: bytes, admitted: dict[str, Any], build_receipt_sha256: str
) -> dict[str, Any]:
    repro = strict_json_object(reproducibility)
    return {
        "schema_version": "dpone.mssql-sqlclient.provenance.v1",
        "build_type": "locked-offline-double-build",
        "reproducibility_receipt_sha256": hashlib.sha256(reproducibility).hexdigest(),
        "build_receipt_sha256": build_receipt_sha256,
        "input_pins_sha256": repro["input_pins_sha256"],
        "sdk_image": repro["sdk_image"],
        "sdk_version": repro["sdk_version"],
        "dependency_inventory_sha256": hashlib.sha256(canonical_json_bytes(receipt["dependencies"])).hexdigest(),
        "runtime_inventory_sha256": hashlib.sha256(canonical_json_bytes(admitted["runtime_inventory"])).hexdigest(),
    }


def _sbom(
    version: str, build_sha256: str, dependencies: list[dict[str, str]], licenses: list[dict[str, str]]
) -> dict[str, Any]:
    expressions = {(row["name"], row["version"]): row["spdx_expression"] for row in licenses}
    packages = [
        {
            "SPDXID": f"SPDXRef-Package-{index}",
            "name": row["name"],
            "versionInfo": row["version"],
            "checksums": [{"algorithm": "SHA256", "checksumValue": row["package_sha256"]}],
            "licenseConcluded": expressions[(row["name"], row["version"])],
            "downloadLocation": "NOASSERTION",
        }
        for index, row in enumerate(dependencies, 1)
    ]
    return {
        "spdxVersion": "SPDX-2.3",
        "dataLicense": "CC0-1.0",
        "SPDXID": "SPDXRef-DOCUMENT",
        "name": f"dpone-mssql-sqlclient-{version}",
        "documentNamespace": f"https://dpone.invalid/spdx/{build_sha256}",
        "creationInfo": {"created": "1970-01-01T00:00:00Z", "creators": ["Tool: dpone-companion-packager"]},
        "packages": packages,
    }


def _operations(version: str, build_sha256: str) -> str:
    return f"""# Install, upgrade and rollback\n\nThis unsigned bundle is not an admission decision or route certification.\n\nInstall the complete extracted root at a root-owned, read-only path such as\n`/opt/dpone/mssql-sqlclient/{version}/{build_sha256}`. Verify the external\nsignature over `signature-input.json`, then independently admit the exact\n`deployment.json` digest and inventories before selection. Never modify a root.\n\nFor upgrade, install the new version beside existing roots, admit it, then change\nthe injected composition-root selection atomically. Keep the previous complete\nroot until rollback expiry. For rollback, select a previously admitted complete\nroot; do not copy files between versions or reconstruct a missing root.\n"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    for name in ("build", "reproducibility", "admission", "licenses", "runtime", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    try:
        assemble(
            version=args.version,
            build=args.build,
            reproducibility=args.reproducibility,
            admission=args.admission,
            licenses=args.licenses,
            runtime=args.runtime,
            output=args.output,
        )
    except (ValueError, OSError, TypeError, KeyError):
        print(json.dumps({"status": "FAIL", "reason": "release.inputs_or_output_invalid"}))
        return 1
    print(json.dumps({"status": "PASS", "scope": "unsigned_package_only"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
