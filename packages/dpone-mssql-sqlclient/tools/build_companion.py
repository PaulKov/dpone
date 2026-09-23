"""Build one fixed Linux Arm64 companion inside an independently admitted SDK.

Run under the approved immutable SDK image with network disabled. The external
controller proves image identity; this tool verifies explicit SDK/source/feed
file pins. Build output hashes are measurements, never implicit approval pins.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import signal
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import Any

from produce_admission import (
    COMPANION_ROLES,
    STEM,
    inventory,
    outside_roots,
    read_object,
    validate_companion,
    verify_tree,
)

from dpone.adapters.mssql_sqlclient_installation import _before, _file_hash, _relative, _root
from dpone.contracts.mssql_tds_validation import _hash
from dpone.contracts.strict_json import canonical_json_bytes

SDK_VERSION = "8.0.425"
SDK_IMAGE = "sha256:5ef85cc12cb25be6ec319a7392d1e9efd53c3bc8abb971c53d8058a473f09053"
DIAGNOSTICS = frozenset({STEM + ".pdb", STEM + ".xml"})


def run_child(command: list[str], *, cwd: Path, environment: dict[str, str], log: Path, deadline: float) -> None:
    """Bound a child process group, kill/reap on timeout, and retain output as a file."""
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ValueError("producer.deadline")
    with log.open("xb") as output:
        process = subprocess.Popen(
            command, cwd=cwd, env=environment, stdout=output, stderr=subprocess.STDOUT, start_new_session=True
        )
        try:
            code = process.wait(timeout=remaining)
        except BaseException:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=10)
            raise
    if code != 0:
        raise ValueError("producer.build_failed")


def verify_sdk(root: Path, expected: dict[str, str], *, deadline_ns: int) -> None:
    """Verify the larger build SDK closure without changing deployment limits."""
    _root(root)
    if type(expected) is not dict or not 1 <= len(expected) <= 8192:
        raise ValueError("producer.sdk_inventory_invalid")
    for name, digest in expected.items():
        _relative(name)
        _hash(digest)
    actual: dict[str, str] = {}
    for path in root.rglob("*"):
        _before(deadline_ns)
        if path.is_symlink() or len(actual) > 8192:
            raise ValueError("producer.sdk_inventory_invalid")
        if path.is_file():
            actual[path.relative_to(root).as_posix()] = _file_hash(path, deadline_ns)
        elif not path.is_dir():
            raise ValueError("producer.sdk_inventory_invalid")
    if actual != expected:
        raise ValueError("producer.sdk_inventory_mismatch")


def dependency_inventory(feed: Path, lock: dict[str, Any]) -> list[dict[str, Any]]:
    """Retain selected locked package hashes and license declarations for review."""
    selected = {}
    for target in lock["dependencies"].values():
        for name, item in target.items():
            selected[(name, item["resolved"])] = item["contentHash"]
    result = []
    for (name, version), content_hash in sorted(selected.items()):
        package = feed / (name.lower() + "." + version.lower() + ".nupkg")
        with zipfile.ZipFile(package) as archive:
            specs = [entry for entry in archive.infolist() if entry.filename.endswith(".nuspec")]
            if len(specs) != 1 or specs[0].file_size > 1024 * 1024:
                raise ValueError("producer.package_metadata_invalid")
            metadata = ET.fromstring(archive.read(specs[0]))
            license_rows = [
                element for element in metadata.iter() if element.tag.split("}")[-1] in {"license", "licenseUrl"}
            ]
            licenses = [
                {"kind": element.tag.split("}")[-1], "type": element.attrib.get("type"), "value": element.text}
                for element in license_rows
            ]
        result.append(
            {
                "name": name,
                "version": version,
                "lock_content_hash": content_hash,
                "package_sha256": hashlib.sha256(package.read_bytes()).hexdigest(),
                "license_declarations": licenses,
                "license_review": "UNVERIFIED",
            }
        )
    return result


def build(*, source: Path, sdk: Path, feed: Path, pins: dict[str, Any], output: Path) -> dict[str, Any]:
    """Verify all approved inputs and stage one locked build in a new directory."""
    if set(pins) != {"source", "sdk", "feed", "sdk_image", "sdk_version"} or (
        pins["sdk_image"] != SDK_IMAGE or pins["sdk_version"] != SDK_VERSION
    ):
        raise ValueError("producer.sdk_profile_invalid")
    if platform.system() != "Linux" or platform.machine() not in {"aarch64", "arm64"}:
        raise ValueError("producer.platform_invalid")
    outside_roots(output, (source, sdk, feed))
    deadline = time.monotonic() + 240
    deadline_ns = time.monotonic_ns() + 240_000_000_000
    for root, name in ((source, "source"), (sdk, "sdk"), (feed, "feed")):
        (verify_sdk if name == "sdk" else verify_tree)(root, pins[name], deadline_ns=deadline_ns)
    required = {"Worker.csproj", "Worker.packages.lock.json"}
    if not required <= set(pins["source"]) or any(
        name not in required and (not name.startswith("worker/") or not name.endswith(".cs")) for name in pins["source"]
    ):
        raise ValueError("producer.source_inventory_invalid")
    if not (sdk / "dotnet").is_file() or not os.access(sdk / "dotnet", os.X_OK):
        raise ValueError("producer.sdk_host_invalid")
    if not pins["feed"] or any(not name.endswith(".nupkg") or "/" in name for name in pins["feed"]):
        raise ValueError("producer.feed_invalid")
    output.mkdir(mode=0o700)
    work = output / "work"
    work.mkdir()
    package = work / "source"
    shutil.copytree(source, package)
    (work / "global.json").write_bytes(
        canonical_json_bytes({"sdk": {"version": SDK_VERSION, "rollForward": "disable", "allowPrerelease": False}})
    )
    config = work / "NuGet.Config"
    # Feed is a fixed mount, so user-specific paths cannot become build inputs.
    configuration = ET.Element("configuration")
    sources = ET.SubElement(configuration, "packageSources")
    ET.SubElement(sources, "clear")
    ET.SubElement(sources, "add", key="approved-offline", value=str(feed))
    ET.ElementTree(configuration).write(config, encoding="utf-8", xml_declaration=True)
    environment = {
        "PATH": "/usr/bin:/bin",
        "HOME": str(work),
        "DOTNET_ROOT": str(sdk),
        "DOTNET_CLI_HOME": str(work),
        "DOTNET_CLI_TELEMETRY_OPTOUT": "1",
        "DOTNET_SKIP_FIRST_TIME_EXPERIENCE": "1",
        "DOTNET_NOLOGO": "1",
        "DOTNET_CLI_USE_MSBUILD_SERVER": "0",
        "MSBUILDDISABLENODEREUSE": "1",
        "DOTNET_MULTILEVEL_LOOKUP": "0",
        "DOTNET_ROLL_FORWARD": "Disable",
        "NUGET_PACKAGES": str(work / "cache"),
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "TZ": "UTC",
    }
    host = str(sdk / "dotnet")
    commands = [[host, "--version"]]
    properties = [
        "-p:Configuration=Release",
        "-p:RuntimeIdentifier=linux-arm64",
        "-p:Deterministic=true",
        "-p:UseSharedCompilation=false",
        "-nodeReuse:false",
        "-p:ContinuousIntegrationBuild=true",
        "-p:PathMap=" + str(work) + "=/_/build",
        "-p:BaseIntermediateOutputPath=" + str(work / "obj") + "/",
        "-p:BaseOutputPath=" + str(work / "bin") + "/",
    ]
    project = str(package / "Worker.csproj")
    commands += [
        [
            host,
            "restore",
            project,
            "--locked-mode",
            "--disable-build-servers",
            "--configfile",
            str(config),
            "-p:NuGetAudit=false",
            *properties,
        ],
        [
            host,
            "publish",
            project,
            "--disable-build-servers",
            "--no-restore",
            "--output",
            str(work / "published"),
            *properties,
        ],
    ]
    try:
        for index, command in enumerate(commands):
            run_child(
                command, cwd=work, environment=environment, log=output / f"command-{index}.log", deadline=deadline
            )
            if index == 0 and (output / "command-0.log").read_text().strip() != SDK_VERSION:
                raise ValueError("producer.sdk_version_mismatch")
        # Detect mutation of admitted inputs and lock even after successful restore.
        for root, name in ((source, "source"), (sdk, "sdk"), (feed, "feed")):
            (verify_sdk if name == "sdk" else verify_tree)(root, pins[name], deadline_ns=deadline_ns)
        verify_tree(package, pins["source"], deadline_ns=deadline_ns)
        published = inventory(work / "published", deadline_ns=deadline_ns)
        if set(published) != set(COMPANION_ROLES) | DIAGNOSTICS:
            raise ValueError("producer.unexpected_publish_inventory")
        companion = output / "companion"
        diagnostics = output / "diagnostics"
        companion.mkdir()
        diagnostics.mkdir()
        for name in published:
            destination = (diagnostics if name in DIAGNOSTICS else companion) / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(work / "published" / name, destination)
        measured = {name: digest for name, digest in published.items() if name not in DIAGNOSTICS}
        validate_companion(companion, measured, deadline_ns=deadline_ns)
        result = {
            "status": "PASS",
            "scope": "build_only",
            "sdk_image_external_controller_pin": SDK_IMAGE,
            "sdk_version": SDK_VERSION,
            "python_version": platform.python_version(),
            "python_sha256": _file_hash(Path(sys.executable).resolve(), deadline_ns),
            "inputs": pins,
            "commands": commands,
            "environment": environment,
            "global_json_sha256": hashlib.sha256((work / "global.json").read_bytes()).hexdigest(),
            "nuget_config_sha256": hashlib.sha256(config.read_bytes()).hexdigest(),
            "dependencies": dependency_inventory(feed, read_object(package / "Worker.packages.lock.json")),
            "companion": measured,
            "diagnostics": {name: published[name] for name in sorted(DIAGNOSTICS)},
        }
        (output / "build-receipt.pending").write_bytes(canonical_json_bytes(result))
        (output / "build-receipt.pending").rename(output / "build-receipt.json")
        return result
    except BaseException:
        (output / "failure.json").write_bytes(
            canonical_json_bytes({"status": "FAIL", "scope": "build_only", "success_receipt_emitted": False})
        )
        raise


def main() -> int:
    """CLI requires approved pins and never discovers trust from local defaults."""
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source", "sdk", "feed", "pins", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    try:
        build(source=args.source, sdk=args.sdk, feed=args.feed, pins=read_object(args.pins), output=args.output)
    except (ValueError, OSError, TypeError, KeyError, subprocess.SubprocessError):
        print(json.dumps({"status": "FAIL", "reason": "producer.build_or_input_invalid"}))
        return 1
    print(json.dumps({"status": "PASS", "scope": "build_only"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
