"""Deterministic Linux wheel build for the version-matched .NET companion."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import subprocess
from pathlib import Path

from setuptools import Distribution, setup
from setuptools.command.build_py import build_py
from wheel.bdist_wheel import bdist_wheel

ROOT = Path(__file__).resolve().parent
PROJECT = ROOT / "companion" / "Dpone.Mssql.SqlClient.csproj"
RUNTIME_IDENTITY = hashlib.sha256(b"Microsoft.NETCore.App\x0010").hexdigest()


class CompanionDistribution(Distribution):
    def has_ext_modules(self) -> bool:
        return True


class BuildCompanion(build_py):
    def run(self) -> None:
        super().run()
        if platform.system() != "Linux" or platform.machine().lower() not in {"x86_64", "amd64"}:
            raise RuntimeError("mssql_sqlclient.build_requires_linux_x86_64")
        writer_identity = _source_digest()
        publish = Path(self.build_lib).parent / "dotnet-publish"
        shutil.rmtree(publish, ignore_errors=True)
        subprocess.run(
            [
                "dotnet",
                "publish",
                str(PROJECT),
                "-c",
                "Release",
                "-f",
                "net10.0",
                "--runtime",
                "linux-x64",
                "--self-contained",
                "false",
                "-p:UseAppHost=false",
                "-p:RestoreLockedMode=true",
                f"-p:WriterIdentity={writer_identity}",
                "--output",
                str(publish),
            ],
            check=True,
            env={"PATH": os.environ.get("PATH", ""), "DOTNET_NOLOGO": "1", "DOTNET_CLI_TELEMETRY_OPTOUT": "1"},
        )
        target = Path(self.build_lib) / "dpone_mssql_sqlclient" / "companion"
        shutil.rmtree(target, ignore_errors=True)
        shutil.copytree(publish, target)
        version = self.distribution.metadata.version
        descriptor = {
            "artifact_sha256": _tree_digest(target),
            "entrypoint": "dpone-mssql-sqlclient.dll",
            "package_version": version,
            "protocol": "dpone.mssql-sqlclient.ipc.v1",
            "runtime_identity_sha256": RUNTIME_IDENTITY,
            "runtime_major": 10,
            "schema_version": 1,
            "writer_identity_sha256": writer_identity,
        }
        (target / "descriptor.json").write_text(
            json.dumps(descriptor, sort_keys=True, separators=(",", ":")), encoding="utf-8"
        )


class LinuxWheel(bdist_wheel):
    def finalize_options(self) -> None:
        super().finalize_options()
        self.root_is_pure = False

    def get_tag(self) -> tuple[str, str, str]:
        return "py3", "none", "manylinux_2_17_x86_64"


def _source_digest() -> str:
    digest = hashlib.sha256(b"dpone.mssql-sqlclient.writer.v1\0")
    paths = sorted((ROOT / "companion").glob("*.cs")) + [PROJECT, ROOT / "companion" / "packages.lock.json"]
    for path in sorted(paths):
        digest.update(path.relative_to(ROOT).as_posix().encode())
        digest.update(b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def _tree_digest(root: Path) -> str:
    digest = hashlib.sha256(b"dpone.mssql-sqlclient.artifact.v1\0")
    for path in sorted(item for item in root.rglob("*") if item.is_file() and item.name != "descriptor.json"):
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


setup(distclass=CompanionDistribution, cmdclass={"build_py": BuildCompanion, "bdist_wheel": LinuxWheel})
