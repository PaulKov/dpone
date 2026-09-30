"""Strict, side-effect-free discovery of the bundled SqlClient companion."""

from __future__ import annotations

import json
import platform
import re
import shutil
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_PROTOCOLS = ("dpone.mssql-sqlclient.ipc.v1", "dpone.mssql-sqlclient.ipc.v2")
_APPLICATION_NAME_VECTOR = (
    "dpone-mssql-sqlclient:"
    + sha256(
        "\0".join(
            (
                "dpone.mssql-sqlclient.application.v2",
                "dpone-parity-vector",
                "5" * 64,
                "781",
                "6" * 64,
            )
        ).encode()
    ).hexdigest()
)
_FIELDS = frozenset(
    {
        "artifact_sha256",
        "entrypoint",
        "package_version",
        "protocol",
        "runtime_identity_sha256",
        "runtime_major",
        "schema_version",
        "writer_identity_sha256",
    }
)


class SqlClientCompanionUnavailable(RuntimeError):
    """Stable, non-sensitive companion discovery failure."""


@dataclass(frozen=True, slots=True)
class SqlClientCompanion:
    """Verified local command and identities for one packaged companion."""

    command: tuple[str, str]
    package_version: str
    artifact_sha256: str
    writer_identity_sha256: str
    runtime_identity_sha256: str
    runtime_major: int
    protocol: str
    protocols: tuple[str, ...]


def capabilities() -> dict[str, object]:
    """Describe packaged support without making an unscoped certification claim."""
    return {
        "schema_version": "dpone.mssql-sqlclient.provider.v1",
        "backend": "mssql_sqlclient",
        "protocol": "dpone.mssql-sqlclient.ipc.v1",
        "protocols": list(_PROTOCOLS),
        "availability": "packaged",
        "certification": "evidence_required",
        "supported_platforms": ["linux_x86_64"],
        "runtime": "Microsoft.NETCore.App 10.x",
    }


def locate(
    *,
    package_root: Path | None = None,
    dotnet_executable: str | None = None,
    system: str | None = None,
    machine: str | None = None,
    run: Callable[..., Any] = subprocess.run,
) -> SqlClientCompanion:
    """Verify package bytes and the required .NET runtime without network I/O."""
    if (system or platform.system()) != "Linux" or (machine or platform.machine()) not in {"x86_64", "amd64"}:
        raise SqlClientCompanionUnavailable("mssql_sqlclient.unsupported_platform")
    root = (package_root or Path(__file__).resolve().parent).resolve()
    companion_root = (root / "companion").resolve()
    descriptor = _descriptor(companion_root / "descriptor.json")
    entrypoint = (companion_root / descriptor["entrypoint"]).resolve()
    if companion_root not in entrypoint.parents or not entrypoint.is_file():
        raise SqlClientCompanionUnavailable("mssql_sqlclient.invalid_descriptor")
    try:
        artifact_digest = _tree_digest(companion_root)
    except OSError as error:
        raise SqlClientCompanionUnavailable("mssql_sqlclient.artifact_unavailable") from error
    if artifact_digest != descriptor["artifact_sha256"]:
        raise SqlClientCompanionUnavailable("mssql_sqlclient.artifact_identity_mismatch")
    dotnet = dotnet_executable or shutil.which("dotnet")
    if not dotnet:
        raise SqlClientCompanionUnavailable("mssql_sqlclient.runtime_unavailable")
    try:
        observed = run(
            (dotnet, "--list-runtimes"),
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
            env={"PATH": "/usr/local/bin:/usr/bin:/bin", "DOTNET_NOLOGO": "1"},
        )
    except Exception as error:
        raise SqlClientCompanionUnavailable("mssql_sqlclient.runtime_unavailable") from error
    expected = f"Microsoft.NETCore.App {descriptor['runtime_major']}."
    if observed.returncode != 0 or not any(line.startswith(expected) for line in observed.stdout.splitlines()):
        raise SqlClientCompanionUnavailable("mssql_sqlclient.runtime_unavailable")
    try:
        self_test = run(
            (dotnet, str(entrypoint), "--self-test-application-name"),
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
            env={"PATH": "/usr/local/bin:/usr/bin:/bin", "DOTNET_NOLOGO": "1"},
        )
    except Exception as error:
        raise SqlClientCompanionUnavailable("mssql_sqlclient.application_identity_unverified") from error
    if self_test.returncode != 0 or self_test.stdout != _APPLICATION_NAME_VECTOR or self_test.stderr:
        raise SqlClientCompanionUnavailable("mssql_sqlclient.application_identity_unverified")
    return SqlClientCompanion(
        command=(dotnet, str(entrypoint)),
        package_version=descriptor["package_version"],
        artifact_sha256=descriptor["artifact_sha256"],
        writer_identity_sha256=descriptor["writer_identity_sha256"],
        runtime_identity_sha256=descriptor["runtime_identity_sha256"],
        runtime_major=descriptor["runtime_major"],
        protocol=descriptor["protocol"],
        protocols=_PROTOCOLS,
    )


def _descriptor(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw, object_pairs_hook=_unique_object)
        if json.dumps(value, sort_keys=True, separators=(",", ":")).encode() != raw:
            raise ValueError
        if not isinstance(value, dict) or set(value) != _FIELDS:
            raise ValueError
        if (
            value["schema_version"] != 1
            or value["protocol"] != "dpone.mssql-sqlclient.ipc.v1"
            or value["runtime_major"] != 10
            or value["entrypoint"] != "dpone-mssql-sqlclient.dll"
            or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", value["package_version"])
            or any(
                _SHA256.fullmatch(value[field]) is None
                for field in ("artifact_sha256", "writer_identity_sha256", "runtime_identity_sha256")
            )
        ):
            raise ValueError
        expected_runtime = sha256(b"Microsoft.NETCore.App\x0010").hexdigest()
        if value["runtime_identity_sha256"] != expected_runtime:
            raise ValueError
        return value
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise SqlClientCompanionUnavailable("mssql_sqlclient.invalid_descriptor") from error


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate key")
        value[key] = item
    return value


def _tree_digest(root: Path) -> str:
    digest = sha256(b"dpone.mssql-sqlclient.artifact.v1\0")
    files = sorted(path for path in root.rglob("*") if path.is_file() and path.name != "descriptor.json")
    if not files:
        raise OSError("empty companion")
    for path in files:
        relative = path.relative_to(root).as_posix().encode()
        digest.update(relative)
        digest.update(b"\0")
        digest.update(sha256(path.read_bytes()).digest())
    return digest.hexdigest()
