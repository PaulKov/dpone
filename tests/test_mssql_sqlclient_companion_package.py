from __future__ import annotations

import importlib
import json
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def provider(monkeypatch: pytest.MonkeyPatch):
    root = Path(__file__).parents[1] / "packages" / "dpone-mssql-sqlclient" / "src"
    monkeypatch.syspath_prepend(str(root))
    module = importlib.import_module("dpone_mssql_sqlclient")
    yield module
    for name in tuple(importlib.sys.modules):
        if name == "dpone_mssql_sqlclient" or name.startswith("dpone_mssql_sqlclient."):
            importlib.sys.modules.pop(name, None)


def _package(tmp_path: Path) -> tuple[Path, bytes]:
    root = tmp_path / "provider"
    companion = root / "companion"
    companion.mkdir(parents=True)
    assembly = b"verified assembly"
    (companion / "dpone-mssql-sqlclient.dll").write_bytes(assembly)
    writer = "7" * 64
    runtime = sha256(b"Microsoft.NETCore.App\x0010").hexdigest()
    artifact = sha256(b"dpone.mssql-sqlclient.artifact.v1\0")
    artifact.update(b"dpone-mssql-sqlclient.dll\0")
    artifact.update(sha256(assembly).digest())
    descriptor = {
        "artifact_sha256": artifact.hexdigest(),
        "entrypoint": "dpone-mssql-sqlclient.dll",
        "package_version": "0.88.0",
        "protocol": "dpone.mssql-sqlclient.ipc.v1",
        "runtime_identity_sha256": runtime,
        "runtime_major": 10,
        "schema_version": 1,
        "writer_identity_sha256": writer,
    }
    (companion / "descriptor.json").write_text(
        json.dumps(descriptor, sort_keys=True, separators=(",", ":")), encoding="utf-8"
    )
    return root, assembly


def _run(command: tuple[str, ...], **_kwargs: object) -> SimpleNamespace:
    if command[-1] == "--self-test-application-name":
        expected = (
            "dpone-mssql-sqlclient:"
            + sha256(
                "\0".join(
                    ("dpone.mssql-sqlclient.application.v2", "dpone-parity-vector", "5" * 64, "781", "6" * 64)
                ).encode()
            ).hexdigest()
        )
        return SimpleNamespace(returncode=0, stdout=expected, stderr="")
    return SimpleNamespace(returncode=0, stdout="Microsoft.NETCore.App 10.0.2 [/dotnet/shared]\n", stderr="")


def test_locator_returns_closed_verified_companion(provider, tmp_path: Path) -> None:
    root, _ = _package(tmp_path)

    result = provider.locate(
        package_root=root,
        dotnet_executable="/usr/bin/dotnet",
        system="Linux",
        machine="x86_64",
        run=_run,
    )

    assert result.command == ("/usr/bin/dotnet", str(root / "companion" / "dpone-mssql-sqlclient.dll"))
    assert result.writer_identity_sha256 == "7" * 64
    assert result.runtime_major == 10
    assert result.package_version == "0.88.0"
    assert result.protocols == (
        "dpone.mssql-sqlclient.ipc.v1",
        "dpone.mssql-sqlclient.ipc.v2",
    )


@pytest.mark.parametrize(
    ("system", "machine", "code"),
    [
        ("Darwin", "arm64", "mssql_sqlclient.unsupported_platform"),
        ("Linux", "aarch64", "mssql_sqlclient.unsupported_platform"),
    ],
)
def test_locator_fails_closed_on_unsupported_platform(
    provider, tmp_path: Path, system: str, machine: str, code: str
) -> None:
    root, _ = _package(tmp_path)

    with pytest.raises(provider.SqlClientCompanionUnavailable, match=code):
        provider.locate(
            package_root=root,
            dotnet_executable="/usr/bin/dotnet",
            system=system,
            machine=machine,
            run=_run,
        )


def test_locator_rejects_artifact_digest_drift(provider, tmp_path: Path) -> None:
    root, _ = _package(tmp_path)
    (root / "companion" / "dpone-mssql-sqlclient.dll").write_bytes(b"changed")

    with pytest.raises(provider.SqlClientCompanionUnavailable, match="mssql_sqlclient.artifact_identity_mismatch"):
        provider.locate(
            package_root=root,
            dotnet_executable="/usr/bin/dotnet",
            system="Linux",
            machine="x86_64",
            run=_run,
        )


def test_locator_rejects_unknown_descriptor_field(provider, tmp_path: Path) -> None:
    root, _ = _package(tmp_path)
    descriptor_path = root / "companion" / "descriptor.json"
    descriptor = json.loads(descriptor_path.read_text())
    descriptor["future"] = True
    descriptor_path.write_text(json.dumps(descriptor, sort_keys=True, separators=(",", ":")))

    with pytest.raises(provider.SqlClientCompanionUnavailable, match="mssql_sqlclient.invalid_descriptor"):
        provider.locate(
            package_root=root,
            dotnet_executable="/usr/bin/dotnet",
            system="Linux",
            machine="x86_64",
            run=_run,
        )


def test_locator_requires_exact_runtime_family(provider, tmp_path: Path) -> None:
    root, _ = _package(tmp_path)

    def run(*_args: object, **_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(returncode=0, stdout="Microsoft.NETCore.App 9.0.9 [/dotnet/shared]\n", stderr="")

    with pytest.raises(provider.SqlClientCompanionUnavailable, match="mssql_sqlclient.runtime_unavailable"):
        provider.locate(
            package_root=root,
            dotnet_executable="/usr/bin/dotnet",
            system="Linux",
            machine="x86_64",
            run=run,
        )


def test_locator_rejects_cross_runtime_application_identity_drift(provider, tmp_path: Path) -> None:
    root, _ = _package(tmp_path)

    def run(command: tuple[str, ...], **_kwargs: object) -> SimpleNamespace:
        if command[-1] == "--self-test-application-name":
            return SimpleNamespace(returncode=0, stdout="dpone-mssql-sqlclient:" + "0" * 64, stderr="")
        return SimpleNamespace(returncode=0, stdout="Microsoft.NETCore.App 10.0.2 [/dotnet/shared]\n", stderr="")

    with pytest.raises(provider.SqlClientCompanionUnavailable, match="application_identity_unverified"):
        provider.locate(
            package_root=root,
            dotnet_executable="/usr/bin/dotnet",
            system="Linux",
            machine="x86_64",
            run=run,
        )


def test_capabilities_report_completed_route_gate(provider) -> None:
    assert provider.capabilities() == {
        "schema_version": "dpone.mssql-sqlclient.provider.v1",
        "backend": "mssql_sqlclient",
        "protocol": "dpone.mssql-sqlclient.ipc.v1",
        "protocols": ["dpone.mssql-sqlclient.ipc.v1", "dpone.mssql-sqlclient.ipc.v2"],
        "availability": "packaged",
        "certification": "evidence_required",
        "supported_platforms": ["linux_x86_64"],
        "runtime": "Microsoft.NETCore.App 10.x",
    }


def test_companion_build_is_independent_of_parent_source_control_metadata() -> None:
    project = (
        Path(__file__).parents[1] / "packages" / "dpone-mssql-sqlclient" / "companion" / "Dpone.Mssql.SqlClient.csproj"
    ).read_text(encoding="utf-8")

    assert "<EnableSourceControlManagerQueries>false</EnableSourceControlManagerQueries>" in project
