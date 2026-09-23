from __future__ import annotations

import hashlib
import os
import tarfile
from pathlib import Path
from typing import Any

import pytest

from dpone.adapters.mssql_sqlclient_companion_trust import admit_sqlclient_companion
from dpone.contracts.mssql_sqlclient_companion_trust import canonical_bytes, sha256_bytes


def test_injected_verifier_is_not_a_supported_admission_input(tmp_path: Path) -> None:
    root, archive, policy, key = release(tmp_path, tier="qualification")
    with pytest.raises(TypeError):
        admit_sqlclient_companion(
            release_root=root,
            archive=archive,
            policy_bytes=canonical_bytes(policy),
            public_key=key,
            sigstore_bundle=b"bundle",
            verifier=object(),  # type: ignore[call-arg]
        )


def test_receipt_is_validated_evidence_not_an_admission_input() -> None:
    assert "receipt" not in admit_sqlclient_companion.__annotations__


@pytest.mark.parametrize("mutation", ["key", "archive", "tree", "policy", "revoked"])
def test_preflight_drift_and_revocation_fail_before_verifier(tmp_path: Path, mutation: str) -> None:
    root, archive, policy, key = release(tmp_path, tier="qualification")
    if mutation == "key":
        key = b"other-key"
    elif mutation == "archive":
        archive.write_bytes(archive.read_bytes() + b"drift")
    elif mutation == "tree":
        (root / "metadata/provenance.json").chmod(0o644)
        (root / "metadata/provenance.json").write_bytes(b"drift")
    elif mutation == "policy":
        policy["companion_version"] = "0.83.9"
    else:
        policy["revoked_key_ids"] = [policy["key_id"]]
    with pytest.raises(ValueError):
        admit_sqlclient_companion(
            release_root=root,
            archive=archive,
            policy_bytes=canonical_bytes(policy),
            public_key=key,
            sigstore_bundle=b"bundle",
        )


def test_unpinned_cosign_executable_is_rejected_before_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import dpone.adapters.mssql_sqlclient_companion_trust as trust
    from dpone.contracts.mssql_sqlclient_companion_trust import SqlClientCompanionTrustPolicy

    root, _, policy, key = release(tmp_path, tier="qualification")
    executable = tmp_path / "cosign"
    executable.write_bytes(b"not-the-pinned-cosign")
    executable.chmod(0o555)
    monkeypatch.setattr(trust.shutil, "which", lambda *args, **kwargs: str(executable))
    with pytest.raises(ValueError, match="companion_untrusted"):
        trust._verify_cosign(
            (root / "signature-input.json").read_bytes(),
            b"bundle",
            key,
            SqlClientCompanionTrustPolicy.from_bytes(canonical_bytes(policy)),
        )


def test_writable_production_root_is_rejected(tmp_path: Path) -> None:
    root, archive, policy, key = release(tmp_path, tier="production")
    root.chmod(0o755)
    with pytest.raises(ValueError, match="companion_untrusted"):
        admit_sqlclient_companion(
            release_root=root,
            archive=archive,
            policy_bytes=canonical_bytes(policy),
            public_key=key,
            sigstore_bundle=b"bundle",
        )


def test_controlled_subprocess_issues_receipt_and_revalidating_capability(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import dpone.adapters.mssql_sqlclient_companion_trust as trust

    executable = tmp_path / "cosign"
    executable.write_text(
        '#!/bin/sh\nif [ "$1" = "version" ]; then echo \'{"gitVersion":"v3.0.4"}\'; fi\nexit 0\n',
        encoding="utf-8",
    )
    executable.chmod(0o555)
    root, archive, policy, key = release(tmp_path / "release", tier="qualification")
    policy["cosign_executable_sha256"] = file_digest(executable)
    monkeypatch.setattr(trust.shutil, "which", lambda *args, **kwargs: str(executable))
    monkeypatch.setattr(
        trust,
        "_dpone_distribution_identity",
        lambda: (policy["dpone_exact_version"], policy["dpone_distribution_sha256"]),
    )

    capability, receipt = admit_sqlclient_companion(
        release_root=root,
        archive=archive,
        policy_bytes=canonical_bytes(policy),
        public_key=key,
        sigstore_bundle=b"bundle",
    )

    assert receipt.status == "VERIFIED"
    assert receipt.verifier_version == "3.0.4"
    capability.assert_current(environment="qualification", require_production=False)
    with pytest.raises(ValueError, match="companion_untrusted"):
        capability.assert_current(environment="qualification", require_production=True)
    changed = root / "companion/Worker.dll"
    changed.chmod(0o644)
    changed.write_bytes(b"drift")
    with pytest.raises(ValueError, match="companion_untrusted"):
        capability.assert_current(environment="qualification", require_production=False)


def test_external_hardlink_and_missing_runtime_are_rejected(tmp_path: Path) -> None:
    root, archive, policy, key = release(tmp_path, tier="qualification")
    target = root / "companion/Worker.dll"
    target.chmod(0o644)
    root.chmod(0o755)
    os.link(target, tmp_path / "external-worker-link")
    with pytest.raises(ValueError, match="companion_untrusted"):
        admit_sqlclient_companion(
            release_root=root,
            archive=archive,
            policy_bytes=canonical_bytes(policy),
            public_key=key,
            sigstore_bundle=b"bundle",
        )
    other_root, other_archive, other_policy, other_key = release(tmp_path / "missing", tier="qualification")
    runtime = other_root / "runtime/dotnet"
    runtime.parent.chmod(0o755)
    runtime.chmod(0o644)
    runtime.unlink()
    with pytest.raises(ValueError, match="companion_untrusted"):
        admit_sqlclient_companion(
            release_root=other_root,
            archive=other_archive,
            policy_bytes=canonical_bytes(other_policy),
            public_key=other_key,
            sigstore_bundle=b"bundle",
        )


def test_archive_growth_and_truncation_during_digest_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import dpone.adapters.mssql_sqlclient_companion_trust_files as files

    growing = tmp_path / "growing.tar"
    growing.write_bytes(b"a" * 64)
    original_read = files.os.read
    calls = 0

    def grow(descriptor: int, size: int) -> bytes:
        nonlocal calls
        value = original_read(descriptor, size)
        calls += 1
        if calls == 1:
            with growing.open("ab") as stream:
                stream.write(b"b" * 128)
        return value

    monkeypatch.setattr(files, "MAX_TREE_BYTES", 128)
    monkeypatch.setattr(files.os, "read", grow)
    with pytest.raises(ValueError, match="companion_untrusted"):
        files.validate_archive_bound(growing, tmp_path, hashlib.sha256(b"a" * 64).hexdigest())

    monkeypatch.setattr(files.os, "read", original_read)
    truncated = tmp_path / "truncated.tar"
    payload = b"c" * 64
    truncated.write_bytes(payload)
    first = True

    def truncate(descriptor: int, size: int) -> bytes:
        nonlocal first
        value = original_read(descriptor, size)
        if first:
            first = False
            truncated.write_bytes(b"")
        return value

    monkeypatch.setattr(files.os, "read", truncate)
    with pytest.raises(ValueError, match="companion_untrusted"):
        files.validate_archive_bound(truncated, tmp_path, hashlib.sha256(payload).hexdigest())


def release(tmp_path: Path, *, tier: str) -> tuple[Path, Path, dict[str, Any], bytes]:
    root = (tmp_path / "install" / ("a" * 64)).absolute()
    (root / "metadata").mkdir(parents=True)
    companion = {
        "Apache.Arrow.dll": b"arrow",
        "Microsoft.Data.SqlClient.dll": b"sqlclient",
        "Worker.deps.json": b"{}",
        "Worker.dll": b"worker",
        "Worker.runtimeconfig.json": b"{}",
    }
    runtime = {
        "dotnet": b"dotnet-host",
        "shared/Microsoft.NETCore.App/8.0.31/libcoreclr.so": b"coreclr",
    }
    deployment = deployment_manifest(
        {name: sha256_bytes(value) for name, value in companion.items()},
        {name: sha256_bytes(value) for name, value in runtime.items()},
    )
    deployment_bytes = canonical_bytes(deployment)
    build = hashlib.sha256(b"dpone.sqlclient.deployment.v1\0" + deployment_bytes).hexdigest()
    files: dict[str, bytes] = {
        "deployment.json": deployment_bytes,
        "metadata/provenance.json": canonical_bytes(
            {"schema_version": "dpone.mssql-sqlclient.provenance.v1", "input_pins_sha256": "9" * 64}
        ),
        **{f"companion/{name}": value for name, value in companion.items()},
        **{f"runtime/{name}": value for name, value in runtime.items()},
    }
    for name, value in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(value)
    rows = [{"path": name, "sha256": sha256_bytes(value)} for name, value in sorted(files.items())]
    manifest = {
        "schema_version": "dpone.mssql-sqlclient.release-manifest.v1",
        "artifact": "dpone-mssql-sqlclient",
        "version": "0.83.0",
        "platform": "linux_arm64",
        "dpone_compatibility": ">=0.83.0,<0.84.0",
        "deployment_build_sha256": build,
        "files": rows,
        "control_files": ["release-manifest.json", "signature-input.json"],
    }
    manifest_bytes = canonical_bytes(manifest)
    (root / "release-manifest.json").write_bytes(manifest_bytes)
    (root / "signature-input.json").write_bytes(
        canonical_bytes(
            {
                "schema_version": "dpone.mssql-sqlclient.signature-input.v1",
                "subject": "release-manifest",
                "subject_sha256": sha256_bytes(manifest_bytes),
                "signature": None,
            }
        )
    )
    archive = (tmp_path / "release.tar").absolute()
    deterministic_tar(root, archive)
    key = b"trusted-public-key"
    environment = "prod" if tier == "production" else "qualification"
    policy = {
        "schema": "dpone.mssql-sqlclient.companion-trust-policy.v1",
        "trust_tier": tier,
        "environment": environment,
        "key_id": "release-2026",
        "public_key_sha256": sha256_bytes(key),
        "companion_schema": manifest["schema_version"],
        "companion_version": manifest["version"],
        "platform": manifest["platform"],
        "dpone_exact_version": "0.83.0",
        "dpone_distribution_sha256": "7" * 64,
        "dpone_compatibility": ">=0.83.0,<0.84.0",
        "cosign_exact_version": "3.0.4",
        "cosign_executable_sha256": "8" * 64,
        "source_identity": file_digest(root / "metadata/provenance.json"),
        "release_identity": f"sha256:{sha256_bytes(manifest_bytes)}",
        "release_manifest_sha256": sha256_bytes(manifest_bytes),
        "archive_sha256": file_digest(archive),
        "deployment_build_sha256": build,
        "revoked_key_ids": [],
        "revoked_release_identities": [],
        "revoked_deployment_builds": [],
        "cosign": {"minimum_version": "3.0.4", "maximum_version_exclusive": "4.0.0", "timeout_seconds": 10},
    }
    for path in sorted(root.rglob("*"), reverse=True):
        path.chmod(0o555 if path.is_dir() else 0o444)
    root.chmod(0o555)
    return root, archive, policy, key


def deployment_manifest(companion: dict[str, str], runtime: dict[str, str]) -> dict[str, Any]:
    rows = [
        ("companion", "Apache.Arrow.dll", "managed_dependency"),
        ("companion", "Microsoft.Data.SqlClient.dll", "managed_dependency"),
        ("companion", "Worker.deps.json", "worker_deps"),
        ("companion", "Worker.dll", "worker_assembly"),
        ("companion", "Worker.runtimeconfig.json", "worker_runtimeconfig"),
        ("dotnet", "dotnet", "dotnet_host"),
        ("dotnet", "shared/Microsoft.NETCore.App/8.0.31/libcoreclr.so", "runtime_library"),
    ]
    return {
        "schema_version": 1,
        "backend": "mssql_sqlclient",
        "platform": "linux_arm64",
        "runtime_version": "8.0.31",
        "sqlclient_version": "7.0.2",
        "arrow_version": "23.0.0",
        "files": [
            {
                "origin": origin,
                "path": path,
                "role": role,
                "sha256": companion[path] if origin == "companion" else runtime[path],
            }
            for origin, path, role in rows
        ],
    }


def deterministic_tar(root: Path, target: Path) -> None:
    with tarfile.open(target, "w", format=tarfile.PAX_FORMAT) as archive:
        for path in [root, *sorted(root.rglob("*"))]:
            name = (Path(root.name) / path.relative_to(root)).as_posix()
            info = archive.gettarinfo(str(path), arcname=name)
            info.uid = info.gid = info.mtime = 0
            info.uname = info.gname = ""
            info.mode = 0o555 if path.is_dir() else 0o444
            if path.is_file():
                with path.open("rb") as stream:
                    archive.addfile(info, stream)
            else:
                archive.addfile(info)


def file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
