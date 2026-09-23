"""Fail-closed admission of an exact externally signed companion release.

The process, imported dpone code and composition root are trusted. External
files, archives, signatures, keys and executables are not. This boundary does
not claim resistance to arbitrary code execution inside the trusted process.
"""

from __future__ import annotations

import importlib.metadata
import json
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.adapters.mssql_sqlclient_companion_trust_files import (
    ERROR,
    RELATIVE,
    assert_snapshot,
    file_digest,
    inventory,
    inventory_digest,
    read_root_file,
    root_snapshot,
    secure_executable_bytes,
    strict_object,
    validate_archive_bound,
)
from dpone.adapters.mssql_sqlclient_installation import AdmittedSqlClientInstallation, validate_manifest
from dpone.contracts.mssql_sqlclient_companion_trust import (
    SqlClientCompanionTrustPolicy,
    SqlClientCompanionVerificationReceipt,
    canonical_bytes,
    sha256_bytes,
)

_CAPABILITY_TOKEN = object()


class AdmittedSqlClientCompanion:
    """Composition-root capability created after direct verification.

    The dpone process is trusted. This object is not a Python sandbox boundary.
    """

    __slots__ = ("_root", "_archive", "_inventory", "_receipt", "_signature", "_bundle", "_key", "_policy")

    def __init__(
        self,
        *,
        root: Path,
        archive: Path,
        inventory_sha256: str,
        receipt: SqlClientCompanionVerificationReceipt,
        signature: bytes,
        bundle: bytes,
        key: bytes,
        policy: SqlClientCompanionTrustPolicy,
        _token: object,
    ) -> None:
        if _token is not _CAPABILITY_TOKEN or receipt.status != "VERIFIED":
            raise ValueError(ERROR)
        self._root, self._archive, self._inventory = root, archive, inventory_sha256
        self._receipt, self._signature, self._bundle, self._key, self._policy = receipt, signature, bundle, key, policy

    @property
    def receipt_digest(self) -> str:
        return self._receipt.digest

    def assert_current(self, *, environment: str, require_production: bool) -> None:
        """Re-verify external authority and exact installed bytes before use."""
        snapshot = root_snapshot(self._root, production=self._receipt.trust_tier == "production")
        if (
            environment != self._receipt.environment
            or (require_production and self._receipt.trust_tier != "production")
            or inventory_digest(self._root) != self._inventory
        ):
            raise ValueError(ERROR)
        validate_archive_bound(self._archive, self._root, self._receipt.archive_sha256)
        _verify_cosign(self._signature, self._bundle, self._key, self._policy)
        assert_snapshot(snapshot)

    def bind_installation(
        self, installation: AdmittedSqlClientInstallation, *, deadline_ns: int
    ) -> AdmittedSqlClientInstallation:
        """Return the exact launch installation only after both admissions agree."""
        self.assert_current(
            environment=self._receipt.environment, require_production=self._receipt.trust_tier == "production"
        )
        if (
            installation.companion_root != self._root / "companion"
            or installation.runtime_root != self._root / "runtime"
            or installation.build_manifest != self._root / "deployment.json"
            or installation.build_sha256 != self._receipt.deployment_build_sha256
        ):
            raise ValueError(ERROR)
        installation.assert_admitted(deadline_ns=deadline_ns)
        return installation


def admit_sqlclient_companion(
    *,
    release_root: Path,
    archive: Path,
    policy_bytes: bytes,
    public_key: bytes,
    sigstore_bundle: bytes,
) -> tuple[AdmittedSqlClientCompanion, SqlClientCompanionVerificationReceipt]:
    """Preflight exact content and invoke the sealed production verifier."""
    policy = SqlClientCompanionTrustPolicy.from_bytes(policy_bytes)
    policy.assert_authorized()
    if sha256_bytes(public_key) != policy.public_key_sha256 or not public_key or len(public_key) > 1024**2:
        raise ValueError(ERROR)
    if not sigstore_bundle or len(sigstore_bundle) > 4 * 1024**2:
        raise ValueError(ERROR)
    snapshot = root_snapshot(release_root, production=policy.trust_tier == "production")
    manifest_bytes = read_root_file(release_root, "release-manifest.json", 2 * 1024**2)
    signature_bytes = read_root_file(release_root, "signature-input.json", 65536)
    manifest = _validate_release_tree(release_root, manifest_bytes, signature_bytes, policy)
    inventory_before = inventory_digest(release_root)
    if _dpone_distribution_identity() != (policy.dpone_exact_version, policy.dpone_distribution_sha256):
        raise ValueError(ERROR)
    validate_archive_bound(archive, release_root, policy.archive_sha256)
    version = _verify_cosign(signature_bytes, sigstore_bundle, public_key, policy)
    inventory_after = inventory_digest(release_root)
    assert_snapshot(snapshot)
    if inventory_after != inventory_before:
        raise ValueError(ERROR)
    receipt = SqlClientCompanionVerificationReceipt(
        trust_tier=policy.trust_tier,
        environment=policy.environment,
        key_id=policy.key_id,
        public_key_sha256=policy.public_key_sha256,
        policy_sha256=policy.digest,
        archive_sha256=policy.archive_sha256,
        sigstore_bundle_sha256=sha256_bytes(sigstore_bundle),
        signature_input_sha256=sha256_bytes(signature_bytes),
        release_manifest_sha256=sha256_bytes(manifest_bytes),
        tree_inventory_sha256=inventory_after,
        deployment_build_sha256=manifest["deployment_build_sha256"],
        source_identity=policy.source_identity,
        release_identity=policy.release_identity,
        verifier_identity="cosign_public_key_v1",
        verifier_version=version,
    )
    capability = AdmittedSqlClientCompanion(
        root=release_root,
        archive=archive,
        inventory_sha256=inventory_after,
        receipt=receipt,
        signature=signature_bytes,
        bundle=sigstore_bundle,
        key=public_key,
        policy=policy,
        _token=_CAPABILITY_TOKEN,
    )
    return capability, receipt


def _validate_release_tree(
    root: Path,
    manifest_bytes: bytes,
    signature_bytes: bytes,
    policy: SqlClientCompanionTrustPolicy,
) -> dict[str, Any]:
    manifest = strict_object(manifest_bytes)
    fields = {
        "schema_version",
        "artifact",
        "version",
        "platform",
        "dpone_compatibility",
        "deployment_build_sha256",
        "files",
        "control_files",
    }
    if (
        set(manifest) != fields
        or manifest["schema_version"] != policy.companion_schema
        or manifest["artifact"] != "dpone-mssql-sqlclient"
        or manifest["version"] != policy.companion_version
        or manifest["platform"] != policy.platform
        or manifest["dpone_compatibility"] != policy.dpone_compatibility
        or manifest["deployment_build_sha256"] != policy.deployment_build_sha256
        or manifest["control_files"] != ["release-manifest.json", "signature-input.json"]
        or sha256_bytes(manifest_bytes) != policy.release_manifest_sha256
    ):
        raise ValueError(ERROR)
    expected = _manifest_files(manifest["files"])
    actual = inventory(root)
    if set(actual) != set(expected) | {"release-manifest.json", "signature-input.json"} or any(
        actual[name] != digest for name, digest in expected.items()
    ):
        raise ValueError(ERROR)
    signature = strict_object(signature_bytes)
    if signature_bytes != canonical_bytes(signature) or signature != {
        "schema_version": "dpone.mssql-sqlclient.signature-input.v1",
        "signature": None,
        "subject": "release-manifest",
        "subject_sha256": policy.release_manifest_sha256,
    }:
        raise ValueError(ERROR)
    if expected.get("metadata/provenance.json") != policy.source_identity:
        raise ValueError(ERROR)
    deployment = read_root_file(root, "deployment.json", 2 * 1024**2)
    deployment_value = validate_manifest(deployment, policy.deployment_build_sha256)
    _validate_deployment_inventory(root, expected, deployment_value)
    if policy.release_identity != f"sha256:{policy.release_manifest_sha256}":
        raise ValueError(ERROR)
    return manifest


def _validate_deployment_inventory(root: Path, expected: dict[str, str], deployment: dict[str, Any]) -> None:
    declared = {
        f"{'companion' if row['origin'] == 'companion' else 'runtime'}/{row['path']}": row["sha256"]
        for row in deployment["files"]
    }
    actual = {name: digest for name, digest in expected.items() if name.startswith(("companion/", "runtime/"))}
    if (
        not declared
        or actual != declared
        or inventory(root / "companion")
        != {
            name.removeprefix("companion/"): digest
            for name, digest in declared.items()
            if name.startswith("companion/")
        }
        or inventory(root / "runtime")
        != {name.removeprefix("runtime/"): digest for name, digest in declared.items() if name.startswith("runtime/")}
    ):
        raise ValueError(ERROR)


def _manifest_files(value: object) -> dict[str, str]:
    if type(value) is not list or not value:
        raise ValueError(ERROR)
    result: dict[str, str] = {}
    for row in value:
        if type(row) is not dict or set(row) != {"path", "sha256"}:
            raise ValueError(ERROR)
        name, digest = row["path"], row["sha256"]
        if (
            type(name) is not str
            or RELATIVE.fullmatch(name) is None
            or type(digest) is not str
            or re.fullmatch(r"[0-9a-f]{64}", digest) is None
            or name in result
        ):
            raise ValueError(ERROR)
        result[name] = digest
    return result


def _verify_cosign(blob: bytes, bundle: bytes, key: bytes, policy: SqlClientCompanionTrustPolicy) -> str:
    executable = shutil.which("cosign", path="/usr/local/bin:/usr/bin:/bin")
    if executable is None:
        raise ValueError(ERROR)
    executable_path = Path(executable).resolve(strict=True)
    executable_bytes = secure_executable_bytes(executable_path, policy.cosign_executable_sha256)
    try:
        with tempfile.TemporaryDirectory(prefix="dpone-companion-trust-") as directory:
            root = Path(directory)
            private_executable = root / "cosign"
            _write_private(private_executable, executable_bytes)
            private_executable.chmod(0o500)
            paths = []
            for name, data in (("subject.json", blob), ("bundle.json", bundle), ("cosign.pub", key)):
                path = root / name
                _write_private(path, data)
                paths.append(path)
            version_result = _run((str(private_executable), "version", "--json"), policy.cosign.timeout_seconds)
            if version_result.returncode != 0:
                raise ValueError(ERROR)
            version = _cosign_version(version_result.stdout)
            if version != policy.cosign_exact_version:
                raise ValueError(ERROR)
            result = _run(
                (
                    str(private_executable),
                    "verify-blob",
                    "--private-infrastructure",
                    "--bundle",
                    str(paths[1]),
                    "--key",
                    str(paths[2]),
                    "--timeout",
                    f"{policy.cosign.timeout_seconds}s",
                    str(paths[0]),
                ),
                policy.cosign.timeout_seconds,
            )
            if file_digest(private_executable, maximum=256 * 1024**2) != policy.cosign_executable_sha256:
                raise ValueError(ERROR)
    except (FileNotFoundError, PermissionError, subprocess.TimeoutExpired, OSError):
        raise ValueError(ERROR) from None
    if result.returncode != 0:
        raise ValueError(ERROR)
    return version


@dataclass(frozen=True, slots=True)
class _CommandResult:
    returncode: int
    stdout: str


def _run(args: tuple[str, ...], timeout: int) -> _CommandResult:
    with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
        result = subprocess.run(
            args,
            check=False,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            timeout=timeout,
            env={"HOME": "/var/empty", "PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"},
        )
        stdout.seek(0)
        raw = stdout.read(65537)
        if len(raw) > 65536:
            raise ValueError(ERROR)
        return _CommandResult(result.returncode, raw.decode(errors="replace"))


def _write_private(path: Path, data: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        pending = memoryview(data)
        while pending:
            written = os.write(descriptor, pending)
            if written <= 0:
                raise OSError
            pending = pending[written:]
    finally:
        os.close(descriptor)


def _cosign_version(raw: str) -> str:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        raise ValueError(ERROR) from None
    version = value.get("gitVersion") or value.get("version") if type(value) is dict else None
    if type(version) is not str:
        raise ValueError(ERROR)
    return version.removeprefix("v")


def _dpone_distribution_identity() -> tuple[str, str]:
    distribution = importlib.metadata.distribution("dpone")
    files = distribution.files
    if files is None:
        raise ValueError(ERROR)
    records = [item for item in files if str(item).endswith(".dist-info/RECORD")]
    if len(records) != 1:
        raise ValueError(ERROR)
    record = Path(str(distribution.locate_file(records[0]))).resolve(strict=True)
    return distribution.version, file_digest(record, maximum=16 * 1024**2)


__all__ = ["AdmittedSqlClientCompanion", "admit_sqlclient_companion"]
