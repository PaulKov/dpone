"""Bounded cosign adapter for exact local bytes and signer identity."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.contracts.blob_signature import CosignVerificationPolicy


import json
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import BinaryIO, Protocol

from dpone.contracts.blob_signature import BlobSignatureVerification, BlobVerificationCommandResult


class _BlobVerificationCommandRunner(Protocol):
    def run(self, args: tuple[str, ...], *, timeout_seconds: int) -> BlobVerificationCommandResult: ...


_SECURITY_MINIMUM = (3, 0, 4)
_VERSION_PATTERN = re.compile(r"^(?:v)?(\d+)\.(\d+)\.(\d+)(?:[-+].*)?$")
_MAX_SUBPROCESS_OUTPUT_BYTES = 64 * 1024


class SubprocessBlobVerificationCommandRunner:
    """Run a fixed command without inheriting secret-heavy environment values."""

    def run(self, args: tuple[str, ...], *, timeout_seconds: int) -> BlobVerificationCommandResult:
        with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
            completed = subprocess.run(
                args,
                check=False,
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                timeout=timeout_seconds,
                env=_sanitized_environment(),
            )
            return BlobVerificationCommandResult(
                returncode=completed.returncode,
                stdout=_bounded_output(stdout),
                stderr=_bounded_output(stderr),
            )


class CosignBlobSignatureVerifier:
    """Verify one blob with a local Sigstore bundle and pinned trusted root."""

    def __init__(
        self,
        *,
        runner: _BlobVerificationCommandRunner | None = None,
        executable: str = "cosign",
    ) -> None:
        self._runner = runner or SubprocessBlobVerificationCommandRunner()
        self._executable = executable

    def verify_blob(
        self,
        *,
        blob: bytes,
        sigstore_bundle: bytes,
        trusted_root: bytes,
        policy: CosignVerificationPolicy,
    ) -> BlobSignatureVerification:
        version = self.certified_version(policy)
        if isinstance(version, BlobSignatureVerification):
            return version
        if not _version_supported(version, policy):
            return BlobSignatureVerification.unverified("version_unsupported")
        try:
            with tempfile.TemporaryDirectory(prefix="dpone-blob-verification-") as directory:
                root = Path(directory)
                blob_path = write_private_verification_file(root / "attestation.json", blob)
                bundle_path = write_private_verification_file(root / "attestation.sigstore.json", sigstore_bundle)
                trusted_root_path = write_private_verification_file(root / "trusted-root.json", trusted_root)
                result = self._runner.run(
                    (
                        self._executable,
                        "verify-blob",
                        "--bundle",
                        str(bundle_path),
                        "--trusted-root",
                        str(trusted_root_path),
                        "--certificate-identity",
                        policy.certificate_identity,
                        "--certificate-oidc-issuer",
                        policy.certificate_oidc_issuer,
                        "--timeout",
                        f"{policy.timeout_seconds}s",
                        str(blob_path),
                    ),
                    timeout_seconds=policy.timeout_seconds,
                )
        except (FileNotFoundError, PermissionError, subprocess.TimeoutExpired, OSError):
            return BlobSignatureVerification.unverified("verifier_unavailable")
        if result.returncode != 0:
            return BlobSignatureVerification.invalid(version)
        return BlobSignatureVerification.verified(version)

    def certified_version(self, policy: CosignVerificationPolicy) -> str | BlobSignatureVerification:
        """Return a policy-bounded verifier version for another Cosign backend."""

        timeout_seconds = policy.timeout_seconds
        try:
            result = self._runner.run(
                (self._executable, "version", "--json"),
                timeout_seconds=timeout_seconds,
            )
        except (FileNotFoundError, PermissionError, subprocess.TimeoutExpired, OSError):
            return BlobSignatureVerification.unverified("verifier_unavailable")
        if result.returncode != 0:
            return BlobSignatureVerification.unverified("verifier_unavailable")
        version = _version_from_json(result.stdout)
        if version is None or not _version_supported(version, policy):
            return BlobSignatureVerification.unverified("version_unsupported")
        return version


def _version_from_json(raw: str) -> str | None:
    try:
        payload = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    value = payload.get("gitVersion") or payload.get("version")
    if not isinstance(value, str) or _version_tuple(value) is None:
        return None
    return value.removeprefix("v")


def _version_supported(version: str, policy: CosignVerificationPolicy) -> bool:
    actual = _version_tuple(version)
    requested_minimum = _version_tuple(policy.minimum_version)
    maximum = _version_tuple(policy.maximum_version_exclusive)
    if actual is None or requested_minimum is None or maximum is None:
        return False
    minimum = max(_SECURITY_MINIMUM, requested_minimum)
    return minimum <= actual < maximum and maximum <= (4, 0, 0)


def _version_tuple(value: str) -> tuple[int, int, int] | None:
    match = _VERSION_PATTERN.fullmatch(str(value).strip())
    if match is None:
        return None
    major, minor, patch = match.groups()
    return int(major), int(minor), int(patch)


def write_private_verification_file(path: Path, data: bytes) -> Path:
    """Write one verifier input with owner-only permissions and no overwrite."""

    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        pending = memoryview(data)
        while pending:
            written = os.write(descriptor, pending)
            if written <= 0:
                raise OSError("failed to write blob verification material")
            pending = pending[written:]
    except BaseException:
        path.unlink(missing_ok=True)
        raise
    finally:
        os.close(descriptor)
    return path


def _bounded_output(handle: BinaryIO) -> str:
    handle.seek(0)
    raw = handle.read(_MAX_SUBPROCESS_OUTPUT_BYTES + 1)
    return bytes(raw[:_MAX_SUBPROCESS_OUTPUT_BYTES]).decode("utf-8", errors="replace")


def _sanitized_environment() -> dict[str, str]:
    return {
        "HOME": "/var/empty",
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
    }


__all__ = [
    "CosignBlobSignatureVerifier",
    "SubprocessBlobVerificationCommandRunner",
    "write_private_verification_file",
]
