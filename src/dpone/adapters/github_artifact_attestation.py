"""Offline GitHub Artifact Attestation verifier with bounded subprocess I/O."""

from __future__ import annotations

import hashlib
import json
import os
import re
import selectors
import stat
import subprocess
import tempfile
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from dpone.contracts.runtime_artifact_attestation import (
    MAX_ATTESTATION_BUNDLE_BYTES,
    GitHubArtifactAttestationPolicy,
    RuntimeArtifactAttestationError,
    github_cli_version_supported,
)

MAX_ATTESTATION_SUBJECT_BYTES = 8 * 1024 * 1024
MAX_ATTESTATION_OUTPUT_BYTES = 128 * 1024
_GH_VERSION_PATTERN = re.compile(r"^gh version (\d+\.\d+\.\d+)(?:\s|$)")


class GitHubArtifactAttestationError(RuntimeArtifactAttestationError):
    """Stable redacted verifier failure."""


@dataclass(frozen=True, slots=True)
class GitHubCommandResult:
    returncode: int
    stdout: bytes
    stderr: bytes


@dataclass(frozen=True, slots=True)
class GitHubAttestationVerification:
    subject_sha256: str
    verifier_version: str
    verified_attestations: int


class GitHubCommandRunner(Protocol):
    def run(self, args: tuple[str, ...], *, timeout_seconds: int) -> GitHubCommandResult: ...


class SubprocessGitHubCommandRunner:
    """Execute fixed GitHub CLI argv without shell or credential inheritance."""

    def run(self, args: tuple[str, ...], *, timeout_seconds: int) -> GitHubCommandResult:
        process = subprocess.Popen(  # noqa: S603 - fixed argv is supplied by the verifier
            args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=_sanitized_environment(),
        )
        stdout, stderr = _communicate_bounded(
            process,
            args=args,
            timeout_seconds=timeout_seconds,
        )
        return GitHubCommandResult(process.returncode, stdout, stderr)


class GitHubArtifactAttestationVerifier:
    """Verify exact local subject bytes with one portable offline bundle."""

    def __init__(
        self,
        *,
        runner: GitHubCommandRunner | None = None,
        executable: str = "gh",
    ) -> None:
        self._runner = runner or SubprocessGitHubCommandRunner()
        self._executable = executable

    def verify_files(
        self,
        *,
        subject_path: Path,
        bundle_path: Path,
        policy: GitHubArtifactAttestationPolicy,
        expected_subject_sha256: str | None = None,
    ) -> GitHubAttestationVerification:
        version = self._verified_version(policy)
        subject = _read_bounded_regular_file(
            subject_path,
            max_bytes=MAX_ATTESTATION_SUBJECT_BYTES,
            code="DPONE_ARTIFACT_ATTESTATION_SUBJECT_INVALID",
            label="attestation subject",
        )
        subject_sha256 = "sha256:" + hashlib.sha256(subject).hexdigest()
        if expected_subject_sha256 is not None and subject_sha256 != expected_subject_sha256:
            raise GitHubArtifactAttestationError(
                "DPONE_ARTIFACT_ATTESTATION_SUBJECT_MISMATCH",
                "runtime attestation subject checksum does not match the pinned release",
            )
        bundle = _read_bounded_regular_file(
            bundle_path,
            max_bytes=MAX_ATTESTATION_BUNDLE_BYTES,
            code="DPONE_ARTIFACT_ATTESTATION_BUNDLE_INVALID",
            label="attestation bundle",
        )
        with tempfile.TemporaryDirectory(prefix="dpone-github-attestation-") as directory:
            root = Path(directory)
            subject_copy = _private_file(root / "release-set.json", subject)
            bundle_copy = _private_file(root / "github.sigstore.jsonl", bundle)
            trusted_root = _private_file(root / "trusted-root.jsonl", policy.trusted_root)
            result = self._run(
                (
                    self._executable,
                    "attestation",
                    "verify",
                    str(subject_copy),
                    "--repo",
                    policy.repository,
                    "--bundle",
                    str(bundle_copy),
                    "--custom-trusted-root",
                    str(trusted_root),
                    "--signer-workflow",
                    policy.signer_workflow,
                    "--signer-digest",
                    policy.signer_digest,
                    "--predicate-type",
                    policy.predicate_type,
                    "--cert-oidc-issuer",
                    policy.cert_oidc_issuer,
                    "--deny-self-hosted-runners",
                    "--format",
                    "json",
                ),
                timeout_seconds=policy.gh.timeout_seconds,
            )
        if result.returncode != 0:
            raise GitHubArtifactAttestationError(
                "DPONE_ARTIFACT_ATTESTATION_VERIFICATION_FAILED",
                "runtime artifact attestation signature or signer policy is invalid",
            )
        count = _verified_result_count(
            result.stdout,
            expected_subject_sha256=subject_sha256,
            expected_predicate_type=policy.predicate_type,
        )
        return GitHubAttestationVerification(
            subject_sha256=subject_sha256,
            verifier_version=version,
            verified_attestations=count,
        )

    def _verified_version(self, policy: GitHubArtifactAttestationPolicy) -> str:
        result = self._run(
            (self._executable, "version"),
            timeout_seconds=policy.gh.timeout_seconds,
        )
        if result.returncode != 0:
            raise GitHubArtifactAttestationError(
                "DPONE_ARTIFACT_ATTESTATION_VERIFIER_UNAVAILABLE",
                "GitHub CLI attestation verifier is unavailable",
            )
        first_line = result.stdout.decode("utf-8", errors="replace").splitlines()[:1]
        match = _GH_VERSION_PATTERN.match(first_line[0]) if first_line else None
        version = match.group(1) if match is not None else ""
        if not github_cli_version_supported(version, policy.gh):
            raise GitHubArtifactAttestationError(
                "DPONE_ARTIFACT_ATTESTATION_VERIFIER_VERSION_UNSUPPORTED",
                "GitHub CLI attestation verifier version is outside the pinned policy",
            )
        return version

    def _run(self, args: tuple[str, ...], *, timeout_seconds: int) -> GitHubCommandResult:
        try:
            return self._runner.run(args, timeout_seconds=timeout_seconds)
        except (FileNotFoundError, PermissionError, subprocess.TimeoutExpired, OSError) as exc:
            raise GitHubArtifactAttestationError(
                "DPONE_ARTIFACT_ATTESTATION_VERIFIER_UNAVAILABLE",
                "GitHub CLI attestation verifier is unavailable",
            ) from exc


def _verified_result_count(
    payload: bytes,
    *,
    expected_subject_sha256: str,
    expected_predicate_type: str,
) -> int:
    try:
        decoded = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GitHubArtifactAttestationError(
            "DPONE_ARTIFACT_ATTESTATION_RESULT_INVALID",
            "GitHub CLI attestation result is not valid JSON",
        ) from exc
    if not isinstance(decoded, list) or not decoded:
        raise GitHubArtifactAttestationError(
            "DPONE_ARTIFACT_ATTESTATION_RESULT_INVALID",
            "GitHub CLI attestation result contains no verified attestations",
        )
    expected_hex = expected_subject_sha256.removeprefix("sha256:")
    for item in decoded:
        verification = _mapping(item, "verification result")
        result = _mapping(verification.get("verificationResult"), "verificationResult")
        statement = _mapping(result.get("statement"), "statement")
        if statement.get("predicateType") != expected_predicate_type:
            raise GitHubArtifactAttestationError(
                "DPONE_ARTIFACT_ATTESTATION_RESULT_INVALID",
                "verified attestation predicate does not match the pinned policy",
            )
        subjects = statement.get("subject")
        if not isinstance(subjects, list) or not subjects:
            raise _subject_mismatch()
        digests = {
            str(digest.get("sha256"))
            for subject in subjects
            if isinstance(subject, Mapping)
            for digest in [subject.get("digest")]
            if isinstance(digest, Mapping)
        }
        if expected_hex not in digests:
            raise _subject_mismatch()
    return len(decoded)


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise GitHubArtifactAttestationError(
            "DPONE_ARTIFACT_ATTESTATION_RESULT_INVALID",
            f"GitHub CLI {label} has an invalid shape",
        )
    return value


def _subject_mismatch() -> GitHubArtifactAttestationError:
    return GitHubArtifactAttestationError(
        "DPONE_ARTIFACT_ATTESTATION_SUBJECT_MISMATCH",
        "verified attestation does not bind the exact staged release-set",
    )


def _read_bounded_regular_file(
    path: Path,
    *,
    max_bytes: int,
    code: str,
    label: str,
) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise GitHubArtifactAttestationError(code, f"{label} is missing or unsafe") from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size <= 0:
            raise GitHubArtifactAttestationError(code, f"{label} is missing or unsafe")
        if metadata.st_size > max_bytes:
            raise GitHubArtifactAttestationError(code, f"{label} exceeds its byte limit")
        chunks: list[bytes] = []
        total = 0
        while total <= max_bytes:
            chunk = os.read(descriptor, min(64 * 1024, max_bytes + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
    finally:
        os.close(descriptor)
    if total > max_bytes:
        raise GitHubArtifactAttestationError(code, f"{label} exceeds its byte limit")
    payload = b"".join(chunks)
    if not payload:
        raise GitHubArtifactAttestationError(code, f"{label} is missing or unsafe")
    return payload


def _private_file(path: Path, data: bytes) -> Path:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        pending = memoryview(data)
        while pending:
            written = os.write(descriptor, pending)
            if written <= 0:
                raise OSError("private attestation file write failed")
            pending = pending[written:]
    except BaseException:
        path.unlink(missing_ok=True)
        raise
    finally:
        os.close(descriptor)
    return path


def _communicate_bounded(
    process: subprocess.Popen[bytes],
    *,
    args: tuple[str, ...],
    timeout_seconds: int,
) -> tuple[bytes, bytes]:
    streams = {"stdout": process.stdout, "stderr": process.stderr}
    if any(stream is None for stream in streams.values()):  # pragma: no cover - construction invariant
        process.kill()
        process.wait()
        raise OSError("attestation verifier pipes are unavailable")
    selector = selectors.DefaultSelector()
    buffers = {name: bytearray() for name in streams}
    try:
        for name, stream in streams.items():
            assert stream is not None
            selector.register(stream, selectors.EVENT_READ, name)
        deadline = time.monotonic() + timeout_seconds
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                _terminate_process(process)
                raise subprocess.TimeoutExpired(args, timeout_seconds)
            events = selector.select(min(remaining, 0.1))
            for key, _ in events:
                chunk = os.read(key.fd, 64 * 1024)
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                buffer = buffers[str(key.data)]
                buffer.extend(chunk)
                if len(buffer) > MAX_ATTESTATION_OUTPUT_BYTES:
                    _terminate_process(process)
                    raise OSError("attestation verifier output exceeds its byte limit")
        process.wait(timeout=max(0.1, deadline - time.monotonic()))
    except subprocess.TimeoutExpired:
        _terminate_process(process)
        raise
    finally:
        selector.close()
        for stream in streams.values():
            if stream is not None:
                stream.close()
    return bytes(buffers["stdout"]), bytes(buffers["stderr"])


def _terminate_process(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        process.kill()
    process.wait()


def _sanitized_environment() -> dict[str, str]:
    return {
        "HOME": "/var/empty",
        "GH_CONFIG_DIR": "/var/empty",
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
    }


__all__ = [
    "GitHubArtifactAttestationError",
    "GitHubArtifactAttestationVerifier",
    "GitHubAttestationVerification",
    "GitHubCommandResult",
    "SubprocessGitHubCommandRunner",
]
