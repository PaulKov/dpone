from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.adapters.cosign_route_attestation import (
    CosignRouteAttestationSignatureVerifier,
    SubprocessRouteAttestationCommandRunner,
)
from dpone.contracts.route_attestation import CosignVerificationPolicy, RouteAttestationCommandResult


@dataclass
class _Runner:
    version: str = "3.0.4"
    verify_returncode: int = 0

    def __post_init__(self) -> None:
        self.calls: list[tuple[tuple[str, ...], int]] = []
        self.verified_files: dict[str, bytes] = {}

    def run(self, args: tuple[str, ...], *, timeout_seconds: int) -> RouteAttestationCommandResult:
        self.calls.append((args, timeout_seconds))
        if args[1:] == ("version", "--json"):
            return RouteAttestationCommandResult(0, f'{{"gitVersion":"v{self.version}"}}', "")
        bundle = Path(args[args.index("--bundle") + 1])
        trusted_root = Path(args[args.index("--trusted-root") + 1])
        blob = Path(args[-1])
        self.verified_files = {
            "bundle": bundle.read_bytes(),
            "trusted_root": trusted_root.read_bytes(),
            "blob": blob.read_bytes(),
        }
        return RouteAttestationCommandResult(self.verify_returncode, "verified", "secret=must-not-leak")


def _policy() -> CosignVerificationPolicy:
    return CosignVerificationPolicy(
        certificate_identity="https://github.com/PaulKov/dpone/.github/workflows/route-attestation.yml@refs/heads/master",
        certificate_oidc_issuer="https://token.actions.githubusercontent.com",
        minimum_version="3.0.4",
        maximum_version_exclusive="4.0.0",
        timeout_seconds=30,
    )


def test_cosign_adapter_uses_private_byte_copies_and_exact_identity() -> None:
    runner = _Runner()
    result = CosignRouteAttestationSignatureVerifier(runner=runner).verify_blob(
        blob=b'{"schema":"dpone.route-attestation.v1"}\n',
        sigstore_bundle=b'{"mediaType":"application/vnd.dev.sigstore.bundle.v0.3+json"}',
        trusted_root=b'{"mediaType":"application/vnd.dev.sigstore.trustedroot+json"}',
        policy=_policy(),
    )

    assert result.status == "verified"
    assert result.verifier_version == "3.0.4"
    args = runner.calls[1][0]
    assert args[:2] == ("cosign", "verify-blob")
    assert "--certificate-identity-regexp" not in args
    assert args[args.index("--certificate-identity") + 1] == _policy().certificate_identity
    assert args[args.index("--certificate-oidc-issuer") + 1] == _policy().certificate_oidc_issuer
    assert runner.verified_files["blob"].startswith(b'{"schema"')
    assert runner.verified_files["bundle"].startswith(b'{"mediaType"')
    assert runner.verified_files["trusted_root"].startswith(b'{"mediaType"')


def test_cosign_adapter_creates_verification_files_atomically_with_private_permissions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_open = os.open
    created: list[tuple[str, int, int]] = []

    def tracked_open(
        path: os.PathLike[str] | str,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        name = Path(path).name
        if name in {"attestation.json", "attestation.sigstore.json", "trusted-root.json"}:
            created.append((name, flags, mode))
        return real_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(os, "open", tracked_open)
    result = CosignRouteAttestationSignatureVerifier(runner=_Runner()).verify_blob(
        blob=b"{}",
        sigstore_bundle=b"{}",
        trusted_root=b"{}",
        policy=_policy(),
    )

    assert result.status == "verified"
    assert {name for name, _, _ in created} == {
        "attestation.json",
        "attestation.sigstore.json",
        "trusted-root.json",
    }
    assert all(flags & os.O_EXCL for _, flags, _ in created)
    assert all(mode == 0o600 for _, _, mode in created)


def test_cosign_adapter_fails_closed_for_unsupported_version() -> None:
    runner = _Runner(version="3.0.3")
    result = CosignRouteAttestationSignatureVerifier(runner=runner).verify_blob(
        blob=b"{}",
        sigstore_bundle=b"{}",
        trusted_root=b"{}",
        policy=_policy(),
    )

    assert result.status == "unverified"
    assert result.code == "DPONE_ROUTE_ATTESTATION_VERIFIER_VERSION_UNSUPPORTED"
    assert len(runner.calls) == 1


def test_cosign_adapter_does_not_expose_verifier_output_on_invalid_signature() -> None:
    result = CosignRouteAttestationSignatureVerifier(runner=_Runner(verify_returncode=1)).verify_blob(
        blob=b"{}",
        sigstore_bundle=b"{}",
        trusted_root=b"{}",
        policy=_policy(),
    )

    assert result.status == "invalid"
    assert result.code == "DPONE_ROUTE_ATTESTATION_SIGNATURE_INVALID"
    assert "secret" not in result.message


def test_subprocess_runner_bounds_output_and_uses_sanitized_noninteractive_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    def fake_run(args: tuple[str, ...], **kwargs: object) -> SimpleNamespace:
        observed.update(kwargs)
        stdout = kwargs["stdout"]
        stderr = kwargs["stderr"]
        stdout.write(b"x" * (128 * 1024))
        stderr.write(b"y" * (128 * 1024))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)
    result = SubprocessRouteAttestationCommandRunner().run(("cosign", "version", "--json"), timeout_seconds=3)

    assert len(result.stdout.encode()) == 64 * 1024
    assert len(result.stderr.encode()) == 64 * 1024
    assert observed["stdin"] is subprocess.DEVNULL
    assert observed["env"] == {
        "HOME": "/var/empty",
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
    }
