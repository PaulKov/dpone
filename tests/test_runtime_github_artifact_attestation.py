from __future__ import annotations

import base64
import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

import pytest

from dpone.adapters.github_artifact_attestation import (
    GitHubArtifactAttestationError,
    GitHubArtifactAttestationVerifier,
    GitHubCommandResult,
    SubprocessGitHubCommandRunner,
)
from dpone.adapters.runtime_github_artifact_attestation import (
    RegistryGitHubArtifactAttestationVerifier,
)
from dpone.contracts.runtime_artifact_attestation import (
    RuntimeArtifactAttestationError,
    RuntimeArtifactAttestationSubject,
    RuntimeArtifactTrustPolicyError,
    parse_runtime_artifact_trust_policy,
    runtime_attestation_bundle_key,
)
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.ports.artifact_registry import ArtifactMetadata
from dpone.readiness.airflow_artifact_attestation import (
    verify_runtime_artifact_attestation,
)
from dpone.readiness.airflow_runtime_init_fetch_config import (
    stock_attestation_verifier,
)
from dpone.runtime.airflow_artifact_attestation_inventory import (
    attestation_publication_spec,
)
from dpone.runtime.airflow_artifact_delivery_models import PublishRequest

_NOW = datetime(2026, 7, 27, tzinfo=UTC)
_RELEASE_ID = "sha256:" + "a" * 64
_SUBJECT = b'{"schema":"dpone.release-set.v2"}\n'
_SUBJECT_SHA = "sha256:" + hashlib.sha256(_SUBJECT).hexdigest()
_BUNDLE = b'{"mediaType":"application/vnd.dev.sigstore.bundle.v0.3+json"}\n'
_ROOT = b'{"rekor":"trusted"}\n'


class RecordingRunner:
    def __init__(self, *, verification: bytes | None = None, version: str = "2.93.0") -> None:
        self.calls: list[tuple[str, ...]] = []
        self._version = version
        self._verification = verification or _verification_output(_SUBJECT_SHA)

    def run(self, args: tuple[str, ...], *, timeout_seconds: int) -> GitHubCommandResult:
        assert timeout_seconds == 30
        self.calls.append(args)
        if args[1:] == ("version",):
            return GitHubCommandResult(0, f"gh version {self._version} (test)\n".encode(), b"")
        return GitHubCommandResult(0, self._verification, b"")


class InMemoryRegistry:
    def __init__(self, bundle: bytes = _BUNDLE) -> None:
        self.bundle = bundle
        self.keys: list[PurePosixPath] = []

    def stat(self, key: PurePosixPath) -> ArtifactMetadata:
        self.keys.append(key)
        return ArtifactMetadata(key=key, size_bytes=len(self.bundle))

    def download_file(self, key: PurePosixPath, destination: Path, *, max_bytes: int) -> None:
        assert key == self.keys[-1]
        assert len(self.bundle) <= max_bytes
        destination.write_bytes(self.bundle)


def test_policy_parser_binds_closed_offline_verifier_contract() -> None:
    parsed = parse_runtime_artifact_trust_policy(_policy(), now=_NOW)

    assert parsed.requires_attestation is True
    assert parsed.verifier is not None
    assert parsed.verifier.repository == "PaulKov/dpone"
    assert parsed.verifier.trusted_root == _ROOT
    assert parsed.verifier.gh.minimum_version == "2.93.0"


def test_policy_parser_rejects_expired_or_tampered_root() -> None:
    expired = _policy(refresh_after="2026-07-26T00:00:00Z")
    with pytest.raises(RuntimeArtifactTrustPolicyError, match="refresh is overdue") as exc_info:
        parse_runtime_artifact_trust_policy(expired, now=_NOW)
    assert exc_info.value.code == "DPONE_ARTIFACT_TRUST_POLICY_EXPIRED"

    tampered = _policy()
    tampered["verifier"]["trusted_root"]["content"] = base64.b64encode(b"other").decode()
    with pytest.raises(RuntimeArtifactTrustPolicyError, match="checksum"):
        parse_runtime_artifact_trust_policy(tampered, now=_NOW)


@pytest.mark.parametrize(
    ("generated_at", "refresh_after", "message"),
    (
        ("2026-07-27T00:06:00Z", "2026-08-01T00:00:00Z", "far in the future"),
        ("2026-07-01T00:00:00Z", "2026-10-01T00:00:01Z", "interval exceeds"),
    ),
)
def test_policy_parser_rejects_unbounded_trusted_root_freshness(
    generated_at: str,
    refresh_after: str,
    message: str,
) -> None:
    policy = _policy(refresh_after=refresh_after)
    policy["verifier"]["trusted_root"]["generated_at"] = generated_at

    with pytest.raises(RuntimeArtifactTrustPolicyError, match=message) as exc_info:
        parse_runtime_artifact_trust_policy(policy, now=_NOW)

    assert exc_info.value.code == "DPONE_ARTIFACT_TRUST_POLICY_INVALID"


def test_policy_parser_rejects_optional_attestations_for_production() -> None:
    policy = _policy()
    policy["attestations"] = "optional"

    with pytest.raises(RuntimeArtifactTrustPolicyError, match="must require attestations") as exc_info:
        parse_runtime_artifact_trust_policy(policy, now=_NOW)

    assert exc_info.value.code == "DPONE_ARTIFACT_TRUST_POLICY_INVALID"


def test_offline_verifier_uses_fixed_policy_argv_and_exact_subject(tmp_path: Path) -> None:
    subject = tmp_path / "release-set.json"
    bundle = tmp_path / "bundle.jsonl"
    subject.write_bytes(_SUBJECT)
    bundle.write_bytes(_BUNDLE)
    runner = RecordingRunner()
    policy = parse_runtime_artifact_trust_policy(_policy(), now=_NOW).verifier
    assert policy is not None

    result = GitHubArtifactAttestationVerifier(runner=runner).verify_files(
        subject_path=subject,
        bundle_path=bundle,
        policy=policy,
        expected_subject_sha256=_SUBJECT_SHA,
    )

    assert result.subject_sha256 == _SUBJECT_SHA
    assert result.verified_attestations == 1
    verify = runner.calls[1]
    assert verify[:3] == ("gh", "attestation", "verify")
    assert _flag(verify, "--repo") == "PaulKov/dpone"
    assert _flag(verify, "--signer-workflow").endswith("/dbt-self-service-dev.yml")
    assert _flag(verify, "--signer-digest") == "1" * 40
    assert "--deny-self-hosted-runners" in verify
    assert "--bundle" in verify
    assert "--custom-trusted-root" in verify


def test_offline_verifier_rejects_foreign_subject_and_unsupported_cli(tmp_path: Path) -> None:
    subject = tmp_path / "release-set.json"
    bundle = tmp_path / "bundle.jsonl"
    subject.write_bytes(_SUBJECT)
    bundle.write_bytes(_BUNDLE)
    policy = parse_runtime_artifact_trust_policy(_policy(), now=_NOW).verifier
    assert policy is not None

    with pytest.raises(GitHubArtifactAttestationError) as mismatch:
        GitHubArtifactAttestationVerifier(
            runner=RecordingRunner(
                verification=_verification_output("sha256:" + "b" * 64),
            )
        ).verify_files(subject_path=subject, bundle_path=bundle, policy=policy)
    assert mismatch.value.code == "DPONE_ARTIFACT_ATTESTATION_SUBJECT_MISMATCH"

    with pytest.raises(GitHubArtifactAttestationError) as unsupported:
        GitHubArtifactAttestationVerifier(runner=RecordingRunner(version="2.92.1")).verify_files(
            subject_path=subject,
            bundle_path=bundle,
            policy=policy,
        )
    assert unsupported.value.code == "DPONE_ARTIFACT_ATTESTATION_VERIFIER_VERSION_UNSUPPORTED"


def test_runtime_bridge_fetches_only_digest_derived_bundle(tmp_path: Path) -> None:
    subject = tmp_path / "release-set.json"
    subject.write_bytes(_SUBJECT)
    registry = InMemoryRegistry()
    runner = RecordingRunner()
    policy = parse_runtime_artifact_trust_policy(_policy(), now=_NOW).verifier
    assert policy is not None

    RegistryGitHubArtifactAttestationVerifier(
        registry=registry,
        policy=policy,
        verifier=GitHubArtifactAttestationVerifier(runner=runner),
    ).verify(
        subject=RuntimeArtifactAttestationSubject(
            release_id=_RELEASE_ID,
            subject_path=subject.absolute(),
            subject_sha256=_SUBJECT_SHA,
        )
    )

    assert registry.keys == [
        runtime_attestation_bundle_key(
            release_id=_RELEASE_ID,
            release_set_sha256=_SUBJECT_SHA,
        )
    ]


def test_stock_runtime_composition_builds_v2_registry_verifier() -> None:
    policy = parse_runtime_artifact_trust_policy(_policy(), now=_NOW)
    registry = InMemoryRegistry()

    verifier = stock_attestation_verifier(registry, policy)

    assert isinstance(verifier, RegistryGitHubArtifactAttestationVerifier)


def test_runtime_bridge_preserves_stable_verifier_error(tmp_path: Path) -> None:
    subject = tmp_path / "release-set.json"
    subject.write_bytes(_SUBJECT)
    policy = parse_runtime_artifact_trust_policy(_policy(), now=_NOW).verifier
    assert policy is not None
    bad = GitHubArtifactAttestationVerifier(runner=RecordingRunner(version="2.92.1"))

    with pytest.raises(RuntimeArtifactAttestationError) as exc_info:
        RegistryGitHubArtifactAttestationVerifier(
            registry=InMemoryRegistry(),
            policy=policy,
            verifier=bad,
        ).verify(
            subject=RuntimeArtifactAttestationSubject(
                release_id=_RELEASE_ID,
                subject_path=subject.absolute(),
                subject_sha256=_SUBJECT_SHA,
            )
        )
    assert exc_info.value.code == "DPONE_ARTIFACT_ATTESTATION_VERIFIER_VERSION_UNSUPPORTED"


def test_production_core_publication_is_authority_neutral_without_bundle(
    tmp_path: Path,
) -> None:
    request = PublishRequest(
        cache_root=tmp_path / "cache",
        release_id=_RELEASE_ID,
        deployment_id="sha256:" + "b" * 64,
        environment="prod",
        artifact_registry_ref="dpone-prod-artifacts",
    )

    assert (
        attestation_publication_spec(
            request,
            release_schema="dpone.release-set.v2",
            release_set_sha256=_SUBJECT_SHA,
        )
        is None
    )


def test_ci_preflight_uses_same_policy_and_returns_structured_result(tmp_path: Path) -> None:
    subject = tmp_path / "release-set.json"
    bundle = tmp_path / "bundle.jsonl"
    policy_path = tmp_path / "policy.json"
    subject.write_bytes(_SUBJECT)
    bundle.write_bytes(_BUNDLE)
    policy_bytes = (json.dumps(_policy(), sort_keys=True) + "\n").encode()
    policy_path.write_bytes(policy_bytes)

    result = verify_runtime_artifact_attestation(
        subject_path=str(subject),
        bundle_path=str(bundle),
        trust_policy_path=str(policy_path),
        expected_trust_policy_sha256="sha256:" + hashlib.sha256(policy_bytes).hexdigest(),
        expected_trust_tier="production",
        expected_subject_sha256=_SUBJECT_SHA,
        verifier=GitHubArtifactAttestationVerifier(runner=RecordingRunner()),
        now=_NOW,
    )

    assert result.passed is True
    assert result.details["status"] == "passed"
    assert result.details["subject_sha256"] == _SUBJECT_SHA
    assert result.details["verifier_version"] == "2.93.0"
    assert (
        GitOpsSchemaValidator().validate(
            result.to_dict(),
            expected_kind="dpone.runtime-artifact-attestation-verification.v1",
        )
        == ()
    )


def test_ci_preflight_requires_pinned_digest_and_matching_trust_tier(tmp_path: Path) -> None:
    subject = tmp_path / "release-set.json"
    bundle = tmp_path / "bundle.jsonl"
    policy_path = tmp_path / "policy.json"
    subject.write_bytes(_SUBJECT)
    bundle.write_bytes(_BUNDLE)
    policy = _policy()
    policy["trust_tier"] = "non_production"
    policy["attestations"] = "optional"
    policy_bytes = (json.dumps(policy, sort_keys=True) + "\n").encode()
    policy_path.write_bytes(policy_bytes)

    missing_digest = verify_runtime_artifact_attestation(
        subject_path=str(subject),
        bundle_path=str(bundle),
        trust_policy_path=str(policy_path),
        expected_trust_policy_sha256=None,
        expected_trust_tier="non_production",
        expected_subject_sha256=_SUBJECT_SHA,
        verifier=GitHubArtifactAttestationVerifier(runner=RecordingRunner()),
        now=_NOW,
    )
    tier_mismatch = verify_runtime_artifact_attestation(
        subject_path=str(subject),
        bundle_path=str(bundle),
        trust_policy_path=str(policy_path),
        expected_trust_policy_sha256="sha256:" + hashlib.sha256(policy_bytes).hexdigest(),
        expected_trust_tier="production",
        expected_subject_sha256=_SUBJECT_SHA,
        verifier=GitHubArtifactAttestationVerifier(runner=RecordingRunner()),
        now=_NOW,
    )

    assert missing_digest.passed is False
    assert missing_digest.errors[0]["code"] == "DPONE_ARTIFACT_TRUST_POLICY_MISMATCH"
    assert missing_digest.errors[0]["fixes"]
    assert missing_digest.errors[0]["docs_url"].endswith("DPONE_ARTIFACT_TRUST_POLICY_MISMATCH.md")
    assert (
        GitOpsSchemaValidator().validate(
            missing_digest.to_dict(),
            expected_kind="dpone.runtime-artifact-attestation-verification.v1",
        )
        == ()
    )
    assert tier_mismatch.passed is False
    assert tier_mismatch.errors[0]["code"] == "DPONE_ARTIFACT_TRUST_POLICY_MISMATCH"


@pytest.mark.parametrize("kind", ["empty", "directory", "fifo"])
def test_offline_verifier_rejects_non_regular_or_empty_subject(
    tmp_path: Path,
    kind: str,
) -> None:
    subject = tmp_path / "release-set.json"
    bundle = tmp_path / "bundle.jsonl"
    bundle.write_bytes(_BUNDLE)
    if kind == "directory":
        subject.mkdir()
    elif kind == "fifo":
        os.mkfifo(subject)
    else:
        subject.write_bytes(b"")
    policy = parse_runtime_artifact_trust_policy(_policy(), now=_NOW).verifier
    assert policy is not None

    with pytest.raises(GitHubArtifactAttestationError) as exc_info:
        GitHubArtifactAttestationVerifier(runner=RecordingRunner()).verify_files(
            subject_path=subject,
            bundle_path=bundle,
            policy=policy,
        )

    assert exc_info.value.code == "DPONE_ARTIFACT_ATTESTATION_SUBJECT_INVALID"


def test_offline_verifier_rejects_empty_bundle_before_gh_verification(tmp_path: Path) -> None:
    subject = tmp_path / "release-set.json"
    bundle = tmp_path / "bundle.jsonl"
    subject.write_bytes(_SUBJECT)
    bundle.write_bytes(b"")
    runner = RecordingRunner()
    policy = parse_runtime_artifact_trust_policy(_policy(), now=_NOW).verifier
    assert policy is not None

    with pytest.raises(GitHubArtifactAttestationError) as exc_info:
        GitHubArtifactAttestationVerifier(runner=runner).verify_files(
            subject_path=subject,
            bundle_path=bundle,
            policy=policy,
        )

    assert exc_info.value.code == "DPONE_ARTIFACT_ATTESTATION_BUNDLE_INVALID"
    assert len(runner.calls) == 1


@pytest.mark.parametrize("stream", ("stdout", "stderr"))
def test_subprocess_verifier_runner_fails_closed_on_oversized_output(
    stream: str,
) -> None:
    script = f"import sys; sys.{stream}.buffer.write(b'x' * (128 * 1024 + 1)); sys.{stream}.flush()"

    with pytest.raises(OSError, match="exceeds its byte limit"):
        SubprocessGitHubCommandRunner().run(
            (sys.executable, "-c", script),
            timeout_seconds=5,
        )


def test_subprocess_verifier_runner_enforces_timeout() -> None:
    with pytest.raises(subprocess.TimeoutExpired):
        SubprocessGitHubCommandRunner().run(
            (sys.executable, "-c", "import time; time.sleep(10)"),
            timeout_seconds=1,
        )


def test_ci_preflight_rejects_fifo_policy_without_blocking(tmp_path: Path) -> None:
    subject = tmp_path / "release-set.json"
    bundle = tmp_path / "bundle.jsonl"
    policy_path = tmp_path / "policy.json"
    subject.write_bytes(_SUBJECT)
    bundle.write_bytes(_BUNDLE)
    os.mkfifo(policy_path)

    result = verify_runtime_artifact_attestation(
        subject_path=str(subject),
        bundle_path=str(bundle),
        trust_policy_path=str(policy_path),
        expected_trust_policy_sha256="sha256:" + "1" * 64,
        expected_trust_tier="production",
        expected_subject_sha256=_SUBJECT_SHA,
        verifier=GitHubArtifactAttestationVerifier(runner=RecordingRunner()),
    )

    assert result.passed is False
    assert result.errors[0]["code"] == "DPONE_ARTIFACT_TRUST_POLICY_INVALID"


def _policy(*, refresh_after: str = "2026-08-27T00:00:00Z") -> dict:
    return {
        "schema": "dpone.runtime-artifact-trust-policy.v2",
        "trust_tier": "production",
        "attestations": "required_for_prod",
        "verifier": {
            "backend": "github_artifact_attestation_v1",
            "repository": "PaulKov/dpone",
            "signer_workflow": "PaulKov/dpone/.github/workflows/dbt-self-service-dev.yml",
            "signer_digest": "1" * 40,
            "predicate_type": "https://slsa.dev/provenance/v1",
            "cert_oidc_issuer": "https://token.actions.githubusercontent.com",
            "deny_self_hosted_runners": True,
            "trusted_root": {
                "encoding": "base64",
                "content": base64.b64encode(_ROOT).decode(),
                "sha256": "sha256:" + hashlib.sha256(_ROOT).hexdigest(),
                "generated_at": "2026-07-01T00:00:00Z",
                "refresh_after": refresh_after,
            },
            "gh": {
                "minimum_version": "2.93.0",
                "maximum_version_exclusive": "3.0.0",
                "timeout_seconds": 30,
            },
        },
    }


def _verification_output(subject_sha256: str) -> bytes:
    return json.dumps(
        [
            {
                "attestation": {},
                "verificationResult": {
                    "signature": {"certificate": {}},
                    "verifiedTimestamps": [{}],
                    "statement": {
                        "subject": [
                            {
                                "name": "release-set.json",
                                "digest": {"sha256": subject_sha256.removeprefix("sha256:")},
                            }
                        ],
                        "predicateType": "https://slsa.dev/provenance/v1",
                        "predicate": {},
                    },
                },
            }
        ]
    ).encode()


def _flag(args: tuple[str, ...], name: str) -> str:
    return args[args.index(name) + 1]
