"""Existing GitHub CLI composition with command doubles, never genuine signatures."""

import json
import stat
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from dpone.adapters.github_artifact_attestation import (
    GitHubArtifactAttestationVerifier,
    GitHubAttestationVerification,
    GitHubCommandResult,
)
from dpone.adapters.nonproduction_github_signature import GitHubNonproductionGrantSignatureVerifier
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError
from dpone.runtime.nonproduction_authentication import NonproductionGrantAuthenticator
from tests.nonproduction_signature_helpers import (
    BUNDLE,
    NOW,
    REPOSITORY,
    ROOT_BYTES,
    WORKFLOW,
    WORKFLOW_COMMIT,
    TrustProvider,
    execution,
    execution_inputs,
    github_document,
    github_policy,
    sha256,
    trust,
)


class CommandDouble:
    def __init__(self, *, changed_digest=False, invalid=False):
        self.calls = []
        self.paths = []
        self.changed_digest, self.invalid = changed_digest, invalid

    def run(self, args, *, timeout_seconds):
        self.calls.append(args)
        assert timeout_seconds == 5
        if args[1:] == ("version",):
            return GitHubCommandResult(0, b"gh version 2.93.0 (double)\n", b"")
        subject = Path(args[3])
        bundle = Path(args[args.index("--bundle") + 1])
        root = Path(args[args.index("--custom-trusted-root") + 1])
        self.paths.extend((subject, bundle, root))
        assert subject.read_bytes() == execution(github_policy()).to_bytes()
        assert bundle.read_bytes() == BUNDLE and root.read_bytes() == ROOT_BYTES
        assert all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in self.paths)
        digest = sha256(b"foreign") if self.changed_digest else sha256(subject.read_bytes())
        output = [
            {
                "verificationResult": {
                    "statement": {
                        "predicateType": "https://slsa.dev/provenance/v1",
                        "subject": [{"digest": {"sha256": digest[7:]}}],
                    }
                }
            }
        ]
        return GitHubCommandResult(1 if self.invalid else 0, json.dumps(output).encode(), b"private diagnostic")


def adapter(command=None):
    return GitHubNonproductionGrantSignatureVerifier(
        verifier=GitHubArtifactAttestationVerifier(runner=command or CommandDouble())
    )


def verify(service, *, snapshot=None, raw=None, bundle=BUNDLE):
    return service.verify(
        grant_bytes=execution(github_policy()).to_bytes() if raw is None else raw,
        sigstore_bundle=bundle,
        trust=snapshot or trust(),
        now=NOW,
    )


def test_real_github_adapter_receives_exact_bytes_and_every_external_signer_pin():
    command = CommandDouble()
    result = verify(adapter(command))
    assert result == execution(github_policy()).signature_subject(github_policy())
    args = command.calls[-1]
    expected = {
        "--repo": REPOSITORY,
        "--signer-workflow": WORKFLOW,
        "--signer-digest": WORKFLOW_COMMIT,
        "--cert-oidc-issuer": "https://token.actions.githubusercontent.com",
        "--predicate-type": "https://slsa.dev/provenance/v1",
    }
    assert all(args[args.index(flag) + 1] == value for flag, value in expected.items())
    assert "--deny-self-hosted-runners" in args
    assert all(not path.exists() for path in command.paths)


@pytest.mark.parametrize("field", ["backend", "issuer", "identity", "trust_root_sha256", "repository"])
def test_external_np_signer_cannot_diverge_from_actual_github_policy(field):
    value = github_policy()
    if field == "repository":
        changed = replace(value, source_repository="https://github.com/other/repository")
    else:
        selected = {
            "backend": "cosign_public_key_v1",
            "issuer": "https://foreign.invalid",
            "identity": f"https://github.com/{WORKFLOW}@{'2' * 40}",
            "trust_root_sha256": sha256(b"foreign"),
        }[field]
        changed = replace(value, signer=replace(value.signer, **{field: selected}))
    command = CommandDouble()
    with pytest.raises(NonproductionAuthorityError):
        adapter(command).require_trust(trust=trust(changed), now=NOW)
    assert command.calls == []


@pytest.mark.parametrize(
    "field",
    [
        "signer_workflow",
        "signer_digest",
        "predicate_type",
        "cert_oidc_issuer",
        "deny_self_hosted_runners",
        "trusted_root",
        "gh",
    ],
)
def test_independent_github_policy_fields_are_reparsed_and_bound(field):
    document = github_document()
    document["verifier"][field] = {
        "signer_workflow": REPOSITORY + "/.github/workflows/foreign.yml",
        "signer_digest": "2" * 40,
        "predicate_type": "https://foreign.invalid/predicate",
        "cert_oidc_issuer": "https://foreign.invalid/issuer",
        "deny_self_hosted_runners": False,
        "trusted_root": {
            "encoding": "base64",
            "content": "eA==",
            "sha256": sha256(ROOT_BYTES),
            "generated_at": "2026-09-10T00:00:00Z",
            "refresh_after": "2026-09-11T00:00:00Z",
        },
        "gh": {"minimum_version": "2.92.0", "maximum_version_exclusive": "3.0.0", "timeout_seconds": 5},
    }[field]
    command = CommandDouble()
    with pytest.raises(NonproductionAuthorityError):
        adapter(command).require_trust(trust=trust(document=document), now=NOW)
    assert command.calls == []


@pytest.mark.parametrize(
    "field,value",
    [("trust_tier", "production"), ("attestations", "optional"), ("schema", "dpone.runtime-artifact-trust-policy.v1")],
)
def test_external_existing_policy_must_use_required_nonproduction_v2(field, value):
    document = github_document()
    document[field] = value
    with pytest.raises(NonproductionAuthorityError):
        adapter().require_trust(trust=trust(document=document), now=NOW)


@pytest.mark.parametrize(
    "kind", ["policy_pin", "verifier_pin", "duplicate", "unknown", "noncanonical", "expired_root", "missing_clock"]
)
def test_original_external_policy_bytes_and_clock_are_strict(kind):
    snapshot = trust()
    now: datetime | None = NOW
    if kind == "policy_pin":
        snapshot = replace(snapshot, policy_sha256=sha256(b"foreign"))
    elif kind == "verifier_pin":
        snapshot = replace(snapshot, verifier_policy_sha256=sha256(b"foreign"))
    elif kind == "missing_clock":
        now = None
    else:
        raw = snapshot.verifier_policy_bytes
        if kind == "duplicate":
            raw = raw[:-1] + b',"trust_tier":"non_production"}'
        elif kind == "unknown":
            raw = raw[:-1] + b',"extra":true}'
        elif kind == "noncanonical":
            raw += b"\n"
        elif kind == "expired_root":
            raw = raw.replace(b"2026-09-11T00:00:00Z", b"2026-09-10T01:00:00Z")
        snapshot = replace(snapshot, verifier_policy_bytes=raw, verifier_policy_sha256=sha256(raw))
    with pytest.raises(NonproductionAuthorityError):
        adapter().require_trust(trust=snapshot, now=now)


@pytest.mark.parametrize("kind", ["digest", "invalid_signature", "raw_exception", "invalid_result"])
def test_verifier_failures_are_redacted_and_never_authenticate(kind):
    command = CommandDouble(changed_digest=kind == "digest", invalid=kind == "invalid_signature")
    service = adapter(command)
    if kind in {"raw_exception", "invalid_result"}:

        class BrokenVerifier:
            def verify_files(self, **_):
                if kind == "raw_exception":
                    raise RuntimeError("token=secret DSN=private")
                return object()

        service = GitHubNonproductionGrantSignatureVerifier(verifier=BrokenVerifier())
    with pytest.raises(NonproductionAuthorityError) as caught:
        verify(service)
    assert "secret" not in str(caught.value) and "private" not in str(caught.value)
    assert caught.value.__suppress_context__
    assert all(not path.exists() for path in command.paths)


@pytest.mark.parametrize("bundle", [b"", b"x" * (8 * 1024 * 1024 + 1), bytearray(b"mutable")])
def test_signature_bundle_is_immutable_and_bounded_before_subprocess(bundle):
    command = CommandDouble()
    with pytest.raises(NonproductionAuthorityError):
        verify(adapter(command), bundle=bundle)
    assert command.calls == []


@pytest.mark.parametrize(
    "raw", [b"", b"x" * (1024 * 1024 + 1), b'{"schema":"dpone.route-qualification.v1"}', bytearray(b"mutable")]
)
def test_only_original_bounded_grant_bytes_reach_verifier(raw):
    command = CommandDouble()
    with pytest.raises(NonproductionAuthorityError):
        verify(adapter(command), raw=raw)
    assert command.calls == []


@pytest.mark.parametrize(
    "change",
    [
        {"verified_attestations": 0},
        {"verified_attestations": True},
        {"verifier_version": "2.92.0"},
        {"subject_sha256": "sha256:" + "0" * 64},
    ],
)
def test_low_level_success_requires_exact_subject_count_and_supported_version(change):
    class ResultDouble:
        def verify_files(self, **_):
            return replace(
                GitHubAttestationVerification(execution(github_policy()).grant_sha256, "2.93.0", 1), **change
            )

    with pytest.raises(NonproductionAuthorityError, match="signature_verification"):
        verify(GitHubNonproductionGrantSignatureVerifier(verifier=ResultDouble()))


def test_actual_adapter_composition_rechecks_root_expiry_after_verifier():
    document = github_document()
    document["verifier"]["trusted_root"]["refresh_after"] = "2026-09-10T01:30:01Z"
    provider = TrustProvider(trust(document=document))
    clocks = iter((NOW, NOW + timedelta(seconds=1)))
    command = CommandDouble()
    service = NonproductionGrantAuthenticator(
        trust_provider=provider, clock=lambda: next(clocks), verifier=adapter(command)
    )
    with pytest.raises(NonproductionAuthorityError, match="signer_trust"):
        service.authenticate_execution(**execution_inputs(execution(github_policy())))
    assert provider.reads == 2 and len(command.calls) == 2
