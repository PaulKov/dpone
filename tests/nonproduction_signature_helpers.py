"""Inert public bytes and injected results; none is cryptographic evidence."""

import base64
import hashlib
from dataclasses import replace

from dpone.contracts.nonproduction_authority import NonproductionSigner
from dpone.contracts.nonproduction_scope import canonical_document
from dpone.contracts.runtime_artifact_attestation import (
    GITHUB_ATTESTATION_BACKEND,
    GITHUB_OIDC_ISSUER,
    GITHUB_PROVENANCE_PREDICATE,
    RUNTIME_ARTIFACT_TRUST_POLICY_V2,
)
from dpone.ports.nonproduction_authentication import NonproductionTrustSnapshot
from tests.nonproduction_authority_helpers import NOW, execution, policy, qualification

ROOT_BYTES = b"inert public root for command-double tests only"
BUNDLE = b"inert bundle for command-double tests only"
REPOSITORY = "synthetic-fixtures/authority"
WORKFLOW = REPOSITORY + "/.github/workflows/grants.yml"
WORKFLOW_COMMIT = "1" * 40


def sha256(raw):
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def github_document():
    return {
        "schema": RUNTIME_ARTIFACT_TRUST_POLICY_V2,
        "trust_tier": "non_production",
        "attestations": "required_for_prod",
        "verifier": {
            "backend": GITHUB_ATTESTATION_BACKEND,
            "repository": REPOSITORY,
            "signer_workflow": WORKFLOW,
            "signer_digest": WORKFLOW_COMMIT,
            "predicate_type": GITHUB_PROVENANCE_PREDICATE,
            "cert_oidc_issuer": GITHUB_OIDC_ISSUER,
            "deny_self_hosted_runners": True,
            "trusted_root": {
                "encoding": "base64",
                "content": base64.b64encode(ROOT_BYTES).decode(),
                "sha256": sha256(ROOT_BYTES),
                "generated_at": "2026-09-10T00:00:00Z",
                "refresh_after": "2026-09-11T00:00:00Z",
            },
            "gh": {"minimum_version": "2.93.0", "maximum_version_exclusive": "3.0.0", "timeout_seconds": 5},
        },
    }


def github_policy(**changes):
    return policy(
        signer=NonproductionSigner(
            GITHUB_ATTESTATION_BACKEND,
            GITHUB_OIDC_ISSUER,
            f"https://github.com/{WORKFLOW}@{WORKFLOW_COMMIT}",
            sha256(ROOT_BYTES),
        ),
        source_repository="https://github.com/" + REPOSITORY,
        **changes,
    )


def trust(value=None, document=None, **changes):
    value = value or github_policy()
    raw = canonical_document(document or github_document())
    return replace(
        NonproductionTrustSnapshot(value.to_bytes(), value.policy_sha256, raw, sha256(raw), value.revocation_epoch),
        **changes,
    )


class TrustProvider:
    def __init__(self, *snapshots):
        self.snapshots = snapshots or (trust(),)
        self.reads = 0

    def read(self):
        value = self.snapshots[min(self.reads, len(self.snapshots) - 1)]
        self.reads += 1
        return value


class SignatureDouble:
    """A trusted-capability double; its result never proves a real signature."""

    def __init__(self, subject):
        self.subject = subject
        self.calls = []

    def require_trust(self, **values):
        self.calls.append(("trust", values))

    def verify(self, **values):
        self.calls.append(("verify", values))
        return self.subject


def qualification_inputs(grant):
    return {
        "grant_bytes": grant.to_bytes(),
        "sigstore_bundle": BUNDLE,
        "expected_scope": grant.scope,
        "qualification_run_id": grant.qualification_run_id,
        "fixture_plan_sha256": grant.fixture_plan_sha256,
        "qualification_plan_sha256": grant.qualification_plan_sha256,
    }


def execution_inputs(grant):
    return {
        "grant_bytes": grant.to_bytes(),
        "sigstore_bundle": BUNDLE,
        "expected_scope": grant.scope,
        "qualified_set_sha256": grant.qualified_set_sha256,
        "native_release_id": grant.native_release_id,
        "parent_release_id": grant.parent_release_id,
        "deployment_id": grant.deployment_id,
        "activation_id": grant.activation_id,
        "workloads": grant.workloads,
    }


__all__ = ["NOW", "execution", "qualification"]
