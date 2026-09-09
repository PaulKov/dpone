from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest

from dpone.adapters.semantic_refresh_vault_authority import (
    VaultSemanticRefreshAssuranceVerifier,
    VaultSemanticRefreshAuthorityIndexReader,
    VaultSemanticRefreshDeploymentVerifier,
    VaultSemanticRefreshRunAdmissionVerifier,
    semantic_refresh_vault_authority_index,
    semantic_refresh_vault_run_authority,
)
from dpone.adapters.semantic_refresh_vault_seal_policy import (
    VaultSemanticRefreshSealPolicyAuthority,
    semantic_refresh_seal_policy_locator,
    semantic_refresh_vault_seal_policy_document,
)
from dpone.contracts.dbt_semantic_refresh_run_contracts import (
    SemanticRefreshRunAdmissionAuthority,
)
from dpone.contracts.semantic_refresh_core import semantic_refresh_sha256
from dpone.contracts.semantic_refresh_route_certification import (
    SemanticRefreshRouteLiveCertificationReceipt,
)
from dpone.contracts.semantic_refresh_runtime_assurance import (
    SemanticRefreshRuntimeAssuranceReceipt,
)
from dpone.ports.semantic_refresh_seal_policy import (
    SemanticRefreshSealPolicyAuthority,
    SemanticRefreshSealPolicySubject,
)

_NOW = datetime(2026, 8, 8, 12, tzinfo=UTC)
_SEAL_POLICY = "sha256:" + "e" * 64


class _Vault:
    def __init__(self, values: dict[str, dict[str, object]]) -> None:
        self.values = values

    def get_secret(self, *, mount_point: str, path: str) -> dict[str, object]:
        assert mount_point == "dpone-kv"
        return self.values[path]


def _route() -> SemanticRefreshRouteLiveCertificationReceipt:
    import json
    from pathlib import Path

    payload = json.loads(Path("tests/fixtures/semantic-refresh-v2/contracts/evidence-golden-v1.json").read_text())
    return SemanticRefreshRouteLiveCertificationReceipt.from_mapping(
        payload["dpone.semantic-refresh-route-live-certification-receipt.v1"]
    )


def _runtime() -> SemanticRefreshRuntimeAssuranceReceipt:
    import json
    from pathlib import Path

    payload = json.loads(Path("tests/fixtures/semantic-refresh-v2/contracts/assurance-golden-v1.json").read_text())
    return SemanticRefreshRuntimeAssuranceReceipt.from_mapping(payload["writer_exclusivity"])


def _seal_subject() -> SemanticRefreshSealPolicySubject:
    return SemanticRefreshSealPolicySubject(
        release_id="sha256:" + "1" * 64,
        deployment_id="sha256:" + "2" * 64,
        model_unique_id="model.analytics.events",
        event_time_source_type="datetime2(6)",
        effective_key_mapping_sha256="sha256:" + "3" * 64,
        ordered_writable_schema_sha256="sha256:" + "4" * 64,
        route_certification_receipt_sha256="sha256:" + "5" * 64,
        writer_exclusivity_assurance_receipt_sha256="sha256:" + "6" * 64,
        ddl_freeze_assurance_receipt_sha256="sha256:" + "7" * 64,
        artifact_authority_sha256="sha256:" + "8" * 64,
        serializer_sha256="sha256:" + "9" * 64,
        parquet_schema_mapping_sha256="sha256:" + "a" * 64,
        utc_semantics_assurance_receipt_sha256="sha256:" + "b" * 64,
    )


def _seal_policy() -> SemanticRefreshSealPolicyAuthority:
    return SemanticRefreshSealPolicyAuthority.build(
        subject=_seal_subject(),
        clickhouse_input_mapping_sha256="sha256:" + "c" * 64,
        codec_mapping_certification_sha256="sha256:" + "d" * 64,
        seal_policy_sha256=_SEAL_POLICY,
        issuer_authority="vault://dpone-kv/semantic-refresh/seal-policies",
        issuer_attestation_sha256="sha256:" + "f" * 64,
        issuer_signature_sha256="sha256:" + "0" * 64,
    )


def test_version_pinned_vault_index_verifies_deployment_route_and_runtime() -> None:
    route = _route()
    runtime = _runtime()
    subject_digest = "sha256:" + "a" * 64
    index = semantic_refresh_vault_authority_index(
        deployment_subject_sha256=subject_digest,
        route_certification_receipt_sha256=route.route_certification_receipt_sha256,
        runtime_assurance_receipt_sha256s=(runtime.runtime_assurance_receipt_sha256,),
        seal_policy_authority_sha256s=(_seal_policy().seal_policy_authority_sha256,),
    )
    vault = _Vault({"semantic-refresh/deployment": {**index, "_metadata": {"version": 1}}})
    reader = VaultSemanticRefreshAuthorityIndexReader(
        client=vault,
        mount_point="dpone-kv",
        path="semantic-refresh/deployment",
        expected_version=1,
    )

    deployment = VaultSemanticRefreshDeploymentVerifier(reader)
    assurance = VaultSemanticRefreshAssuranceVerifier(reader, now=lambda: _NOW)
    subject = cast(Any, SimpleNamespace(subject_sha256=subject_digest))

    assert deployment.verify(subject) is True
    assert (
        assurance.verify_route(
            route,
            expected_coordinate_sha256=semantic_refresh_sha256(route.coordinates.to_dict()),
        )
        is True
    )
    assert assurance.verify_runtime(runtime) is True


def test_vault_index_version_or_digest_drift_fails_closed() -> None:
    route = _route()
    index = semantic_refresh_vault_authority_index(
        deployment_subject_sha256="sha256:" + "a" * 64,
        route_certification_receipt_sha256=route.route_certification_receipt_sha256,
        runtime_assurance_receipt_sha256s=("sha256:" + "f" * 64,),
        seal_policy_authority_sha256s=(_seal_policy().seal_policy_authority_sha256,),
    )
    vault = _Vault(
        {
            "semantic-refresh/deployment": {
                **index,
                "route_certification_receipt_sha256": "sha256:" + "f" * 64,
                "_metadata": {"version": 2},
            }
        }
    )
    reader = VaultSemanticRefreshAuthorityIndexReader(
        client=vault,
        mount_point="dpone-kv",
        path="semantic-refresh/deployment",
        expected_version=1,
    )

    assert (
        VaultSemanticRefreshDeploymentVerifier(reader).verify(
            cast(Any, SimpleNamespace(subject_sha256="sha256:" + "a" * 64))
        )
        is False
    )
    assert (
        VaultSemanticRefreshAssuranceVerifier(reader, now=lambda: _NOW).verify_route(
            route,
            expected_coordinate_sha256=semantic_refresh_sha256(route.coordinates.to_dict()),
        )
        is False
    )


def test_run_admission_uses_create_once_digest_derived_vault_path() -> None:
    unsigned = {
        "plan_bundle_sha256": "sha256:" + "b" * 64,
        "schema": "dpone.semantic-refresh-vault-run-authority.v1",
        "workflow_execution_id": "scheduled__2026-08-08",
    }
    authority = SemanticRefreshRunAdmissionAuthority(
        workflow_execution_id=str(unsigned["workflow_execution_id"]),
        plan_bundle_sha256=str(unsigned["plan_bundle_sha256"]),
        authority_receipt_sha256=semantic_refresh_sha256(unsigned),
    )
    document = semantic_refresh_vault_run_authority(authority)
    suffix = authority.authority_receipt_sha256[7:]
    vault = _Vault({f"semantic-refresh/runs/{suffix}": {**document, "_metadata": {"version": 1}}})
    verifier = VaultSemanticRefreshRunAdmissionVerifier(
        client=vault,
        mount_point="dpone-kv",
        path_prefix="semantic-refresh/runs",
    )

    assert verifier.verify(authority) is True
    vault.values[f"semantic-refresh/runs/{suffix}"]["_metadata"] = {"version": 2}
    assert verifier.verify(authority) is False


def test_unpinned_runtime_receipt_is_rejected() -> None:
    route = _route()
    runtime = _runtime()
    index = semantic_refresh_vault_authority_index(
        deployment_subject_sha256="sha256:" + "a" * 64,
        route_certification_receipt_sha256=route.route_certification_receipt_sha256,
        runtime_assurance_receipt_sha256s=("sha256:" + "f" * 64,),
        seal_policy_authority_sha256s=(_seal_policy().seal_policy_authority_sha256,),
    )
    reader = VaultSemanticRefreshAuthorityIndexReader(
        client=_Vault({"semantic-refresh/deployment": {**index, "_metadata": {"version": 1}}}),
        mount_point="dpone-kv",
        path="semantic-refresh/deployment",
        expected_version=1,
    )
    assert VaultSemanticRefreshAssuranceVerifier(reader, now=lambda: _NOW).verify_runtime(runtime) is False


def test_seal_policy_is_version_pinned_subject_bound_and_index_authorized() -> None:
    policy = _seal_policy()
    route = _route()
    index_document = semantic_refresh_vault_authority_index(
        deployment_subject_sha256="sha256:" + "a" * 64,
        route_certification_receipt_sha256=route.route_certification_receipt_sha256,
        runtime_assurance_receipt_sha256s=("sha256:" + "b" * 64,),
        seal_policy_authority_sha256s=(policy.seal_policy_authority_sha256,),
    )
    locator = semantic_refresh_seal_policy_locator(policy.subject)
    vault = _Vault(
        {
            "semantic-refresh/deployment": {**index_document, "_metadata": {"version": 3}},
            f"semantic-refresh/seal-policies/{locator}": {
                **semantic_refresh_vault_seal_policy_document(policy),
                "_metadata": {"version": 1},
            },
        }
    )
    index = VaultSemanticRefreshAuthorityIndexReader(
        client=vault,
        mount_point="dpone-kv",
        path="semantic-refresh/deployment",
        expected_version=3,
    )
    resolver = VaultSemanticRefreshSealPolicyAuthority(
        client=vault,
        index=index,
        mount_point="dpone-kv",
        path_prefix="semantic-refresh/seal-policies",
    )

    assert resolver.load(policy.subject) == policy

    document = vault.values[f"semantic-refresh/seal-policies/{locator}"]
    document["seal_policy_sha256"] = "sha256:" + "0" * 64
    with pytest.raises(Exception, match="seal policy"):
        resolver.load(policy.subject)


def test_unindexed_seal_policy_fails_closed() -> None:
    policy = _seal_policy()
    route = _route()
    index_document = semantic_refresh_vault_authority_index(
        deployment_subject_sha256="sha256:" + "a" * 64,
        route_certification_receipt_sha256=route.route_certification_receipt_sha256,
        runtime_assurance_receipt_sha256s=("sha256:" + "b" * 64,),
        seal_policy_authority_sha256s=("sha256:" + "f" * 64,),
    )
    locator = semantic_refresh_seal_policy_locator(policy.subject)
    vault = _Vault(
        {
            "semantic-refresh/deployment": {**index_document, "_metadata": {"version": 1}},
            f"semantic-refresh/seal-policies/{locator}": {
                **policy.to_dict(),
                "_metadata": {"version": 1},
            },
        }
    )
    index = VaultSemanticRefreshAuthorityIndexReader(
        client=vault,
        mount_point="dpone-kv",
        path="semantic-refresh/deployment",
        expected_version=1,
    )

    with pytest.raises(Exception, match="pinned index"):
        VaultSemanticRefreshSealPolicyAuthority(
            client=vault,
            index=index,
            mount_point="dpone-kv",
            path_prefix="semantic-refresh/seal-policies",
        ).load(policy.subject)
