"""Workspace v3 metadata binds sources and all DEV identities, not signatures."""

import json
from dataclasses import replace

import jsonschema
import pytest

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.dbt_promotion import DbtProdMirrorError, DbtPromotionTrustDescriptor
from dpone.contracts.dbt_publish_schema_contracts import dbt_schema_contracts
from dpone.contracts.dbt_workspace_promotion import (
    DbtWorkspacePromotionDescriptor,
    validate_workspace_mirror_paths,
)


def _descriptor() -> DbtWorkspacePromotionDescriptor:
    return DbtWorkspacePromotionDescriptor(
        release_id="sha256:" + "a" * 64,
        mirror_root="audit/dbt-workspace",
        source_snapshot_path=".dpone/dbt/source-snapshot.json",
        source_snapshot_sha256="sha256:" + "b" * 64,
        dev_deployment_id="sha256:" + "c" * 64,
        dev_evidence_ref="github-actions://example/repo/actions/runs/123",
        trust=DbtPromotionTrustDescriptor.validated(
            subject_sha256="sha256:" + "d" * 64,
            artifact_name="evidence",
            producer_workflow=".github/workflows/accept.yml",
            source_commit="e" * 40,
            evidence_set_id="sha256:" + "f" * 64,
            campaign_request_sha256="sha256:" + "0" * 64,
        ),
    )


def _reseal(payload):
    payload["promotion_id"] = canonical_fingerprint({key: val for key, val in payload.items() if key != "promotion_id"})
    return json.dumps(payload).encode()


def test_round_trip_binds_every_source_and_trust_field() -> None:
    descriptor = _descriptor()
    payload = descriptor.to_dict()
    jsonschema.validate(payload, dbt_schema_contracts()["dpone.dbt-prod-promotion.v3"])
    assert payload["schema"] == "dpone.dbt-prod-promotion.v3"
    assert DbtWorkspacePromotionDescriptor.from_payload(json.dumps(payload).encode()) == descriptor
    assert payload["promotion_id"] == canonical_fingerprint({k: v for k, v in payload.items() if k != "promotion_id"})


@pytest.mark.parametrize("key", list(_descriptor().to_dict()))
def test_no_v3_field_is_optional(key: str) -> None:
    payload = _descriptor().to_dict()
    del payload[key]
    with pytest.raises(DbtProdMirrorError):
        DbtWorkspacePromotionDescriptor.from_payload(json.dumps(payload).encode())


@pytest.mark.parametrize("schema", ["dpone.dbt-prod-promotion.v1", "dpone.dbt-prod-promotion.v2", "unknown"])
def test_resealed_old_or_unknown_schema_is_not_workspace(schema: str) -> None:
    payload = _descriptor().to_dict()
    payload["schema"] = schema
    with pytest.raises(DbtProdMirrorError):
        DbtWorkspacePromotionDescriptor.from_payload(_reseal(payload))


def test_unknown_and_duplicate_fields_rejected() -> None:
    payload = _descriptor().to_dict()
    payload["unexpected"] = 1
    with pytest.raises(DbtProdMirrorError):
        DbtWorkspacePromotionDescriptor.from_payload(_reseal(payload))
    encoded = json.dumps(_descriptor().to_dict())
    with pytest.raises(DbtProdMirrorError):
        DbtWorkspacePromotionDescriptor.from_payload((encoded[:-1] + ', "schema": "duplicate"}').encode())


def test_deep_json_below_byte_bound_is_a_domain_error() -> None:
    payload = b'{"unexpected":' + b"[" * 30_000 + b"0" + b"]" * 30_000 + b"}"
    with pytest.raises(DbtProdMirrorError):
        DbtWorkspacePromotionDescriptor.from_payload(payload)


@pytest.mark.parametrize("key", ["release_id", "source_snapshot_sha256", "dev_deployment_id", "dev_evidence_set_id"])
def test_changed_identity_needs_new_fingerprint(key: str) -> None:
    payload = _descriptor().to_dict()
    payload[key] = "sha256:" + "9" * 64
    with pytest.raises(DbtProdMirrorError, match="fingerprint"):
        DbtWorkspacePromotionDescriptor.from_payload(json.dumps(payload).encode())


@pytest.mark.parametrize("field", ["evidence_set_id", "campaign_request_sha256", "source_commit", "artifact_name"])
def test_direct_unvalidated_trust_constructor_cannot_bypass_contract(field: str) -> None:
    descriptor = _descriptor()
    invalid = replace(descriptor.trust, **{field: None if "sha256" in field or field == "evidence_set_id" else ""})
    with pytest.raises(DbtProdMirrorError):
        replace(descriptor, trust=invalid)


@pytest.mark.parametrize(
    "path",
    [
        ".",
        "../other",
        "/tmp/other",
        "a//b",
        "a/../b",
        "a/",
        "a\\b",
        ".GIT/a",
        ".worktrees/a",
        ".dpone-dbt-promotion.transaction/a",
        "a/e\u0301",
        "a/\x7f",
        "a" * 1025,
    ],
)
def test_unsafe_destination_rejected(path: str) -> None:
    with pytest.raises(DbtProdMirrorError):
        replace(_descriptor(), mirror_root=path)


@pytest.mark.parametrize(
    "paths",
    [
        ("mirror", "mirror/snapshot", "descriptor"),
        ("mirror", "metadata", "metadata/descriptor"),
        ("mirror/child", "mirror", "descriptor"),
        ("mirror", "descriptor/child", "descriptor"),
        ("Mirror", "mirror", "descriptor"),
        ("x/Mirror", "x/mirror/snapshot", "descriptor"),
        ("mirror", "metadata/snapshot", "Metadata/descriptor"),
    ],
)
def test_destinations_are_pairwise_disjoint_and_portable(paths: tuple[str, str, str]) -> None:
    with pytest.raises(DbtProdMirrorError):
        validate_workspace_mirror_paths(*paths)


def test_nested_disjoint_destinations_remain_valid() -> None:
    assert tuple(
        map(str, validate_workspace_mirror_paths("audit/dbt", ".dpone/dbt/snapshot.json", ".dpone/dbt/promotion.json"))
    ) == ("audit/dbt", ".dpone/dbt/snapshot.json", ".dpone/dbt/promotion.json")
