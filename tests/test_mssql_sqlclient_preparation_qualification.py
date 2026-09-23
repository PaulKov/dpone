"""Qualification authority is exact, canonical and fail closed."""

from __future__ import annotations

from hashlib import sha256

import pytest

from dpone.contracts.mssql_sqlclient_preparation import PROFILE, QUERY_PROFILE
from dpone.contracts.mssql_sqlclient_preparation_qualification import (
    PreparationBaselineAuthority,
)
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.services.mssql_sqlclient_preparation_qualification import admit_preparation_baseline

H = "a" * 64
IMAGE = "sha256:" + "b" * 64


def baseline_payload() -> bytes:
    return canonical_json_bytes(
        {
            "schema": "dpone.sqlclient.preparation-baseline.v1",
            "profile": PROFILE,
            "query_profile": QUERY_PROFILE,
            "server_build": {
                "product_version": "16.0.test",
                "edition": "Developer Edition",
                "engine_edition": 3,
                "platform": "Linux",
                "image_digest": IMAGE,
            },
            "database_profile": {
                "compatibility_level": 160,
                "collation": "SQL_Latin1_General_CP1_CI_AS",
                "containment": 0,
                "trustworthy": 0,
                "database_chaining": 0,
            },
            "provenance": {
                "provisioning_script_sha256": "1" * 64,
                "observation_script_sha256": "2" * 64,
                "canonicalizer_sha256": "3" * 64,
                "first_raw_evidence_sha256": "4" * 64,
                "repeat_raw_evidence_sha256": "5" * 64,
                "reviewed_projection_sha256": "6" * 64,
                "review_receipt_sha256": "7" * 64,
            },
            "stock_server_permissions": [],
            "stock_database_permissions": [],
            "effective_server_permissions": [],
            "effective_database_permissions": [],
            "token_rules": {
                "login": [{"subject": "writer", "type": "SQL LOGIN", "usage": "GRANT OR DENY"}],
                "user": [{"subject": "writer", "type": "SQL USER", "usage": "GRANT OR DENY"}],
            },
        }
    )


def qualification_payload(baseline: bytes) -> bytes:
    value = strict_json_object(baseline)
    return canonical_json_bytes(
        {
            "schema": "dpone.sqlclient.preparation-qualification.v1",
            "status": "PASS",
            "baseline_sha256": sha256(baseline).hexdigest(),
            "baseline_byte_count": len(baseline),
            "profile": PROFILE,
            "query_profile": QUERY_PROFILE,
            "sql_image_digest": IMAGE,
            "server_build_sha256": sha256(canonical_json_bytes(value["server_build"])).hexdigest(),
            "database_profile_sha256": sha256(canonical_json_bytes(value["database_profile"])).hexdigest(),
            "producer_source_sha256": "8" * 64,
            "control_source_sha256": "9" * 64,
            "first_raw_sha256": "4" * 64,
            "repeat_raw_sha256": "5" * 64,
            "reviewed_projection_sha256": "6" * 64,
        }
    )


def authority(receipt: bytes) -> PreparationBaselineAuthority:
    return PreparationBaselineAuthority(sha256(receipt).hexdigest())


def test_exact_receipt_admits_opaque_baseline() -> None:
    baseline = baseline_payload()
    receipt = qualification_payload(baseline)
    approved = authority(receipt)
    admitted = admit_preparation_baseline(baseline, receipt, approved)
    admitted.assert_authority(approved)
    assert admitted.baseline_bytes is baseline
    assert admitted.qualification_bytes is receipt
    assert admitted.evidence_identity() == {
        "baseline_sha256": sha256(baseline).hexdigest(),
        "qualification_sha256": sha256(receipt).hexdigest(),
        "authority_sha256": admitted.evidence_identity()["authority_sha256"],
    }


@pytest.mark.parametrize(
    "field",
    [
        "baseline_sha256",
        "baseline_byte_count",
        "profile",
        "query_profile",
        "sql_image_digest",
        "server_build_sha256",
        "database_profile_sha256",
        "producer_source_sha256",
        "control_source_sha256",
        "first_raw_sha256",
        "repeat_raw_sha256",
        "reviewed_projection_sha256",
        "status",
    ],
)
def test_every_receipt_binding_mutation_is_rejected(field: str) -> None:
    baseline = baseline_payload()
    original = qualification_payload(baseline)
    body = strict_json_object(original)
    body[field] = 1 if field == "baseline_byte_count" else ("FAIL" if field == "status" else H)
    changed = canonical_json_bytes(body)
    with pytest.raises(ValueError, match="qualification_invalid"):
        admit_preparation_baseline(baseline, changed, authority(original))


def test_self_authorized_receipt_cannot_replace_deployment_authority() -> None:
    baseline = baseline_payload()
    receipt = qualification_payload(baseline)
    approved = authority(receipt)
    body = strict_json_object(receipt)
    body["producer_source_sha256"] = "c" * 64
    forged = canonical_json_bytes(body)
    with pytest.raises(ValueError, match="qualification_invalid"):
        admit_preparation_baseline(baseline, forged, approved)


def test_opaque_value_rejects_foreign_authority_after_admission() -> None:
    baseline = baseline_payload()
    receipt = qualification_payload(baseline)
    approved = authority(receipt)
    admitted = admit_preparation_baseline(baseline, receipt, approved)
    with pytest.raises(ValueError, match="qualification_invalid"):
        admitted.assert_authority(PreparationBaselineAuthority("c" * 64))


def test_admitted_value_rejects_post_admission_substitution() -> None:
    baseline = baseline_payload()
    receipt = qualification_payload(baseline)
    admitted = admit_preparation_baseline(baseline, receipt, authority(receipt))
    for name, value in (("_baseline", b"changed"), ("_qualification", b"changed"), ("_authority", object())):
        with pytest.raises((AttributeError, TypeError)):
            setattr(admitted, name, value)
        with pytest.raises((AttributeError, TypeError)):
            object.__setattr__(admitted, name, value)
    admitted.assert_admitted(authority(receipt))


def test_reused_raw_evidence_is_rejected() -> None:
    baseline = baseline_payload()
    body = strict_json_object(qualification_payload(baseline))
    body["repeat_raw_sha256"] = body["first_raw_sha256"]
    receipt = canonical_json_bytes(body)
    with pytest.raises(ValueError, match="qualification_invalid"):
        admit_preparation_baseline(baseline, receipt, authority(receipt))


def test_direct_tuple_construction_cannot_cross_composition_authority() -> None:
    from dpone.app.mssql_sqlclient_preparation_composition import _admit_baseline
    from dpone.contracts.mssql_sqlclient_preparation_qualification import AdmittedPreparationBaseline

    approved_baseline = baseline_payload()
    approved_receipt = qualification_payload(approved_baseline)
    body = strict_json_object(approved_receipt)
    body["producer_source_sha256"] = "c" * 64
    forged_receipt = canonical_json_bytes(body)
    forged = tuple.__new__(
        AdmittedPreparationBaseline,
        (approved_baseline, forged_receipt, authority(forged_receipt)),
    )
    with pytest.raises(ValueError, match="qualification_invalid"):
        _admit_baseline(forged, authority(approved_receipt))
