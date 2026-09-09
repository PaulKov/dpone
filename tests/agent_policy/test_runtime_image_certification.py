from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from tests.agent_policy._runtime_image_certification_helpers import (
    DIGEST,
    IMAGE,
    OTHER_DIGEST,
    SOURCE_REF,
    SOURCE_SHA,
    VERSION,
    _inputs,
    _spdx,
    _verification,
    _write_json,
)
from tools.agent_policy import runtime_image_certification as producer
from tools.agent_policy import runtime_image_promotion as promotion


def test_producer_constructs_complete_pass_from_validated_bytes(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)

    artifacts = producer.produce_certification(
        inputs,
        certification_schema=promotion.CERTIFICATION_SCHEMA,
        required_check_ids=promotion.REQUIRED_CHECK_IDS,
    )
    certification = promotion.validate_certification(
        artifacts.certification,
        expected=promotion.CertificationIdentity(IMAGE, VERSION, DIGEST, SOURCE_SHA, SOURCE_REF),
    )
    rendered_manifest = producer.render_json(artifacts.context_manifest)

    assert certification.schema == promotion.CERTIFICATION_SCHEMA_V2
    assert certification.to_payload()["checks"] == [
        {"id": check_id, "status": "PASS", "subject_digest": DIGEST} for check_id in promotion.REQUIRED_CHECK_IDS
    ]
    assert certification.to_payload()["build"]["context_manifest_sha256"] == (
        f"sha256:{hashlib.sha256(rendered_manifest.encode()).hexdigest()}"
    )
    assert [item["path"] for item in artifacts.context_manifest["files"]] == sorted(
        item["path"] for item in artifacts.context_manifest["files"]
    )


def test_producer_accepts_syft_spdx_without_document_describes_field(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    spdx = _spdx()
    del spdx["documentDescribes"]
    spdx["relationships"] = [
        {
            "spdxElementId": "SPDXRef-DOCUMENT",
            "relatedSpdxElement": "SPDXRef-Package-dpone",
            "relationshipType": "DESCRIBES",
        }
    ]
    _write_json(inputs.sbom, spdx)
    _write_json(inputs.sbom_verification, _verification(producer.SBOM_PREDICATE, spdx))

    artifacts = producer.produce_certification(
        inputs,
        certification_schema=promotion.CERTIFICATION_SCHEMA,
        required_check_ids=promotion.REQUIRED_CHECK_IDS,
    )
    assert artifacts.certification["status"] == "PASS"


def test_producer_rejects_spdx_without_document_subject(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    spdx = _spdx()
    del spdx["documentDescribes"]
    spdx["relationships"] = [
        {
            "spdxElementId": "SPDXRef-Package-dpone",
            "relatedSpdxElement": "SPDXRef-Package-other",
            "relationshipType": "DEPENDS_ON",
        }
    ]
    _write_json(inputs.sbom, spdx)
    _write_json(inputs.sbom_verification, _verification(producer.SBOM_PREDICATE, spdx))

    with pytest.raises(producer.CertificationEvidenceError) as raised:
        producer.produce_certification(
            inputs,
            certification_schema=promotion.CERTIFICATION_SCHEMA,
            required_check_ids=promotion.REQUIRED_CHECK_IDS,
        )
    assert raised.value.code == "SPDX_INVALID"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("image", "docker.io/paulkov/dpone-runtime"),
        ("version", "0.73.2-rc.1"),
        ("release_tag", "v0.73.1"),
        ("source_commit_sha", "c" * 39),
        ("source_ref", "refs/heads/main"),
        ("source_repository", "../.."),
        ("signer_workflow", "PaulKov/dpone/.github/workflows/release.yml"),
        ("digest", OTHER_DIGEST.upper()),
        ("platform", "linux/arm64"),
        ("run_id", "0"),
        ("run_attempt", "0"),
    ],
)
def test_producer_rejects_invalid_release_identity(tmp_path: Path, field: str, value: str) -> None:
    inputs = replace(_inputs(tmp_path), **{field: value})

    with pytest.raises(producer.CertificationEvidenceError):
        producer.produce_certification(
            inputs,
            certification_schema=promotion.CERTIFICATION_SCHEMA,
            required_check_ids=promotion.REQUIRED_CHECK_IDS,
        )


@pytest.mark.parametrize("failure", ["missing", "skip", "digest", "source", "duplicate"])
def test_producer_rejects_incomplete_or_mismatched_check_receipts(tmp_path: Path, failure: str) -> None:
    inputs = _inputs(tmp_path)
    receipt = inputs.checks_root / f"{promotion.REQUIRED_CHECK_IDS[0]}.json"
    if failure == "missing":
        receipt.unlink()
    elif failure == "duplicate":
        receipt.write_text('{"schema":"x","schema":"y"}', encoding="utf-8")
    else:
        payload = json.loads(receipt.read_text(encoding="utf-8"))
        payload[{"skip": "status", "digest": "subject_digest", "source": "source_commit_sha"}[failure]] = {
            "skip": "SKIP",
            "digest": OTHER_DIGEST,
            "source": "d" * 40,
        }[failure]
        _write_json(receipt, payload)

    with pytest.raises(producer.CertificationEvidenceError):
        producer.produce_certification(
            inputs,
            certification_schema=promotion.CERTIFICATION_SCHEMA,
            required_check_ids=promotion.REQUIRED_CHECK_IDS,
        )


@pytest.mark.parametrize(
    ("receipt", "field", "value"),
    [
        ("provenance", "predicateType", "https://example.invalid/wrong"),
        ("provenance", "subject_digest", OTHER_DIGEST),
        ("provenance", "issuer", "https://issuer.example.invalid"),
        ("provenance", "sourceRepositoryURI", "https://github.com/attacker/repository"),
        ("provenance", "sourceRepositoryDigest", "d" * 40),
        ("provenance", "sourceRepositoryRef", "refs/tags/v9.9.9"),
        ("provenance", "subjectAlternativeName", "https://github.com/attacker/workflow.yml@refs/heads/main"),
        ("sbom", "predicate", {"spdxVersion": "SPDX-2.3"}),
    ],
)
def test_producer_rejects_unbound_attestation_verification(
    tmp_path: Path,
    receipt: str,
    field: str,
    value: Any,
) -> None:
    inputs = _inputs(tmp_path)
    path = inputs.provenance_verification if receipt == "provenance" else inputs.sbom_verification
    payload = json.loads(path.read_text(encoding="utf-8"))
    result = payload[0]["verificationResult"]
    if field == "subject_digest":
        result["statement"]["subject"][0]["digest"]["sha256"] = str(value).removeprefix("sha256:")
    elif field in result["statement"]:
        result["statement"][field] = value
    else:
        result["signature"]["certificate"][field] = value
    _write_json(path, payload)

    with pytest.raises(producer.CertificationEvidenceError):
        producer.produce_certification(
            inputs,
            certification_schema=promotion.CERTIFICATION_SCHEMA,
            required_check_ids=promotion.REQUIRED_CHECK_IDS,
        )


def test_producer_rejects_verification_without_verified_timestamp(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    payload = json.loads(inputs.provenance_verification.read_text(encoding="utf-8"))
    payload[0]["verificationResult"]["verifiedTimestamps"] = []
    _write_json(inputs.provenance_verification, payload)

    with pytest.raises(producer.CertificationEvidenceError):
        producer.produce_certification(
            inputs,
            certification_schema=promotion.CERTIFICATION_SCHEMA,
            required_check_ids=promotion.REQUIRED_CHECK_IDS,
        )


def test_producer_rejects_invalid_or_oversized_spdx(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    _write_json(inputs.sbom, {"spdxVersion": "SPDX-2.2", "packages": []})
    with pytest.raises(producer.CertificationEvidenceError):
        producer.produce_certification(
            inputs,
            certification_schema=promotion.CERTIFICATION_SCHEMA,
            required_check_ids=promotion.REQUIRED_CHECK_IDS,
        )

    inputs.sbom.write_bytes(b" " * (producer.MAX_SPDX_BYTES + 1))
    with pytest.raises(producer.CertificationEvidenceError):
        producer.produce_certification(
            inputs,
            certification_schema=promotion.CERTIFICATION_SCHEMA,
            required_check_ids=promotion.REQUIRED_CHECK_IDS,
        )
