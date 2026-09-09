from __future__ import annotations

import json
from copy import deepcopy

import jsonschema
import pytest
from tests.agent_policy._runtime_image_promotion_helpers import (
    COMMIT_SHA,
    DIGEST,
    IMAGE,
    ROOT,
    VERSION,
    FakeRegistry,
    Lookup,
    RecordingJournal,
    certification_payload,
    publication_payload,
)
from tools.agent_policy import runtime_image_promotion as promotion


def test_retry_after_interruption_is_same_digest_idempotent() -> None:
    present = Lookup(200, {"docker-content-digest": DIGEST}, True)
    latest = Lookup(200, {"docker-content-digest": DIGEST}, True, version=VERSION)
    registry = FakeRegistry(
        [
            present,
            present,
            Lookup(404, {}, True),
            Lookup(404, {}, True),
            present,
            Lookup(404, {}, True),
            Lookup(404, {}, True),
            latest,
        ]
    )
    certification = promotion.validate_certification(certification_payload())

    receipt = promotion.promote_certified_image(
        certification=certification,
        certification_sha256=f"sha256:{'1' * 64}",
        registry=registry,
        journal=RecordingJournal(),
    )

    assert [item["decision"] for item in receipt.to_payload()["transitions"]] == [
        "NOOP_SAME",
        "CREATED",
        "CREATED",
    ]
    assert registry.created == [
        (f"{IMAGE}:sha-{COMMIT_SHA[:12]}", DIGEST),
        (f"{IMAGE}:latest", DIGEST),
    ]


def test_owned_json_schemas_accept_canonical_receipts_and_are_strict() -> None:
    certification_schema = json.loads(
        (ROOT / "docs/schemas/release/runtime-image-certification-v2.schema.json").read_text(encoding="utf-8")
    )
    publication_schema = json.loads(
        (ROOT / "docs/schemas/release/runtime-image-publication.schema.json").read_text(encoding="utf-8")
    )
    jsonschema.Draft202012Validator.check_schema(certification_schema)
    jsonschema.Draft202012Validator.check_schema(publication_schema)
    jsonschema.validate(certification_payload(), certification_schema)
    jsonschema.validate(publication_payload(), publication_schema)

    extra = deepcopy(certification_payload())
    extra["release"]["unexpected"] = "forbidden"
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(extra, certification_schema)


def test_historical_v1_certification_remains_valid() -> None:
    schema = json.loads(
        (ROOT / "docs/schemas/release/runtime-image-certification.schema.json").read_text(encoding="utf-8")
    )
    payload = certification_payload()
    payload["schema"] = promotion.CERTIFICATION_SCHEMA_V1
    payload["checks"] = [item for item in payload["checks"] if item["id"] in promotion.LEGACY_REQUIRED_CHECK_IDS]

    jsonschema.validate(payload, schema)
    certification = promotion.validate_certification(payload)

    assert tuple(item["id"] for item in certification.to_payload()["checks"]) == (promotion.LEGACY_REQUIRED_CHECK_IDS)
