from __future__ import annotations

import json
from pathlib import Path

import pytest
from tests.agent_policy.test_runtime_image_certification import _inputs, _write_json
from tools.agent_policy import runtime_image_certification as producer
from tools.agent_policy import runtime_image_promotion as promotion


def test_cosign_is_mandatory_for_runtime_image_certification(tmp_path: Path) -> None:
    assert "cosign" in promotion.REQUIRED_CHECK_IDS
    inputs = _inputs(tmp_path)
    cosign_receipt = inputs.checks_root / "cosign.json"
    cosign_receipt.unlink()

    with pytest.raises(producer.CertificationEvidenceError):
        producer.produce_certification(
            inputs,
            certification_schema=promotion.CERTIFICATION_SCHEMA,
            required_check_ids=promotion.REQUIRED_CHECK_IDS,
        )

    inputs = _inputs(tmp_path)
    cosign_receipt = inputs.checks_root / "cosign.json"
    payload = json.loads(cosign_receipt.read_text(encoding="utf-8"))
    payload["status"] = "SKIP"
    _write_json(cosign_receipt, payload)

    with pytest.raises(producer.CertificationEvidenceError):
        producer.produce_certification(
            inputs,
            certification_schema=promotion.CERTIFICATION_SCHEMA,
            required_check_ids=promotion.REQUIRED_CHECK_IDS,
        )
