from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from tests.agent_policy._runtime_image_certification_helpers import (
    DIGEST,
    IMAGE,
    SIGNER_WORKFLOW,
    SOURCE_REF,
    SOURCE_REPOSITORY,
    SOURCE_SHA,
    VERSION,
    _inputs,
)
from tools.agent_policy import runtime_image_certification as producer
from tools.agent_policy import runtime_image_promotion as promotion


def test_context_manifest_is_deterministic_bounded_and_secret_excluding(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    hidden = inputs.context_root / "src/dpone/.env"
    hidden.write_text("SECRET=value\n", encoding="utf-8")
    first = producer.produce_certification(
        inputs,
        certification_schema=promotion.CERTIFICATION_SCHEMA,
        required_check_ids=promotion.REQUIRED_CHECK_IDS,
    )
    second = producer.produce_certification(
        inputs,
        certification_schema=promotion.CERTIFICATION_SCHEMA,
        required_check_ids=promotion.REQUIRED_CHECK_IDS,
    )

    assert first.context_manifest == second.context_manifest
    assert all(item["path"] != "src/dpone/.env" for item in first.context_manifest["files"])
    changed = inputs.context_root / "src/dpone/module.py"
    changed.write_text("value = 'changed'\n", encoding="utf-8")
    third = producer.produce_certification(
        inputs,
        certification_schema=promotion.CERTIFICATION_SCHEMA,
        required_check_ids=promotion.REQUIRED_CHECK_IDS,
    )
    assert third.context_manifest != first.context_manifest


def test_context_manifest_rejects_dockerignore_drift_and_symlinks(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    inputs.dockerignore.write_text("**\n!everything/**\n", encoding="utf-8")
    with pytest.raises(producer.CertificationEvidenceError):
        producer.produce_certification(
            inputs,
            certification_schema=promotion.CERTIFICATION_SCHEMA,
            required_check_ids=promotion.REQUIRED_CHECK_IDS,
        )

    inputs = _inputs(tmp_path / "second")
    target = inputs.context_root / "outside.py"
    target.write_text("secret = True\n", encoding="utf-8")
    (inputs.context_root / "src/dpone/link.py").symlink_to(target)
    with pytest.raises(producer.CertificationEvidenceError):
        producer.produce_certification(
            inputs,
            certification_schema=promotion.CERTIFICATION_SCHEMA,
            required_check_ids=promotion.REQUIRED_CHECK_IDS,
        )


def test_attempt_scoped_writer_is_atomic_and_never_overwrites(tmp_path: Path) -> None:
    output = tmp_path / "evidence.json"
    producer.write_new(output, '{"status":"PASS"}\n')
    assert output.read_text(encoding="utf-8") == '{"status":"PASS"}\n'

    with pytest.raises(FileExistsError):
        producer.write_new(output, '{"status":"FAIL"}\n')
    assert output.read_text(encoding="utf-8") == '{"status":"PASS"}\n'
    assert list(tmp_path.glob(".evidence.json.*.tmp")) == []


def test_certify_cli_writes_context_first_and_pass_last(tmp_path: Path, capsys: Any) -> None:
    inputs = _inputs(tmp_path)
    output = tmp_path / "evidence/runtime-image-certification.json"
    args = [
        "certify",
        "--image",
        IMAGE,
        "--version",
        VERSION,
        "--release-tag",
        f"v{VERSION}",
        "--source-commit",
        SOURCE_SHA,
        "--source-ref",
        SOURCE_REF,
        "--source-repository",
        SOURCE_REPOSITORY,
        "--signer-workflow",
        SIGNER_WORKFLOW,
        "--digest",
        DIGEST,
        "--platform",
        "linux/amd64",
        "--run-id",
        "12345",
        "--run-attempt",
        "2",
        "--dockerfile",
        str(inputs.dockerfile),
        "--dockerignore",
        str(inputs.dockerignore),
        "--context-root",
        str(inputs.context_root),
        "--context-manifest-output",
        str(inputs.context_manifest_output),
        "--sbom",
        str(inputs.sbom),
        "--provenance-verification",
        str(inputs.provenance_verification),
        "--sbom-verification",
        str(inputs.sbom_verification),
        "--checks-root",
        str(inputs.checks_root),
        "--output",
        str(output),
    ]

    assert promotion.main(args) == 0
    assert inputs.context_manifest_output.is_file()
    assert output.read_text(encoding="utf-8") == capsys.readouterr().out
    promotion.validate_certification(json.loads(output.read_text(encoding="utf-8")))

    original = output.read_bytes()
    assert promotion.main(args) == 2
    assert output.read_bytes() == original
    assert json.loads(capsys.readouterr().out)["error"]["code"] == "CERTIFICATION_OUTPUT_INVALID"

    (inputs.checks_root / f"{promotion.REQUIRED_CHECK_IDS[0]}.json").unlink()
    output.unlink()
    inputs.context_manifest_output.unlink()
    assert promotion.main(args) == 2
    assert not output.exists()
    assert json.loads(capsys.readouterr().out)["error"]["code"] == "CHECK_RECEIPT_INVALID"
