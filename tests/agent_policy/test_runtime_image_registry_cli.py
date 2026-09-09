from __future__ import annotations

import importlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from tests.agent_policy._runtime_image_registry_helpers import (
    DIGEST,
    NEWER_DIGEST,
    ROOT,
    VERSION,
    PromotionRegistry,
    certification_payload,
    promotion_args,
    validation_args,
)
from tools.agent_policy import runtime_image_promotion as promotion
from tools.agent_policy import runtime_image_registry as registry


@pytest.mark.parametrize("source_flag", ["--certification", "--input"])
def test_cli_validates_certification_independently(
    tmp_path: Path,
    capsys: Any,
    source_flag: str,
) -> None:
    source = tmp_path / "certification.json"
    source.write_text(json.dumps(certification_payload()), encoding="utf-8")

    assert promotion.main(validation_args(source, source_flag)) == 0
    assert capsys.readouterr().out == json.dumps(certification_payload(), indent=2, sort_keys=True) + "\n"


def test_cli_validates_historical_v1_but_blocks_new_promotion_before_side_effects(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: Any,
) -> None:
    source = tmp_path / "certification-v1.json"
    output = tmp_path / "publication.json"
    payload = certification_payload()
    payload["schema"] = promotion.CERTIFICATION_SCHEMA_V1
    payload["checks"] = [item for item in payload["checks"] if item["id"] in promotion.LEGACY_REQUIRED_CHECK_IDS]
    source.write_text(json.dumps(payload), encoding="utf-8")

    assert promotion.main(validation_args(source)) == 0
    capsys.readouterr()

    monkeypatch.delenv("GHCR_TOKEN", raising=False)

    def forbid_registry(**_: Any) -> Any:
        raise AssertionError("v1 rejection must happen before registry construction")

    def forbid_journal(**_: Any) -> Any:
        raise AssertionError("v1 rejection must happen before journal creation")

    monkeypatch.setattr(registry, "AuthenticatedOciRegistry", forbid_registry)
    monkeypatch.setattr(registry, "PublicationJournal", forbid_journal)

    assert promotion.main(promotion_args(source, output)) == 2
    assert not output.exists()
    assert not registry.journal_path_for(output).exists()
    assert json.loads(capsys.readouterr().out) == {
        "error": {"code": "CERTIFICATION_SCHEMA_NOT_PROMOTABLE"},
        "status": "FAIL",
    }


def test_cli_rejects_duplicate_json_keys_without_echoing_input(tmp_path: Path, capsys: Any) -> None:
    source = tmp_path / "duplicate.json"
    source.write_text('{"schema":"secret-value","schema":"duplicate"}', encoding="utf-8")

    assert promotion.main(validation_args(source)) == 2
    output = capsys.readouterr().out
    assert "secret-value" not in output
    assert json.loads(output) == {"error": {"code": "JSON_INVALID"}, "status": "FAIL"}


@pytest.mark.parametrize("command", ["certify", "validate-certification", "promote"])
def test_cli_help_is_stable_and_never_accepts_a_token_value(command: str) -> None:
    completed = subprocess.run(
        [sys.executable, str(ROOT / "tools/agent_policy/runtime_image_promotion.py"), command, "--help"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert "--token-env" in completed.stdout if command == "promote" else "--token-env" not in completed.stdout
    assert "--token " not in completed.stdout


def test_cli_promote_returns_partial_exit_and_writes_sanitized_receipt(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: Any,
) -> None:
    source = tmp_path / "certification.json"
    output = tmp_path / "publication.json"
    source.write_text(json.dumps(certification_payload()), encoding="utf-8")
    fake = PromotionRegistry(
        [
            registry.RegistryLookup(200, {"docker-content-digest": DIGEST}, True),
            registry.RegistryLookup(200, {"docker-content-digest": DIGEST}, True),
            registry.RegistryLookup(200, {"docker-content-digest": NEWER_DIGEST}, True),
        ]
    )
    monkeypatch.setattr(registry, "AuthenticatedOciRegistry", lambda **_: fake)
    monkeypatch.setenv("GHCR_TOKEN", "workflow-secret")

    args = promotion_args(source, output)
    output.write_text("existing evidence\n", encoding="utf-8")
    assert promotion.main(args) == 2
    assert output.read_text(encoding="utf-8") == "existing evidence\n"
    assert json.loads(capsys.readouterr().out)["error"]["code"] == "PUBLICATION_OUTPUT_INVALID"
    output.unlink()

    exit_code = promotion.main(args)
    receipt = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 3
    assert receipt["status"] == "FAIL"
    assert receipt["outcome"] == "PARTIAL_ALIAS_PENDING"
    assert "workflow-secret" not in capsys.readouterr().out


def test_cli_never_writes_pass_before_terminal_journal_is_durable(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: Any,
) -> None:
    journal_module = importlib.import_module("tools.agent_policy.runtime_image_publication_journal")
    source = tmp_path / "certification.json"
    output = tmp_path / "publication.json"
    source.write_text(json.dumps(certification_payload()), encoding="utf-8")
    present = registry.RegistryLookup(200, {"docker-content-digest": DIGEST}, True)
    latest = registry.RegistryLookup(
        200,
        {"docker-content-digest": DIGEST},
        True,
        version=VERSION,
    )
    fake = PromotionRegistry([present, present, present, present, latest, latest])
    monkeypatch.setattr(registry, "AuthenticatedOciRegistry", lambda **_: fake)
    monkeypatch.setenv("GHCR_TOKEN", "workflow-secret")

    def fail_terminal(*_: Any, **__: Any) -> None:
        raise journal_module.PublicationJournalError("PUBLICATION_JOURNAL_WRITE_FAILED")

    monkeypatch.setattr(registry.PublicationJournal, "complete", fail_terminal)

    assert promotion.main(promotion_args(source, output)) == 2
    assert not output.exists()
    assert json.loads(capsys.readouterr().out) == {
        "error": {"code": "PUBLICATION_JOURNAL_WRITE_FAILED"},
        "status": "FAIL",
    }


def test_cli_replays_completed_journal_when_receipt_write_was_interrupted(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: Any,
) -> None:
    producer = importlib.import_module("tools.agent_policy.runtime_image_certification")
    source = tmp_path / "certification.json"
    output = tmp_path / "publication.json"
    source.write_text(json.dumps(certification_payload()), encoding="utf-8")
    present = registry.RegistryLookup(200, {"docker-content-digest": DIGEST}, True)
    latest = registry.RegistryLookup(
        200,
        {"docker-content-digest": DIGEST},
        True,
        version=VERSION,
    )
    fake = PromotionRegistry([present, present, present, present, latest, latest])
    monkeypatch.setattr(registry, "AuthenticatedOciRegistry", lambda **_: fake)
    monkeypatch.setenv("GHCR_TOKEN", "workflow-secret")
    real_write_new = producer.write_new
    failed = False

    def fail_first_receipt_write(path: Path, rendered: str) -> None:
        nonlocal failed
        if path == output and not failed:
            failed = True
            raise OSError("interrupted receipt write")
        real_write_new(path, rendered)

    monkeypatch.setattr(producer, "write_new", fail_first_receipt_write)
    args = promotion_args(source, output)

    assert promotion.main(args) == 2
    assert not output.exists()
    journal_path = registry.journal_path_for(output)
    assert json.loads(journal_path.read_text(encoding="utf-8"))["state"] == "COMPLETE"
    capsys.readouterr()

    def forbid_registry_replay(**_: Any) -> Any:
        raise AssertionError("completed journal replay must not mutate the registry")

    monkeypatch.setattr(registry, "AuthenticatedOciRegistry", forbid_registry_replay)

    assert promotion.main(args) == 0
    assert json.loads(output.read_text(encoding="utf-8"))["status"] == "PASS"
    assert json.loads(capsys.readouterr().out)["status"] == "PASS"
