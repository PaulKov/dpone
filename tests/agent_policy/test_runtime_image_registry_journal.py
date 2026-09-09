from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Any

import pytest
from tests.agent_policy._runtime_image_registry_helpers import (
    DIGEST,
    IMAGE,
    VERSION,
    PromotionRegistry,
    certification_payload,
)
from tools.agent_policy import runtime_image_promotion as promotion
from tools.agent_policy import runtime_image_registry as registry


def test_publication_journal_fsyncs_bounded_pending_repair_evidence(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    journal_module = importlib.import_module("tools.agent_policy.runtime_image_publication_journal")
    fsynced: list[int] = []
    monkeypatch.setattr(journal_module.os, "fsync", fsynced.append)
    path = tmp_path / "runtime-image-publication.journal.json"
    journal = journal_module.PublicationJournal(
        path=path,
        candidate_digest=DIGEST,
        certification_sha256=f"sha256:{'1' * 64}",
    )

    journal.prepare_mutation(
        role="version",
        reference=f"{IMAGE}:{VERSION}",
        expected_digest=DIGEST,
        before={"state": "ABSENT"},
        decision="CREATED",
    )

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["state"] == "MUTATION_PENDING"
    assert payload["pending_mutation"]["expected_digest"] == DIGEST
    assert payload["transitions"] == []
    assert len(fsynced) >= 2
    assert len(path.read_bytes()) <= journal_module.MAX_JOURNAL_BYTES
    assert "workflow-secret" not in path.read_text(encoding="utf-8")


def test_interruption_after_mutation_keeps_machine_readable_pending_intent(tmp_path: Path) -> None:
    journal_module = importlib.import_module("tools.agent_policy.runtime_image_publication_journal")
    path = tmp_path / "runtime-image-publication.journal.json"
    journal = journal_module.PublicationJournal(
        path=path,
        candidate_digest=DIGEST,
        certification_sha256=f"sha256:{'1' * 64}",
    )

    class InterruptingRegistry(PromotionRegistry):
        def create_alias(self, reference: str, digest: str) -> registry.RegistryCommand:
            del reference, digest
            raise KeyboardInterrupt

    certification = promotion.validate_certification(certification_payload())
    adapter = InterruptingRegistry(
        [
            registry.RegistryLookup(404, {}, True),
            registry.RegistryLookup(404, {}, True),
        ]
    )

    with pytest.raises(KeyboardInterrupt):
        promotion.promote_certified_image(
            certification=certification,
            certification_sha256=f"sha256:{'1' * 64}",
            registry=adapter,
            journal=journal,
        )

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["state"] == "MUTATION_PENDING"
    assert payload["pending_mutation"]["reference"] == f"{IMAGE}:{VERSION}"
    assert payload["pending_mutation"]["expected_digest"] == DIGEST


def test_interruption_before_next_alias_keeps_completed_transition(tmp_path: Path) -> None:
    journal_module = importlib.import_module("tools.agent_policy.runtime_image_publication_journal")
    path = tmp_path / "runtime-image-publication.journal.json"
    journal = journal_module.PublicationJournal(
        path=path,
        candidate_digest=DIGEST,
        certification_sha256=f"sha256:{'1' * 64}",
    )

    class InterruptingRegistry(PromotionRegistry):
        def lookup(self, reference: str, *, include_version: bool = False) -> registry.RegistryLookup:
            if not self.lookups:
                raise KeyboardInterrupt
            return super().lookup(reference, include_version=include_version)

    certification = promotion.validate_certification(certification_payload())
    adapter = InterruptingRegistry(
        [
            registry.RegistryLookup(404, {}, True),
            registry.RegistryLookup(404, {}, True),
            registry.RegistryLookup(200, {"docker-content-digest": DIGEST}, True),
        ]
    )

    with pytest.raises(KeyboardInterrupt):
        promotion.promote_certified_image(
            certification=certification,
            certification_sha256=f"sha256:{'1' * 64}",
            registry=adapter,
            journal=journal,
        )

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["state"] == "IN_PROGRESS"
    assert payload["pending_mutation"] is None
    assert [item["role"] for item in payload["transitions"]] == ["version"]
