from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tools.agent_policy import runtime_image_promotion as promotion

ROOT = Path(__file__).resolve().parents[2]
DIGEST = f"sha256:{'a' * 64}"
NEWER_DIGEST = f"sha256:{'b' * 64}"
COMMIT_SHA = "c" * 40
IMAGE = "ghcr.io/paulkov/dpone-runtime"
VERSION = "0.73.2"


def certification_payload(
    *,
    version: str = VERSION,
    digest: str = DIGEST,
    commit_sha: str = COMMIT_SHA,
) -> dict[str, Any]:
    return {
        "schema": promotion.CERTIFICATION_SCHEMA,
        "status": "PASS",
        "idempotency_key": f"{IMAGE}|{version}|{digest}",
        "release": {
            "version": version,
            "tag": f"v{version}",
            "source_commit_sha": commit_sha,
            "source_ref": f"refs/tags/v{version}",
        },
        "subject": {
            "name": IMAGE,
            "digest": digest,
            "platform": "linux/amd64",
        },
        "build": {
            "run_id": "12345",
            "run_attempt": "2",
            "dockerfile_sha256": f"sha256:{'d' * 64}",
            "dockerignore_sha256": f"sha256:{'e' * 64}",
            "context_manifest_sha256": f"sha256:{'f' * 64}",
        },
        "checks": [
            {"id": check_id, "status": "PASS", "subject_digest": digest} for check_id in promotion.REQUIRED_CHECK_IDS
        ],
        "attestations": {kind: {"status": "PASS", "subject_digest": digest} for kind in ("provenance", "sbom")},
    }


def certification_identity() -> promotion.CertificationIdentity:
    return promotion.CertificationIdentity(
        image=IMAGE,
        version=VERSION,
        digest=DIGEST,
        source_commit_sha=COMMIT_SHA,
        source_ref=f"refs/tags/v{VERSION}",
    )


def publication_payload() -> dict[str, Any]:
    present = {"state": "PRESENT", "digest": DIGEST}
    return {
        "schema": "dpone.runtime-image-publication.v1",
        "status": "PASS",
        "outcome": "PUBLISHED",
        "candidate_digest": DIGEST,
        "certification_sha256": f"sha256:{'1' * 64}",
        "transitions": [
            {
                "role": "version",
                "reference": f"{IMAGE}:{VERSION}",
                "before": {"state": "ABSENT"},
                "decision": "CREATED",
                "after": present,
                "status": "PASS",
            },
            {
                "role": "sha",
                "reference": f"{IMAGE}:sha-{COMMIT_SHA[:12]}",
                "before": present,
                "decision": "NOOP_SAME",
                "after": present,
                "status": "PASS",
            },
            {
                "role": "latest",
                "reference": f"{IMAGE}:latest",
                "before": {"state": "ABSENT"},
                "decision": "CREATED",
                "after": {**present, "version": VERSION},
                "status": "PASS",
            },
        ],
    }


@dataclass(frozen=True)
class Lookup:
    status_code: int | None
    headers: dict[str, str]
    authenticated: bool
    error_code: str | None = None
    version: str | None = None


@dataclass(frozen=True)
class Command:
    ok: bool
    error_code: str | None = None


class RecordingJournal:
    def __init__(self, events: list[str] | None = None) -> None:
        self.events = events if events is not None else []
        self.transitions: list[dict[str, Any]] = []

    def prepare_mutation(self, **_: Any) -> None:
        self.events.append("journal:pending")

    def record_transition(self, transition: dict[str, Any]) -> None:
        self.events.append(f"journal:{transition['role']}")
        self.transitions.append(deepcopy(transition))

    def complete(self, receipt: promotion.PublicationReceipt) -> None:
        self.events.append("journal:complete")
        assert receipt.to_payload()["transitions"] == self.transitions


class FakeRegistry:
    def __init__(
        self,
        lookups: list[Lookup],
        commands: list[Command] | None = None,
        events: list[str] | None = None,
    ) -> None:
        self.lookups = list(lookups)
        self.commands = list(commands or [])
        self.created: list[tuple[str, str]] = []
        self.events = events

    def lookup(self, reference: str, *, include_version: bool = False) -> Lookup:
        if self.events is not None:
            self.events.append(f"lookup:{reference}")
        del include_version
        return self.lookups.pop(0)

    def create_alias(self, reference: str, digest: str) -> Command:
        if self.events is not None:
            self.events.append(f"create:{reference}")
        self.created.append((reference, digest))
        return self.commands.pop(0) if self.commands else Command(ok=True)
