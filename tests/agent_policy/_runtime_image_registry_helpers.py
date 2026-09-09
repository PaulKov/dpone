from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tools.agent_policy import runtime_image_promotion as promotion
from tools.agent_policy import runtime_image_registry as registry

DIGEST = f"sha256:{'a' * 64}"
NEWER_DIGEST = f"sha256:{'b' * 64}"
IMAGE = "ghcr.io/paulkov/dpone-runtime"
ROOT = Path(__file__).resolve().parents[2]
VERSION = "0.73.2"
COMMIT_SHA = "c" * 40
MEDIA_TYPES = (
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.docker.distribution.manifest.v2+json",
)


def certification_payload() -> dict[str, Any]:
    return {
        "schema": promotion.CERTIFICATION_SCHEMA,
        "status": "PASS",
        "idempotency_key": f"{IMAGE}|{VERSION}|{DIGEST}",
        "release": {
            "version": VERSION,
            "tag": f"v{VERSION}",
            "source_commit_sha": COMMIT_SHA,
            "source_ref": f"refs/tags/v{VERSION}",
        },
        "subject": {"name": IMAGE, "digest": DIGEST, "platform": "linux/amd64"},
        "build": {
            "run_id": "12345",
            "run_attempt": "2",
            "dockerfile_sha256": f"sha256:{'d' * 64}",
            "dockerignore_sha256": f"sha256:{'e' * 64}",
            "context_manifest_sha256": f"sha256:{'f' * 64}",
        },
        "checks": [
            {"id": check_id, "status": "PASS", "subject_digest": DIGEST} for check_id in promotion.REQUIRED_CHECK_IDS
        ],
        "attestations": {kind: {"status": "PASS", "subject_digest": DIGEST} for kind in ("provenance", "sbom")},
    }


def validation_args(source: Path, source_flag: str = "--certification") -> list[str]:
    return [
        "validate-certification",
        source_flag,
        str(source),
        "--image",
        IMAGE,
        "--version",
        VERSION,
        "--digest",
        DIGEST,
        "--source-commit",
        COMMIT_SHA,
        "--source-ref",
        f"refs/tags/v{VERSION}",
    ]


def promotion_args(source: Path, output: Path) -> list[str]:
    return [
        "promote",
        "--certification",
        str(source),
        "--image",
        IMAGE,
        "--version",
        VERSION,
        "--digest",
        DIGEST,
        "--source-commit-sha",
        COMMIT_SHA,
        "--source-ref",
        f"refs/tags/v{VERSION}",
        "--actor",
        "release-engineer",
        "--token-env",
        "GHCR_TOKEN",
        "--output",
        str(output),
    ]


@dataclass(frozen=True)
class Process:
    returncode: int
    stdout: str = ""


class HttpScript:
    def __init__(self, *responses: registry.HttpResponse | Exception) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, str, dict[str, str]]] = []

    def __call__(self, method: str, url: str, headers: dict[str, str]) -> registry.HttpResponse:
        self.calls.append((method, url, headers))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class PromotionRegistry:
    def __init__(self, lookups: list[registry.RegistryLookup]) -> None:
        self.lookups = list(lookups)

    def lookup(self, reference: str, *, include_version: bool = False) -> registry.RegistryLookup:
        del reference, include_version
        return self.lookups.pop(0)

    def create_alias(self, reference: str, digest: str) -> registry.RegistryCommand:
        del reference, digest
        return registry.RegistryCommand(True)


def token_response() -> registry.HttpResponse:
    return registry.HttpResponse(200, {}, json.dumps({"token": "registry-bearer"}).encode())


def adapter(http: HttpScript, **overrides: Any) -> registry.AuthenticatedOciRegistry:
    return registry.AuthenticatedOciRegistry(
        image=IMAGE,
        actor="release-engineer",
        token="workflow-secret",
        requester=http,
        sleeper=overrides.pop("sleeper", lambda _: None),
        runner=overrides.pop("runner", lambda _: Process(0)),
        **overrides,
    )
