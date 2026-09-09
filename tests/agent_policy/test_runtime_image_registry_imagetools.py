from __future__ import annotations

import hashlib
import json

import pytest
from tests.agent_policy._runtime_image_registry_helpers import (
    IMAGE,
    MEDIA_TYPES,
    HttpScript,
    Process,
    adapter,
    token_response,
)
from tools.agent_policy import runtime_image_registry as registry


@pytest.mark.parametrize("media_type", MEDIA_TYPES)
def test_imagetools_create_preserves_exact_source_digest_for_supported_media_types(
    media_type: str,
) -> None:
    commands: list[tuple[str, ...]] = []
    manifest = json.dumps(
        {"mediaType": media_type, "schemaVersion": 2},
        separators=(",", ":"),
    ).encode()
    source_digest = f"sha256:{hashlib.sha256(manifest).hexdigest()}"

    def runner(command: tuple[str, ...]) -> Process:
        commands.append(command)
        return Process(0)

    http = HttpScript(
        token_response(),
        registry.HttpResponse(
            200,
            {
                "Content-Type": media_type,
                "Docker-Content-Digest": source_digest,
            },
            manifest,
        ),
    )
    adapter_instance = adapter(http, runner=runner)

    result = adapter_instance.create_alias(f"{IMAGE}:0.73.2", source_digest)

    assert result.ok is True
    assert http.calls[1][0] == "GET"
    assert http.calls[1][1].endswith(f"/manifests/{source_digest}")
    assert http.calls[1][2]["Accept"].split(", ") == list(MEDIA_TYPES)
    assert commands == [
        (
            "docker",
            "buildx",
            "imagetools",
            "create",
            "--prefer-index=false",
            "--tag",
            f"{IMAGE}:0.73.2",
            f"{IMAGE}@{source_digest}",
        )
    ]


def test_imagetools_create_rejects_untrusted_source_manifest_before_runner() -> None:
    manifest = b'{"mediaType":"application/example","schemaVersion":2}'
    source_digest = f"sha256:{hashlib.sha256(manifest).hexdigest()}"
    commands: list[tuple[str, ...]] = []
    http = HttpScript(
        token_response(),
        registry.HttpResponse(
            200,
            {
                "Content-Type": "application/example",
                "Docker-Content-Digest": source_digest,
            },
            manifest,
        ),
    )

    result = adapter(http, runner=lambda command: commands.append(command)).create_alias(
        f"{IMAGE}:0.73.2",
        source_digest,
    )

    assert result.ok is False
    assert result.error_code == "SOURCE_MANIFEST_MEDIA_TYPE_UNSUPPORTED"
    assert commands == []


def test_imagetools_create_rejects_source_bytes_not_matching_digest() -> None:
    source_digest = f"sha256:{'d' * 64}"
    http = HttpScript(
        token_response(),
        registry.HttpResponse(
            200,
            {
                "Content-Type": MEDIA_TYPES[1],
                "Docker-Content-Digest": source_digest,
            },
            b'{"schemaVersion":2}',
        ),
    )

    result = adapter(http).create_alias(f"{IMAGE}:0.73.2", source_digest)

    assert result.ok is False
    assert result.error_code == "SOURCE_MANIFEST_DIGEST_MISMATCH"


def test_imagetools_failure_never_returns_subprocess_output() -> None:
    manifest = b'{"schemaVersion":2}'
    source_digest = f"sha256:{hashlib.sha256(manifest).hexdigest()}"
    adapter_instance = adapter(
        HttpScript(
            token_response(),
            registry.HttpResponse(
                200,
                {
                    "Content-Type": MEDIA_TYPES[1],
                    "Docker-Content-Digest": source_digest,
                },
                manifest,
            ),
        ),
        runner=lambda _: Process(1, "https://signed.example/?token=workflow-secret"),
    )

    result = adapter_instance.create_alias(f"{IMAGE}:0.73.2", source_digest)

    assert result.ok is False
    assert result.error_code == "IMAGETOOLS_CREATE_FAILED"
    assert "workflow-secret" not in repr(result)
