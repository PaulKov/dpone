from __future__ import annotations

from tests.agent_policy._runtime_image_registry_helpers import (
    DIGEST,
    IMAGE,
    HttpScript,
    Process,
    adapter,
    token_response,
)
from tools.agent_policy import runtime_image_registry as registry


def test_latest_lookup_reads_version_from_observed_digest_not_mutable_tag() -> None:
    commands: list[tuple[str, ...]] = []

    def runner(command: tuple[str, ...]) -> Process:
        commands.append(command)
        return Process(0, '{"org.opencontainers.image.version":"0.74.0"}\n')

    http = HttpScript(
        token_response(),
        registry.HttpResponse(200, {"Docker-Content-Digest": DIGEST}, b""),
    )

    result = adapter(http, runner=runner).lookup(f"{IMAGE}:latest", include_version=True)

    assert result.version == "0.74.0"
    assert commands == [
        (
            "docker",
            "buildx",
            "imagetools",
            "inspect",
            f"{IMAGE}@{DIGEST}",
            "--format",
            "{{json .Image.Config.Labels}}",
        )
    ]


def test_missing_or_invalid_latest_version_metadata_fails_closed() -> None:
    for stdout in ("{}", "not-json", '{"org.opencontainers.image.version":"0.73.2-rc.1"}'):
        http = HttpScript(
            token_response(),
            registry.HttpResponse(200, {"Docker-Content-Digest": DIGEST}, b""),
        )
        adapter_instance = adapter(http, runner=lambda _: Process(0, stdout))

        result = adapter_instance.lookup(f"{IMAGE}:latest", include_version=True)

        assert result.error_code == "VERSION_METADATA_INVALID"
