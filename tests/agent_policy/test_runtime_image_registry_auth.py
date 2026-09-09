from __future__ import annotations

from tests.agent_policy._runtime_image_registry_helpers import (
    DIGEST,
    HttpScript,
    adapter,
    token_response,
)
from tools.agent_policy import runtime_image_registry as registry


def test_lookup_exchanges_secret_for_bearer_and_uses_authenticated_head() -> None:
    import base64

    http = HttpScript(
        token_response(),
        registry.HttpResponse(200, {"Docker-Content-Digest": DIGEST}, b""),
    )

    result = adapter(http).lookup("ghcr.io/paulkov/dpone-runtime:0.73.2")

    assert result.status_code == 200
    assert result.authenticated is True
    assert result.headers["docker-content-digest"] == DIGEST
    token_call, lookup_call = http.calls
    assert token_call[0] == "GET"
    expected_basic = base64.b64encode(b"release-engineer:workflow-secret").decode()
    assert token_call[2]["Authorization"] == f"Basic {expected_basic}"
    assert lookup_call[0] == "HEAD"
    assert lookup_call[2]["Authorization"] == "Bearer registry-bearer"
    assert lookup_call[2]["Accept"].split(", ") == [
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    ]


def test_authenticated_404_is_preserved_for_pure_classification() -> None:
    http = HttpScript(token_response(), registry.HttpResponse(404, {}, b""))

    result = adapter(http).lookup("ghcr.io/paulkov/dpone-runtime:missing")

    assert result.status_code == 404
    assert result.authenticated is True
    assert result.error_code is None


def test_retry_is_bounded_and_honors_capped_retry_after() -> None:
    delays: list[float] = []
    http = HttpScript(
        registry.HttpResponse(429, {"Retry-After": "999"}, b""),
        token_response(),
        registry.HttpResponse(503, {}, b""),
        registry.HttpResponse(200, {"Docker-Content-Digest": DIGEST}, b""),
    )

    result = adapter(http, sleeper=delays.append, max_attempts=2, retry_after_cap=3.0).lookup(
        "ghcr.io/paulkov/dpone-runtime:0.73.2"
    )

    assert result.status_code == 200
    assert delays == [3.0, 0.25]
    assert len(http.calls) == 4


def test_non_retryable_auth_failure_fails_without_secret_leak() -> None:
    http = HttpScript(registry.HttpResponse(401, {}, b"workflow-secret in body"))

    result = adapter(http).lookup("ghcr.io/paulkov/dpone-runtime:0.73.2")

    assert result.error_code == "REGISTRY_AUTH_FAILED"
    assert result.authenticated is False
    assert len(http.calls) == 1
    assert "workflow-secret" not in repr(result)


def test_transport_retry_exhaustion_is_sanitized() -> None:
    http = HttpScript(OSError("signed-url?secret=workflow-secret"), OSError("again"))

    result = adapter(http, max_attempts=2).lookup("ghcr.io/paulkov/dpone-runtime:0.73.2")

    assert result.error_code == "REGISTRY_TRANSPORT_ERROR"
    assert "workflow-secret" not in repr(result)
