from __future__ import annotations

import http.client
import json
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import jsonschema
import pytest

from dpone.adapters.studio_http import (
    StudioThreadingHTTPServer,
    build_studio_http_handler,
)
from dpone.readiness.studio_api import StudioApplicationService
from dpone.readiness.studio_api_routes import studio_routes
from dpone.readiness.studio_composition import build_studio_api_service
from dpone.readiness.studio_entrypoint import build_studio_router, studio_metadata
from dpone.readiness.studio_http_models import (
    StudioApiError,
    StudioAuditLog,
    StudioHttpConfig,
)
from dpone.readiness.studio_openapi import studio_openapi_document


def _server(
    root: Path,
    *,
    config: StudioHttpConfig | None = None,
    audit: StudioAuditLog | None = None,
    service: StudioApplicationService | None = None,
) -> tuple[StudioThreadingHTTPServer, threading.Thread, str, StudioAuditLog]:
    audit_log = audit or StudioAuditLog()
    effective_config = config or StudioHttpConfig()
    application = service or build_studio_api_service(root=root, audit=audit_log)
    router = build_studio_router(
        application,
        config=effective_config,
        legacy_studio_metadata=studio_metadata(host="127.0.0.1", port=0),
    )
    handler = build_studio_http_handler(
        router=router,
        config=effective_config,
        audit=audit_log,
    )
    server = StudioThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    return server, thread, f"http://{host}:{port}", audit_log


def _close(server: StudioThreadingHTTPServer, thread: threading.Thread) -> None:
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


def _json_request(
    base_url: str,
    path: str,
    *,
    method: str = "GET",
    payload: object | None = None,
    headers: dict[str, str] | None = None,
) -> dict:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = Request(
        f"{base_url}{path}",
        data=data,
        headers=(
            {
                **({"Content-Type": "application/json"} if data is not None else {}),
                **(headers or {}),
            }
        ),
        method=method,
    )
    with urlopen(request, timeout=5) as response:  # noqa: S310 - local test server
        return json.loads(response.read())


def _error_payload(error: HTTPError) -> dict:
    return json.loads(error.read())


def test_openapi_paths_and_methods_match_the_route_registry() -> None:
    document = studio_openapi_document()
    actual = {(path, method.upper()) for path, methods in document["paths"].items() for method in methods}
    expected = {(route.path, route.method) for route in studio_routes()}

    assert actual == expected


def test_generated_openapi_artifact_is_current_and_describes_api_limits() -> None:
    document = studio_openapi_document()
    generated = json.loads(Path("docs/api/studio-openapi.json").read_text(encoding="utf-8"))

    assert generated == document
    assert "408" in document["paths"]["/api/v1/plans"]["post"]["responses"]
    parameters = document["paths"]["/api/v1/pipelines"]["get"]["parameters"]
    limit = next(item for item in parameters if item["name"] == "limit")
    assert limit["schema"]["minimum"] == 1
    assert limit["schema"]["maximum"] == 200
    assert document["components"]["securitySchemes"]["bearerAuth"]["scheme"] == "bearer"
    assert document["x-dpone-auth-policy"] == {
        "loopback": "local_operator",
        "non_loopback": "bearer_required",
    }
    assert document["paths"]["/healthz"]["get"]["security"] == []
    assert document["paths"]["/api/v1/meta"]["get"]["security"] == []
    assert document["components"]["schemas"]["PlanResponse"]["additionalProperties"] is False
    assert document["components"]["schemas"]["StaticCheckResponse"]["additionalProperties"] is False
    legacy_headers = document["paths"]["/api/studio"]["get"]["responses"]["200"]["headers"]
    assert set(legacy_headers) == {"Deprecation", "Sunset", "Link"}
    canonical_capability = json.loads(
        Path("src/dpone/schema/capability-discovery.schema.json").read_text(encoding="utf-8")
    )
    canonical_pipeline = json.loads(Path("src/dpone/schema/pipeline-summary.schema.json").read_text(encoding="utf-8"))
    assert (
        document["components"]["schemas"]["CapabilityDiscoveryResponse"]["required"]
        == (canonical_capability["required"])
    )
    assert document["components"]["schemas"]["PipelineSummary"]["required"] == canonical_pipeline["required"]


def test_token_profile_openapi_requires_bearer_and_wrong_method_stays_405(
    tmp_path: Path,
) -> None:
    config = StudioHttpConfig(token="test-token")
    server, thread, base_url, _ = _server(tmp_path, config=config)
    try:
        openapi = _json_request(base_url, "/openapi.json")
        with pytest.raises(HTTPError) as wrong_method:
            _json_request(base_url, "/healthz", method="POST", payload={})
    finally:
        _close(server, thread)

    assert openapi["x-dpone-auth-policy"]["loopback"] == "bearer_required"
    assert openapi["paths"]["/api/v1/meta"]["get"]["security"] == [{"bearerAuth": []}]
    assert wrong_method.value.code == 405
    assert _error_payload(wrong_method.value)["errors"][0]["code"] == "DPONE_STUDIO_METHOD_NOT_ALLOWED"


@pytest.mark.parametrize("method", ["HEAD", "TRACE", "CONNECT", "PROPFIND"])
def test_all_unsupported_http_methods_are_structured_and_audited_once(
    tmp_path: Path,
    method: str,
) -> None:
    audit = StudioAuditLog()
    server, thread, base_url, _ = _server(tmp_path, audit=audit)
    parsed = urlparse(base_url)
    connection = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=5)
    try:
        connection.request(method, "/healthz")
        response = connection.getresponse()
        body = response.read()
    finally:
        connection.close()
        _close(server, thread)

    assert response.status == 405
    assert response.getheader("Content-Type") == "application/json; charset=utf-8"
    if method != "HEAD":
        assert json.loads(body)["errors"][0]["code"] == "DPONE_STUDIO_METHOD_NOT_ALLOWED"
    else:
        assert body == b""
    events = audit.snapshot(limit=10)["events"]
    assert len(events) == 1
    assert events[0]["method"] == method
    assert events[0]["status"] == 405


def test_malformed_http_request_is_structured_and_audited_once(tmp_path: Path) -> None:
    audit = StudioAuditLog()
    server, thread, _, _ = _server(tmp_path, audit=audit)
    host, port = server.server_address
    try:
        with socket.create_connection((host, port), timeout=5) as connection:
            connection.sendall(b"GET /healthz HTTP/INVALID\r\n\r\n")
            chunks: list[bytes] = []
            while chunk := connection.recv(8192):
                chunks.append(chunk)
            response = b"".join(chunks)
    finally:
        _close(server, thread)

    _, body = response.split(b"\r\n\r\n", maxsplit=1)
    assert response.startswith(b"HTTP/1.0 400")
    assert b"Content-Type: application/json; charset=utf-8" in response
    assert json.loads(body)["errors"][0]["code"] == "DPONE_STUDIO_HTTP_REQUEST_INVALID"
    events = audit.snapshot(limit=10)["events"]
    assert len(events) == 1
    assert events[0]["method"] == "INVALID"
    assert events[0]["status"] == 400


def test_openapi_capability_response_schema_validates_runtime_payload(tmp_path: Path) -> None:
    server, thread, base_url, _ = _server(tmp_path)
    try:
        payload = _json_request(base_url, "/api/v1/capabilities")
    finally:
        _close(server, thread)

    document = studio_openapi_document()
    jsonschema.validate(
        payload,
        {
            "$ref": "#/components/schemas/CapabilityDiscoveryResponse",
            "components": document["components"],
        },
    )


def test_body_limit_cors_and_path_confinement_fail_closed(tmp_path: Path) -> None:
    server, thread, base_url, _ = _server(tmp_path)
    try:
        host, port = server.server_address
        connection = http.client.HTTPConnection(host, port, timeout=5)
        connection.putrequest("POST", "/api/v1/plans")
        connection.putheader("Content-Type", "application/json")
        connection.putheader("Content-Length", str(1024 * 1024 + 1))
        connection.endheaders()
        oversized_response = connection.getresponse()
        oversized_status = oversized_response.status
        oversized_payload = json.loads(oversized_response.read())
        connection.close()

        with pytest.raises(HTTPError) as origin:
            _json_request(
                base_url,
                "/api/v1/meta",
                headers={"Origin": "https://evil.example"},
            )

        with pytest.raises(HTTPError) as traversal:
            _json_request(
                base_url,
                "/api/v1/manifests/draft",
                method="POST",
                payload={
                    "source_type": "postgres",
                    "sink_type": "mssql",
                    "strategy": "full_refresh",
                    "manifest_path": "../../outside.batch.yaml",
                },
            )

        outside = tmp_path.parent / f"{tmp_path.name}-outside"
        outside.mkdir()
        (tmp_path / "escape").symlink_to(outside, target_is_directory=True)
        with pytest.raises(HTTPError) as symlink:
            _json_request(
                base_url,
                "/api/gitops/prepare",
                method="POST",
                payload={"manifest_path": "escape/pipeline.batch.yaml"},
            )

        oversized_manifest = tmp_path / "oversized.batch.yaml"
        oversized_manifest.write_bytes(b"a" * (1024 * 1024 + 1))
        with pytest.raises(HTTPError) as oversized_path:
            _json_request(
                base_url,
                "/api/v1/plans",
                method="POST",
                payload={"manifest_path": oversized_manifest.name},
            )
    finally:
        _close(server, thread)

    assert oversized_status == 413
    assert oversized_payload["errors"][0]["code"] == "DPONE_STUDIO_BODY_TOO_LARGE"
    assert origin.value.code == 403
    assert traversal.value.code == 403
    assert symlink.value.code == 403
    assert oversized_path.value.code == 413


def test_body_at_exact_limit_is_not_rejected_as_oversized(tmp_path: Path) -> None:
    server, thread, _, _ = _server(tmp_path)
    try:
        host, port = server.server_address
        prefix = b'{"padding":"'
        suffix = b'"}'
        payload = prefix + (b"a" * (1024 * 1024 - len(prefix) - len(suffix))) + suffix
        connection = http.client.HTTPConnection(host, port, timeout=5)
        connection.request(
            "POST",
            "/api/v1/manifests/draft",
            body=payload,
            headers={
                "Content-Type": "application/json",
                "Content-Length": str(len(payload)),
            },
        )
        response = connection.getresponse()
        status = response.status
        response.read()
        connection.close()
    finally:
        _close(server, thread)

    assert len(payload) == 1024 * 1024
    assert status != 413


def test_exact_same_origin_is_echoed_without_wildcard(tmp_path: Path) -> None:
    server, thread, base_url, _ = _server(tmp_path)
    try:
        request = Request(
            f"{base_url}/api/v1/meta",
            headers={"Origin": base_url},
            method="GET",
        )
        with urlopen(request, timeout=5) as response:  # noqa: S310 - local test server
            assert response.headers["Access-Control-Allow-Origin"] == base_url
            assert response.headers["Access-Control-Allow-Origin"] != "*"
    finally:
        _close(server, thread)


def test_loopback_rejects_untrusted_host_and_redacts_unmatched_audit_path(
    tmp_path: Path,
) -> None:
    audit = StudioAuditLog()
    server, thread, _, _ = _server(tmp_path, audit=audit)
    try:
        host, port = server.server_address
        connection = http.client.HTTPConnection(host, port, timeout=5)
        connection.putrequest("GET", "/healthz", skip_host=True)
        connection.putheader("Host", "attacker.example")
        connection.endheaders()
        response = connection.getresponse()
        host_status = response.status
        response.read()
        connection.close()

        connection = http.client.HTTPConnection(host, port, timeout=5)
        connection.request("GET", "/missing/vault-token-super-secret")
        response = connection.getresponse()
        missing_status = response.status
        response.read()
        connection.close()
    finally:
        _close(server, thread)

    events = audit.snapshot(limit=10)["events"]
    assert host_status == 403
    assert missing_status == 404
    assert all("vault-token-super-secret" not in event["path"] for event in events)
    assert events[-1]["path"] == "/<unmatched>"


def test_malformed_encoded_path_returns_structured_404_and_audit_event(
    tmp_path: Path,
) -> None:
    audit = StudioAuditLog()
    server, thread, base_url, _ = _server(tmp_path, audit=audit)
    try:
        with pytest.raises(HTTPError) as captured:
            _json_request(base_url, "/%00")
    finally:
        _close(server, thread)

    assert captured.value.code == 404
    assert _error_payload(captured.value)["errors"][0]["code"] == "DPONE_STUDIO_ROUTE_INVALID"
    events = audit.snapshot(limit=10)["events"]
    assert len(events) == 1
    assert events[0]["path"] == "/<invalid>"


def test_loopback_localhost_host_header_is_accepted(tmp_path: Path) -> None:
    server, thread, _, _ = _server(
        tmp_path,
        config=StudioHttpConfig(host="localhost"),
    )
    host, port = server.server_address
    connection = http.client.HTTPConnection(host, port, timeout=5)
    try:
        connection.request("GET", "/healthz", headers={"Host": f"localhost:{port}"})
        response = connection.getresponse()
        payload = json.loads(response.read())
    finally:
        connection.close()
        _close(server, thread)

    assert response.status == 200
    assert payload["status"] == "ok"


@pytest.mark.parametrize(
    "origin",
    (
        "https://studio.example?tenant=x",
        "https://user:password@studio.example",
        "https://studio.example#fragment",
        "https://"  # Preserve the URI scheme boundary for exact privacy review.
        "studio.example\r\nX-Injected: true",
    ),
)
def test_remote_cors_allowlist_requires_exact_origin(origin: str) -> None:
    with pytest.raises(StudioApiError) as captured:
        StudioHttpConfig(
            host="0.0.0.0",
            allow_remote=True,
            token="secret",
            cors_origins=(origin,),
        ).validate()

    assert captured.value.code == "DPONE_STUDIO_CORS_ORIGIN_INVALID"


def test_cors_response_origin_never_reflects_untrusted_request_input() -> None:
    local = StudioHttpConfig()
    assert (
        local.response_origin(
            "http://127.0.0.1:8790",
            "127.0.0.1:8790",
        )
        == "http://127.0.0.1:8790"
    )
    assert (
        local.response_origin(
            "http://127.0.0.1:8790\r\nX-Injected: true",
            "127.0.0.1:8790",
        )
        is None
    )

    remote = StudioHttpConfig(
        host="0.0.0.0",
        allow_remote=True,
        token="secret",
        cors_origins=("https://studio.example",),
    )
    assert remote.response_origin("https://studio.example", "0.0.0.0:8790") == "https://studio.example"


def test_malformed_json_and_invalid_quality_shape_use_400_and_422(
    tmp_path: Path,
) -> None:
    server, thread, base_url, _ = _server(tmp_path)
    try:
        request = Request(
            f"{base_url}/api/v1/plans",
            data=b"{",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(HTTPError) as malformed:
            urlopen(request, timeout=5)  # noqa: S310 - local test server

        with pytest.raises(HTTPError) as invalid_quality:
            _json_request(
                base_url,
                "/api/quality/check",
                method="POST",
                payload={"rows": [1], "checks": []},
            )

        with pytest.raises(HTTPError) as ambiguous_plan:
            _json_request(
                base_url,
                "/api/v1/plans",
                method="POST",
                payload={
                    "manifest_path": "pipeline.yaml",
                    "manifest_yaml": "kind: dpone.batch.v1",
                },
            )
    finally:
        _close(server, thread)

    assert malformed.value.code == 400
    assert _error_payload(malformed.value)["errors"][0]["code"] == "DPONE_STUDIO_JSON_INVALID"
    assert invalid_quality.value.code == 422
    assert _error_payload(invalid_quality.value)["errors"][0]["code"] == ("DPONE_STUDIO_QUALITY_INPUT_INVALID")
    assert ambiguous_plan.value.code == 422


def test_authenticated_request_errors_keep_audit_actor(tmp_path: Path) -> None:
    audit = StudioAuditLog()
    server, thread, base_url, _ = _server(
        tmp_path,
        audit=audit,
        config=StudioHttpConfig(token="secret"),
    )
    request = Request(
        f"{base_url}/api/v1/plans",
        data=b"{",
        headers={
            "Authorization": "Bearer secret",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with pytest.raises(HTTPError) as captured:
            urlopen(request, timeout=5)  # noqa: S310 - local test server
    finally:
        _close(server, thread)

    assert captured.value.code == 400
    assert audit.snapshot(limit=10)["events"][-1]["actor"] == "shared_token"


def test_timeout_returns_before_blocked_application_finishes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = threading.Event()

    def blocked_health(_self: StudioApplicationService) -> dict[str, str]:
        release.wait(timeout=2)
        return {"status": "ok", "service": "dpone-studio-api", "version": "v1"}

    monkeypatch.setattr(StudioApplicationService, "health", blocked_health)
    server, thread, base_url, _ = _server(
        tmp_path,
        config=StudioHttpConfig(request_timeout_seconds=0.05),
    )
    started = time.monotonic()
    try:
        with pytest.raises(HTTPError) as timed_out:
            _json_request(base_url, "/healthz")
        elapsed = time.monotonic() - started
    finally:
        release.set()
        _close(server, thread)

    assert timed_out.value.code == 408
    assert elapsed < 0.5


def test_one_hundred_concurrent_requests_create_ordered_audit_events(tmp_path: Path) -> None:
    audit = StudioAuditLog()
    server, thread, base_url, _ = _server(tmp_path, audit=audit)
    try:
        with ThreadPoolExecutor(max_workers=20) as executor:
            results = list(
                executor.map(
                    lambda _: _json_request(base_url, "/healthz"),
                    range(100),
                )
            )
    finally:
        _close(server, thread)

    events = audit.snapshot(limit=200)["events"]
    assert all(item["status"] == "ok" for item in results)
    assert len(events) == 100
    assert [event["sequence"] for event in events] == list(range(1, 101))


def test_thirty_third_in_flight_request_is_rejected_and_capacity_recovers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = 0
    entered_lock = threading.Lock()
    all_entered = threading.Event()
    release = threading.Event()

    def blocking_health(_self: StudioApplicationService) -> dict[str, str]:
        nonlocal entered
        with entered_lock:
            entered += 1
            if entered == 32:
                all_entered.set()
        release.wait(timeout=5)
        return {"status": "ok", "service": "dpone-studio-api", "version": "v1"}

    monkeypatch.setattr(StudioApplicationService, "health", blocking_health)
    server, thread, base_url, _ = _server(tmp_path)
    try:
        with ThreadPoolExecutor(max_workers=32) as executor:
            pending = [executor.submit(_json_request, base_url, "/healthz") for _ in range(32)]
            assert all_entered.wait(timeout=5)

            with pytest.raises(HTTPError) as rejected:
                _json_request(base_url, "/healthz")

            release.set()
            assert all(future.result()["status"] == "ok" for future in pending)
            assert _json_request(base_url, "/healthz")["status"] == "ok"
    finally:
        release.set()
        _close(server, thread)

    assert rejected.value.code == 429
    assert _error_payload(rejected.value)["errors"][0]["code"] == "DPONE_STUDIO_CONCURRENCY_LIMIT"


def test_timeout_and_internal_failure_do_not_leak_exception_text(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = build_studio_api_service(root=tmp_path)

    def slow_health(_self: StudioApplicationService) -> dict[str, str]:
        time.sleep(0.03)
        return {"status": "ok"}

    monkeypatch.setattr(StudioApplicationService, "health", slow_health)
    server, thread, base_url, _ = _server(
        tmp_path,
        service=service,
        config=StudioHttpConfig(request_timeout_seconds=0.01),
    )
    try:
        with pytest.raises(HTTPError) as timed_out:
            _json_request(base_url, "/healthz")
    finally:
        _close(server, thread)

    assert timed_out.value.code == 408
    assert _error_payload(timed_out.value)["errors"][0]["code"] == "DPONE_STUDIO_REQUEST_TIMEOUT"

    def leaking_plan(
        _self: StudioApplicationService,
        _payload: object,
    ) -> dict[str, object]:
        raise RuntimeError("vault-token-super-secret")

    monkeypatch.setattr(StudioApplicationService, "plan", leaking_plan)
    server, thread, base_url, _ = _server(tmp_path, service=service)
    try:
        with pytest.raises(HTTPError) as internal:
            _json_request(
                base_url,
                "/api/v1/plans",
                method="POST",
                payload={"manifest_yaml": "kind: invalid"},
            )
    finally:
        _close(server, thread)

    body = internal.value.read().decode("utf-8")
    assert internal.value.code == 500
    assert "vault-token-super-secret" not in body
    assert "DPONE_STUDIO_INTERNAL_ERROR" in body
