from __future__ import annotations

import json
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest
import yaml
from jsonschema import ValidationError
from jsonschema import validate as validate_json

from dpone.commands.studio_cmd import build_studio_handler
from dpone.contracts.capability_discovery import CapabilityDiscoverySnapshot, CapabilityIssue
from dpone.readiness import studio_composition, studio_legacy_operations, studio_ui_assets
from dpone.readiness.managed import ConnectorScaffoldService
from dpone.readiness.studio_api import StudioApiService
from dpone.readiness.studio_api_routes import studio_routes
from dpone.readiness.studio_composition import build_studio_api_service
from dpone.readiness.studio_entrypoint import studio_metadata
from dpone.readiness.studio_http_models import StudioApiError, StudioHttpConfig
from dpone.readiness.studio_ui_assets import (
    StudioUiAssetsStatus,
    probe_studio_ui_assets,
    validate_studio_ui_assets,
)


def _server():
    payload = ConnectorScaffoldService().studio_payload(host="127.0.0.1", port=0)
    server = ThreadingHTTPServer(("127.0.0.1", 0), build_studio_handler(payload))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    return server, thread, f"http://{host}:{port}"


def _get_json(base_url: str, path: str) -> dict:
    with urlopen(f"{base_url}{path}", timeout=5) as response:  # noqa: S310 - local test server
        return json.loads(response.read())


def _get_json_with_headers(base_url: str, path: str, headers: dict[str, str]) -> dict:
    request = Request(f"{base_url}{path}", headers=headers, method="GET")
    with urlopen(request, timeout=5) as response:  # noqa: S310 - local test server
        return json.loads(response.read())


def _post_json(base_url: str, path: str, payload: dict, *, headers: dict[str, str] | None = None) -> dict:
    request = Request(
        f"{base_url}{path}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", **(headers or {})},
        method="POST",
    )
    with urlopen(request, timeout=5) as response:  # noqa: S310 - local test server
        return json.loads(response.read())


def _error_payload(error: HTTPError) -> dict:
    return json.loads(error.read())


def _validate_component(payload: dict, spec: dict, component: str) -> None:
    validate_json(
        payload,
        {
            "$ref": f"#/components/schemas/{component}",
            "components": spec["components"],
        },
    )


def test_legacy_python_studio_facade_remains_usable_during_deprecation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.warns(DeprecationWarning, match="StudioApiService"):
        service = StudioApiService(artifact_roots=("artifacts",))

    assert service.artifact_roots == ("artifacts",)
    with pytest.warns(DeprecationWarning, match="openapi"):
        assert service.openapi()["openapi"] == "3.1.0"
    with pytest.warns(DeprecationWarning, match="certification_matrix"):
        assert "connectors" in service.certification_matrix()
    with pytest.warns(DeprecationWarning, match="record_audit"):
        service.record_audit(method="GET", path="/legacy", status=200)
    with pytest.warns(DeprecationWarning, match="capabilities"):
        legacy_capabilities = service.capabilities()
    assert "sources" in legacy_capabilities
    assert "schema" not in legacy_capabilities
    assert service.capability_snapshot()["schema"] == ("dpone.capability-discovery.v1")
    events = service.audit_events(limit=10)["events"]
    assert events[-1]["path"] == "/legacy"
    assert service.security_policy()["remote_enabled"] is False


def test_legacy_studio_http_types_remain_importable_from_studio_api() -> None:
    from dpone.readiness.studio_api import StudioApiError as LegacyStudioApiError
    from dpone.readiness.studio_api import StudioAuditLog as LegacyStudioAuditLog
    from dpone.readiness.studio_api import StudioHttpConfig as LegacyStudioHttpConfig
    from dpone.readiness.studio_http_models import StudioAuditLog

    assert LegacyStudioApiError is StudioApiError
    assert LegacyStudioAuditLog is StudioAuditLog
    assert LegacyStudioHttpConfig is StudioHttpConfig


def test_studio_ui_asset_probe_distinguishes_absent_compatible_and_incompatible(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(studio_ui_assets, "find_spec", lambda _name: None)
    assert probe_studio_ui_assets().status == "not_installed"
    package = tmp_path / "dpone_studio_assets"
    package.mkdir()
    (package / "index.html").write_text("<!doctype html>", encoding="utf-8")
    manifest = {
        "schema": "dpone.studio-assets.v1",
        "api_version": "v1",
        "package_version": "0.1.0",
        "entrypoint": "index.html",
    }
    (package / "dpone-studio-assets.json").write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )

    compatible = validate_studio_ui_assets(package, version="0.1.0")
    manifest["api_version"] = "v2"
    (package / "dpone-studio-assets.json").write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )
    incompatible = validate_studio_ui_assets(package, version="0.1.0")

    assert compatible == StudioUiAssetsStatus(
        status="installed",
        version="0.1.0",
        reason_code="ui_assets_compatible",
    )
    assert incompatible.status == "incompatible"
    assert incompatible.reason_code == "ui_protocol_incompatible"


def test_direct_studio_metadata_uses_the_canonical_route_registry() -> None:
    payload = ConnectorScaffoldService().studio_payload(
        host="127.0.0.1",
        port=8765,
    )

    assert payload["endpoints"] == [route.path for route in studio_routes() if not route.deprecated]


def test_studio_cli_and_api_use_the_same_ui_asset_projection(tmp_path: Path) -> None:
    ui_assets = StudioUiAssetsStatus(
        status="installed",
        version="0.1.7",
        reason_code="ui_assets_compatible",
    )

    cli_payload = studio_metadata(
        host="127.0.0.1",
        port=8765,
        ui_assets=ui_assets,
    )
    api_payload = build_studio_api_service(
        root=tmp_path,
        ui_assets=ui_assets,
    ).meta()

    assert {key: cli_payload[key] for key in ("ui_status", "ui_version", "ui_reason_code")} == {
        key: api_payload[key] for key in ("ui_status", "ui_version", "ui_reason_code")
    }
    assert api_payload["usability_status"] == "UNVERIFIED"
    assert api_payload["release_verdict"] == "NO-GO"


def test_studio_api_v1_exposes_openapi_health_and_capabilities() -> None:
    server, thread, base_url = _server()
    try:
        spec = _get_json(base_url, "/openapi.json")
        health = _get_json(base_url, "/healthz")
        capabilities = _get_json(base_url, "/api/v1/capabilities")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert spec["openapi"] == "3.1.0"
    assert "/api/v1/manifests/draft" in spec["paths"]
    assert "/api/manifests/draft" in spec["paths"]
    assert spec["paths"]["/api/manifests/draft"]["post"]["deprecated"] is True
    assert "/api/security/policy" in spec["paths"]
    assert "/api/reconciliation/preview" in spec["paths"]
    assert health == {"status": "ok", "service": "dpone-studio-api", "version": "v1"}
    assert capabilities["schema"] == "dpone.capability-discovery.v1"
    assert any(item["id"] == "postgres" for item in capabilities["connectors"])
    assert any(item["id"] == "clickhouse" for item in capabilities["connectors"])
    operation_ids = [operation["operationId"] for methods in spec["paths"].values() for operation in methods.values()]
    assert len(operation_ids) == len(set(operation_ids))


def test_studio_recipe_list_propagates_malformed_catalog_issue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "dpone.yaml").write_text(
        """
schema: dpone.project.v1
authoring:
  primary_source_policy: one_per_pipeline
  recipe_catalog:
    path: recipes/catalog.yaml
    trusted_catalog_ids: [data-platform]
""".lstrip(),
        encoding="utf-8",
    )
    catalog = tmp_path / "recipes" / "catalog.yaml"
    catalog.parent.mkdir()
    catalog.write_text("schema: [\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    server, thread, base_url = _server()
    try:
        spec = _get_json(base_url, "/openapi.json")
        payload = _get_json(base_url, "/api/v1/recipes")
        with pytest.raises(HTTPError) as draft:
            _post_json(
                base_url,
                "/api/v1/manifests/draft",
                {
                    "source_type": "mssql",
                    "sink_type": "clickhouse",
                    "strategy": "incremental_merge",
                },
            )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    _validate_component(payload, spec, "RecipeListResponse")
    assert payload["passed"] is False
    assert payload["issues"][0]["code"] == "DPONE_RECIPE_CATALOG_INVALID"
    assert draft.value.code == 422
    assert _error_payload(draft.value)["errors"][0]["code"] == "DPONE_RECIPE_CATALOG_INVALID"
    assert not (tmp_path / "pipelines").exists()


def test_studio_refreshes_capability_snapshot_for_each_use_case(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshots = (
        CapabilityDiscoverySnapshot(connectors=(), routes=(), recipes=()),
        CapabilityDiscoverySnapshot(
            connectors=(),
            routes=(),
            recipes=(),
            issues=(
                CapabilityIssue(
                    code="DPONE_CAPABILITY_EVIDENCE_STALE",
                    entity_kind="route_certification_evidence",
                    entity_id="matrix",
                    message="Evidence expired.",
                ),
            ),
        ),
    )
    calls = 0

    class _Service:
        def snapshot(self) -> CapabilityDiscoverySnapshot:
            nonlocal calls
            snapshot = snapshots[min(calls, len(snapshots) - 1)]
            calls += 1
            return snapshot

    monkeypatch.setattr(
        studio_composition,
        "build_capability_discovery_service",
        lambda **_kwargs: _Service(),
    )
    service = studio_composition.build_studio_api_service(root=tmp_path)

    first = service.capability_snapshot()
    second = service.capability_snapshot()

    assert first["issues"] == []
    assert second["issues"][0]["code"] == "DPONE_CAPABILITY_EVIDENCE_STALE"
    assert first["snapshot_id"] != second["snapshot_id"]


def test_studio_canonical_draft_rejects_unknown_or_missing_route_fields() -> None:
    server, thread, base_url = _server()
    try:
        with pytest.raises(HTTPError) as typo:
            _post_json(base_url, "/api/v1/manifests/draft", {"typo": True})
        with pytest.raises(HTTPError) as missing_strategy:
            _post_json(
                base_url,
                "/api/v1/manifests/draft",
                {"source_type": "postgres", "sink_type": "mssql"},
            )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert typo.value.code == 422
    assert _error_payload(typo.value)["errors"][0]["code"] == "DPONE_STUDIO_REQUEST_SCHEMA_INVALID"
    assert missing_strategy.value.code == 422
    assert _error_payload(missing_strategy.value)["errors"][0]["code"] == "DPONE_STUDIO_REQUEST_SCHEMA_INVALID"


def test_studio_draft_keeps_api_endpoint_family_separate_from_rest_provider() -> None:
    server, thread, base_url = _server()
    try:
        draft = _post_json(
            base_url,
            "/api/v1/manifests/draft",
            {
                "source_type": "api",
                "sink_type": "clickhouse",
                "strategy": "full_refresh",
            },
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    manifest = yaml.safe_load(draft["manifest_yaml"])
    assert draft["valid"] is True
    assert manifest["defaults"]["source"]["type"] == "api"


def test_studio_openapi_types_every_canonical_success_and_error_payload() -> None:
    server, thread, base_url = _server()
    draft_request = {
        "source_type": "postgres",
        "sink_type": "mssql",
        "strategy": "incremental_merge",
        "source_connection": "postgres_oltp",
        "sink_connection": "mssql_dwh",
        "source_schema": "public",
        "source_table": "orders",
        "target_schema": "landing",
        "target_table": "orders",
        "unique_key": "id",
        "quality_checks": ["min_rows"],
    }
    try:
        spec = _get_json(base_url, "/openapi.json")
        responses = {
            "OpenApiDocument": spec,
            "HealthResponse": _get_json(base_url, "/healthz"),
            "StudioMetaResponse": _get_json(base_url, "/api/v1/meta"),
            "CapabilityDiscoveryResponse": _get_json(base_url, "/api/v1/capabilities"),
            "RecipeListResponse": _get_json(base_url, "/api/v1/recipes"),
            "PipelineSummaryListResponse": _get_json(base_url, "/api/v1/pipelines"),
        }
        draft = _post_json(base_url, "/api/v1/manifests/draft", draft_request)
        responses["ManifestDraftResponse"] = draft
        responses["PlanResponse"] = _post_json(
            base_url,
            "/api/v1/plans",
            {"manifest_yaml": draft["manifest_yaml"]},
        )
        responses["StaticCheckResponse"] = _post_json(
            base_url,
            "/api/v1/checks/static",
            {"manifest_yaml": draft["manifest_yaml"]},
        )
        with pytest.raises(HTTPError) as invalid:
            _post_json(base_url, "/api/v1/manifests/draft", {"typo": True})
        error_payload = _error_payload(invalid.value)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    for component, response in responses.items():
        _validate_component(response, spec, component)
    _validate_component(error_payload, spec, "ErrorEnvelope")
    draft_schema = spec["components"]["schemas"]["ManifestDraftRequest"]
    assert draft_schema["additionalProperties"] is False
    assert set(draft_schema["required"]) == {"source_type", "sink_type", "strategy"}
    for name, schema in spec["components"]["schemas"].items():
        required = schema.get("required", [])
        if required:
            assert set(required).issubset(schema.get("properties", {})), name


def test_studio_plan_request_schema_and_runtime_reject_the_same_invalid_selector() -> None:
    server, thread, base_url = _server()
    try:
        spec = _get_json(base_url, "/openapi.json")
        valid = {
            "manifest_yaml": "kind: dpone.batch.v1\nprocesses: []\n",
            "selector": "public.orders",
        }
        _validate_component(valid, spec, "PlanRequest")
        invalid = {**valid, "selector": 7}
        with pytest.raises(ValidationError):
            _validate_component(invalid, spec, "PlanRequest")
        with pytest.raises(HTTPError) as runtime_error:
            _post_json(base_url, "/api/v1/plans", invalid)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert runtime_error.value.code == 422
    assert _error_payload(runtime_error.value)["errors"][0]["code"] == ("DPONE_STUDIO_REQUEST_SCHEMA_INVALID")


def test_studio_plan_and_static_check_reject_unknown_route_on_all_aliases() -> None:
    server, thread, base_url = _server()
    try:
        draft = _post_json(
            base_url,
            "/api/v1/manifests/draft",
            {
                "source_type": "postgres",
                "sink_type": "mssql",
                "strategy": "incremental_merge",
            },
        )
        manifest = yaml.safe_load(draft["manifest_yaml"])
        manifest["schemas"]["public"]["tables"][0] = {
            "table": "orders",
            "overrides": {
                "source": {
                    "type": "definitely_unknown",
                }
            },
        }
        manifest_yaml = yaml.safe_dump(manifest, sort_keys=False)
        errors = []
        for path in ("/api/v1/plans", "/api/plan", "/api/v1/checks/static"):
            with pytest.raises(HTTPError) as captured:
                _post_json(base_url, path, {"manifest_yaml": manifest_yaml})
            errors.append(captured.value)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert [error.code for error in errors] == [404, 404, 404]
    assert {_error_payload(error)["errors"][0]["code"] for error in errors} == {"DPONE_ROUTE_NOT_SUPPORTED"}


def test_studio_route_validation_uses_canonical_connector_defaults() -> None:
    server, thread, base_url = _server()
    try:
        draft = _post_json(
            base_url,
            "/api/v1/manifests/draft",
            {
                "source_type": "postgres",
                "sink_type": "mssql",
                "strategy": "incremental_merge",
            },
        )
        manifest = yaml.safe_load(draft["manifest_yaml"])
        manifest["defaults"]["source"].pop("type")
        manifest["defaults"]["sink"]["type"] = "definitely_unknown"
        manifest_yaml = yaml.safe_dump(manifest, sort_keys=False)
        errors = []
        for path in ("/api/v1/plans", "/api/plan", "/api/v1/checks/static"):
            with pytest.raises(HTTPError) as captured:
                _post_json(base_url, path, {"manifest_yaml": manifest_yaml})
            errors.append(captured.value)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert [error.code for error in errors] == [404, 404, 404]
    assert {_error_payload(error)["errors"][0]["code"] for error in errors} == {"DPONE_ROUTE_NOT_SUPPORTED"}


def test_legacy_studio_aliases_emit_deprecation_headers() -> None:
    server, thread, base_url = _server()
    try:
        with urlopen(f"{base_url}/api/connectors", timeout=5) as response:  # noqa: S310
            payload = json.loads(response.read())
            deprecation = response.headers["Deprecation"]
            sunset = response.headers["Sunset"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert payload["mode"] == "local_development_adapter"
    assert payload["auth"] == "local_only"
    assert payload["ui_status"] == "not_installed"
    assert deprecation == "true"
    assert sunset == "Fri, 23 Jul 2027 00:00:00 GMT"


def test_studio_api_v1_generates_manifest_plan_quality_and_gitops() -> None:
    server, thread, base_url = _server()
    draft_payload = {
        "source_type": "postgres",
        "sink_type": "mssql",
        "source_connection": "postgres_oltp",
        "sink_connection": "mssql_dwh",
        "source_schema": "public",
        "source_table": "orders",
        "target_schema": "landing",
        "target_table": "orders",
        "strategy": "incremental_merge",
        "unique_key": "event_id",
        "quality_checks": ["not_null", "unique", "source_target_count"],
    }
    try:
        spec = _get_json(base_url, "/openapi.json")
        draft = _post_json(base_url, "/api/v1/manifests/draft", draft_payload)
        plan = _post_json(
            base_url,
            "/api/v1/plans",
            {"manifest_yaml": draft["manifest_yaml"], "selector": "public.orders"},
        )
        quality = _post_json(
            base_url,
            "/api/quality/check",
            {
                "rows": [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Bob"}],
                "checks": [{"type": "unique", "columns": ["id"], "mode": "fail"}],
            },
        )
        gitops = _post_json(base_url, "/api/gitops/prepare", {"manifest_path": draft["manifest_path"]})
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert draft["valid"] is True
    manifest = yaml.safe_load(draft["manifest_yaml"])
    assert "checks" not in manifest["quality"]
    assert manifest["defaults"]["state"] == {
        "type": "mssql",
        "reuse": "sink",
        "atomicity": "target_atomic",
        "provisioning": "external",
        "table": {"name": "dpone_load_attempt"},
    }
    assert manifest["quality"]["gates"] == draft["quality_gates"]
    assert [gate["type"] for gate in draft["quality_gates"]] == [
        "not_null",
        "unique",
        "row_count_reconciliation",
    ]
    not_null_gate, unique_gate, _ = draft["quality_gates"]
    assert "column" not in not_null_gate
    assert "columns" not in unique_gate
    assert draft["quality_checks"][0]["column"] == "event_id"
    assert draft["quality_checks"][1]["columns"] == ["event_id"]
    assert any("not yet executable" in warning for warning in draft["warnings"])
    assert "event_id" in draft["manifest_yaml"]
    assert "row_count_reconciliation" in draft["manifest_yaml"]
    assert draft["commands"][0]["argv"][:3] == ["git", "switch", "-c"]
    assert draft["commands"][0]["argv"][3].startswith("codex/")
    assert plan["dry_run"] is True
    assert plan["bulk_path"] == "postgres_copy_to_mssql_bcp"
    _validate_component(draft, spec, "ManifestDraftResponse")
    _validate_component(plan, spec, "PlanResponse")
    assert quality["passed"] is True
    assert gitops["pull_request"]["draft"] is True
    assert any(command["argv"][:2] == ["dpone", "plan"] for command in gitops["commands"])


def test_studio_draft_runtime_matches_openapi_for_empty_quality_checks() -> None:
    server, thread, base_url = _server()
    payload = {
        "source_type": "postgres",
        "sink_type": "mssql",
        "strategy": "incremental_merge",
        "quality_checks": [],
    }
    try:
        spec = _get_json(base_url, "/openapi.json")
        with pytest.raises(HTTPError) as captured:
            _post_json(base_url, "/api/v1/manifests/draft", payload)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    quality_schema = spec["components"]["schemas"]["ManifestDraftRequest"]["properties"]["quality_checks"]
    assert quality_schema["minItems"] == 1
    assert captured.value.code == 422
    assert _error_payload(captured.value)["errors"][0]["code"] == "DPONE_STUDIO_REQUEST_SCHEMA_INVALID"


def test_studio_plan_rejects_absolute_certification_artifact_before_read() -> None:
    server, thread, base_url = _server()
    try:
        draft = _post_json(
            base_url,
            "/api/v1/manifests/draft",
            {
                "source_type": "mssql",
                "sink_type": "clickhouse",
                "strategy": "incremental_merge",
                "source_table": "orders",
                "unique_key": "id",
            },
        )
        manifest = yaml.safe_load(draft["manifest_yaml"])
        source_options = manifest["defaults"]["source"].setdefault("options", {})
        source_options["native_transfer"] = {
            "execution": {
                "certification": {
                    "mode": "certified_only",
                    "artifact": "/etc/passwd",
                }
            }
        }
        with pytest.raises(HTTPError) as captured:
            _post_json(
                base_url,
                "/api/v1/plans",
                {"manifest_yaml": yaml.safe_dump(manifest, sort_keys=False)},
            )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert captured.value.code == 422
    error = _error_payload(captured.value)["errors"][0]
    assert error["code"] == "DPONE_STUDIO_PLAN_ARTIFACT_UNSAFE"


def test_studio_plan_and_static_check_confine_effective_override_artifacts() -> None:
    server, thread, base_url = _server()
    try:
        draft = _post_json(
            base_url,
            "/api/v1/manifests/draft",
            {
                "source_type": "mssql",
                "sink_type": "clickhouse",
                "strategy": "incremental_merge",
                "source_table": "orders",
                "unique_key": "id",
            },
        )
        manifest = yaml.safe_load(draft["manifest_yaml"])
        manifest["schemas"]["public"]["tables"][0]["overrides"] = {
            "source": {
                "options": {
                    "native_transfer": {
                        "execution": {
                            "certification": {
                                "mode": "certified_only",
                                "artifact": "/etc/passwd",
                            }
                        }
                    }
                }
            }
        }
        manifest_yaml = yaml.safe_dump(manifest, sort_keys=False)
        errors = []
        for path in ("/api/v1/plans", "/api/plan", "/api/v1/checks/static"):
            with pytest.raises(HTTPError) as captured:
                _post_json(base_url, path, {"manifest_yaml": manifest_yaml})
            errors.append(captured.value)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert [error.code for error in errors] == [422, 422, 422]
    assert {_error_payload(error)["errors"][0]["code"] for error in errors} == {"DPONE_STUDIO_PLAN_ARTIFACT_UNSAFE"}


def test_missing_pipeline_explain_matches_openapi_and_offers_init() -> None:
    server, thread, base_url = _server()
    try:
        spec = _get_json(base_url, "/openapi.json")
        payload = _get_json(
            base_url,
            "/api/v1/pipelines/definitely_missing/explain",
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    _validate_component(payload, spec, "PipelineExplainResponse")
    assert payload["artifact_state"]["dag_spec"] == "unavailable"
    assert payload["next_actions"] == [
        {
            "id": "init_pipeline",
            "label": "Create the missing pipeline source.",
            "argv": [
                "dpone",
                "init",
                "pipeline",
                "definitely_missing",
            ],
        }
    ]
    assert all("command" not in fix for error in payload["errors"] for fix in error["fixes"])


def test_studio_api_v1_lists_run_artifacts_and_certification_matrix() -> None:
    server, thread, base_url = _server()
    try:
        runs = _get_json(base_url, "/api/runs")
        matrix = _get_json(base_url, "/api/certification/matrix")
        canonical = _get_json(base_url, "/api/v1/capabilities")
        state = _get_json(base_url, "/api/state/inspect?backend=postgres&state_type=xmin&identity=public.orders")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert runs["artifact_roots"]
    assert "postgres" in matrix["connectors"]
    assert matrix["levels"] == ["certified", "experimental", "community"]
    declarations = {item["id"]: set(item["capability_ids"]) for item in canonical["connectors"]}
    assert set(matrix["connectors"]) == set(declarations)
    assert all(set(matrix["connectors"][connector]) == capabilities for connector, capabilities in declarations.items())
    assert {result["status"] for capabilities in matrix["connectors"].values() for result in capabilities.values()} == {
        "unknown"
    }
    assert state["operation"] == "inspect"
    assert state["backend"] == "postgres"


def test_legacy_runs_are_bounded_and_cursor_paginated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_root = tmp_path / "test_artifacts"
    artifact_root.mkdir()
    for index in range(5):
        (artifact_root / f"{index:02}.md").write_text(f"report {index}\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    server, thread, base_url = _server()
    try:
        first = _get_json(base_url, "/api/runs?limit=2")
        second = _get_json(base_url, f"/api/runs?limit=2&cursor={first['next_cursor']}")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert [item["name"] for item in first["artifacts"]] == ["00.md", "01.md"]
    assert [item["name"] for item in second["artifacts"]] == ["02.md", "03.md"]
    assert first["discovered"] == 5
    assert first["truncated"] is False


def test_legacy_runs_stop_at_the_configured_scan_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_root = tmp_path / "test_artifacts"
    artifact_root.mkdir()
    for index in range(6):
        (artifact_root / f"{index:02}.txt").write_text("not a report\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(studio_legacy_operations, "_MAX_RUN_SCAN_ENTRIES", 3)
    server, thread, base_url = _server()
    try:
        result = _get_json(base_url, "/api/runs")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert result["artifacts"] == []
    assert result["truncated"] is True
    assert result["scanned_entries"] == 4


def test_studio_api_v1_exposes_maturity_management_contracts() -> None:
    server, thread, base_url = _server()
    try:
        security = _get_json(base_url, "/api/security/policy")
        audit = _get_json(base_url, "/api/audit/events")
        slo = _get_json(base_url, "/api/observability/slo")
        deploy = _get_json(base_url, "/api/deploy/guide")
        schema = _get_json(base_url, "/api/schema/explorer?source=postgres&sink=clickhouse")
        reconciliation = _post_json(
            base_url,
            "/api/reconciliation/preview",
            {"source_type": "postgres", "sink_type": "clickhouse", "unique_key": "id"},
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert security["mode"] == "local_only"
    assert security["rbac"] == "not_implemented"
    assert "events" in audit
    assert slo["slo"]["row_loss_tolerance"] == 0
    assert deploy["topology"] == "local_development_adapter"
    assert schema["generated_column_prefix"] == "__dpone__nc__"
    assert reconciliation["target_policy"].startswith("shadow-table")
    assert reconciliation["warnings"]


def test_studio_api_v1_optional_token_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DPONE_STUDIO_TOKEN", "test-token")
    server, thread, base_url = _server()
    try:
        health = _get_json(base_url, "/healthz")
        with pytest.raises(HTTPError) as exc:
            _get_json(base_url, "/api/security/policy")
        secured = _get_json_with_headers(
            base_url,
            "/api/security/policy",
            {"Authorization": "Bearer test-token"},
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert health["status"] == "ok"
    assert exc.value.code == 401
    assert secured["mode"] == "shared_token"


def test_studio_remote_config_requires_opt_in_token_and_exact_cors() -> None:
    with pytest.raises(StudioApiError) as disabled:
        StudioHttpConfig(host="0.0.0.0").validate()
    assert disabled.value.code == "DPONE_STUDIO_REMOTE_NOT_ALLOWED"

    with pytest.raises(StudioApiError) as missing_token:
        StudioHttpConfig(
            host="0.0.0.0",
            allow_remote=True,
            cors_origins=("https://studio.example",),
        ).validate()
    assert missing_token.value.code == "DPONE_STUDIO_REMOTE_TOKEN_REQUIRED"

    with pytest.raises(StudioApiError) as missing_cors:
        StudioHttpConfig(
            host="0.0.0.0",
            allow_remote=True,
            token="secret",
        ).validate()
    assert missing_cors.value.code == "DPONE_STUDIO_REMOTE_CORS_REQUIRED"

    StudioHttpConfig(
        host="0.0.0.0",
        allow_remote=True,
        token="secret",
        cors_origins=("https://studio.example",),
    ).validate()


def test_studio_rejects_unknown_route_method_media_type_and_string_boolean() -> None:
    server, thread, base_url = _server()
    try:
        with pytest.raises(HTTPError) as missing:
            _get_json(base_url, "/does-not-exist")
        request = Request(
            f"{base_url}/api/v1/meta",
            data=b"{}",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(HTTPError) as wrong_method:
            urlopen(request, timeout=5)  # noqa: S310 - local test server
        request = Request(
            f"{base_url}/api/v1/plans",
            data=b"{}",
            headers={"Content-Type": "text/plain"},
            method="POST",
        )
        with pytest.raises(HTTPError) as media_type:
            urlopen(request, timeout=5)  # noqa: S310 - local test server
        with pytest.raises(HTTPError) as boolean:
            _post_json(
                base_url,
                "/api/reconciliation/preview",
                {
                    "source_type": "postgres",
                    "sink_type": "clickhouse",
                    "apply_deletes": "false",
                },
            )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert missing.value.code == 404
    assert wrong_method.value.code == 405
    assert media_type.value.code == 415
    assert boolean.value.code == 422
