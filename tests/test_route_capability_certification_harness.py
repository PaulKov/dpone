from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.ops.route_capability_certification import (
    DEFAULT_BENCHMARK_ROUTES,
    CertificationRouteRequest,
    CertificationRouteRun,
    RouteCapabilityCertificationRequest,
    RouteCapabilityCertificationService,
    _object_prefix,
)
from dpone.ops.route_capability_certification_runner import (
    RunManifestRouteCertificationRunner,
    _apply_route_overrides,
)


def test_certification_runner_builds_safe_overrides_and_writes_artifacts(tmp_path: Path) -> None:
    runner = _FakeRouteRunner()
    request = RouteCapabilityCertificationRequest(
        manifest_path=tmp_path / "account_sales.yaml",
        scenario="work-item_account_sales_benchmark",
        output_dir=tmp_path / "out",
        run_id="run-42",
    )

    report = RouteCapabilityCertificationService(route_runner=runner).certify(request)

    assert report.passed is True
    assert report.preferred_route_id == "object_storage_pull_s3"
    assert runner.route_calls == list(DEFAULT_BENCHMARK_ROUTES)
    first = runner.requests[0]
    assert first.target_schema == "DWH_Raw"
    assert first.target_table == "__dpone_cert_work-item_account_sales_typed_raw_streaming"
    assert first.object_prefix == "s3://dpone-stage/certification/work-item/run-42/typed_raw_streaming/"
    assert report.artifact_index["certification_json"].endswith("certification.json")
    assert (request.output_dir / "certification.json").exists()
    assert (request.output_dir / "certification.md").exists()
    assert (request.output_dir / "route_decisions.jsonl").read_text(encoding="utf-8").count("\n") == 5
    assert json.loads((request.output_dir / "quality_report.json").read_text(encoding="utf-8"))["passed"] is True


def test_preflight_only_does_not_execute_source_route_when_blocked(tmp_path: Path) -> None:
    runner = _FakeRouteRunner(preflight_blockers=("sink.auth.named_collection_missing",))
    request = RouteCapabilityCertificationRequest(
        manifest_path=tmp_path / "manifest.yaml",
        scenario="preflight_only",
        output_dir=tmp_path / "out",
        run_id="run-preflight",
    )

    report = RouteCapabilityCertificationService(route_runner=runner).certify(request)

    assert report.passed is False
    assert runner.route_calls == []
    assert runner.preflight_calls == ["object_storage_pull_s3cluster", "object_storage_pull_s3", "direct_push_columnar"]
    assert "sink.auth.named_collection_missing" in report.blockers
    payload = json.loads((request.output_dir / "certification.json").read_text(encoding="utf-8"))
    assert payload["scenario"] == "preflight_only"
    assert payload["safe_overrides"]["source_io_started"] is False


def test_certification_object_prefix_inherits_manifest_object_storage_prefix(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(
        """
source:
  options:
    native_transfer:
      snapshot:
        columnar_fast_path:
          object_storage:
            uri_prefix: s3://example-data-bucket/dpone-stage/prod/mart/orders/{run_id}/
sink: {}
""",
        encoding="utf-8",
    )
    request = RouteCapabilityCertificationRequest(
        manifest_path=manifest,
        scenario="preflight_only",
        output_dir=tmp_path / "out",
        run_id="run-1",
    )

    assert _object_prefix(request, "object_storage_pull_s3") == (
        "s3://example-data-bucket/dpone-stage/prod/mart/orders/run-1/object_storage_pull_s3/"
    )


def test_production_preflight_runner_does_not_call_full_run_service(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text("source: {}\nsink: {}\n", encoding="utf-8")
    run_service = _RunServiceMustNotRun()
    monkeypatch.setattr(
        "dpone.ops.route_capability_certification_runner._run_preflight",
        lambda path, request: {"route_id": request.route_id, "result": {"errors": []}, "warnings": []},
    )

    result = RunManifestRouteCertificationRunner(run_service=run_service).preflight(
        CertificationRouteRequest(
            manifest_path=manifest,
            scenario="preflight_only",
            route_id="object_storage_pull_s3",
            run_id="run-1",
            target_schema="DWH_Raw",
            target_table="__dpone_cert_manifest_object_storage_pull_s3",
            object_prefix="s3://bucket/cert/run-1/object_storage_pull_s3/",
            keep_artifacts=False,
        )
    )

    assert result["passed"] is True
    assert run_service.run_calls == 0


def test_production_preflight_runner_records_hydration_exception_as_blocker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text("source: {}\nsink: {}\n", encoding="utf-8")
    monkeypatch.setattr(
        "dpone.ops.route_capability_certification_runner._run_preflight",
        lambda path, request: (_ for _ in ()).throw(RuntimeError("AIRFLOW_CONN_CLICKHOUSE is not set")),
    )

    result = RunManifestRouteCertificationRunner(run_service=_RunServiceMustNotRun()).preflight(
        CertificationRouteRequest(
            manifest_path=manifest,
            scenario="preflight_only",
            route_id="object_storage_pull_s3",
            run_id="run-1",
            target_schema="DWH_Raw",
            target_table="__dpone_cert_manifest_object_storage_pull_s3",
            object_prefix="s3://bucket/cert/run-1/object_storage_pull_s3/",
            keep_artifacts=False,
        )
    )

    assert result["passed"] is False
    assert result["blockers"] == ["preflight_failed:RuntimeError"]
    assert result["run_summary"]["result"]["route_capabilities"]["message"] == "AIRFLOW_CONN_CLICKHOUSE is not set"


def test_certification_route_overrides_enable_top_level_runtime_capabilities(tmp_path: Path) -> None:
    request = CertificationRouteRequest(
        manifest_path=tmp_path / "manifest.yaml",
        scenario="preflight_only",
        route_id="direct_push_columnar",
        run_id="run-1",
        target_schema="DWH_Raw",
        target_table="__dpone_cert_manifest_direct_push_columnar",
        object_prefix="s3://bucket/cert/run-1/direct_push_columnar/",
        keep_artifacts=False,
    )

    config = _apply_route_overrides(
        {
            "runtime": {"storage": {"profile": "worker_local"}},
            "source": {"options": {"native_transfer": {"snapshot": {}}}},
            "sink": {"options": {"clickhouse_bulk": {"mode": "http"}}},
        },
        request,
        preflight_only=True,
    )

    assert config["runtime"]["storage"]["profile"] == "worker_local"
    assert config["runtime"]["capabilities"] == {
        "mode": "required",
        "requested_route_id": "direct_push_columnar",
    }
    assert config["sink"]["options"]["runtime"]["capabilities"] == {
        "mode": "required",
        "requested_route_id": "direct_push_columnar",
    }
    assert config["sink"]["options"]["native_transfer"]["snapshot"]["columnar_fast_path"] == {
        "mode": "required",
        "provider": "direct_push_columnar",
    }
    assert config["source"]["options"]["native_transfer"]["snapshot"]["columnar_fast_path"] == {
        "mode": "required",
        "provider": "direct_push_columnar",
    }


def test_certification_route_overrides_preserve_object_storage_access_contracts(tmp_path: Path) -> None:
    request = CertificationRouteRequest(
        manifest_path=tmp_path / "manifest.yaml",
        scenario="preflight_only",
        route_id="object_storage_pull_s3",
        run_id="run-1",
        target_schema="DWH_Raw",
        target_table="__dpone_cert_manifest_object_storage_pull_s3",
        object_prefix="s3://bucket/cert/run-1/object_storage_pull_s3/",
        keep_artifacts=False,
    )

    config = _apply_route_overrides(
        {
            "source": {
                "options": {
                    "native_transfer": {
                        "snapshot": {
                            "columnar_fast_path": {
                                "mode": "auto",
                                "provider": "object_storage_pull",
                                "object_storage": {
                                    "uri_prefix": "s3://bucket/prod/mart/orders/{run_id}/",
                                    "runtime_access": {"connection_id": "s3_writer"},
                                    "clickhouse_read_access": {
                                        "mode": "named_collection",
                                        "named_collection": "dpone_stage",
                                    },
                                },
                            }
                        }
                    }
                }
            },
            "sink": {"options": {"clickhouse_bulk": {"columnar_pull": {"cluster": "dwh"}}}},
        },
        request,
        preflight_only=True,
    )

    object_storage = config["source"]["options"]["native_transfer"]["snapshot"]["columnar_fast_path"]["object_storage"]
    assert object_storage["uri_prefix"] == "s3://bucket/cert/run-1/object_storage_pull_s3/"
    assert object_storage["cleanup_policy"] == "eager"
    assert object_storage["runtime_access"]["connection_id"] == "s3_writer"
    assert object_storage["clickhouse_read_access"]["named_collection"] == "dpone_stage"


def test_quality_report_detects_null_distinct_hash_and_lineage_mismatches(tmp_path: Path) -> None:
    runner = _FakeRouteRunner(
        route_results={
            "object_storage_pull_s3": _run(
                "object_storage_pull_s3",
                source_quality={
                    "row_count": 10,
                    "null_counts": {"amount": 1},
                    "distinct_counts": {"status": 2},
                    "typed_hash": "source",
                },
                target_quality={
                    "row_count": 10,
                    "null_counts": {"amount": 0},
                    "distinct_counts": {"status": 3},
                    "typed_hash": "target",
                    "lineage_columns": ["__dpone__run_id"],
                    "cleanup": {"stale_artifacts": []},
                },
            )
        },
    )
    request = RouteCapabilityCertificationRequest(
        manifest_path=tmp_path / "manifest.yaml",
        scenario="small_live",
        output_dir=tmp_path / "out",
        routes=("object_storage_pull_s3",),
    )

    report = RouteCapabilityCertificationService(route_runner=runner).certify(request)

    assert report.passed is False
    assert set(report.blockers) >= {
        "object_storage_pull_s3.null_counts_mismatch",
        "object_storage_pull_s3.distinct_counts_mismatch",
        "object_storage_pull_s3.typed_hash_mismatch",
        "object_storage_pull_s3.lineage_columns_missing",
    }
    quality = json.loads((request.output_dir / "quality_report.json").read_text(encoding="utf-8"))
    assert quality["routes"][0]["checks"]["null_counts"]["passed"] is False
    assert quality["routes"][0]["checks"]["lineage_columns"]["missing"] == [
        "__dpone__load_id",
        "__dpone__loaded_at",
        "__dpone__extracted_at",
    ]


def test_certify_route_capabilities_cli_outputs_json_and_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "certify",
                "route-capabilities",
                "--manifest",
                str(tmp_path / "manifest.yaml"),
                "--scenario",
                "preflight_only",
                "--output",
                str(tmp_path / "out"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code in {0, 1}
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.route_capability_certification.v1"
    assert payload["artifact_index"]["certification_json"].endswith("certification.json")
    assert Path(payload["artifact_index"]["certification_json"]).exists()


def _run(
    route_id: str,
    *,
    passed: bool = True,
    duration_seconds: float = 10.0,
    source_quality: dict[str, object] | None = None,
    target_quality: dict[str, object] | None = None,
) -> CertificationRouteRun:
    source_quality = source_quality or {
        "row_count": 10,
        "null_counts": {"amount": 1},
        "distinct_counts": {"status": 2},
        "typed_hash": "hash",
    }
    target_quality = target_quality or {
        "row_count": 10,
        "null_counts": {"amount": 1},
        "distinct_counts": {"status": 2},
        "typed_hash": "hash",
        "lineage_columns": [
            "__dpone__run_id",
            "__dpone__load_id",
            "__dpone__loaded_at",
            "__dpone__extracted_at",
        ],
        "cleanup": {"stale_artifacts": []},
    }
    return CertificationRouteRun(
        route_id=route_id,
        passed=passed,
        run_summary={"route_capabilities": {"summary": {"selected_route_id": route_id}}},
        route_decisions=[{"selected_route_id": route_id, "secret": "must-redact"}],
        load_steps=[{"step_id": "staging_loaded", "duration_seconds": duration_seconds}],
        source_quality=source_quality,
        target_quality=target_quality,
        cleanup={"stale_artifacts": []},
        duration_seconds=duration_seconds,
    )


class _FakeRouteRunner:
    def __init__(
        self,
        *,
        preflight_blockers: tuple[str, ...] = (),
        route_results: dict[str, CertificationRouteRun] | None = None,
    ) -> None:
        self.preflight_blockers = preflight_blockers
        self.route_results = route_results or {}
        self.preflight_calls: list[str] = []
        self.route_calls: list[str] = []
        self.requests: list[object] = []

    def preflight(self, request):
        self.preflight_calls.append(request.route_id)
        return {
            "route_id": request.route_id,
            "passed": not self.preflight_blockers,
            "blockers": list(self.preflight_blockers),
            "warnings": [],
        }

    def run_route(self, request) -> CertificationRouteRun:
        self.route_calls.append(request.route_id)
        self.requests.append(request)
        return self.route_results.get(request.route_id) or _run(
            request.route_id,
            duration_seconds={"object_storage_pull_s3": 5.0}.get(request.route_id, 10.0),
        )


class _RunServiceMustNotRun:
    def __init__(self) -> None:
        self.run_calls = 0

    def run(self, *args, **kwargs):
        del args, kwargs
        self.run_calls += 1
        raise AssertionError("preflight must not call full manifest run")


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message
