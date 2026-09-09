from __future__ import annotations

import json
from pathlib import Path

from dpone.integration_matrix import DEFAULT_INTEGRATION_MATRIX
from dpone.ops.route_readiness import RouteReadinessService
from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.ops.routes.evidence import RouteEvidenceReader
from dpone.ops.routes.models import RouteKey, RouteProfile
from dpone.ops.routes.policy import RouteReadinessPolicy


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_route_key_normalizes_and_renders_canonical_ids() -> None:
    key = RouteKey.of(" Postgres ", " MSSQL ", " Incremental_Merge ")

    assert key.source == "postgres"
    assert key.sink == "mssql"
    assert key.strategy == "incremental_merge"
    assert key.pair_id == "postgres_to_mssql"
    assert key.case_id == "postgres_to_mssql__incremental_merge"
    assert key.colon_id == "postgres:mssql:incremental_merge"
    assert key.to_dict() == {
        "source": "postgres",
        "sink": "mssql",
        "strategy": "incremental_merge",
        "pair_id": "postgres_to_mssql",
        "case_id": "postgres_to_mssql__incremental_merge",
        "colon_id": "postgres:mssql:incremental_merge",
    }


def test_catalog_builds_route_profile_from_matrix_and_strategy_certification() -> None:
    profile = RouteProfileCatalog.default().get(RouteKey.of("postgres", "mssql", "incremental_merge"))

    assert profile.key.case_id == "postgres_to_mssql__incremental_merge"
    assert profile.docs_link == "docs/source-sink/postgres-to-mssql.md"
    assert profile.install_extras == ("postgres", "mssql")
    assert profile.native_fast_path == "postgres_copy_to_mssql_bcp"
    assert profile.certification_status == "contract_gate"
    assert "lossless_transport_contract" in profile.required_evidence
    assert "benchmark_slo" in profile.required_evidence
    assert "type_matrix" in profile.required_evidence
    assert "route_schema_evolution" in profile.required_evidence
    assert "route_reconciliation_repair" in profile.required_evidence


def test_catalog_has_profile_for_every_integration_matrix_case() -> None:
    catalog = RouteProfileCatalog.default()

    missing = [
        case.case_id
        for case in DEFAULT_INTEGRATION_MATRIX.cases
        if catalog.get(RouteKey.of(case.source, case.sink, case.strategy)) is None
    ]

    assert missing == []


def test_mssql_clickhouse_profile_carries_type_fidelity_evidence() -> None:
    profile = RouteProfileCatalog.default().get(RouteKey.of("mssql", "clickhouse", "incremental_merge"))

    assert profile is not None
    assert profile.docs_link == "docs/source-sink/mssql-to-clickhouse.md"
    assert profile.native_fast_path == "mssql_bcp_queryout_to_clickhouse_typed_wire"
    assert "type_fidelity" in profile.required_evidence
    assert "typed_hash" in profile.required_evidence
    assert "wide_type_certification" in profile.required_evidence
    assert "route_schema_evolution" in profile.required_evidence
    assert "route_reconciliation_repair" in profile.required_evidence


def test_clickhouse_mssql_profile_requires_industrial_landing_evidence() -> None:
    catalog = RouteProfileCatalog.default()
    profile = catalog.get(RouteKey.of("clickhouse", "mssql", "full_refresh"))

    assert profile is not None
    assert profile.docs_link == "docs/source-sink/clickhouse-to-mssql.md"
    assert profile.native_fast_path == "clickhouse_streaming_to_mssql_bcp_staging"
    assert profile.default_slo_hints == {"rows_per_second_min": 15000, "phase": "target_load_finalize"}
    assert "type_matrix" in profile.required_evidence
    assert "route_schema_evolution" in profile.required_evidence
    assert "bcp_bulk_readiness" in profile.required_evidence
    assert "source_boundary_profile" in profile.required_evidence
    assert "benchmark_slo" in profile.required_evidence
    unsupported = catalog.get(RouteKey.of("clickhouse", "mssql", "incremental_append"))
    assert unsupported is not None
    assert unsupported.certification_status == "not_supported"


def test_evidence_reader_normalizes_passed_failed_and_missing_artifacts(tmp_path: Path) -> None:
    passed = _write_json(tmp_path / "passed.json", {"passed": True, "summary": "green"})
    failed = _write_json(tmp_path / "failed.json", {"passed": False, "blockers": ["hash.mismatch"]})

    reader = RouteEvidenceReader()
    passed_item = reader.read(name="matrix_case", path=passed, required=True)
    failed_item = reader.read(name="typed_hash", path=failed, required=True)
    missing_item = reader.read(name="docs_runbook", path=tmp_path / "missing.json", required=True)

    assert passed_item.passed is True
    assert passed_item.summary == "green"
    assert failed_item.passed is False
    assert failed_item.blockers == ("hash.mismatch",)
    assert missing_item.passed is False
    assert missing_item.missing is True
    assert missing_item.blockers == ("docs_runbook.missing",)


def test_evidence_reader_requires_pass_status_for_certification_slots(tmp_path: Path) -> None:
    statusless = _write_json(tmp_path / "statusless.json", {"passed": True})
    verified = _write_json(
        tmp_path / "verified.json",
        {"passed": True, "evidence_status": "PASS", "production_certification": "VERIFIED"},
    )
    generic = _write_json(tmp_path / "generic.json", {"passed": True})
    reader = RouteEvidenceReader()

    assert reader.read(name="route_live_evidence_bundle", path=statusless, required=True).passed is False
    assert reader.read(name="route_live_evidence_bundle", path=verified, required=True).passed is True
    assert reader.read(name="docs_runbook", path=generic, required=True).passed is True


def test_policy_scores_levels_and_next_actions_from_profile_and_evidence(tmp_path: Path) -> None:
    key = RouteKey.of("postgres", "mssql", "incremental_merge")
    profile = RouteProfile(
        key=key,
        docs_link="docs/source-sink/postgres-to-mssql.md",
        install_extras=("postgres", "mssql"),
        required_profiles=("postgres_local", "mssql_local"),
        live_profiles=("postgres_live", "mssql_live"),
        local_service_supported=True,
        external_credentials_required=False,
        certification_status="contract_gate",
        native_fast_path="postgres_copy_to_mssql_bcp",
        required_evidence=("matrix_case", "docs_runbook"),
        default_slo_hints={"rows_per_second_min": 80000},
    )
    green = RouteEvidenceReader().read(
        name="matrix_case",
        path=_write_json(tmp_path / "matrix.json", {"passed": True, "summary": "matrix green"}),
        required=True,
    )
    missing = RouteEvidenceReader().read(name="docs_runbook", path=None, required=True)

    decision = RouteReadinessPolicy().evaluate(profile=profile, evidence=(green, missing))

    assert decision.passed is False
    assert decision.score == 50.0
    assert decision.level == "blocked"
    assert decision.blockers == ("docs_runbook.missing",)
    assert decision.next_actions == ("Provide or regenerate required route evidence `docs_runbook`.",)


def test_route_readiness_service_writes_stable_json_and_markdown(tmp_path: Path) -> None:
    catalog = RouteProfileCatalog.default()
    profile = catalog.get(RouteKey.of("postgres", "mssql", "incremental_merge"))
    assert profile is not None
    artifacts = {
        name: _write_json(tmp_path / f"{name}.json", {"passed": True, "summary": f"{name} ok"})
        for name in profile.required_evidence
    }

    report = RouteReadinessService(catalog=catalog).evaluate(
        output_dir=tmp_path / "readiness",
        source="postgres",
        sink="mssql",
        strategy="incremental_merge",
        artifacts=artifacts,
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    markdown = Path(report.markdown_path).read_text(encoding="utf-8")
    assert payload["schema_version"] == "dpone.route_readiness.v1"
    assert payload["route"]["case_id"] == "postgres_to_mssql__incremental_merge"
    assert payload["passed"] is True
    assert payload["level"] == "certified"
    assert "# dpone route readiness" in markdown
    assert "postgres_to_mssql__incremental_merge" in markdown


def test_route_readiness_service_reports_unknown_routes_without_heavy_execution(tmp_path: Path) -> None:
    report = RouteReadinessService().evaluate(
        output_dir=tmp_path / "readiness",
        source="unknown",
        sink="mssql",
        strategy="incremental_merge",
        artifacts={},
    )

    assert report.passed is False
    assert report.level == "unknown"
    assert report.blockers == ("route.unsupported:unknown:mssql:incremental_merge",)
