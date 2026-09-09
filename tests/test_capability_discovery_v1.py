from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import jsonschema
import pytest

from dpone.contracts.airflow_deployment import release_id
from dpone.contracts.capability_discovery import (
    RecipeDiscoveryEntry,
    RouteCertificationVariant,
)
from dpone.contracts.connector_declarations import (
    built_in_connector_declarations,
    canonical_connector_id,
    canonical_endpoint_type,
)
from dpone.ops.checksums import sha256_file
from dpone.ops.route_certification_matrix import (
    RouteCertificationMatrixRequest,
    RouteCertificationMatrixService,
)
from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.readiness.capability_discovery_composition import build_capability_discovery_service
from dpone.readiness.capability_discovery_service import (
    CapabilityDiscoveryError,
    CapabilityDiscoveryService,
)

_CERTIFICATION_EVIDENCE_NOW = datetime(2026, 7, 23, 10, 30, tzinfo=UTC)


def test_capability_snapshot_is_schema_valid_deterministic_and_complete(tmp_path: Path) -> None:
    service = build_capability_discovery_service(root=tmp_path)

    first = service.snapshot().to_dict()
    second = service.snapshot().to_dict()

    schema = json.loads(Path("src/dpone/schema/capability-discovery.schema.json").read_text(encoding="utf-8"))
    jsonschema.validate(first, schema)
    assert first == second
    assert first["snapshot_id"].startswith("sha256:")
    assert [item["id"] for item in first["connectors"]] == [
        "bigquery",
        "clickhouse",
        "kafka",
        "mssql",
        "mysql",
        "postgres",
        "rest",
    ]
    rest = next(item for item in first["connectors"] if item["id"] == "rest")
    assert rest["endpoint_types"] == ["api"]
    assert rest["roles"] == ["source"]
    assert rest["capability_ids"] == ["auth", "pagination", "incremental_cursor", "state"]
    assert canonical_connector_id("api") == "rest"


@pytest.mark.parametrize(
    "alias",
    (
        "mssql",
        "MSSQL",
        "microsoft mssql",
        "microsoft_mssql",
        "odbc",
        "sqlserver",
        "sql_server",
        "sql-server",
    ),
)
def test_mssql_aliases_share_the_canonical_connector_identity(alias: str) -> None:
    assert canonical_connector_id(alias) == "mssql"


@pytest.mark.parametrize("alias", ("postgres", "postgresql", "PostgreSQL"))
def test_postgres_aliases_share_the_canonical_connector_identity(alias: str) -> None:
    assert canonical_connector_id(alias) == "postgres"


def test_endpoint_identity_preserves_api_while_connector_identity_resolves_rest() -> None:
    assert canonical_endpoint_type("api") == "api"
    assert canonical_connector_id("api") == "rest"


def test_capability_snapshot_separates_support_certification_and_evidence(tmp_path: Path) -> None:
    snapshot = build_capability_discovery_service(root=tmp_path).snapshot().to_dict()
    route = next(item for item in snapshot["routes"] if item["id"] == "mssql:clickhouse:incremental_merge")

    assert route["support"]["status"] == "supported"
    assert route["certification"] == {
        "level": "experimental",
        "evidence_status": "UNVERIFIED",
        "reason_codes": ["route_certification_evidence_missing"],
        "variants": [],
    }
    assert route["beginner"] == {
        "recipe_available": True,
        "recipe_refs": ["mssql-to-clickhouse-incremental"],
        "default_recipe_ref": "mssql-to-clickhouse-incremental",
    }


def test_unknown_route_certification_gate_fails_closed() -> None:
    profile = RouteProfileCatalog.default().profiles()[0]
    service = CapabilityDiscoveryService(
        connectors=built_in_connector_declarations(),
        route_profiles=(replace(profile, certification_status="future_gate"),),
        recipes=(),
    )

    route = service.snapshot().routes[0]

    assert route.support.status == "not_supported"
    assert route.support.limitations == ("certification_gate_unknown",)


def test_unsupported_external_recipe_is_not_scaffoldable() -> None:
    service = CapabilityDiscoveryService(
        connectors=built_in_connector_declarations(),
        route_profiles=RouteProfileCatalog.default().profiles(),
        recipes=(
            RecipeDiscoveryEntry(
                ref="unsupported-custom@1",
                origin="platform",
                status="stable",
                route_id="unknown_source:unknown_sink:full_refresh",
                scaffoldable=True,
            ),
        ),
    )

    recipe = service.snapshot().recipes[0]

    assert recipe.support_status == "unknown"
    assert recipe.scaffoldable is False
    assert recipe.scaffold_argv == ()
    assert "route_not_supported" in recipe.reason_codes
    with pytest.raises(CapabilityDiscoveryError) as captured:
        service.resolve_scaffold_recipe(recipe.ref)
    assert captured.value.code == "DPONE_ROUTE_NOT_SUPPORTED"


def test_capability_snapshot_projects_built_in_recipe_route_metadata(tmp_path: Path) -> None:
    snapshot = build_capability_discovery_service(root=tmp_path).snapshot().to_dict()
    recipe = next(item for item in snapshot["recipes"] if item["ref"] == "postgres-to-clickhouse-full-refresh")

    assert recipe["source"] == "postgres"
    assert recipe["sink"] == "clickhouse"
    assert recipe["strategy"] == "full_refresh"
    assert recipe["route_id"] == "postgres:clickhouse:full_refresh"
    assert recipe["support_status"] == "supported"
    assert recipe["evidence_status"] == "UNVERIFIED"
    assert recipe["scaffold_argv"] == [
        "dpone",
        "init",
        "pipeline",
        "<pipeline_id>",
        "--recipe",
        "postgres-to-clickhouse-full-refresh",
    ]


def test_malformed_external_recipe_catalog_is_an_issue_not_an_exception(
    tmp_path: Path,
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
    catalog_path = tmp_path / "recipes/catalog.yaml"
    catalog_path.parent.mkdir()
    catalog_path.write_text("schema: [\n", encoding="utf-8")

    service = build_capability_discovery_service(root=tmp_path)
    snapshot = service.snapshot()

    assert [item.code for item in snapshot.issues] == [
        "DPONE_RECIPE_CATALOG_INVALID",
    ]
    assert any(item.ref == "mssql-to-clickhouse-incremental" for item in snapshot.recipes)
    with pytest.raises(CapabilityDiscoveryError, match="authoring authority") as exc:
        service.resolve_scaffold_recipe("mssql-to-clickhouse-incremental")
    assert exc.value.code == "DPONE_RECIPE_CATALOG_INVALID"


def test_capability_snapshot_preserves_six_dimensional_certification_variants() -> None:
    variant = RouteCertificationVariant(
        id="mssql_clickhouse_incremental_merge_airflow_kpo",
        route_id="mssql:clickhouse:incremental_merge",
        transport="native_bcp_to_clickhouse",
        schema_evolution="widening",
        airflow_runtime_mode="kpo",
        level="route-certified",
        evidence_status="PASS",
        evidence_refs=("sha256:" + "a" * 64,),
        reason_codes=(),
    )
    service = CapabilityDiscoveryService(
        connectors=built_in_connector_declarations(),
        route_profiles=RouteProfileCatalog.default().profiles(),
        recipes=(),
        certification_variants=(variant,),
    )

    route = next(
        item for item in service.snapshot().to_dict()["routes"] if item["id"] == "mssql:clickhouse:incremental_merge"
    )

    assert route["certification"]["level"] == "route-certified"
    assert route["certification"]["evidence_status"] == "PASS"
    assert route["certification"]["variants"][0]["transport"] == "native_bcp_to_clickhouse"


def test_non_pass_evidence_cannot_promote_route() -> None:
    variant = RouteCertificationVariant(
        id="variant",
        route_id="mssql:clickhouse:incremental_merge",
        transport="native_bcp_to_clickhouse",
        schema_evolution="widening",
        airflow_runtime_mode="kpo",
        level="route-certified",
        evidence_status="UNVERIFIED",
        evidence_refs=(),
        reason_codes=("evidence_stale",),
    )

    with pytest.raises(CapabilityDiscoveryError) as exc:
        CapabilityDiscoveryService(
            connectors=built_in_connector_declarations(),
            route_profiles=RouteProfileCatalog.default().profiles(),
            recipes=(),
            certification_variants=(variant,),
        )

    assert exc.value.code == "DPONE_CAPABILITY_EVIDENCE_UNPROVEN_LEVEL"


def test_failing_variant_dominates_passing_variant_at_route_summary() -> None:
    variants = (
        RouteCertificationVariant(
            id="failed_variant",
            route_id="mssql:clickhouse:incremental_merge",
            transport="native_bcp_to_clickhouse",
            schema_evolution="strict",
            airflow_runtime_mode="kpo",
            level="experimental",
            evidence_status="FAIL",
            evidence_refs=(),
            reason_codes=("live_reconciliation_failed",),
        ),
        RouteCertificationVariant(
            id="passing_variant",
            route_id="mssql:clickhouse:incremental_merge",
            transport="native_bcp_to_clickhouse",
            schema_evolution="widening",
            airflow_runtime_mode="kpo",
            level="route-certified",
            evidence_status="PASS",
            evidence_refs=("sha256:" + "a" * 64,),
            reason_codes=(),
        ),
    )
    service = CapabilityDiscoveryService(
        connectors=built_in_connector_declarations(),
        route_profiles=RouteProfileCatalog.default().profiles(),
        recipes=(),
        certification_variants=variants,
    )

    route = next(item for item in service.snapshot().routes if item.id == "mssql:clickhouse:incremental_merge")

    assert route.certification.level == "experimental"
    assert route.certification.evidence_status == "FAIL"


def test_multiple_recipes_without_default_fail_before_resolution() -> None:
    recipes = (
        RecipeDiscoveryEntry(
            ref="a@1.0.0",
            origin="test",
            status="stable",
            route_id="mssql:clickhouse:incremental_merge",
            scaffoldable=True,
        ),
        RecipeDiscoveryEntry(
            ref="b@1.0.0",
            origin="test",
            status="stable",
            route_id="mssql:clickhouse:incremental_merge",
            scaffoldable=True,
        ),
    )
    service = CapabilityDiscoveryService(
        connectors=built_in_connector_declarations(),
        route_profiles=RouteProfileCatalog.default().profiles(),
        recipes=recipes,
    )

    with pytest.raises(CapabilityDiscoveryError) as exc:
        service.resolve_beginner_recipe("mssql:clickhouse:incremental_merge")

    assert exc.value.code == "DPONE_ROUTE_RECIPE_AMBIGUOUS"


def test_certification_matrix_requires_exact_commit_and_verified_content(
    tmp_path: Path,
) -> None:
    evidence_path = tmp_path / "route-certification-matrix.json"
    evidence_path.write_text(
        json.dumps(
            {
                "schema": "dpone.route-certification-matrix.v1",
                "expected_commit": "a" * 40,
                "evaluated_at": "2026-07-23T10:00:00Z",
                "has_input_failures": False,
                "counts": {
                    "experimental": 0,
                    "route-certified": 1,
                    "production-certified": 0,
                    "enterprise-certified": 0,
                },
                "errors": [],
                "rows": [
                    {
                        "route_id": "mssql_clickhouse_incremental_merge_airflow_kpo",
                        "dimensions": {
                            "source": "mssql",
                            "sink": "clickhouse",
                            "strategy": "incremental_merge",
                            "transport": "native_bcp_to_clickhouse",
                            "schema_evolution": "widening",
                            "airflow_runtime_mode": "kpo",
                        },
                        "sampling_mode": "pushdown",
                        "status": "route-certified",
                        "catalog_status": "experimental",
                        "capability_status": "contract_gate",
                        "contract_status": "PASS",
                        "live_status": "PASS",
                        "production_status": "UNVERIFIED",
                        "docs_link": "docs/source-sink/mssql-to-clickhouse.md",
                        "blockers": [],
                        "proofs": [
                            {
                                "evidence_set": "fabricated",
                                "evidence_status": "PASS",
                                "certification_level": "route-certified",
                                "release_id": "sha256:" + "b" * 64,
                                "deployment_id": None,
                                "environment": None,
                                "signer_identity": None,
                                "expires_at": None,
                                "release_set_sha256": "sha256:" + "b" * 64,
                                "certification_bundle_sha256": "sha256:" + "d" * 64,
                                "attestation_sha256": None,
                                "verification_sha256": None,
                                "production_attempted": False,
                                "blockers": [],
                            }
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    foreign = build_capability_discovery_service(
        root=tmp_path,
        certification_matrix_path=evidence_path,
        expected_commit="c" * 40,
        clock=lambda: _CERTIFICATION_EVIDENCE_NOW,
    ).snapshot()
    exact = build_capability_discovery_service(
        root=tmp_path,
        certification_matrix_path=evidence_path,
        expected_commit="a" * 40,
        clock=lambda: _CERTIFICATION_EVIDENCE_NOW,
    ).snapshot()

    assert foreign.issues[0].code == "DPONE_CAPABILITY_EVIDENCE_FOREIGN_COMMIT"
    route = next(item for item in exact.routes if item.id == "mssql:clickhouse:incremental_merge")
    assert route.certification.level == "experimental"
    assert route.certification.evidence_status == "UNVERIFIED"
    assert route.certification.variants[0].evidence_refs == ()


def test_certification_matrix_is_stale_against_injected_current_clock(
    tmp_path: Path,
) -> None:
    evidence_path = tmp_path / "route-certification-matrix.json"
    evidence_path.write_text(
        json.dumps(
            {
                "schema": "dpone.route-certification-matrix.v1",
                "expected_commit": "a" * 40,
                "evaluated_at": "2000-01-01T00:00:00Z",
                "has_input_failures": False,
                "counts": {
                    "experimental": 0,
                    "route-certified": 0,
                    "production-certified": 0,
                    "enterprise-certified": 0,
                },
                "errors": [],
                "rows": [],
            }
        ),
        encoding="utf-8",
    )

    snapshot = build_capability_discovery_service(
        root=tmp_path,
        certification_matrix_path=evidence_path,
        expected_commit="a" * 40,
        certification_max_age_hours=1,
        clock=lambda: datetime(2026, 7, 23, 10, 0, tzinfo=UTC),
    ).snapshot()

    assert snapshot.issues[0].code == "DPONE_CAPABILITY_EVIDENCE_STALE"
    assert all(route.certification.evidence_status == "UNVERIFIED" for route in snapshot.routes)


def test_certification_matrix_cannot_join_pass_and_digest_from_different_proofs(
    tmp_path: Path,
) -> None:
    evidence_path = tmp_path / "route-certification-matrix.json"
    evidence_path.write_text(
        json.dumps(
            {
                "schema": "dpone.route-certification-matrix.v1",
                "expected_commit": "a" * 40,
                "evaluated_at": "2026-07-23T10:00:00Z",
                "has_input_failures": False,
                "counts": {
                    "experimental": 0,
                    "route-certified": 1,
                    "production-certified": 0,
                    "enterprise-certified": 0,
                },
                "errors": [],
                "rows": [
                    {
                        "route_id": "mssql_clickhouse_incremental_merge_airflow_kpo",
                        "dimensions": {
                            "source": "mssql",
                            "sink": "clickhouse",
                            "strategy": "incremental_merge",
                            "transport": "native_bcp_to_clickhouse",
                            "schema_evolution": "widening",
                            "airflow_runtime_mode": "kpo",
                        },
                        "sampling_mode": "pushdown",
                        "status": "route-certified",
                        "catalog_status": "experimental",
                        "capability_status": "contract_gate",
                        "contract_status": "PASS",
                        "live_status": "PASS",
                        "production_status": "UNVERIFIED",
                        "docs_link": "docs/source-sink/mssql-to-clickhouse.md",
                        "blockers": [],
                        "proofs": [
                            {
                                "evidence_set": "missing_digest",
                                "evidence_status": "PASS",
                                "certification_level": "route-certified",
                                "release_id": None,
                                "deployment_id": None,
                                "environment": None,
                                "signer_identity": None,
                                "expires_at": None,
                                "release_set_sha256": None,
                                "certification_bundle_sha256": None,
                                "attestation_sha256": None,
                                "verification_sha256": None,
                                "production_attempted": False,
                                "blockers": [],
                            },
                            {
                                "evidence_set": "failed_digest",
                                "evidence_status": "FAIL",
                                "certification_level": "experimental",
                                "release_id": None,
                                "deployment_id": None,
                                "environment": None,
                                "signer_identity": None,
                                "expires_at": None,
                                "release_set_sha256": "sha256:" + "b" * 64,
                                "certification_bundle_sha256": "sha256:" + "d" * 64,
                                "attestation_sha256": None,
                                "verification_sha256": None,
                                "production_attempted": False,
                                "blockers": ["evidence_stale"],
                            },
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    snapshot = build_capability_discovery_service(
        root=tmp_path,
        certification_matrix_path=evidence_path,
        expected_commit="a" * 40,
        clock=lambda: _CERTIFICATION_EVIDENCE_NOW,
    ).snapshot()
    route = next(item for item in snapshot.routes if item.id == "mssql:clickhouse:incremental_merge")

    assert route.certification.level == "experimental"
    assert route.certification.evidence_status == "FAIL"
    assert route.certification.variants[0].evidence_refs == ()


def test_certification_matrix_promotes_only_canonically_revalidated_proof(
    tmp_path: Path,
) -> None:
    commit = "a" * 40
    evidence_dir = tmp_path / "evidence-a"
    evidence_dir.mkdir()
    release = {
        "schema": "dpone.release-set.v1",
        "release_id": "",
        "artifacts": {
            "dag_specs": [],
            "workload_packs": [],
            "canonical_schemas": [],
        },
        "provenance": {
            "source_commit": commit,
            "build_id": "pytest",
            "built_at": "2026-07-23T09:00:00Z",
        },
    }
    release["release_id"] = release_id(release)
    _write_json(evidence_dir / "release-set.json", release)
    live_stage = evidence_dir / "route_live_certification.json"
    _write_json(
        live_stage,
        {
            "schema_version": "dpone.route_live_certification.v1",
            "passed": True,
            "evidence_status": "PASS",
            "blockers": [],
        },
    )
    _write_json(
        evidence_dir / "route_certification_bundle.json",
        {
            "schema_version": "dpone.route_certification_bundle.v1",
            "release": "v0.73.17",
            "profile": "vendor_live",
            "route": {
                "source": "mssql",
                "sink": "clickhouse",
                "strategy": "incremental_merge",
            },
            "route_profile": None,
            "passed": True,
            "evidence_status": "PASS",
            "level": "certified",
            "score": 100.0,
            "blockers": [],
            "warnings": [],
            "next_actions": [],
            "required_evidence": ["route_live_evidence_bundle"],
            "stages": [
                {
                    "name": "route_live_evidence_bundle",
                    "path": str(live_stage),
                    "sha256": sha256_file(live_stage),
                    "passed": True,
                    "required": True,
                    "summary": "approved live evidence",
                    "blockers": [],
                }
            ],
            "matrix_claim": {
                "schema": "dpone.route-matrix-claim.v1",
                "release_id": release["release_id"],
                "source_commit": commit,
                "certified_at": "2026-07-23T09:30:00Z",
                "route": {
                    "route_id": "mssql_clickhouse_incremental_merge_airflow_kpo",
                    "source": "mssql",
                    "sink": "clickhouse",
                    "strategy": "incremental_merge",
                    "transport": "native_bcp_to_clickhouse",
                    "schema_evolution": "widening",
                    "airflow_runtime_mode": "kpo",
                    "sampling_mode": "pushdown",
                },
            },
        },
    )
    matrix = RouteCertificationMatrixService(clock=lambda: datetime(2026, 7, 23, 10, 0, tzinfo=UTC)).publish(
        RouteCertificationMatrixRequest(
            expected_commit=commit,
            evidence_dirs=(evidence_dir,),
            output_dir=tmp_path / "matrix",
        )
    )

    snapshot = build_capability_discovery_service(
        root=tmp_path,
        certification_matrix_path=matrix.json_path,
        certification_evidence_dirs=(evidence_dir,),
        expected_commit=commit,
        clock=lambda: datetime(2026, 7, 23, 10, 0, tzinfo=UTC),
    ).snapshot()
    route = next(item for item in snapshot.routes if item.id == "mssql:clickhouse:incremental_merge")

    assert route.certification.level == "route-certified", route.certification.to_dict()
    assert route.certification.evidence_status == "PASS"
    assert len(route.certification.variants[0].evidence_refs) == 2

    (tmp_path / "dpone.yaml").write_text(
        "\n".join(
            (
                "schema: dpone.project.v1",
                "capability_discovery:",
                "  certification_evidence:",
                f"    matrix_path: {matrix.json_path.relative_to(tmp_path).as_posix()}",
                f"    expected_commit: {commit}",
                "    evidence_dirs:",
                f"      - {evidence_dir.relative_to(tmp_path).as_posix()}",
                "    max_age_hours: 168",
                "",
            )
        ),
        encoding="utf-8",
    )
    project_snapshot = build_capability_discovery_service(
        root=tmp_path,
        clock=lambda: datetime(2026, 7, 23, 10, 0, tzinfo=UTC),
    ).snapshot()
    project_route = next(item for item in project_snapshot.routes if item.id == "mssql:clickhouse:incremental_merge")

    assert project_route.certification.level == "route-certified"
    assert project_route.certification.evidence_status == "PASS"


def test_unsafe_project_evidence_config_fails_closed(
    tmp_path: Path,
) -> None:
    (tmp_path / "dpone.yaml").write_text(
        "\n".join(
            (
                "schema: dpone.project.v1",
                "capability_discovery:",
                "  certification_evidence:",
                "    matrix_path: ../foreign/route-certification-matrix.json",
                f"    expected_commit: {'a' * 40}",
                "    evidence_dirs: []",
                "",
            )
        ),
        encoding="utf-8",
    )

    snapshot = build_capability_discovery_service(root=tmp_path).snapshot()

    assert snapshot.issues[0].code == "DPONE_CAPABILITY_EVIDENCE_CONFIG_INVALID"
    assert all(route.certification.evidence_status == "UNVERIFIED" for route in snapshot.routes)


def _write_json(path: Path, payload: object) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
