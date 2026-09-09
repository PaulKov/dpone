"""Domain-first ``init pipeline`` catalog membership and reconcile audit."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from dpone.gitops.airflow_dag_spec_builder import AirflowDagSpecBuilder
from dpone.gitops.domain_dag_spec_source import (
    MEMBERSHIP_MISSING_WARNING_CODE,
    DiscoveredAirflowPipeline,
    membership_gap_warnings,
)
from dpone.manifest.project_config import resolve_project_layout
from dpone.readiness.airflow_pipeline_catalog_membership import (
    MembershipConflictError,
    _insert_workload_entry,
    plan_membership_patch,
    register_domain_membership,
)
from dpone.readiness.airflow_scaffold_apply import ScaffoldApplier, ScaffoldFile
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service

CATALOG_TEXT = """\
domain: crm
defaults:
  owner: data-crm

# membership: one entry per Airflow-eligible pipeline (comment must survive).

workloads:
  aaa_first:
    manifest: ../../../workloads/crm/pipelines/aaa_first/pipeline.yaml
  zzz_last:
    manifest: ../../../workloads/crm/pipelines/zzz_last/pipeline.yaml
"""

FOUR_SPACE_CATALOG_TEXT = """\
domain: crm
workloads:
    aaa_first:
      manifest: ../../../workloads/crm/pipelines/aaa_first/pipeline.yaml
"""


NESTED_LABELS_CATALOG = """\
domain: assortment_pricing
workloads:
  aaa_first:
    manifest: ../../../workloads/assortment_pricing/pipelines/aaa_first/pipeline.yaml
  competitive_price_calculation:
    manifest: ../../../workloads/assortment_pricing/pipelines/competitive_price_calculation/pipeline.yaml
    labels:
      - keep-me-on-aaa
"""


def _seed_domain_first(tmp_path: Path, *, pipeline_id: str = "orders_daily") -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain_first").passed
    assert service.init_domain(
        domain="crm",
        owner_team="data-crm",
        owner_contact="crm@example.com",
        approver_team="data-platform",
    ).passed
    result = service.init_pipeline(
        pipeline_id=pipeline_id,
        domain="crm",
        route="mssql:clickhouse:incremental_merge",
        from_locator="mssql_dev:dbo.orders",
        to_locator="clickhouse_dev:analytics.orders",
        unique_key="order_id",
        airflow=None,
    )
    assert result.passed, result.errors


def _write_system_workload_set(tmp_path: Path) -> Path:
    path = tmp_path / ".dpone/config/project.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump({"gitops": {"includes": [{"path": "domains/*.yaml"}]}}, sort_keys=False),
        encoding="utf-8",
    )
    return path


def test_init_pipeline_registers_domain_catalog_membership(tmp_path: Path) -> None:
    _seed_domain_first(tmp_path)

    catalog_path = tmp_path / ".dpone/config/domains/crm.yaml"
    assert catalog_path.is_file()
    payload = yaml.safe_load(catalog_path.read_text(encoding="utf-8"))
    assert payload["domain"] == "crm"
    assert payload["workloads"]["orders_daily"] == {
        "manifest": "../../../workloads/crm/pipelines/orders_daily/pipeline.yaml"
    }
    assert "dags" not in payload


def test_registered_pipeline_resolves_via_system_workload_set(tmp_path: Path) -> None:
    _seed_domain_first(tmp_path)
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_dag(
        dag_id="DAG__crm__orders__refresh",
        domain="crm",
        schedule="0 6 * * *",
        pipelines=("orders_daily",),
    ).passed
    workload_set = tmp_path / ".dpone/config/project.yaml"
    assert workload_set.is_file()

    report = AirflowDagSpecBuilder(repo_root=tmp_path).build(workload_set=str(workload_set), env="dev")

    assert not report.blockers, report.blockers
    assert [spec.dag_id for spec in report.specs] == ["DAG__crm__orders__refresh"]
    assert not [warning for warning in report.warnings if warning.code == MEMBERSHIP_MISSING_WARNING_CODE], (
        report.warnings
    )


def test_unregistered_pipeline_emits_membership_gap_warning(tmp_path: Path) -> None:
    _seed_domain_first(tmp_path)
    workload_set = _write_system_workload_set(tmp_path)
    (tmp_path / ".dpone/config/domains/crm.yaml").unlink()

    report = AirflowDagSpecBuilder(repo_root=tmp_path).build(workload_set=str(workload_set), env="dev")

    gap_warnings = [warning for warning in report.warnings if warning.code == MEMBERSHIP_MISSING_WARNING_CODE]
    assert len(gap_warnings) == 1, report.warnings
    assert gap_warnings[0].path == "workloads/crm/pipelines/orders_daily/pipeline.yaml"
    assert not Path(gap_warnings[0].path).is_absolute()
    assert "orders_daily" in gap_warnings[0].message
    assert ".dpone/config/domains/crm.yaml" in gap_warnings[0].message
    assert "resolved workload set" in gap_warnings[0].message
    assert "manifest:" in gap_warnings[0].message
    assert "  orders_daily:" in gap_warnings[0].message


def test_membership_gap_warning_does_not_block_reconcile(tmp_path: Path) -> None:
    _seed_domain_first(tmp_path)
    workload_set = _write_system_workload_set(tmp_path)
    (tmp_path / ".dpone/config/domains/crm.yaml").unlink()

    report = AirflowDagSpecBuilder(repo_root=tmp_path).build(workload_set=str(workload_set), env="dev")

    assert [warning.code for warning in report.warnings if warning.code == MEMBERSHIP_MISSING_WARNING_CODE]
    assert not report.blockers


def test_membership_gap_warning_skips_non_airflow_pipeline(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain_first").passed
    assert service.init_domain(
        domain="crm",
        owner_team="data-crm",
        owner_contact="crm@example.com",
        approver_team="data-platform",
    ).passed
    result = service.init_pipeline(
        pipeline_id="offline_batch",
        domain="crm",
        route="mssql:clickhouse:incremental_merge",
        from_locator="mssql_dev:dbo.orders",
        to_locator="clickhouse_dev:analytics.orders",
        unique_key="order_id",
        airflow=False,
    )
    assert result.passed, result.errors
    catalog_path = tmp_path / ".dpone/config/domains/crm.yaml"
    assert not catalog_path.exists()
    workload_set = _write_system_workload_set(tmp_path)

    report = AirflowDagSpecBuilder(repo_root=tmp_path).build(workload_set=str(workload_set), env="dev")

    assert not [warning for warning in report.warnings if warning.code == MEMBERSHIP_MISSING_WARNING_CODE]


def test_membership_gap_warnings_skip_resolved_and_emit_for_missing() -> None:
    discovered = {
        "orders_daily": DiscoveredAirflowPipeline(
            pipeline_id="orders_daily",
            source_label="workloads/crm/pipelines/orders_daily/pipeline.yaml",
            domain="crm",
        )
    }
    assert membership_gap_warnings(None, set()) == ()
    assert membership_gap_warnings(discovered, {"orders_daily"}) == ()
    warnings = membership_gap_warnings(discovered, set())
    assert [warning.code for warning in warnings] == [MEMBERSHIP_MISSING_WARNING_CODE]
    assert warnings[0].path == "workloads/crm/pipelines/orders_daily/pipeline.yaml"
    assert ".dpone/config/domains/crm.yaml" in warnings[0].message


def test_second_init_pipeline_appends_catalog_membership(tmp_path: Path) -> None:
    _seed_domain_first(tmp_path)
    service = build_airflow_self_service_service(root=tmp_path)
    result = service.init_pipeline(
        pipeline_id="invoices_daily",
        domain="crm",
        route="mssql:clickhouse:incremental_merge",
        from_locator="mssql_dev:dbo.invoices",
        to_locator="clickhouse_dev:analytics.invoices",
        unique_key="invoice_id",
        airflow=None,
    )
    assert result.passed, result.errors
    payload = yaml.safe_load((tmp_path / ".dpone/config/domains/crm.yaml").read_text(encoding="utf-8"))
    assert set(payload["workloads"]) == {"orders_daily", "invoices_daily"}


def test_plan_membership_patch_is_idempotent_for_existing_entry(tmp_path: Path) -> None:
    _seed_domain_first(tmp_path)
    layout = resolve_project_layout(tmp_path)

    patch = plan_membership_patch(
        tmp_path,
        layout=layout,
        domain="crm",
        pipeline_id="orders_daily",
        pipeline_path=Path("workloads/crm/pipelines/orders_daily/pipeline.yaml"),
    )

    assert patch is None


def test_plan_membership_patch_conflicts_on_manifest_mismatch(tmp_path: Path) -> None:
    _seed_domain_first(tmp_path)
    layout = resolve_project_layout(tmp_path)
    catalog_path = tmp_path / ".dpone/config/domains/crm.yaml"
    catalog_path.write_text(
        "domain: crm\nworkloads:\n  orders_daily:\n    manifest: ../../../elsewhere.yaml\n",
        encoding="utf-8",
    )

    with pytest.raises(MembershipConflictError):
        plan_membership_patch(
            tmp_path,
            layout=layout,
            domain="crm",
            pipeline_id="orders_daily",
            pipeline_path=Path("workloads/crm/pipelines/orders_daily/pipeline.yaml"),
        )


def test_cross_file_catalog_registration_makes_repeat_init_no_op(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain_first").passed
    assert service.init_domain(
        domain="crm",
        owner_team="data-crm",
        owner_contact="crm@example.com",
        approver_team="data-platform",
    ).passed
    wide_mart = tmp_path / ".dpone/config/domains/wide_mart.yaml"
    wide_mart.parent.mkdir(parents=True, exist_ok=True)
    wide_mart.write_text(
        "domain: wide_mart\nworkloads:\n"
        "  orders_daily:\n"
        "    manifest: ../../../workloads/crm/pipelines/orders_daily/pipeline.yaml\n",
        encoding="utf-8",
    )

    first = service.init_pipeline(
        pipeline_id="orders_daily",
        domain="crm",
        route="mssql:clickhouse:incremental_merge",
        from_locator="mssql_dev:dbo.orders",
        to_locator="clickhouse_dev:analytics.orders",
        unique_key="order_id",
        airflow=None,
    )
    assert first.passed, first.errors
    crm_catalog = tmp_path / ".dpone/config/domains/crm.yaml"
    assert not crm_catalog.exists()

    second = service.init_pipeline(
        pipeline_id="orders_daily",
        domain="crm",
        route="mssql:clickhouse:incremental_merge",
        from_locator="mssql_dev:dbo.orders",
        to_locator="clickhouse_dev:analytics.orders",
        unique_key="order_id",
        airflow=None,
    )
    assert second.passed, second.errors
    assert not crm_catalog.exists()

    workload_set = _write_system_workload_set(tmp_path)
    report = AirflowDagSpecBuilder(repo_root=tmp_path).build(workload_set=str(workload_set), env="dev")
    assert not [blocker for blocker in report.blockers if blocker.code == "workload_id_duplicate"]


def test_membership_conflict_failure_includes_compensated_scaffold_receipt(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain_first").passed
    assert service.init_domain(
        domain="crm",
        owner_team="data-crm",
        owner_contact="crm@example.com",
        approver_team="data-platform",
    ).passed
    layout = resolve_project_layout(tmp_path)
    pipeline_path = Path("workloads/crm/pipelines/orders_daily/pipeline.yaml")
    applier = ScaffoldApplier(tmp_path)
    plan = applier.apply(
        (
            ScaffoldFile.yaml(
                pipeline_path,
                {"metadata": {"id": "orders_daily"}},
            ),
        )
    )
    assert plan.created_files

    with patch(
        "dpone.readiness.airflow_pipeline_catalog_membership.apply_catalog_content",
        return_value=None,
    ):
        compensated_plan, failure = register_domain_membership(
            plan,
            root=tmp_path,
            layout=layout,
            domain="crm",
            pipeline_id="orders_daily",
            pipeline_path=pipeline_path,
            applier=applier,
        )

    assert failure is not None
    assert not failure.passed
    assert compensated_plan.apply_failed
    assert failure.changes
    assert failure.details is not None
    assert "rollback_journal" in failure.details
    assert any(change.action == "rolled_back" for change in failure.changes)
    assert not (tmp_path / pipeline_path).exists()


def test_plan_membership_patch_rejects_foreign_catalog_domain(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain_first").passed
    assert service.init_domain(
        domain="crm",
        owner_team="data-crm",
        owner_contact="crm@example.com",
        approver_team="data-platform",
    ).passed
    layout = resolve_project_layout(tmp_path)
    catalog_path = tmp_path / ".dpone/config/domains/crm.yaml"
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    catalog_path.write_text("domain: finance\nworkloads: {}\n", encoding="utf-8")

    with pytest.raises(MembershipConflictError, match="declares domain 'finance'"):
        plan_membership_patch(
            tmp_path,
            layout=layout,
            domain="crm",
            pipeline_id="orders_daily",
            pipeline_path=Path("workloads/crm/pipelines/orders_daily/pipeline.yaml"),
        )


def test_insert_workload_entry_keeps_comments() -> None:
    manifest_ref = "../../../workloads/crm/pipelines/mmm_new/pipeline.yaml"

    updated = _insert_workload_entry(CATALOG_TEXT, "mmm_new", manifest_ref)

    assert "# membership: one entry per Airflow-eligible pipeline" in updated
    assert updated.index("aaa_first:") < updated.index("mmm_new:") < updated.index("zzz_last:")
    payload = yaml.safe_load(updated)
    assert set(payload["workloads"]) == {"aaa_first", "mmm_new", "zzz_last"}


def test_insert_workload_entry_appends_when_block_unsorted() -> None:
    unsorted_text = CATALOG_TEXT.replace("aaa_first", "zzz_pre")
    manifest_ref = "../../../workloads/crm/pipelines/bbb_new/pipeline.yaml"

    updated = _insert_workload_entry(unsorted_text, "bbb_new", manifest_ref)

    assert updated.index("zzz_last:") < updated.index("bbb_new:")
    payload = yaml.safe_load(updated)
    assert set(payload["workloads"]) == {"zzz_pre", "bbb_new", "zzz_last"}


def test_insert_workload_entry_recognizes_header_with_comment() -> None:
    text = "domain: crm\n# membership: one entry per Airflow-eligible pipeline\nworkloads:  # membership\n"
    manifest_ref = "../../../workloads/crm/pipelines/mmm_new/pipeline.yaml"

    updated = _insert_workload_entry(text, "mmm_new", manifest_ref)

    assert updated.count("workloads:") == 1
    assert "# membership: one entry per Airflow-eligible pipeline" in updated
    assert "workloads:  # membership" in updated
    payload = yaml.safe_load(updated)
    assert payload["workloads"]["mmm_new"]["manifest"].endswith("mmm_new/pipeline.yaml")


@pytest.mark.parametrize("flow_header", ["workloads: {}", "workloads: []"])
def test_insert_workload_entry_normalizes_flow_form_header(flow_header: str) -> None:
    text = f"domain: crm\n{flow_header}\n"
    manifest_ref = "../../../workloads/crm/pipelines/orders_daily/pipeline.yaml"

    updated = _insert_workload_entry(text, "orders_daily", manifest_ref)

    assert updated.count("workloads:") == 1
    assert flow_header not in updated
    payload = yaml.safe_load(updated)
    assert payload["workloads"]["orders_daily"]["manifest"].endswith("orders_daily/pipeline.yaml")


def test_insert_workload_entry_recognizes_crlf_header() -> None:
    text = "domain: crm\r\nworkloads:   \r\n"
    manifest_ref = "../../../workloads/crm/pipelines/orders_daily/pipeline.yaml"

    updated = _insert_workload_entry(text, "orders_daily", manifest_ref)

    assert updated.count("workloads:") == 1
    payload = yaml.safe_load(updated.replace("\r\n", "\n"))
    assert "orders_daily" in payload["workloads"]


def test_insert_workload_entry_preserves_nested_labels_under_existing_workload() -> None:
    manifest_ref = "../../../workloads/assortment_pricing/pipelines/ccc/pipeline.yaml"

    updated = _insert_workload_entry(NESTED_LABELS_CATALOG, "ccc", manifest_ref)

    payload = yaml.safe_load(updated)
    assert payload["workloads"]["competitive_price_calculation"]["labels"] == ["keep-me-on-aaa"]
    assert payload["workloads"]["ccc"]["manifest"].endswith("ccc/pipeline.yaml")


def test_insert_workload_entry_preserves_four_space_indented_keys() -> None:
    manifest_ref = "../../../workloads/crm/pipelines/mmm_new/pipeline.yaml"

    updated = _insert_workload_entry(FOUR_SPACE_CATALOG_TEXT, "mmm_new", manifest_ref)

    assert "    aaa_first:" in updated
    payload = yaml.safe_load(updated)
    assert set(payload["workloads"]) == {"aaa_first", "mmm_new"}


def test_membership_conflict_preflight_leaves_no_partial_apply(tmp_path: Path) -> None:
    service = build_airflow_self_service_service(root=tmp_path)
    assert service.init_project(airflow=True, layout="domain_first").passed
    assert service.init_domain(
        domain="crm",
        owner_team="data-crm",
        owner_contact="crm@example.com",
        approver_team="data-platform",
    ).passed
    catalog_path = tmp_path / ".dpone/config/domains/crm.yaml"
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    catalog_path.write_text(
        "domain: crm\nworkloads:\n  orders_daily:\n    manifest: ../../../elsewhere.yaml\n",
        encoding="utf-8",
    )
    pipeline_path = tmp_path / "workloads/crm/pipelines/orders_daily/pipeline.yaml"

    result = service.init_pipeline(
        pipeline_id="orders_daily",
        domain="crm",
        route="mssql:clickhouse:incremental_merge",
        from_locator="mssql_dev:dbo.orders",
        to_locator="clickhouse_dev:analytics.orders",
        unique_key="order_id",
        airflow=None,
    )

    assert not result.passed
    assert not pipeline_path.exists()
    assert catalog_path.read_text(encoding="utf-8").endswith("../../../elsewhere.yaml\n")
