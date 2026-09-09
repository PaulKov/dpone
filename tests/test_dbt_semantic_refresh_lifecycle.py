from __future__ import annotations

from dataclasses import replace

import pytest

from dpone.contracts.dbt_semantic_refresh_lifecycle import (
    SQLSERVER_LIFECYCLE_REPORT_SCHEMA,
    SemanticRefreshLifecycleLock,
    SemanticRefreshLifecycleObservation,
    evaluate_semantic_refresh_lifecycle,
)
from dpone.contracts.semantic_refresh_lifecycle_policy import (
    SemanticRefreshSqlServerLifecyclePolicy,
)
from dpone.runtime.dbt_semantic_refresh_run_authority import (
    SemanticRefreshDbtProofComparator,
)


def _policy(
    *,
    project_policy_digest: str = "sha256:" + "6" * 64,
) -> SemanticRefreshSqlServerLifecyclePolicy:
    return SemanticRefreshSqlServerLifecyclePolicy.build(
        python_version="3.12.12",
        runtime_image_digest="sha256:" + "1" * 64,
        pyodbc_version="5.2.0",
        odbc_driver="ODBC Driver 18 for SQL Server",
        sqlserver_version="16.0.1000.6",
        compatibility_level=160,
        macro_closure_sha256="sha256:" + "4" * 64,
        adapter_policy_digest="sha256:" + "7" * 64,
        project_policy_digest=project_policy_digest,
        profile_policy_digest="sha256:" + "8" * 64,
        invocation_policy_digest="sha256:" + "9" * 64,
        package_artifacts_digest="sha256:" + "2" * 64,
        materialization_closure_digest="sha256:" + "3" * 64,
        dispatch_closure_digest="sha256:" + "5" * 64,
        driver_digest="sha256:" + "a" * 64,
    )


def _lock(policy: SemanticRefreshSqlServerLifecyclePolicy | None = None) -> SemanticRefreshLifecycleLock:
    lifecycle = policy or _policy()
    return SemanticRefreshLifecycleLock.create(
        python_version=lifecycle.python_version,
        image_digest=lifecycle.runtime_image_digest,
        pyodbc_version=lifecycle.pyodbc_version,
        driver_name=lifecycle.odbc_driver,
        driver_version="18.5.1.1",
        sqlserver_version=lifecycle.sqlserver_version,
        compatibility_level=lifecycle.compatibility_level,
        package_artifacts_sha256=lifecycle.package_artifacts_digest,
        materialization_sha256=lifecycle.materialization_closure_digest,
        macro_closure_sha256=lifecycle.macro_closure_sha256,
        dispatch_sha256=lifecycle.dispatch_closure_digest,
        project_flags_sha256=lifecycle.project_policy_digest,
        policy_digests=tuple(
            sorted(
                (
                    lifecycle.adapter_policy_digest,
                    lifecycle.profile_policy_digest,
                    lifecycle.invocation_policy_digest,
                    lifecycle.driver_digest,
                )
            )
        ),
    )


def _observation(lock: SemanticRefreshLifecycleLock) -> SemanticRefreshLifecycleObservation:
    return SemanticRefreshLifecycleObservation(
        certification_coordinate_sha256=lock.certification_coordinate_sha256,
        dbt_core_version="1.12.3",
        dbt_sqlserver_version="1.11.1",
        target_exists=True,
        target_relation_type="table",
        adapter_branch="existing_table_incremental",
        full_refresh=False,
        intermediate_relation_absent=True,
        backup_relation_absent=True,
        adapter_options={
            "enabled": True,
            "materialized": "incremental",
            "incremental_strategy": "dpone_scope_merge",
            "on_schema_change": "fail",
            "contract_enforced": True,
            "pre_hook": (),
            "post_hook": (),
            "grants": {},
            "persist_docs": {},
            "as_columnstore": False,
            "indexes": (),
            "drop_unmanaged_indexes": False,
            "prefer_single_alter_column": False,
            "query_options": {},
            "query_options_raw": (),
            "sql_header": None,
            "incremental_predicates": (),
            "predicates": (),
            "column_types": {},
            "auto_provision_aad_principals": False,
            "column_type_expansion_max_rows": None,
        },
        pre_begin_target_mutations=(),
        in_transaction_target_mutations=("dpone_scope_merge",),
        post_strategy_target_mutations=("adapter_commit",),
        post_commit_mutations=("drop_attempt_local_temp",),
    )


def test_complete_pinned_existing_table_lifecycle_is_proven() -> None:
    policy = _policy()
    lock = _lock(policy)
    report = evaluate_semantic_refresh_lifecycle(
        lock,
        _observation(lock),
        lifecycle_policy=policy,
    )

    assert report.status == "PROVEN"
    assert report.issues == ()
    assert report.lifecycle_policy_sha256 == policy.sqlserver_lifecycle_policy_sha256
    assert report.lifecycle_evaluation_sha256.startswith("sha256:")
    assert report.certification_coordinate_sha256 == lock.certification_coordinate_sha256
    assert report.schema == SQLSERVER_LIFECYCLE_REPORT_SCHEMA
    assert report.schema != "dpone.semantic-refresh-sqlserver-lifecycle-policy.v1"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("dbt_core_version", "1.11.12"),
        ("dbt_sqlserver_version", "1.10.0"),
        ("target_exists", False),
        ("target_relation_type", "view"),
        ("adapter_branch", "create_table_as"),
        ("full_refresh", True),
        ("intermediate_relation_absent", False),
        ("backup_relation_absent", False),
        ("pre_begin_target_mutations", ("drop_intermediate",)),
        ("in_transaction_target_mutations", ("merge",)),
        ("post_strategy_target_mutations", ("apply_grants", "adapter_commit")),
        ("post_commit_mutations", ("drop_unscoped_temp",)),
    ],
)
def test_every_noncertified_branch_or_mutation_phase_fails_closed(
    field: str,
    value: object,
) -> None:
    lock = _lock()
    observation = replace(_observation(lock), **{field: value})

    report = evaluate_semantic_refresh_lifecycle(
        lock,
        observation,
        lifecycle_policy=_policy(),
    )

    assert report.status == "NONCONFORMANT"
    assert field in {issue.field for issue in report.issues}


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("incremental_strategy", "merge"),
        ("on_schema_change", "append_new_columns"),
        ("contract_enforced", False),
        ("grants", {"select": ("analyst",)}),
        ("indexes", ({"columns": ("event_id",)},)),
        ("query_options", {"MAXDOP": 4}),
        ("incremental_predicates", ("event_id > 0",)),
        ("column_type_expansion_max_rows", 1000),
    ],
)
def test_mutation_sensitive_adapter_options_are_closed(field: str, value: object) -> None:
    lock = _lock()
    options = dict(_observation(lock).adapter_options)
    options[field] = value

    report = evaluate_semantic_refresh_lifecycle(
        lock,
        replace(_observation(lock), adapter_options=options),
        lifecycle_policy=_policy(),
    )

    assert report.status == "NONCONFORMANT"
    assert f"adapter_options.{field}" in {issue.field for issue in report.issues}


def test_unknown_adapter_option_is_closed_until_recertified() -> None:
    lock = _lock()
    options = dict(_observation(lock).adapter_options)
    options["future_mutation_option"] = True

    report = evaluate_semantic_refresh_lifecycle(
        lock,
        replace(_observation(lock), adapter_options=options),
        lifecycle_policy=_policy(),
    )

    assert report.status == "NONCONFORMANT"
    assert report.issues[0].field == "adapter_options.future_mutation_option"


def test_missing_scratch_or_coordinate_evidence_is_unverified() -> None:
    lock = _lock()
    report = evaluate_semantic_refresh_lifecycle(
        lock,
        replace(
            _observation(lock),
            certification_coordinate_sha256=None,
            intermediate_relation_absent=None,
        ),
        lifecycle_policy=_policy(),
    )

    assert report.status == "UNVERIFIED"
    assert {issue.field for issue in report.issues} == {
        "certification_coordinate_sha256",
        "intermediate_relation_absent",
    }


def test_canonical_lifecycle_policy_mismatch_fails_closed() -> None:
    policy = _policy()
    mismatched = _policy(project_policy_digest="sha256:" + "b" * 64)
    lock = _lock(policy)

    report = evaluate_semantic_refresh_lifecycle(
        lock,
        _observation(lock),
        lifecycle_policy=mismatched,
    )

    assert report.status == "NONCONFORMANT"
    assert "project_flags_sha256" in {issue.field for issue in report.issues}


def test_runtime_preflight_rechecks_exact_selection_and_all_proof_digests() -> None:
    result = SemanticRefreshDbtProofComparator().verify(
        expected_selected_unique_ids=("model.analytics.orders",),
        observed_selected_unique_ids=("model.analytics.orders",),
        expected_proof_digests=(
            "sha256:" + "8" * 64,
            "sha256:" + "9" * 64,
        ),
        observed_proof_digests=(
            "sha256:" + "8" * 64,
            "sha256:" + "9" * 64,
        ),
        proof_statuses=("PROVEN", "PROVEN", "PROVEN"),
    )

    assert result.status == "PROVEN"
    assert result.can_execute is True
    assert result.preflight_sha256.startswith("sha256:")


@pytest.mark.parametrize(
    ("selected", "digests", "statuses"),
    [
        (("model.analytics.other",), ("sha256:" + "8" * 64,), ("PROVEN",)),
        (("model.analytics.orders",), ("sha256:" + "0" * 64,), ("PROVEN",)),
        (("model.analytics.orders",), ("sha256:" + "8" * 64,), ("UNVERIFIED",)),
    ],
)
def test_runtime_preflight_fails_closed_on_selection_proof_or_status_drift(
    selected: tuple[str, ...],
    digests: tuple[str, ...],
    statuses: tuple[str, ...],
) -> None:
    result = SemanticRefreshDbtProofComparator().verify(
        expected_selected_unique_ids=("model.analytics.orders",),
        observed_selected_unique_ids=selected,
        expected_proof_digests=("sha256:" + "8" * 64,),
        observed_proof_digests=digests,
        proof_statuses=statuses,
    )

    assert result.can_execute is False
    assert result.status in {"NONCONFORMANT", "UNVERIFIED"}
