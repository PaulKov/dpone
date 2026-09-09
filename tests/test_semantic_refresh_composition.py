from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, cast

import pytest

from dpone.adapters.semantic_refresh_clickhouse_http_client import (
    ClickHousePublicationHttpClient,
)
from dpone.adapters.semantic_refresh_mssql_publication import (
    MssqlSemanticRefreshPublicationState,
)
from dpone.app.semantic_refresh_activation_composition import (
    SemanticRefreshActivationRuntime,
    SemanticRefreshProductionActivationUnavailableError,
    build_semantic_refresh_activation_runtime,
)
from dpone.app.semantic_refresh_airflow_application import (
    SemanticRefreshAirflowApplication,
    build_semantic_refresh_airflow_application,
)
from dpone.app.semantic_refresh_composition import build_semantic_refresh_publication_runtime
from dpone.app.semantic_refresh_recovery_composition import (
    SemanticRefreshFailedScratchCleanupRuntime,
    SemanticRefreshRecoveryDecision,
    SemanticRefreshRecoveryRuntime,
    build_semantic_refresh_failed_scratch_cleanup_runtime,
    build_semantic_refresh_recovery_runtime,
)
from dpone.contracts.semantic_refresh_plan_refs import ReplacementActionBinding
from dpone.contracts.semantic_refresh_types import (
    ReplacementAction,
    SqlServerModelOutcome,
)
from dpone.ports.semantic_refresh_clickhouse_connection import (
    ClickHouseClusterConnectionAuthority,
    clickhouse_cluster_topology_sha256,
)
from dpone.ports.semantic_refresh_production_activation import (
    UnavailableSemanticRefreshProductionActivationGuard,
)
from dpone.readiness.dbt_semantic_refresh_runtime_proof import (
    SemanticRefreshImmutableProofRechecker,
)
from dpone.runtime.semantic_refresh_model_publication import (
    SemanticRefreshFailedPrecommitCleanupService,
    SemanticRefreshModelPublicationService,
)

_UTC = timezone.utc  # noqa: UP017 - package type-checks against Python 3.10.


class _Unused:
    endpoint_authority_id = "http://127.0.0.1:58124/"

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"composition performed unexpected I/O through {name}")

    def verify(self, _subject: object) -> bool:
        return True


def _digest(character: str) -> str:
    return "sha256:" + character * 64


class _DeploymentAuthorities:
    def persist_deployment_authorities(self, **kwargs: object) -> object:
        return kwargs

    def plan_deployment_authorities(self, **kwargs: object) -> object:
        return {"planned": kwargs}

    def persist_planned_deployment_authorities(self, **kwargs: object) -> object:
        return {"persisted": kwargs}


class _MssqlActivation:
    def __init__(self) -> None:
        self.activated: tuple[object, object] | None = None

    def activate_deployment(self, *, subject: object, plan_bundle: object) -> None:
        self.activated = (subject, plan_bundle)


class _Failure:
    def terminalize(self, workflow_id: str) -> object:
        return SimpleNamespace(
            workflow_execution_binding_sha256=_digest("b"),
            terminal_summary_sha256=_digest("c"),
            workflow_id=workflow_id,
        )


class _Predecessor:
    def load_predecessor(self, workflow_id: str) -> object:
        return SimpleNamespace(
            workflow_execution_binding_sha256=_digest("b"),
            terminal_summary_sha256=_digest("c"),
            workflow_id=workflow_id,
            models=(
                SimpleNamespace(
                    model_unique_id="model.analytics.events",
                    mssql_outcome="COMMITTED_WITH_IMAGES",
                ),
            ),
        )


class _Replacement:
    def propose(self, outcomes: dict[str, SqlServerModelOutcome]) -> tuple[ReplacementActionBinding, ...]:
        assert outcomes == {"model.analytics.events": SqlServerModelOutcome.COMMITTED_WITH_IMAGES}
        return (
            ReplacementActionBinding(
                action_id="model.analytics.events",
                outcome=SqlServerModelOutcome.COMMITTED_WITH_IMAGES,
                action=ReplacementAction.RESTORE_THEN_REBUILD,
            ),
        )


class _CleanupAuthority:
    value = object()

    def load(self, binding: str, operation_id: str) -> object:
        assert binding == _digest("b")
        assert operation_id == _digest("o")
        return self.value


class _Cleaner:
    def __init__(self) -> None:
        self.authority: object | None = None
        self.receipt = object()

    def cleanup(self, authority: object) -> object:
        self.authority = authority
        return self.receipt


class _Resources:
    def __init__(self) -> None:
        self.released: tuple[str, str] | None = None
        self.closure = object()

    def release_operation(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> None:
        self.released = (workflow_execution_binding_sha256, operation_id)

    def assert_released(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> object:
        assert self.released == (workflow_execution_binding_sha256, operation_id)
        return self.closure


class _CleanupAcks:
    def __init__(self) -> None:
        self.persisted: tuple[object, object] | None = None
        self.ack = object()

    def persist_exact(self, *, scratch: object, resources: object) -> object:
        self.persisted = (scratch, resources)
        return self.ack


def test_activation_composition_is_inert_and_uses_full_authority_store() -> None:
    runtime = build_semantic_refresh_activation_runtime(
        mssql_connection_factory=lambda: _Unused(),
        release_deployment_verifier=_Unused(),
        release_template_verifier=_Unused(),
        authority_store_ref="mssql://control/dpone_control/activation_authorities",
    )

    assert runtime.deployment is not None
    assert runtime.mssql is not None

    with pytest.raises(
        SemanticRefreshProductionActivationUnavailableError,
        match="DPONE_SEMANTIC_REFRESH_PRODUCTION_ACTIVATION_UNAVAILABLE",
    ):
        runtime.mssql.activate_deployment(
            subject=object(),  # type: ignore[arg-type]
            plan_bundle=object(),  # type: ignore[arg-type]
        )


def test_activation_preview_blocks_production_mutation_before_any_dependency() -> None:
    mssql = _MssqlActivation()
    deployment_subject = object()
    plan_bundle = _Unused()
    runtime = SemanticRefreshActivationRuntime(
        deployment=_DeploymentAuthorities(),  # type: ignore[arg-type]
        mssql=mssql,  # type: ignore[arg-type]
        activation_guard=UnavailableSemanticRefreshProductionActivationGuard(),
    )

    with pytest.raises(
        SemanticRefreshProductionActivationUnavailableError,
        match="DPONE_SEMANTIC_REFRESH_PRODUCTION_ACTIVATION_UNAVAILABLE",
    ):
        runtime.persist_deployment_authorities(
            template_pack={"pack": "template"},
            deployment_subject=deployment_subject,  # type: ignore[arg-type]
            plan_bundle=plan_bundle,  # type: ignore[arg-type]
            route_certification=_Unused(),  # type: ignore[arg-type]
            runtime_assurances=(),
            persisted_at="2026-08-08T00:00:00Z",
        )

    assert mssql.activated is None


def test_activation_preview_can_plan_but_cannot_persist_successor() -> None:
    mssql = _MssqlActivation()
    runtime = SemanticRefreshActivationRuntime(
        deployment=_DeploymentAuthorities(),  # type: ignore[arg-type]
        mssql=mssql,  # type: ignore[arg-type]
        activation_guard=UnavailableSemanticRefreshProductionActivationGuard(),
    )
    plan_bundle = _Unused()

    planned = runtime.plan_deployment_authorities(
        template_pack={"pack": "template"},
        plan_bundle=plan_bundle,  # type: ignore[arg-type]
        route_certification=_Unused(),  # type: ignore[arg-type]
        runtime_assurances=(),
        persisted_at="2026-08-08T00:00:00Z",
    )
    with pytest.raises(
        SemanticRefreshProductionActivationUnavailableError,
        match="DPONE_SEMANTIC_REFRESH_PRODUCTION_ACTIVATION_UNAVAILABLE",
    ):
        runtime.persist_planned_deployment_authorities(
            template_pack={"pack": "template"},
            plan_bundle=plan_bundle,  # type: ignore[arg-type]
            authority=planned,  # type: ignore[arg-type]
        )

    assert mssql.activated is None


def test_two_argument_runtime_constructor_defaults_to_preview_block() -> None:
    mssql = _MssqlActivation()
    deployment = _DeploymentAuthorities()
    runtime = SemanticRefreshActivationRuntime(
        deployment=deployment,  # type: ignore[arg-type]
        mssql=mssql,  # type: ignore[arg-type]
    )
    plan_bundle = _Unused()

    with pytest.raises(
        SemanticRefreshProductionActivationUnavailableError,
        match="DPONE_SEMANTIC_REFRESH_PRODUCTION_ACTIVATION_UNAVAILABLE",
    ):
        runtime.persist_deployment_authorities(
            template_pack={"pack": "template"},
            deployment_subject=object(),  # type: ignore[arg-type]
            plan_bundle=plan_bundle,  # type: ignore[arg-type]
            route_certification=_Unused(),  # type: ignore[arg-type]
            runtime_assurances=(),
            persisted_at="2026-08-08T00:00:00Z",
        )

    assert mssql.activated is None


def test_publication_composition_is_inert_and_uses_protected_runtime() -> None:
    runtime = build_semantic_refresh_publication_runtime(
        mssql_connection_factory=lambda: _Unused(),
        mssql_connection_authority_id="mssql://warehouse",
        artifact_stores=_Unused(),
        seal_policy=_Unused(),
        clickhouse_http_client=cast(ClickHousePublicationHttpClient, _Unused()),
        clickhouse_connection_authority=ClickHouseClusterConnectionAuthority(
            clickhouse_cluster_authority_id="clickhouse://local-cluster",
            endpoint_authority_id="http://127.0.0.1:58124/",
            cluster_name="default",
            host_names=("localhost",),
            topology_sha256=clickhouse_cluster_topology_sha256((("localhost", 9000, 1, 1),)),
        ),
        now=lambda: datetime(2026, 8, 8, tzinfo=_UTC),
    )

    assert isinstance(runtime.models, SemanticRefreshModelPublicationService)
    assert isinstance(runtime.publication_state, MssqlSemanticRefreshPublicationState)


def test_complete_airflow_application_composes_without_parse_time_io(tmp_path) -> None:
    connection_calls = 0

    def _connection() -> _Unused:
        nonlocal connection_calls
        connection_calls += 1
        raise AssertionError("application construction must not open MSSQL")

    application = build_semantic_refresh_airflow_application(
        mssql_connection_factory=_connection,
        mssql_connection_authority_id="mssql://warehouse",
        authority_store_ref="mssql://control/dpone_control/activation-authorities",
        artifact_stores=_Unused(),
        seal_policy=_Unused(),
        clickhouse_http_client=cast(ClickHousePublicationHttpClient, _Unused()),
        clickhouse_connection_authority=ClickHouseClusterConnectionAuthority(
            clickhouse_cluster_authority_id="clickhouse://local-cluster",
            endpoint_authority_id="http://127.0.0.1:58124/",
            cluster_name="default",
            host_names=("localhost",),
            topology_sha256=clickhouse_cluster_topology_sha256((("localhost", 9000, 1, 1),)),
        ),
        run_admission_verifier=_Unused(),
        immutable_proof_rechecker=cast(
            SemanticRefreshImmutableProofRechecker,
            _Unused(),
        ),
        package_source_root=tmp_path / "dbt-dpone",
        pod_identity_root=tmp_path / "podinfo",
        pod_uid_relative_path="uid",
        clock=lambda: datetime(2026, 8, 8, tzinfo=_UTC),
    )

    assert isinstance(application, SemanticRefreshAirflowApplication)
    assert connection_calls == 0


def test_recovery_composition_is_inert_and_exposes_one_protected_controller() -> None:
    runtime = build_semantic_refresh_recovery_runtime(
        mssql_connection_factory=lambda: _Unused(),
    )

    assert isinstance(runtime, SemanticRefreshRecoveryRuntime)
    assert tuple(runtime.reconcile.__annotations__) == ("workflow_execution_id", "return")
    assert tuple(runtime.admit_successor.__annotations__) == (
        "workflow_execution_binding_sha256",
        "return",
    )
    assert runtime.plan_verifier is not None


def test_recovery_controller_derives_actions_from_durable_predecessor() -> None:
    runtime = SemanticRefreshRecoveryRuntime(
        failure=_Failure(),  # type: ignore[arg-type]
        replacement=_Replacement(),  # type: ignore[arg-type]
        predecessor=_Predecessor(),  # type: ignore[arg-type]
        admission=_Unused(),  # type: ignore[arg-type]
        plan_verifier=_Unused(),  # type: ignore[arg-type]
    )

    decision = runtime.reconcile("scheduled__2026-08-08")

    assert isinstance(decision, SemanticRefreshRecoveryDecision)
    assert decision.replacement_actions[0].action is ReplacementAction.RESTORE_THEN_REBUILD


def test_failed_precommit_cleanup_loads_protected_authority_before_clickhouse() -> None:
    authority = _CleanupAuthority()
    cleaner = _Cleaner()
    resources = _Resources()
    acknowledgements = _CleanupAcks()
    runtime = SemanticRefreshFailedScratchCleanupRuntime(
        service=SemanticRefreshFailedPrecommitCleanupService(
            authority=authority,  # type: ignore[arg-type]
            cleaner=cleaner,  # type: ignore[arg-type]
            resources=resources,  # type: ignore[arg-type]
            released_resources=resources,  # type: ignore[arg-type]
            cleanup_ack=acknowledgements,  # type: ignore[arg-type]
        ),
    )

    ack = runtime.cleanup(
        workflow_execution_binding_sha256=_digest("b"),
        operation_id=_digest("o"),
    )

    assert ack is acknowledgements.ack
    assert cleaner.authority is authority.value
    assert resources.released == (_digest("b"), _digest("o"))
    assert acknowledgements.persisted == (cleaner.receipt, resources.closure)


def test_failed_precommit_cleanup_composition_is_parse_inert() -> None:
    runtime = build_semantic_refresh_failed_scratch_cleanup_runtime(
        mssql_connection_factory=lambda: _Unused(),
        clickhouse_http_client=_Unused(),
        clickhouse_connection_authority=ClickHouseClusterConnectionAuthority(
            clickhouse_cluster_authority_id="clickhouse://local-cluster",
            endpoint_authority_id="http://127.0.0.1:58124/",
            cluster_name="default",
            host_names=("localhost",),
            topology_sha256=clickhouse_cluster_topology_sha256((("localhost", 9000, 1, 1),)),
        ),
        now=lambda: datetime(2026, 8, 8, tzinfo=_UTC),
    )

    assert isinstance(runtime, SemanticRefreshFailedScratchCleanupRuntime)
