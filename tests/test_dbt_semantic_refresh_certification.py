from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.contracts.dbt_publish_models import (
    CompiledDbtModel,
    DbtColumnArtifact,
    DbtManifestArtifact,
    DbtModelArtifact,
    DbtPublishIntent,
    DbtPublishProfile,
    DbtPublishStrategyPolicy,
    DbtWorkflowProfile,
)
from dpone.contracts.dbt_semantic_refresh_certification import (
    SemanticRefreshCertificationDecision,
)
from dpone.contracts.semantic_refresh_profile import SemanticRefreshProfilePolicy
from dpone.services.dbt_publish_compiler import DbtDponeCompiler

DIGEST = "sha256:" + "a" * 64


def _semantic_profile() -> SemanticRefreshProfilePolicy:
    return SemanticRefreshProfilePolicy.from_mapping(
        {
            "schema": "dpone.semantic-refresh-profile.v1",
            "enabled": True,
            "capability": "scope_stable_event_fact",
            "scope": {"grain": "day", "timezone": "UTC", "interval": "half_open"},
            "mutation": {"protocol": "update_insert_v1", "deletes": "ignore_missing"},
            "initial_load": "require_existing_complete_relation",
            "concurrency": "exclusive_workflow",
            "source_snapshot": "snapshot",
            "publication": {
                "database_engine": "Atomic",
                "table_engine": "MergeTree",
                "replica_count": 1,
                "strategy": "full_table_exchange",
            },
            "workflow_publish_atomicity": "none",
            "automatic_sql_retry": False,
        }
    )


def _model() -> DbtModelArtifact:
    return DbtModelArtifact(
        unique_id="model.project.events",
        name="events",
        original_file_path="models/events.sql",
        database="DWH",
        schema="mart",
        alias="events",
        materialized="incremental",
        contract_enforced=True,
        columns=("event_id",),
        column_contracts=(DbtColumnArtifact("event_id", "bigint", False),),
        group="analytics",
        tags=(),
        meta={},
        unique_key=("event_id",),
        depends_on=(),
        fqn=("project", "events"),
    )


class _Verifier:
    def verify(self, request):
        return SemanticRefreshCertificationDecision(
            request_sha256=request.request_sha256,
            status="CERTIFIED",
            receipt_sha256="sha256:" + "b" * 64,
            certified_at="2026-08-01T00:00:00Z",
            expires_at="2026-09-01T00:00:00Z",
        )


def _compiler(*, certified: bool) -> DbtDponeCompiler:
    model = _model()
    intent = DbtPublishIntent(True, "semantic", "daily")
    profile = DbtPublishProfile(
        name="semantic",
        source_type="mssql",
        source_connection_ref="mssql_prod",
        sink_type="clickhouse",
        sink_connection_ref="clickhouse_prod",
        target_schema="mart",
        staging_schema="staging",
        runtime_image=DIGEST,
        semantic_refresh=_semantic_profile(),
    )
    workflow = DbtWorkflowProfile("daily", None, "2026-01-01", "UTC", "data", (), False, 1)
    strategy_policy = DbtPublishStrategyPolicy(("incremental_merge",))
    artifact = DbtManifestArtifact("manifest.json", DIGEST, 12, "1.12.3", None, "project", (model,))
    registry = SimpleNamespace(
        profile=lambda name: profile if name == "semantic" else None,
        workflow=lambda name: workflow if name == "daily" else None,
        strategy_policy=lambda name: strategy_policy if name == "semantic" else None,
    )

    class _ModelCompiler:
        def compile(self, selected, selected_intent, selected_profile, _policy, *, supported_strategies=None):
            del supported_strategies
            return CompiledDbtModel(
                selected,
                selected_intent,
                selected_profile,
                {"mode": "semantic_refresh_v2"},
                {},
                "events",
                {},
            )

        def strategy_candidates(self, *_args, **_kwargs):
            return ()

    compiler = DbtDponeCompiler(
        reader=SimpleNamespace(read=lambda _path: (artifact, ())),
        resolver=SimpleNamespace(resolve=lambda _model: (intent, ())),
        model_compiler=_ModelCompiler(),
        profile_loader=SimpleNamespace(load=lambda _manifest, _profiles: (registry, ())),
        route_capabilities=SimpleNamespace(),
        project_policy=SimpleNamespace(validate_manifest=lambda _path: ()),
        graph_policy=SimpleNamespace(
            validate=lambda *_args: (),
            validate_semantic_refresh=lambda *_args, **_kwargs: (),
        ),
        require_certified_routes=True,
        semantic_refresh_certification_coordinate_sha256=(DIGEST if certified else None),
        semantic_refresh_certification_verifier=(_Verifier() if certified else None),
        semantic_refresh_certification_verification_time=("2026-08-08T00:00:00Z" if certified else None),
    )
    return compiler


def test_production_compile_graduates_only_with_request_bound_receipt() -> None:
    report = _compiler(certified=True).build("manifest.json")

    assert report.passed
    assert report.models[0].route_capability == {
        "capability": "scope_stable_event_fact",
        "status": "CERTIFIED",
        "certification_coordinate_sha256": DIGEST,
        "certification_receipt_sha256": "sha256:" + "b" * 64,
    }


def test_production_compile_blocks_without_protected_verifier() -> None:
    report = _compiler(certified=False).build("manifest.json")

    assert not report.passed
    assert {issue.code for issue in report.blockers} == {"DPONE_DBT_V2_LIVE_UNVERIFIED"}
    assert report.models == ()


def test_certification_decision_cannot_attach_a_receipt_to_unverified_status() -> None:
    with pytest.raises(ValueError, match="UNVERIFIED decision"):
        SemanticRefreshCertificationDecision(DIGEST, "UNVERIFIED", "sha256:" + "b" * 64)


@pytest.mark.parametrize(
    ("verification_time", "revoked"),
    [("2026-09-01T00:00:00Z", False), ("2026-08-08T00:00:00Z", True)],
)
def test_expired_or_revoked_receipt_never_graduates(verification_time: str, revoked: bool) -> None:
    if revoked:
        decision = SemanticRefreshCertificationDecision(DIGEST, "UNVERIFIED", revoked=True)
    else:
        decision = SemanticRefreshCertificationDecision(
            DIGEST,
            "CERTIFIED",
            "sha256:" + "b" * 64,
            "2026-08-01T00:00:00Z",
            "2026-09-01T00:00:00Z",
        )
    assert decision.valid_at(verification_time) is False
