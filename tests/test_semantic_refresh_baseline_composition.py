"""Application composition tests for baseline plan/apply."""

from __future__ import annotations

from dpone.app.semantic_refresh_baseline_composition import (
    build_semantic_refresh_baseline_issuance_runtime,
)
from dpone.contracts.dbt_semantic_refresh_baseline_issuance import (
    SemanticRefreshBaselineIssuanceService,
)
from tests.test_dbt_semantic_refresh_baseline_issuance import NOW, _evidence, _subject
from tests.test_semantic_refresh_mssql_baseline import _Connection, _Cursor


class _Provider:
    def observe(self, _plan: object) -> object:
        raise AssertionError("plan must not observe engines")


class _Authority:
    def verify_plan(self, _plan: object) -> bool:
        return True

    def verify_evidence(self, *_args: object, **_kwargs: object) -> bool:
        raise AssertionError("plan must not verify observed evidence")


class _Assurances:
    def verify_route(self, *_args: object, **_kwargs: object) -> bool:
        raise AssertionError("plan must not verify route evidence")

    def verify_runtime(self, *_args: object, **_kwargs: object) -> bool:
        raise AssertionError("plan must not verify runtime evidence")


class _EvidenceProvider:
    def observe(self, plan):
        return _evidence(plan)


class _VerifiedAuthority:
    def verify_plan(self, _plan: object) -> bool:
        return True

    def verify_evidence(self, *_args: object, **_kwargs: object) -> bool:
        return True


class _VerifiedAssurances:
    def verify_route(self, *_args: object, **_kwargs: object) -> bool:
        return True

    def verify_runtime(self, *_args: object, **_kwargs: object) -> bool:
        return True


def test_baseline_composition_is_parse_inert_and_plan_does_not_open_mssql() -> None:
    connection_calls = 0

    def connection_factory() -> object:
        nonlocal connection_calls
        connection_calls += 1
        raise AssertionError("plan must not open MSSQL")

    runtime = build_semantic_refresh_baseline_issuance_runtime(
        mssql_connection_factory=connection_factory,
        evidence_provider=_Provider(),
        authority_verifier=_Authority(),
        assurance_verifier=_Assurances(),
        clock=lambda: NOW,
    )

    plan = runtime.plan(_subject())

    assert isinstance(runtime.service, SemanticRefreshBaselineIssuanceService)
    assert plan.subject == _subject()
    assert connection_calls == 0


def test_baseline_composition_applies_through_create_only_mssql_store() -> None:
    connection = _Connection(_Cursor())
    runtime = build_semantic_refresh_baseline_issuance_runtime(
        mssql_connection_factory=lambda: connection,
        evidence_provider=_EvidenceProvider(),
        authority_verifier=_VerifiedAuthority(),
        assurance_verifier=_VerifiedAssurances(),
        clock=lambda: NOW,
    )

    receipt = runtime.apply(runtime.plan(_subject()))

    assert receipt.status == "COMPLETE"
    assert connection.commits == 1
    assert any(sql.startswith("INSERT INTO") for sql, _ in connection.cursor_instance.executions)
