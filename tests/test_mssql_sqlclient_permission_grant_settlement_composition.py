"""Composition selects the distinct GRANT strategy and exact P8a owner."""

from pathlib import Path
from typing import Any, cast

import pytest

from dpone.app import mssql_sqlclient_permission_grant_settlement_composition as composition
from dpone.app.mssql_sqlclient_departure_runner import DepartureRunCompletion
from dpone.contracts.mssql_sqlclient_departure_evidence_types import (
    SqlClientDepartureEvidenceReceipt,
    evidence_name,
)
from dpone.contracts.mssql_sqlclient_permission_grant_departure import (
    PermissionGrantDepartureCompletion,
    SqlClientPermissionGrantDepartureRequest,
)
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_worker import TdsChildExit
from tests.test_mssql_sqlclient_permission_grant_departure import (
    grant_departure_fixture,
    grant_evidence_payloads,
)
from tests.test_mssql_tds_permission_grant_release import held_setup, invoke_release


def test_composition_builds_distinct_grant_plan_and_typed_completion(tmp_path, monkeypatch):
    held, evidence, events = held_setup()
    local = invoke_release(held, evidence, events)
    _, fixture_request, result = grant_departure_fixture()
    _, _, payloads = grant_evidence_payloads()
    captured = {}

    class Runner:
        def __init__(self, facts, owner, evidence_factory, launch_factory):
            captured["facts"] = facts

        def run(self):
            facts = captured["facts"]
            request = SqlClientPermissionGrantDepartureRequest(plan=facts.plan, startup=fixture_request.startup)
            attempt = attempt_identity_digest(facts.plan.attempt)
            receipts = tuple(
                SqlClientDepartureEvidenceReceipt(
                    facts.plan.helper_id,
                    attempt,
                    kind,
                    evidence_name(facts.plan.helper_id, attempt, kind, str(index + 1) * 64),
                    str(index + 1) * 64,
                    1,
                )
                for index, kind in enumerate(payloads)
            )
            return DepartureRunCompletion(
                facts.plan,
                request,
                result,
                TdsChildExit(request.startup.process, 0, True),
                receipts,
            )

    def settle(owner, run, close, **kwargs):
        completion = run()
        assert type(completion) is PermissionGrantDepartureCompletion
        assert completion.request.plan.grant_evidence is held.result
        assert completion.request.plan.schema == "dpone.sqlclient.permission-grant-departure-plan.v1"
        return "settled"

    monkeypatch.setattr(composition, "DepartureRunner", Runner)
    monkeypatch.setattr(composition, "settle_permission_grant_remotely", settle)
    monkeypatch.setattr(
        composition,
        "_admission",
        lambda launcher: (object(), "a" * 64, held.result.operation.implementation_sha256, "/tmp/dpone", 1024),
    )
    result_value = composition.settle_mssql_sqlclient_permission_grant(
        cast(Any, object()),
        local,
        Path(tmp_path),
        cast(Any, object()),
        fixture_request.plan.management_admission,
        fixture_request.plan.writer_admission,
        cast(Any, lambda: None),
        deadline=2.0,
        helper_startup_timeout=1.0,
        cleanup_deadline=3.0,
    )
    assert result_value == "settled"


def test_relative_evidence_root_is_rejected_before_composition_effect(monkeypatch):
    monkeypatch.setattr(
        composition,
        "_admission",
        lambda *args: (_ for _ in ()).throw(AssertionError("effect")),
    )
    with pytest.raises(ValueError):
        composition.settle_mssql_sqlclient_permission_grant(
            cast(Any, object()),
            cast(Any, object()),
            Path("relative"),
            cast(Any, object()),
            cast(Any, object()),
            cast(Any, object()),
            cast(Any, object()),
            deadline=2.0,
            helper_startup_timeout=1.0,
            cleanup_deadline=3.0,
        )
