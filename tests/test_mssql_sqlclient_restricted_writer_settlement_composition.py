"""P9b composition preserves exact P9a owner identities."""

from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from time import monotonic

import pytest

from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown
from dpone.app import mssql_sqlclient_restricted_writer_settlement_composition as module
from dpone.app.mssql_sqlclient_restricted_writer_settlement_composition import (
    _CleanupCustody,
    make_restricted_writer_departure_plan,
    settle_mssql_sqlclient_restricted_writer,
    settle_mssql_sqlclient_restricted_writer_managed,
)
from dpone.contracts.mssql_sqlclient_restricted_writer_settlement import RestrictedWriterSettlementOperations
from dpone.contracts.mssql_sqlclient_restricted_writer_settlement_codec import (
    encode_request,
    remote_settlement_payload,
    validate_result,
)
from dpone.services.mssql_tds_restricted_writer_verification import RestrictedWriterVerifyLocalUnknown
from tests.test_mssql_sqlclient_permission_grant_departure import grant_departure_fixture
from tests.test_mssql_sqlclient_restricted_writer_verify_request import launch_request
from tests.test_mssql_tds_restricted_writer_verification import Association, execute

OPERATIONS = RestrictedWriterSettlementOperations(encode_request, validate_result, remote_settlement_payload)


def test_cleanup_custody_never_truth_coerces_first_failure_and_replay_is_noop():
    effects = []

    class BoolBomb(BaseException):
        def __bool__(self):
            raise AssertionError("cleanup failure truth coercion")

    first = BoolBomb("first")
    custody = _CleanupCustody()

    def fail_first():
        effects.append("verifier")
        raise first

    def fail_second():
        effects.append("coordinator")
        raise RuntimeError("second")

    custody.add("verifier", fail_first)
    custody.add("coordinator", fail_second)
    custody.add("evidence", lambda: effects.append("evidence"))

    with pytest.raises(BoolBomb) as caught:
        custody.close()
    assert caught.value is first
    assert effects == ["verifier", "coordinator", "evidence"]

    custody.close()
    assert effects == ["verifier", "coordinator", "evidence"]


def retained_plan():
    _, grant_request, _ = grant_departure_fixture()
    launch = launch_request()
    launch = replace(
        launch,
        request=replace(
            launch.request,
            implementation_sha256=grant_request.plan.grant_evidence.operation.implementation_sha256,
        ),
    )
    association = Association()
    association._verify_identity = grant_request.plan.grant_evidence.operation
    retained, _, _, _ = execute(launch=launch, association=association)
    retained._owner._association._verify_grant_ref = grant_request.plan.grant_evidence
    plan = make_restricted_writer_departure_plan(
        retained,
        grant_request.plan.grant_evidence,
        grant_request.plan.management_admission,
        grant_request.plan.writer_admission,
        implementation_sha256=launch.request.implementation_sha256,
        package_root="/tmp/dpone",
        admission_sha256="a" * 64,
        startup_deadline=1.0,
        operation_deadline=2.0,
        max_address_space_bytes=1024,
        operations=OPERATIONS,
    )
    return retained, plan


def test_plan_retains_exact_request_and_result_objects():
    retained, plan = retained_plan()
    assert plan.verify_request is retained._owner._request
    assert plan.verify_result is retained._owner._result


def test_composition_rejects_relative_evidence_root_before_opening_pool():
    retained, plan = retained_plan()

    class Pool:
        def open(self, *args, **kwargs):
            raise AssertionError("effect")

    with pytest.raises(ValueError, match="composition_invalid"):
        settle_mssql_sqlclient_restricted_writer(
            Pool(),
            retained,
            object(),
            lambda: (_ for _ in ()).throw(AssertionError("effect")),
            plan,
            lambda value: (_ for _ in ()).throw(AssertionError("effect")),
            Path("relative"),
            deadline=2.0,
            cleanup_deadline=3.0,
            operations=OPERATIONS,
        )


def test_equal_value_plan_substitution_is_rejected_before_actor_open():
    retained, plan = retained_plan()

    class Pool:
        def open(self, *args, **kwargs):
            raise AssertionError("effect")

    with pytest.raises(ValueError, match="composition_invalid"):
        settle_mssql_sqlclient_restricted_writer(
            Pool(),
            retained,
            object(),
            lambda: (_ for _ in ()).throw(AssertionError("effect")),
            replace(plan, verify_request=replace(plan.verify_request)),
            lambda value: (_ for _ in ()).throw(AssertionError("effect")),
            Path("/tmp/evidence"),
            deadline=2.0,
            cleanup_deadline=3.0,
            operations=OPERATIONS,
        )


def test_equal_value_grant_substitution_is_rejected_before_any_effect():
    retained, plan = retained_plan()

    class Pool:
        def open(self, *args, **kwargs):
            raise AssertionError("effect")

    with pytest.raises(ValueError, match="composition_invalid"):
        settle_mssql_sqlclient_restricted_writer(
            Pool(),
            retained,
            object(),
            lambda: (_ for _ in ()).throw(AssertionError("effect")),
            replace(plan, grant_evidence=deepcopy(plan.grant_evidence)),
            lambda value: (_ for _ in ()).throw(AssertionError("effect")),
            Path("/tmp/evidence"),
            deadline=2.0,
            cleanup_deadline=3.0,
            operations=OPERATIONS,
        )


def test_replay_loses_claim_before_factory_pool_or_resource_allocation():
    retained, plan = retained_plan()
    effects = []

    class Pool:
        def open(self, *args, **kwargs):
            effects.append("pool")
            raise RuntimeError("synthetic allocation failure")

    class Coordinator:
        def close(self, *, deadline):
            effects.append("coordinator-close")

    def factory(value):
        effects.append("factory")
        return lambda: None, lambda deadline: effects.append("verifier-close")

    def coordinator_factory():
        effects.append("coordinator")
        return Coordinator()

    with pytest.raises(RuntimeError, match="allocation failure"):
        settle_mssql_sqlclient_restricted_writer(
            Pool(),
            retained,
            object(),
            coordinator_factory,
            plan,
            factory,
            Path("/tmp/evidence"),
            deadline=2.0,
            cleanup_deadline=3.0,
            operations=OPERATIONS,
        )
    assert effects == ["factory", "pool", "verifier-close"]
    before = list(effects)
    with pytest.raises(ValueError, match="remote_settlement_unknown"):
        settle_mssql_sqlclient_restricted_writer(
            Pool(),
            retained,
            object(),
            coordinator_factory,
            plan,
            factory,
            Path("/tmp/evidence"),
            deadline=2.0,
            cleanup_deadline=3.0,
            operations=OPERATIONS,
        )
    assert effects == before


def test_unknown_pool_open_gateway_is_closed_exactly_once():
    retained, plan = retained_plan()
    effects = []

    class Gateway:
        def close(self, *, deadline):
            effects.append(("gateway-close", deadline))

    gateway = Gateway()

    class Pool:
        def open(self, *args, **kwargs):
            raise TdsJournalActorUnknown(gateway)

    class Coordinator:
        def close(self, *, deadline):
            effects.append(("coordinator-close", deadline))

    with pytest.raises(TdsJournalActorUnknown):
        settle_mssql_sqlclient_restricted_writer(
            Pool(),
            retained,
            object(),
            Coordinator,
            plan,
            lambda value: (lambda: None, lambda deadline: effects.append(("verifier-close", deadline))),
            Path("/tmp/evidence"),
            deadline=2.0,
            cleanup_deadline=3.0,
            operations=OPERATIONS,
        )
    assert effects == [
        ("gateway-close", 3.0),
        ("verifier-close", 3.0),
    ]


def test_reentrant_pool_owner_substitution_closes_pre_transfer_custody_once():
    retained, plan = retained_plan()
    exact_owner = retained._owner
    effects = []

    class Evidence:
        def close(self, *, deadline):
            effects.append(("evidence-close", deadline))

    class Pool:
        def open(self, *args, **kwargs):
            effects.append(("pool-open", None))
            object.__setattr__(exact_owner, "_coordinator_custody", object())
            object.__setattr__(exact_owner, "_coordinator_custody_ref", object())
            return Evidence()

    class Coordinator:
        def close(self, *, deadline):
            effects.append(("coordinator-close", deadline))

    def coordinator_factory():
        effects.append(("coordinator-factory", None))
        return Coordinator()

    def verifier_factory(value):
        effects.append(("verifier-factory", None))
        return lambda: None, lambda deadline: effects.append(("verifier-close", deadline))

    with pytest.raises(ValueError, match="remote_settlement_unknown"):
        settle_mssql_sqlclient_restricted_writer(
            Pool(),
            retained,
            exact_owner._association,
            coordinator_factory,
            plan,
            verifier_factory,
            Path("/tmp/evidence"),
            deadline=2.0,
            cleanup_deadline=3.0,
            operations=OPERATIONS,
        )

    assert effects == [
        ("verifier-factory", None),
        ("pool-open", None),
        ("verifier-close", 3.0),
        ("evidence-close", 3.0),
    ]
    before = list(effects)
    with pytest.raises(RestrictedWriterVerifyLocalUnknown):
        settle_mssql_sqlclient_restricted_writer(
            Pool(),
            retained,
            exact_owner._association,
            coordinator_factory,
            plan,
            verifier_factory,
            Path("/tmp/evidence"),
            deadline=2.0,
            cleanup_deadline=3.0,
            operations=OPERATIONS,
        )
    assert effects == before


def test_interruption_after_service_takes_shared_custody_cannot_double_close(monkeypatch):
    retained, plan = retained_plan()
    effects = []

    class Evidence:
        def close(self, *, deadline):
            effects.append(("evidence-close", deadline))

    class Pool:
        def open(self, *args, **kwargs):
            effects.append(("pool-open", None))
            return Evidence()

    class Coordinator:
        def close(self, *, deadline):
            effects.append(("coordinator-close", deadline))

    def interrupted_service(*args, **kwargs):
        kwargs["cleanup_custody"]()
        raise RuntimeError("synthetic mid-custody interruption")

    monkeypatch.setattr(module, "settle_restricted_writer", interrupted_service)

    with pytest.raises(RuntimeError, match="mid-custody interruption"):
        settle_mssql_sqlclient_restricted_writer(
            Pool(),
            retained,
            retained._owner._association,
            Coordinator,
            plan,
            lambda value: (lambda: None, lambda deadline: effects.append(("verifier-close", deadline))),
            Path("/tmp/evidence"),
            deadline=2.0,
            cleanup_deadline=3.0,
            operations=OPERATIONS,
        )

    assert effects == [
        ("pool-open", None),
        ("verifier-close", 3.0),
        ("evidence-close", 3.0),
    ]


def test_managed_replay_loses_claim_before_admission_factory_or_settlement(monkeypatch):
    retained, plan = retained_plan()
    origin = retained._owner._association
    effects = []
    deadline = monotonic() + 10.0
    monkeypatch.setattr(
        module,
        "_admission",
        lambda launcher: (
            effects.append("admission") or (object(), "a" * 64, plan.implementation_sha256, "/tmp/dpone", 1024)
        ),
    )
    monkeypatch.setattr(
        module,
        "make_verifier_factory",
        lambda *args: effects.append("factory") or object(),
    )

    def fail_settlement(*args, **kwargs):
        effects.append("settlement")
        raise RuntimeError("synthetic settlement failure")

    monkeypatch.setattr(module, "_settle_claimed_mssql_sqlclient_restricted_writer", fail_settlement)

    def invoke():
        return settle_mssql_sqlclient_restricted_writer_managed(
            object(),
            object(),
            retained,
            origin,
            object(),
            object(),
            object(),
            plan.management_admission,
            plan.writer_admission,
            lambda: object(),
            Path("/tmp/evidence"),
            supervisor_token="token",
            deadline=deadline,
            helper_startup_timeout=1.0,
            cleanup_deadline=deadline,
            operations=OPERATIONS,
        )

    with pytest.raises(RuntimeError, match="settlement failure"):
        invoke()
    assert effects == ["admission", "factory", "settlement"]
    before = list(effects)
    with pytest.raises(ValueError, match="remote_settlement_unknown"):
        invoke()
    assert effects == before
