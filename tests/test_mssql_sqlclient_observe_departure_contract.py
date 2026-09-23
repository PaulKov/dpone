"""Pure synthetic original OBSERVE records never certify actual containment."""

from dataclasses import fields, replace

import pytest

from dpone.contracts.mssql_sqlclient_observe_departure import (
    SqlClientObserveDeparturePlan,
    SqlClientObserveDepartureRequest,
)
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
from tests.mssql_sqlclient_departure_v2_fixtures import alias, corrupt, leaves
from tests.test_mssql_sqlclient_departure_ipc_v2 import request as create_request


def request(departure=None):
    return from_create_request(create_request(departure))


def from_create_request(old):
    omit = {
        "schema",
        "create_operation",
        "create_process",
        "create_result_sha256",
        "create_local_exit_sha256",
        "creator_admission",
    }
    plan = SqlClientObserveDeparturePlan(
        **{f.name: getattr(old.plan, f.name) for f in fields(old.plan) if f.name not in omit},
        observe_operation=replace(old.plan.create_operation, command=TdsCoordinatorCommand.OBSERVE),
        observe_process=old.plan.create_process,
        management_admission=old.plan.creator_admission,
        original_registration_artifact_sha256="1" * 64,
        original_authority_artifact_sha256="2" * 64,
        original_authority_sha256="3" * 64,
        original_containment_artifact_sha256="4" * 64,
        preparation_artifact_sha256="5" * 64,
    )
    return SqlClientObserveDepartureRequest(plan=plan, startup=old.startup)


def test_original_and_helper_are_distinct():
    r = request()
    with pytest.raises(ValueError):
        replace(r, startup=replace(r.startup, process=r.plan.observe_process))
    with pytest.raises(ValueError):
        replace(r.plan, observe_operation=replace(r.plan.observe_operation, command=TdsCoordinatorCommand.CREATE))
    with pytest.raises(ValueError):
        replace(r.plan, principal=replace(r.plan.principal, name="writer"))


def test_all_original_scalar_aliases_reject():
    r = request()
    for path, value in leaves(r):
        bad = corrupt(request(), path, alias(value))
        with pytest.raises(ValueError):
            bad.__post_init__()
