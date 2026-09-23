"""Pure OBSERVE predictions authenticate links, never producer acknowledgements."""

from dataclasses import replace

import pytest

from dpone.contracts.mssql_sqlclient_departure_chain import reconstruct_observe_departure_chain
from tests.test_mssql_sqlclient_observe_departure_evidence import chain


def test_canonical_six_records_match_independent_codec_chain():
    request, objects, payloads = chain()
    records = reconstruct_observe_departure_chain(
        request.plan,
        request,
        request.startup,
        objects[3].result,
        objects[4].exit,
        observer_admission=request.plan.observer_admission,
    )
    assert tuple(record.payload for record in records) == payloads
    assert records[0].observe_plan is request.plan
    assert all(record.observe_request is request for record in records[1:])


@pytest.mark.parametrize("field", ["plan", "startup", "exit", "admission"])
def test_cross_binding_rejected(field):
    request, objects, _ = chain()
    plan, startup, exit = request.plan, request.startup, objects[4].exit
    admission = plan.observer_admission
    if field == "plan":
        plan = replace(plan, preparation_artifact_sha256="9" * 64)
    elif field == "startup":
        startup = replace(startup, package_root="/different")
    elif field == "exit":
        exit = replace(exit, identity=plan.observe_process)
    else:
        admission = replace(admission, server=replace(admission.server, server_name="different"))
    with pytest.raises(ValueError):
        reconstruct_observe_departure_chain(
            plan,
            request,
            startup,
            objects[3].result,
            exit,
            observer_admission=admission,
        )


def test_equal_copies_predict_bytes_without_claiming_acknowledgements():
    from copy import deepcopy

    request, objects, payloads = chain()
    copied = deepcopy(request)
    records = reconstruct_observe_departure_chain(
        deepcopy(request.plan),
        copied,
        deepcopy(request.startup),
        deepcopy(objects[3].result),
        deepcopy(objects[4].exit),
        observer_admission=deepcopy(request.plan.observer_admission),
    )
    assert tuple(record.payload for record in records) == payloads
    assert records[1].observe_request is copied
    assert copied is not request


def test_aliases_in_all_input_leaves_fail_closed():
    from tests.mssql_sqlclient_departure_v2_fixtures import alias, corrupt, leaves

    request, objects, _ = chain()
    args = dict(
        plan=request.plan,
        request=request,
        startup=request.startup,
        result=objects[3].result,
        local_exit=objects[4].exit,
        observer_admission=request.plan.observer_admission,
    )
    for field, value in args.items():
        for path, leaf in leaves(value):
            with pytest.raises(ValueError):
                reconstruct_observe_departure_chain(**{**args, field: corrupt(value, path, alias(leaf))})
