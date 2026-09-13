"""Expected transfer invocation must match runtime policy and trusted source."""

from copy import deepcopy
from dataclasses import replace

import pytest

from dpone.app.composition_pack_execution_dispatcher import verify_transfer_pack_operation
from dpone.app.composition_transfer_invocation import expected_transfer_invocation
from tests.test_composition_pack_execution_dispatcher import _ORDINARY_MANIFEST, _transfer_operation_case


@pytest.mark.parametrize("reconciliation,expected", [(False, "retained_business_process"), (True, "b_ordinary")])
def test_verified_process_override_uses_actual_hydration_policy(reconciliation, expected):
    manifest = deepcopy(_ORDINARY_MANIFEST)
    manifest["source"]["options"] = {"state_identity": {"process": "retained_business_process"}}
    manifest["source"]["options"]["reconciliation"] = {"enabled": reconciliation}
    attempt, operation, write, mutation, plan = _transfer_operation_case(map_index=2)
    identity = expected_transfer_invocation(attempt, write, verified_manifest=manifest, runtime_environment="test")
    assert identity.process == expected
    assert identity.run_id == attempt.dag_run_id
    assert identity.task_partition == f"{write.workflow_id}:{attempt.task_id}[2]"
    request = replace(operation.attempt.request, invocation=identity)
    operation = replace(operation, attempt=replace(operation.attempt, request=request))
    verify_transfer_pack_operation(
        attempt, operation, write, mutation, plan, verified_manifest=manifest, runtime_environment="test"
    )


def test_missing_verified_environment_cannot_invent_runtime_context():
    from dpone.contracts.composition_activation import CompositionAdmissionError

    attempt, _, write, _, _ = _transfer_operation_case()
    with pytest.raises(CompositionAdmissionError, match="transfer_operation"):
        expected_transfer_invocation(attempt, write, verified_manifest=_ORDINARY_MANIFEST, runtime_environment=None)
