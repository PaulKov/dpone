"""Source plans own wire interpretation; acquisition still verifies pack bytes."""

import pytest

from dpone.contracts.dbt_source_inventory_binding import DbtSourcePlan
from dpone.services.dbt_release_workflow_reader import DbtDevEvidenceReleaseError, read_dbt_workload_pack
from tests.test_dbt_release_source_reader import _read, _tree


def test_plan_decodes_selection_and_binds_verified_pack_trio(tmp_path):
    root, release, inventory = _tree(tmp_path)
    plan = DbtSourcePlan.from_release(release, inventory, expected_release_id=release["release_id"])
    source = inventory.projects[0].workflows[0]
    descriptor = plan.artifacts.payloads[source.runtime_payload_ids[2]]
    selection = plan.decode_selection((root / descriptor["path"]).read_bytes())
    pack = read_dbt_workload_pack(root, plan.artifacts.workloads[source.workload_id], source.workload_id)
    execution = plan.execution_from_pack(source, pack)
    assert execution.workflow_id == source.workflow_id
    assert execution.selection_lock == selection


@pytest.mark.parametrize("payload", [b"[]", b'{"schema":1,"schema":2}', b"not json"])
def test_plan_retains_selection_decode_error_label(payload):
    with pytest.raises(DbtDevEvidenceReleaseError, match="selection lock"):
        DbtSourcePlan.decode_selection(payload)


def test_plan_rejects_foreign_trio_before_unpacking_execution(tmp_path):
    _, release, inventory = _tree(tmp_path)
    plan = DbtSourcePlan.from_release(release, inventory, expected_release_id=release["release_id"])
    source = inventory.projects[0].workflows[0]
    with pytest.raises(DbtDevEvidenceReleaseError, match="embedded workload trio differs"):
        plan.execution_from_pack(source, {"runtime_payload_ids": ["foreign"], "runtime_payload": None})


def test_plan_does_not_downgrade_legacy_pack_to_v1(tmp_path):
    root, release, inventory = _tree(tmp_path, legacy_pack=True)
    plan = DbtSourcePlan.from_release(release, inventory, expected_release_id=release["release_id"])
    source = inventory.projects[0].workflows[0]
    pack = read_dbt_workload_pack(root, plan.artifacts.workloads[source.workload_id], source.workload_id)
    with pytest.raises(DbtDevEvidenceReleaseError, match="execution pack differs from release wire version"):
        plan.execution_from_pack(source, pack)


def test_reader_verifies_pack_fingerprint_before_pure_execution_policy(tmp_path, monkeypatch):
    from dpone.services import dbt_release_workflow_reader

    root, release, _ = _tree(tmp_path)

    def rejected(_):
        raise dbt_release_workflow_reader.PackIdentityError("invalid fingerprint")

    monkeypatch.setattr(dbt_release_workflow_reader, "verify_pack_fingerprint", rejected)
    monkeypatch.setattr(DbtSourcePlan, "execution_from_pack", lambda *args: pytest.fail("unverified pack used"))
    with pytest.raises(DbtDevEvidenceReleaseError, match="workload pack identity is invalid"):
        _read(root, release)
