"""Private packet representation tests, never an authority certificate."""

from copy import deepcopy

import pytest

from dpone.contracts.dbt_physical_transport_delivery import PhysicalTransportDelivery
from dpone.contracts.native_delivery_json import encode_native_delivery_json
from tests.support.dbt_mssql_physical_registration import registration_inputs


def packet_payload():
    registration = registration_inputs()
    ref = {"locator": "original", "sha256": "sha256:" + "a" * 64}
    from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration

    raw = MssqlPhysicalRuntimeRegistration(**registration).to_dict()
    raw["limits"]["max_metadata_bytes"] = 65536
    uuid = "00000000-0000-0000-0000-000000000001"
    return {
        "schema": "dpone.dbt-physical-transport-delivery.v1",
        "launch_id": uuid,
        "command_index": 0,
        "parent_pid": 123,
        "admitted_monotonic_ns": 1,
        "deadline_monotonic_ns": 100000000000000000,
        "generation_id": uuid,
        "executor_invocation_id": uuid,
        "guard_epoch": 1,
        "command_plan": ref,
        "toolchain": ref,
        "qualification": ref,
        "registration_id": uuid,
        "registration_sha256": ref["sha256"],
        "platform_subject": raw["platform_subject"],
        "trusted_profile": raw["trusted_profile"],
        "limits": raw["limits"],
        "plan_set": ref,
        "model_database": raw["model_database"],
        "model_schema": "models",
        "adapter_type": "dpone_sqlserver",
        "profile_name": "profile",
        "target_name": "target",
        "argv_sha256": ref["sha256"],
        "profile_file_device": 1,
        "profile_file_inode": 2,
        "profile_file_sha256": ref["sha256"],
        "local_schema": "dpone_physical",
    }


def test_canonical_immutable_packet_detaches_input_and_repr_hides_digest():
    raw = packet_payload()
    packet = PhysicalTransportDelivery(encode_native_delivery_json(raw))
    copy = packet.to_dict()
    copy["limits"]["max_metadata_bytes"] = 1
    assert packet.to_dict() == raw
    assert raw["profile_file_sha256"] not in repr(packet)


@pytest.mark.parametrize(
    "field,value",
    [
        ("adapter_type", "sqlserver"),
        ("command_index", True),
        ("parent_pid", 0),
        ("deadline_monotonic_ns", 1),
        ("local_schema", "dbo"),
        ("extra", 1),
        ("profile_file_inode", False),
        ("generation_id", "not-a-uuid"),
    ],
)
def test_closed_packet_rejects_drift(field, value):
    raw = packet_payload()
    raw[field] = value
    with pytest.raises(ValueError):
        PhysicalTransportDelivery(encode_native_delivery_json(raw))


def test_noncanonical_duplicates_and_policy_budget_reject():
    raw = packet_payload()
    encoded = encode_native_delivery_json(raw)
    for payload in (encoded + b" ", b'{"schema":1,"schema":1}'):
        with pytest.raises(ValueError):
            PhysicalTransportDelivery(payload)
    limited = deepcopy(raw)
    limited["limits"]["max_metadata_bytes"] = 1
    with pytest.raises(ValueError):
        PhysicalTransportDelivery(encode_native_delivery_json(limited))


def test_authority_producer_rejects_forged_resolved_dtos_and_callbacks():
    from dpone.adapters.dbt_mssql_physical_protocol_delivery import AuthenticatedPhysicalTransportDeliveryProducer

    with pytest.raises(ValueError):
        AuthenticatedPhysicalTransportDeliveryProducer(
            policy_reader=lambda: packet_payload(),
            registration_store=object(),
            originals=object(),
            refs=object(),
            registration=object(),
            executor=object(),
            toolchain=object(),
            qualification=object(),
            plan_set=object(),
            profile=object(),
            cancellation=object(),
        )
