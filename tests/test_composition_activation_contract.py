"""Offline parent authority and stale/partial readback regressions."""

from dataclasses import replace

import pytest

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.composition_activation import (
    CompositionActivationOccurrence,
    CompositionActivationReceipt,
    CompositionActivationRequest,
    CompositionAdmissionError,
    CompositionOccurrenceContext,
    CompositionPhysicalResource,
    CompositionWorkloadAdmission,
)


def digest(value):
    return canonical_fingerprint({"synthetic": value})


def request():
    physical = digest("database continuity")
    guard = canonical_fingerprint(
        {
            "schema": "dpone.composition-physical-domain.v1",
            "connector": "mssql",
            "service_id": "synthetic-service",
            "physical_subject_sha256": physical,
        }
    )
    writes = tuple(sorted((digest("native write"), digest("ordinary write"))))
    return CompositionActivationRequest(
        CompositionOccurrenceContext(
            "10000000-0000-4000-8000-000000000001",
            "test",
            digest("parent"),
            digest("deployment"),
            None,
            digest("runtime"),
        ),
        digest("sources"),
        (
            CompositionWorkloadAdmission("a_native", "native", digest("native pack"), "sqlserver_dbt_v1", (writes[0],)),
            CompositionWorkloadAdmission(
                "b_ordinary", "standalone", digest("ordinary pack"), "postgres_mssql_full_refresh_v1", (writes[1],)
            ),
        ),
        (
            CompositionPhysicalResource(
                guard, "mssql", "synthetic-service", physical, digest("initial observation"), writes
            ),
        ),
    )


def occurrence(state="ACTIVE"):
    value = request()
    return CompositionActivationOccurrence(
        value, CompositionActivationReceipt(value.request_sha256, state, ((value.resources[0].guard_id, 1),))
    )


def test_native_only_subset_cannot_be_parent_authority():
    value = request()
    with pytest.raises(CompositionAdmissionError, match="complete_parent_required"):
        replace(value, workloads=value.workloads[:1])


def test_missing_or_duplicate_physical_write_is_rejected():
    value = request()
    with pytest.raises(CompositionAdmissionError, match="physical_partition"):
        replace(value, resources=(replace(value.resources[0], write_subjects=value.workloads[0].write_subjects),))
    with pytest.raises(CompositionAdmissionError, match="physical_partition"):
        replace(
            value,
            workloads=(
                value.workloads[0],
                replace(value.workloads[1], write_subjects=value.workloads[0].write_subjects),
            ),
        )


def test_credential_alias_cannot_be_used_as_physical_guard():
    value = request()
    with pytest.raises(CompositionAdmissionError, match="physical_guard"):
        replace(value.resources[0], guard_id=digest("alias or credential"))


def test_foreign_receipt_and_missing_guard_cannot_acknowledge_activation():
    value = occurrence()
    with pytest.raises(CompositionAdmissionError, match="occurrence_readback"):
        replace(value, receipt=replace(value.receipt, request_sha256=digest("foreign")))
    with pytest.raises(CompositionAdmissionError, match="occurrence_state"):
        value.require_state("PREPARED")


@pytest.mark.parametrize("epoch", [0, -1, True, "1"])
def test_fencing_epoch_must_be_positive_integer(epoch):
    value = occurrence()
    with pytest.raises(CompositionAdmissionError, match="guard_epoch"):
        replace(value.receipt, guard_epochs=((value.request.resources[0].guard_id, epoch),))


def test_request_serialization_retains_parent_and_all_workloads():
    value = request()
    payload = value.to_dict()
    assert payload["schema"] == "dpone.composition-activation-request.v1"
    assert payload["context"]["release_id"] == digest("parent")
    assert [row["workload_id"] for row in payload["workloads"]] == ["a_native", "b_ordinary"]
    assert value.request_sha256 == canonical_fingerprint(payload)


@pytest.mark.parametrize(
    "activation_id", ["00000000-0000-0000-0000-000000000000", "10000000-0000-1000-8000-000000000001"]
)
def test_occurrence_identity_requires_uuid_v4(activation_id):
    with pytest.raises(CompositionAdmissionError, match="activation_id"):
        replace(request().context, activation_id=activation_id)


def test_mutable_guard_pair_cannot_change_a_frozen_receipt():
    value = occurrence()
    with pytest.raises(CompositionAdmissionError, match="guard_epochs"):
        replace(value.receipt, guard_epochs=([value.request.resources[0].guard_id, 1],))
