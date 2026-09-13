"""Concrete backend groups aliases and retains complete catalog evidence."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.adapters.composition_mssql_enrollment import mssql_physical_domain
from dpone.app.composition_physical_backend import CompositionProtectedPhysicalBackend
from dpone.contracts.composition_activation import CompositionOccurrenceContext
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
from dpone.services.composition_physical_admission import observe_composition_writes
from tests.test_dbt_workspace_mssql_physical_authority import _observation

SERVICE = "10000000-0000-4000-8000-000000000001"
CONTEXT = CompositionOccurrenceContext(
    "10000000-0000-4000-8000-000000000002",
    "synthetic",
    "sha256:" + "a" * 64,
    "sha256:" + "b" * 64,
    None,
    "sha256:" + "c" * 64,
)


def backend(*, collision=False, incomplete=False):
    original = _observation(equivalence=(11, 11) if collision else (11, 12))
    pin = original.request.pin
    properties = {
        "database": "DWH",
        "composition_service_id": SERVICE,
        "database_authorities": {
            "DWH": {
                "database_id": pin.database_id,
                "database_guid": str(pin.database_guid),
                "create_token": pin.create_token,
            }
        },
    }
    connection = ResolvedBindingConnection(
        SimpleNamespace(database="DWH"), {}, ResolvedConnectionDescriptor("mssql", properties)
    )
    calls = []

    def observe(request, connection):
        calls.append(request)
        return replace(original, request=request, slots=original.slots[:1] if incomplete else original.slots)

    enrollment = SimpleNamespace(resolve=lambda connection, write, context: mssql_physical_domain(SERVICE, pin))
    result = CompositionProtectedPhysicalBackend(
        inputs=SimpleNamespace(resolve_connection=lambda context, ref: connection),
        execution_capabilities=SimpleNamespace(
            execution_cells=frozenset(), require_execution=lambda plan, context: None
        ),
        mssql_enrollment=enrollment,
        clickhouse_enrollment=SimpleNamespace(),
        mssql_observer=SimpleNamespace(observe=observe),
    )
    writes = (original.request.writes[0], replace(original.request.writes[1], connection_ref="other_alias"))
    return result, writes, calls


def test_alias_union_has_one_catalog_comparison():
    instance, writes, calls = backend()
    resources = observe_composition_writes(writes, context=CONTEXT, backend=instance)
    assert len(resources) == 1 and len(calls) == 1
    assert len(calls[0].writes) == 2
    assert tuple(row.connection_ref for row in calls[0].writes) == ("warehouse", "other_alias")


@pytest.mark.parametrize("failure", ["collision", "incomplete"])
def test_complete_domain_collisions_and_partial_observations_reject(failure):
    instance, writes, _ = backend(collision=failure == "collision", incomplete=failure == "incomplete")
    with pytest.raises(CompositionAdmissionError):
        observe_composition_writes(writes, context=CONTEXT, backend=instance)


def test_missing_execution_capability_cannot_be_advertised():
    instance, _, _ = backend()
    assert instance.execution_cells == frozenset()


def test_alias_incarnation_drift_before_catalog_rejected():
    instance, writes, calls = backend()
    domain = instance.resolve_domain(writes[0], CONTEXT)
    instance._mssql_enrollment.resolve = lambda connection, write, context: replace(
        domain, physical_subject_sha256="sha256:" + "f" * 64
    )
    with pytest.raises(CompositionAdmissionError, match="physical_binding_drift"):
        instance.observe_domain(domain, writes, CONTEXT)
    assert not calls
