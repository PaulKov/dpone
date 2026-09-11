"""Cross-alias collision checks must use one complete physical-domain query."""

from dataclasses import replace

import pytest

from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_physical import CompositionDomainObservation, CompositionPhysicalDomain
from dpone.contracts.dbt_relation_writes import DbtRelationWrite
from dpone.contracts.dbt_workspace_activation import dbt_relation_write_subject
from dpone.services.composition_physical_admission import observe_composition_writes
from tests.test_composition_activation_contract import digest


def writes():
    native = DbtRelationWrite(
        "native", "native_flow", "model.synthetic.value", "model", "mssql", "writer_a", "synthetic", "data", "value"
    )
    ordinary = DbtRelationWrite(
        "standalone", "copy_flow", "copy", "transfer", "mssql", "writer_b", "synthetic", "data", "VALUE"
    )
    return native, ordinary


class Backend:
    def __init__(self, *, collision=True):
        self.calls = []
        self.collision = collision
        self.domain = CompositionPhysicalDomain(
            "mssql", "10000000-0000-4000-8000-000000000002", digest("database continuity")
        )

    def resolve_domain(self, write, context):
        self.calls.append(("resolve", write.connection_ref))
        return self.domain

    def observe_domain(self, domain, rows, context):
        self.calls.append(("observe", tuple(row.connection_ref for row in rows)))
        return CompositionDomainObservation(
            domain,
            tuple((dbt_relation_write_subject(row), 0 if self.collision else i) for i, row in enumerate(rows)),
            digest("catalog"),
        )


def test_aliases_are_merged_before_catalog_collation_comparison():
    backend = Backend()
    with pytest.raises(CompositionAdmissionError, match="physical_target_collision"):
        observe_composition_writes(writes(), context=None, backend=backend)
    assert backend.calls[-1] == ("observe", ("writer_a", "writer_b"))
    assert sum(event[0] == "observe" for event in backend.calls) == 1


def test_distinct_targets_in_same_domain_share_one_guard():
    backend = Backend(collision=False)
    result = observe_composition_writes(writes(), context=None, backend=backend)
    assert len(result) == 1
    assert result[0].write_subjects == tuple(sorted(map(dbt_relation_write_subject, writes())))


def test_native_helper_collision_with_ordinary_target_is_not_ignored():
    native, ordinary = writes()
    native = replace(native, relation="value__dbt_backup", role="backup")
    ordinary = replace(ordinary, relation="VALUE__DBT_BACKUP")
    with pytest.raises(CompositionAdmissionError, match="physical_target_collision"):
        observe_composition_writes((native, ordinary), context=None, backend=Backend())


def test_missing_catalog_slot_rejects_complete_parent():
    class Partial(Backend):
        def observe_domain(self, domain, rows, context):
            return super().observe_domain(domain, rows[:1], context)

    with pytest.raises(CompositionAdmissionError, match="physical_observation_closure"):
        observe_composition_writes(writes(), context=None, backend=Partial(collision=False))


def test_mutable_catalog_slot_cannot_change_equivalence_after_validation():
    with pytest.raises(CompositionAdmissionError, match="physical_observation_closure"):
        CompositionDomainObservation(Backend().domain, ([digest("write"), 0],), digest("catalog"))
