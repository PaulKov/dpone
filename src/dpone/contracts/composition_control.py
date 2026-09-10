"""Stable contract boundary for parent admission composition.

Ports and application services consume these explicit parent/source/physical
values through one boundary. Algorithms remain in their cohesive owner modules;
this facade performs no I/O, registration, backend selection or policy dispatch.
Internal contract modules import each other directly to avoid facade cycles.
"""

from dpone.contracts.composition_activation import (
    CompositionActivationOccurrence,
    CompositionActivationReceipt,
    CompositionActivationRequest,
    CompositionAdmissionError,
    CompositionOccurrenceContext,
    CompositionPhysicalResource,
    CompositionWorkloadAdmission,
)
from dpone.contracts.composition_attempt import (
    CompositionAttemptIdentity,
    CompositionAttemptReceipt,
    require_composition_attempt_admission,
)
from dpone.contracts.composition_execution import (
    CompositionExecutionPlan,
    composition_generated_transfer_cell,
    composition_transfer_cell,
)
from dpone.contracts.composition_physical import CompositionDomainObservation, CompositionPhysicalDomain
from dpone.contracts.composition_sources import CompositionSourceSnapshot
from dpone.contracts.dbt_relation_writes import DbtRelationWrite
from dpone.contracts.dbt_workspace_activation import dbt_relation_write_subject

__all__ = [
    "CompositionActivationOccurrence",
    "CompositionActivationReceipt",
    "CompositionActivationRequest",
    "CompositionAdmissionError",
    "CompositionOccurrenceContext",
    "CompositionPhysicalResource",
    "CompositionWorkloadAdmission",
    "CompositionAttemptIdentity",
    "CompositionAttemptReceipt",
    "require_composition_attempt_admission",
    "CompositionExecutionPlan",
    "composition_generated_transfer_cell",
    "composition_transfer_cell",
    "CompositionDomainObservation",
    "CompositionPhysicalDomain",
    "CompositionSourceSnapshot",
    "DbtRelationWrite",
    "dbt_relation_write_subject",
]
