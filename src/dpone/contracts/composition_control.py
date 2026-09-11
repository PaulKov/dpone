"""Stable contract boundary for parent admission composition.

Ports, adapters and application services consume these explicit parent/source/physical
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
    require_composition_attempt_scope,
)
from dpone.contracts.composition_execution import (
    CompositionExecutionPlan,
    composition_generated_transfer_cell,
    composition_transfer_cell,
)
from dpone.contracts.composition_persistence import (
    decode_activation_request,
    decode_attempt_identity,
    decode_attempt_proof,
    encode_activation_request,
    encode_attempt_identity,
    encode_attempt_proof,
)
from dpone.contracts.composition_physical import CompositionDomainObservation, CompositionPhysicalDomain
from dpone.contracts.composition_proof import (
    CompositionAttemptProof,
    CompositionProofAuthority,
    composition_attempt_epoch_subject,
)
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
    "require_composition_attempt_scope",
    "CompositionAttemptProof",
    "CompositionProofAuthority",
    "composition_attempt_epoch_subject",
    "decode_activation_request",
    "decode_attempt_identity",
    "decode_attempt_proof",
    "encode_activation_request",
    "encode_attempt_identity",
    "encode_attempt_proof",
    "CompositionExecutionPlan",
    "composition_generated_transfer_cell",
    "composition_transfer_cell",
    "CompositionDomainObservation",
    "CompositionPhysicalDomain",
    "CompositionSourceSnapshot",
    "DbtRelationWrite",
    "dbt_relation_write_subject",
]
