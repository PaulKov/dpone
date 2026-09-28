"""Shared runtime boundary for durable replay capabilities and wire contracts.

Runtime composition, governance and sinks consume the same selection and target
observation vocabulary. Canonical values remain owned by contracts and the store
interface by ports. This module only re-exports them; it makes no authority,
policy or serialization decisions.
"""

from dpone.ports.quality_replay import (
    QualityReplayStore,
    canonical_json_bytes,
    contracts,
    selection_contracts,
    target_contracts,
)

ReplaySelectionError = selection_contracts.ReplaySelectionError
require_replay_boolean = selection_contracts.require_replay_boolean
replay_selection = selection_contracts.replay_selection
validate_replay_configuration = selection_contracts.validate_replay_configuration
validate_normalized_replay_selection = selection_contracts.validate_normalized_replay_selection

TargetAcceptanceRequest = target_contracts.TargetAcceptanceRequest
TargetAcceptanceError = target_contracts.TargetAcceptanceError
MAX_FRAME_BYTES = target_contracts.MAX_FRAME_BYTES
MAX_UINT64 = target_contracts.MAX_UINT64
UNAVAILABLE_WARNING = target_contracts.UNAVAILABLE_WARNING
unavailable_observation = target_contracts.unavailable_observation
validate_target_observation = target_contracts.validate_target_observation

__all__ = [
    "MAX_FRAME_BYTES",
    "MAX_UINT64",
    "UNAVAILABLE_WARNING",
    "QualityReplayStore",
    "ReplaySelectionError",
    "TargetAcceptanceError",
    "TargetAcceptanceRequest",
    "canonical_json_bytes",
    "contracts",
    "replay_selection",
    "require_replay_boolean",
    "unavailable_observation",
    "validate_normalized_replay_selection",
    "validate_replay_configuration",
    "validate_target_observation",
]
