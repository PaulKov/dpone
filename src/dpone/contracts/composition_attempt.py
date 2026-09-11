"""Compatibility exports for durable attempt originals."""

from dpone.contracts.composition_persistence import CompositionAttemptIdentity as CompositionAttemptIdentity
from dpone.contracts.composition_persistence import CompositionAttemptReceipt as CompositionAttemptReceipt
from dpone.contracts.composition_persistence import (
    require_composition_attempt_admission as require_composition_attempt_admission,
)
from dpone.contracts.composition_persistence import (
    require_composition_attempt_scope as require_composition_attempt_scope,
)
