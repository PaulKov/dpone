"""One bounded target completion attempt under the store's exact reader guard."""

from __future__ import annotations

from time import monotonic
from typing import Any

from dpone.runtime.governance.quality_target_plan import target_request
from dpone.runtime.quality_replay_contracts import (
    UNAVAILABLE_WARNING,
    QualityReplayStore,
    TargetAcceptanceError,
    contracts,
    validate_target_observation,
)


def complete_target(
    store: QualityReplayStore, config: Any, capsule: contracts.QualityReplayCapsule, policy: Any
) -> contracts.QualityReplayCapsule:
    """Never rescan COMPLETE and never release a worker of uncertain lifetime."""
    if capsule.state == "FAILED":
        raise contracts.ReplayQualityEvidenceError("FAILED")
    request = target_request(capsule.core, capsule.core_digest, store.reader_token(config))
    reader = store.target_reader
    if capsule.state == "PREPARED":
        capsule = store.transition(config, capsule, "TARGET_PENDING")
    deadline = monotonic() + 60.0
    try:
        if capsule.state == "COMPLETE":
            observation = capsule.target
        else:
            observation = reader.collect(request, deadline=deadline)
        validate_target_observation(request, observation, allow_unavailable=True)
        if observation["warnings"] == [UNAVAILABLE_WARNING] and policy.mode != "warn_only":
            raise TargetAcceptanceError("INCOMPLETE")
        if observation["replica"] != capsule.core["target_plan"]["replicas"][0]:
            raise TargetAcceptanceError("MISMATCH")
        if monotonic() >= deadline:
            raise TargetAcceptanceError("INCOMPLETE")
        reader.verify_generation(request, deadline=deadline)
        if monotonic() >= deadline:
            raise TargetAcceptanceError("INCOMPLETE")
        store.reader_token(config)
        if capsule.state != "COMPLETE":
            capsule = store.transition(config, capsule, "COMPLETE", target=observation)
        return capsule
    except TargetAcceptanceError as error:
        if not error.quiescent or not error.output_revoked:
            store.retain_guard(config)
        elif capsule.state != "COMPLETE" and error.code.endswith(("_INVALID", "_MISMATCH", "_FAILED")):
            store.transition(config, capsule, "FAILED")
        raise
    except BaseException:
        # Cancellation or unexpected adapter failure cannot assert quiescence.
        store.retain_guard(config)
        raise
