"""Offline saga faults; fake persistence is never SQL/session evidence."""

from types import SimpleNamespace

import pytest

from dpone.contracts.composition_activation import (
    CompositionActivationOccurrence,
    CompositionActivationReceipt,
    CompositionAdmissionError,
)
from dpone.services.composition_activation_coordinator import CompositionActivationCoordinator
from tests.test_composition_activation_contract import request


class Harness:
    def __init__(self):
        self.request = request()
        self.events = []
        self.current = None
        self.failure = None
        self.drift_epoch = False
        self.coordinator = CompositionActivationCoordinator(
            inputs=self, preparation=self, stores=SimpleNamespace(build=lambda **kwargs: self)
        )

    def load_context(self, **kwargs):
        return self.request.context

    def load_sources(self, **kwargs):
        return object()

    def read(self, activation_id):
        self.events.append("read")
        return self.current

    def prepare(self, value=None, **kwargs):
        if value is None:
            self.events.append("full_physical_preparation")
            if self.failure:
                raise CompositionAdmissionError(self.failure)
            return self.request
        self.events.append("reserve")
        self.current = self._receipt("PREPARED")
        return self.current

    def require_existing(self, value, **kwargs):
        self.events.append("stable_binding_check")
        assert value == self.request

    def activate(self, value):
        self.events.append("activate")
        self.current = self._receipt("ACTIVE")
        return self.current

    def begin_retirement(self, value):
        self.events.append("close_admission")
        self.current = self._receipt("RETIRING")
        return self.current

    def finalize_retirement(self, value):
        self.events.append("require_quiescence_and_outcome")
        if self.failure:
            raise CompositionAdmissionError(self.failure)
        self.current = self._receipt("RETIRED")
        return self.current

    def _receipt(self, state):
        return CompositionActivationOccurrence(
            self.request,
            CompositionActivationReceipt(
                self.request.request_sha256,
                state,
                ((self.request.resources[0].guard_id, 2 if self.drift_epoch else 1),),
            ),
        )

    def args(self, tmp_path):
        context = self.request.context
        return dict(
            projection_root=tmp_path,
            activation_id=context.activation_id,
            environment=context.environment,
            release_id=context.release_id,
            deployment_id=context.deployment_id,
            previous_deployment_id=context.previous_deployment_id,
        )


def test_complete_preparation_precedes_any_reservation_or_predecessor_drain(tmp_path):
    h = Harness()
    prepared = h.coordinator.prepare(**h.args(tmp_path))
    assert prepared.receipt.state == "PREPARED"
    assert h.events == ["read", "full_physical_preparation", "reserve"]


def test_missing_capability_does_not_mutate_control_state(tmp_path):
    h = Harness()
    h.failure = "complete_execution_capability_unavailable"
    with pytest.raises(CompositionAdmissionError):
        h.coordinator.prepare(**h.args(tmp_path))
    assert h.current is None
    assert "reserve" not in h.events


def test_retry_uses_original_admission_instead_of_mutable_catalog_digest(tmp_path):
    h = Harness()
    prepared = h.coordinator.prepare(**h.args(tmp_path))
    h.failure = "fresh_catalog_changed_after_valid_ddl"
    h.events.clear()
    assert h.coordinator.prepare(**h.args(tmp_path)) == prepared
    assert h.events == ["read", "stable_binding_check"]
    active = h.coordinator.activate(prepared, projection_root=tmp_path)
    assert active.request == prepared.request
    assert "full_physical_preparation" not in h.events


def test_changed_epochs_cannot_acknowledge_same_activation(tmp_path):
    h = Harness()
    prepared = h.coordinator.prepare(**h.args(tmp_path))
    h.drift_epoch = True
    with pytest.raises(CompositionAdmissionError, match="transition_readback"):
        h.coordinator.activate(prepared, projection_root=tmp_path)


def test_unknown_attempt_blocks_retirement_after_new_admission_is_closed(tmp_path):
    h = Harness()
    prepared = h.coordinator.prepare(**h.args(tmp_path))
    active = h.coordinator.activate(prepared, projection_root=tmp_path)
    retiring = h.coordinator.begin_retirement(active, projection_root=tmp_path)
    h.failure = "attempt_commit_unknown"
    with pytest.raises(CompositionAdmissionError, match="attempt_commit_unknown"):
        h.coordinator.finalize_retirement(retiring, projection_root=tmp_path)
    assert h.current.receipt.state == "RETIRING"
    assert h.current.receipt.guard_epochs == active.receipt.guard_epochs


def test_require_active_never_creates_missing_occurrence(tmp_path):
    h = Harness()
    with pytest.raises(CompositionAdmissionError, match="occurrence_missing"):
        h.coordinator.require_active(**h.args(tmp_path))
    assert h.events == ["read"]


def test_foreign_projection_context_rejected_before_source_or_store(tmp_path):
    h = Harness()
    args = h.args(tmp_path)
    args["environment"] = "other"
    with pytest.raises(CompositionAdmissionError, match="projection_context"):
        h.coordinator.prepare(**args)
    assert h.events == []
