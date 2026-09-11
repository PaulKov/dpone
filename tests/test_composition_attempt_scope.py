"""Audit exact attempt scope without pretending a retiring parent is ACTIVE."""

from dataclasses import replace

import pytest

from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_attempt import require_composition_attempt_admission, require_composition_attempt_scope
from tests.test_composition_activation_contract import digest, occurrence
from tests.test_composition_attempt_fencing import attempt


def test_retirement_scope_audit_does_not_reopen_attempt_admission():
    retiring = occurrence("RETIRING")
    value = attempt()
    assert require_composition_attempt_scope(retiring, value) == frozenset(guard for guard, _ in value.guard_epochs)
    with pytest.raises(CompositionAdmissionError, match="occurrence_state"):
        require_composition_attempt_admission(retiring, value, ())


@pytest.mark.parametrize("field", ["activation_request_sha256", "pack_sha256"])
def test_retirement_rejects_foreign_parent_or_pack(field):
    value = replace(attempt(), **{field: digest("foreign")})
    with pytest.raises(CompositionAdmissionError):
        require_composition_attempt_scope(occurrence("RETIRING"), value)
