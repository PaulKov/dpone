"""Construction origin is a local exact reference, never a factory invocation."""

from time import monotonic

import pytest

from dpone.contracts.bounded_window import WindowContractError
from dpone.services.mssql_tds_attempt import TdsAttempt, TdsAttemptUnknown
from tests.test_mssql_tds_attempt import create
from tests.test_mssql_tds_attempt import setup as setup


def test_actual_constructor_retains_exact_factory_without_calling_it(setup):
    attempt = create(setup)
    attempt._assert_composition_origin(setup[3])

    class EqualFactory:
        def __eq__(self, other):
            pytest.fail("origin compared by equality")

        def __call__(self):
            pytest.fail("origin assertion invoked factory")

    with pytest.raises(WindowContractError, match="origin"):
        attempt._assert_composition_origin(EqualFactory())
    attempt._assert_composition_origin(setup[3])


def test_legacy_constructor_does_not_gain_origin_but_observations_work(setup):
    original = create(setup)
    attempt = TdsAttempt(original._lifecycle, original._directory)
    assert attempt.lifecycle == original.lifecycle
    with pytest.raises(WindowContractError, match="origin"):
        attempt._assert_composition_origin(None)
    with pytest.raises(WindowContractError, match="origin"):
        attempt._assert_composition_origin(setup[3])


@pytest.mark.parametrize("state", ["closed", "poisoned", "busy", "pid", "thread"])
def test_origin_cannot_bypass_existing_local_lifecycle_guards(setup, state):
    attempt = create(setup)
    if state == "closed":
        attempt.close(deadline=monotonic() + 2)
    elif state == "pid":
        attempt._pid -= 1
    elif state == "thread":
        attempt._thread = object()
    else:
        setattr(attempt, "_" + state, True)
    with pytest.raises((WindowContractError, TdsAttemptUnknown)):
        attempt._assert_composition_origin(setup[3])
