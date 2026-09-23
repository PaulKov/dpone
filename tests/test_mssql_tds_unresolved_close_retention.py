"""Ambiguous unresolved-launch close cannot be repeated during containment."""

from types import SimpleNamespace

import pytest

from dpone.app.mssql_tds_coordinator_supervision import TdsCoordinatorSupervisionUnknown, fail_coordinator
from dpone.ports.mssql_tds_worker import TdsLaunchUnknown
from tests.test_mssql_tds_coordinator_supervisor import Harness


@pytest.mark.parametrize("fails", [False, True])
def test_unresolved_launch_close_records_attempt_and_never_retries(fails):
    h = Harness()
    closes = []

    def close():
        closes.append(1)
        if fails:
            raise OSError("ambiguous close")

    launch = SimpleNamespace(contain=lambda **kw: None, close=close)

    def spawn(**kw):
        raise TdsLaunchUnknown(launch)

    h.launcher.spawn = spawn
    with pytest.raises(TdsCoordinatorSupervisionUnknown) as caught:
        h.run()
    retained = caught.value.retained
    with pytest.raises(TdsCoordinatorSupervisionUnknown):
        fail_coordinator(retained, RuntimeError(), termination_timeout=50.0, clock=lambda: h.now)
    assert closes == [1]
    assert retained.unresolved_close_attempted is True
    assert retained.unresolved_closed is (not fails)
    assert retained.unresolved_launch is launch
    assert retained.containment_deadline == 15.0
