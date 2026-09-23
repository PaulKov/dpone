"""Concrete session admission rejects mismatched intent before any SQL."""

from dataclasses import replace

import pytest

from dpone.adapters.mssql_sqlclient_observe_session import SqlClientObserveSession
from tests.test_mssql_sqlclient_observe_handshake import credentials
from tests.test_mssql_tds_coordinator_supervisor import Harness


def test_mismatched_envelope_never_constructs_connection(monkeypatch):
    request, value, startup = credentials()
    from dpone.adapters import mssql_sqlclient_observe_session as module

    monkeypatch.setattr(module, "TdsCoordinatorConnection", lambda *args: pytest.fail("premature SQL"))
    with pytest.raises(ValueError):
        SqlClientObserveSession(
            replace(request, operation_deadline_ns=request.operation_deadline_ns + 1),
            value,
            startup,
            Harness().admission,
        )
