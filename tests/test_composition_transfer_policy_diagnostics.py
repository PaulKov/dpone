"""Safe permission observations only; these offline rows certify no SQL rights."""

from types import SimpleNamespace

import pytest

from tests.integration.composition import mssql_transfer_fence_live_support as support


class Cursor:
    def __init__(self, rows):
        self.steps = iter(rows)
        self.calls = []

    def execute(self, statement, *parameters):
        self.calls.append((statement, parameters))
        self.rows = next(self.steps)

    def fetchall(self):
        return self.rows


def test_permission_diagnostic_preserves_coordinates_without_principal_identity():
    cursor = Cursor(
        (((1, 1, "S", 1, 0, 0, 1, 2),), (("G", 1, -123, 0, "SELECT", 1),), (("G", 1, 456, 0, "EXECUTE", 0),))
    )
    credentials = SimpleNamespace(login_name="PRIVATE_NAME", login_sid=b"PRIVATE_SID")
    value = support.observe_transfer_user_policy(cursor, "owned", credentials)
    assert value["policy_rows"] == [[1, 1, "S", 1, 0, 0, 1, 2]]
    assert value["public_permissions"]["rows"] == [["G", 1, -123, 0, "SELECT", 1]]
    assert "PRIVATE" not in support.observation_document(value)


def test_permission_observation_bounds_and_invalid_text():
    cursor = Cursor((tuple(("G", 1, index, 0, "SELECT", 1) for index in range(33)),))
    value = support.observe_transfer_permissions(cursor)
    assert len(value["rows"]) == 32 and value["truncated"]
    bad = Cursor(((("G", 1, 2, 0, "PRIVATE;password", 0),),))
    with pytest.raises(RuntimeError, match="transfer_permission_observation_shape"):
        support.observe_transfer_permissions(bad)


def test_policy_observer_delegates_after_capture_and_preserves_rejection(monkeypatch):
    events = []
    monkeypatch.setattr(
        support, "observe_transfer_user_policy", lambda *args: events.append("capture") or {"safe": True}
    )

    def denied(*args):
        events.append("require")
        raise RuntimeError("original rejection")

    monkeypatch.setattr(support.MssqlCompositionTransferAccess, "require", denied)
    observer = support.ObservedTransferAccess(lambda *args: events.append("record"))
    with pytest.raises(RuntimeError, match="original rejection"):
        observer.require(SimpleNamespace(cursor=object(), schema="owned"), object())
    assert events == ["capture", "record", "require"]
