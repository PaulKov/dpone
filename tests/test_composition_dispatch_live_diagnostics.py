"""Offline tests for diagnostics only; no SQL server or route evidence."""

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from dpone.contracts.composition_identity import CompositionAdmissionError
from tests.integration.composition import test_composition_dispatch_schema_live as live


def install_transaction(monkeypatch, error):
    class Cursor:
        description = None

        def execute(self, *args):
            if error is not None:
                raise error

        def nextset(self):
            return False

    @contextmanager
    def transaction(*args, **kwargs):
        try:
            yield SimpleNamespace(cursor=Cursor())
        except CompositionAdmissionError:
            raise
        except Exception:
            raise CompositionAdmissionError("control_operation_unknown") from None

    monkeypatch.setattr(live, "observed_transaction", transaction)


def test_unrelated_error_is_not_hidden_or_accepted(monkeypatch):
    install_transaction(monkeypatch, RuntimeError("42000", "private driver body (208)"))
    with pytest.raises(AssertionError, match="208"):
        live.reject_invariant(None, "unused", stage="deny_delete", record_property=lambda *args: None)


def test_forbidden_mutation_accepted_fails_explicitly(monkeypatch):
    install_transaction(monkeypatch, None)
    with pytest.raises(AssertionError, match="forbidden_mutation_accepted"):
        live.reject_invariant(None, "unused", stage="deny_delete", record_property=lambda *args: None)


def test_expected_invariant_passes_without_driver_body(monkeypatch):
    install_transaction(monkeypatch, RuntimeError("42000", "private driver body (51000)"))
    records = []
    live.reject_invariant(None, "unused", stage="deny_delete", record_property=lambda *args: records.append(args))
    assert "private" not in str(records)
    assert "51000" in str(records)


def test_terminator_error_is_not_added_to_allowed_codes(monkeypatch):
    install_transaction(monkeypatch, RuntimeError("23000", "private duplicate (2627); terminated (3621)"))
    with pytest.raises(AssertionError, match="3621"):
        live.reject_invariant(
            None, "unused", stage="deny_duplicate_claim", record_property=lambda *args: None, codes=(2601, 2627)
        )


def test_observer_records_all_numeric_codes_before_sanitizer(monkeypatch):
    import json

    class Cursor:
        def execute(self, *args):
            raise RuntimeError("23000", "PRIVATE_PASSWORD duplicate (2627); statement terminated (3621)")

    connection = SimpleNamespace(cursor=lambda: Cursor())
    case = SimpleNamespace(database=SimpleNamespace(connect=lambda: connection), schema="owned", service_id="synthetic")

    @contextmanager
    def transaction(factory, *args):
        try:
            yield SimpleNamespace(cursor=factory().cursor())
        except Exception:
            raise CompositionAdmissionError("control_operation_unknown") from None

    monkeypatch.setattr(live, "composition_control_transaction", transaction)
    records = []
    with pytest.raises(CompositionAdmissionError, match="control_operation_unknown"):
        with live.observed_transaction(case, "claim_insert", lambda *args: records.append(args)) as ledger:
            ledger.cursor.execute("PRIVATE_SQL", "PRIVATE_PARAMETER")
    assert records[0][0] == "dpone.dispatch.claim_insert"
    assert "PRIVATE" not in str(records)
    value = json.loads(records[0][1])
    assert value["events"][0]["native_codes"] == [2627, 3621]
    assert value["events"][0]["operation"] == "execute"
    assert value["events"][0]["sqlstate"] == "23000"


def test_observer_rejects_untrusted_stage_before_io():
    with pytest.raises(ValueError, match="dispatch_diagnostic_stage"):
        with live.observed_transaction(None, "private caller text", lambda *args: None):
            raise AssertionError("unreachable")
