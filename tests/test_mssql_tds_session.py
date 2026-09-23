"""Remote session continuity is distinct from mutation or settlement authority."""

from dataclasses import replace
from datetime import datetime
from uuid import UUID

import pytest

from dpone.adapters.mssql_tds_session import _INITIALIZE, TdsCoordinatorSession, TdsSessionObservationError
from dpone.contracts.mssql_tds_session import TdsRemoteSessionIdentity

NONCE = b"n" * 31 + b"\0"
CONNECTION = UUID("11111111-1111-1111-1111-111111111111")
DATABASE = UUID("22222222-2222-2222-2222-222222222222")
STAMP = datetime(2026, 1, 1, 12, 0, 0)


def observation():
    return [
        CONNECTION,
        72,
        STAMP,
        STAMP,
        None,
        "TCP",
        "TSQL",
        CONNECTION,
        0,
        NONCE,
        0,
        0,
        0,
        "server",
        "machine",
        "instance",
        "replica",
        "database",
        5,
        DATABASE,
        "login",
        b"sid",
        "original",
        b"originalsid",
        "user",
        1,
        b"usersid",
    ]


class Cursor:
    def __init__(self):
        self.row = observation()
        self.calls = []
        self.rows = []
        self.error = None
        self.duplicate = False
        self.description = None
        self.completion = None
        self.bad_description = False

    def execute(self, sql, *parameters):
        self.calls.append((sql, parameters))
        if self.error is not None:
            raise self.error
        self.rows = [] if sql == _INITIALIZE else ([self.row, self.row] if self.duplicate else [self.row])
        self.description = None if sql == _INITIALIZE else (("column",),)
        if self.bad_description:
            self.description = (("unexpected",),) if sql == _INITIALIZE else None

    def nextset(self):
        assert not self.rows
        if isinstance(self.completion, BaseException):
            raise self.completion
        return self.completion

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None


def test_initialize_and_read_only_recheck_preserve_trailing_zero_nonce():
    cursor = Cursor()
    session = TdsCoordinatorSession(cursor)
    identity = session.initialize(NONCE)
    assert identity.nonce == NONCE
    cursor.calls.clear()
    assert session.require_same(identity) is None
    assert len(cursor.calls) == 1
    assert "SET CONTEXT_INFO" not in cursor.calls[0][0]


def test_initialize_is_one_shot_even_after_lost_set_ack():
    cursor = Cursor()
    cursor.error = RuntimeError("private driver diagnostic")
    session = TdsCoordinatorSession(cursor)
    with pytest.raises(TdsSessionObservationError, match="unavailable"):
        session.initialize(NONCE)
    cursor.error = None
    with pytest.raises(TdsSessionObservationError):
        session.initialize(NONCE)
    assert len(cursor.calls) == 1


@pytest.mark.parametrize(
    ("index", "value"),
    [
        (0, UUID("33333333-3333-3333-3333-333333333333")),
        (1, 73),
        (2, datetime(2026, 1, 2)),
        (3, datetime(2026, 1, 2)),
        (9, b""),
        (13, "other-server"),
        (17, "other-db"),
        (18, 6),
        (19, UUID("44444444-4444-4444-4444-444444444444")),
        (20, "other-login"),
        (21, b"changed-sid"),
        (23, b"changed-original-sid"),
        (25, 2),
        (26, b"changed-user-sid"),
    ],
)
def test_changed_incarnation_or_authority_poison_without_nonce_restoration(index, value):
    cursor = Cursor()
    session = TdsCoordinatorSession(cursor)
    identity = session.initialize(NONCE)
    cursor.row[index] = value
    cursor.calls.clear()
    with pytest.raises(TdsSessionObservationError):
        session.require_same(identity)
    cursor.row = observation()
    with pytest.raises(TdsSessionObservationError):
        session.require_same(identity)
    assert len(cursor.calls) == 1
    assert "SET CONTEXT_INFO" not in cursor.calls[0][0]


@pytest.mark.parametrize(
    ("index", "value"),
    [
        (0, str(CONNECTION)),
        (1, True),
        (2, "2026-01-01"),
        (4, CONNECTION),
        (5, "Session"),
        (6, "SOAP"),
        (7, DATABASE),
        (8, 1),
        (8, False),
        (9, NONCE + bytes(96)),
        (12, 2),
        (13, None),
        (13, ""),
        (18, True),
        (19, str(DATABASE)),
        (21, bytearray(b"sid")),
        (23, b""),
    ],
)
def test_incomplete_or_ambiguous_observation_rejects(index, value):
    cursor = Cursor()
    cursor.row[index] = value
    with pytest.raises(TdsSessionObservationError):
        TdsCoordinatorSession(cursor).initialize(NONCE)


def test_multiple_connection_rows_reject_before_filtering():
    cursor = Cursor()
    cursor.duplicate = True
    with pytest.raises(TdsSessionObservationError):
        TdsCoordinatorSession(cursor).initialize(NONCE)


def test_transaction_state_changes_do_not_change_identity():
    cursor = Cursor()
    session = TdsCoordinatorSession(cursor)
    identity = session.initialize(NONCE)
    cursor.row[10:12] = [1, 1]
    session.require_same(identity, in_transaction=True)
    with pytest.raises(TdsSessionObservationError):
        session.require_same(identity, in_transaction=False)


@pytest.mark.parametrize("nonce", [b"", bytes(32), b"x" * 31, b"x" * 33, bytearray(b"x" * 32), None])
def test_invalid_nonce_rejected_before_sql(nonce):
    cursor = Cursor()
    with pytest.raises(ValueError):
        TdsCoordinatorSession(cursor).initialize(nonce)
    assert cursor.calls == []


def test_valid_identity_and_invalid_record_types():
    identity = TdsRemoteSessionIdentity(CONNECTION, 72, STAMP, STAMP, NONCE, b"a" * 32)
    for changes in (
        {"session_id": True},
        {"connection_id": str(CONNECTION)},
        {"authority_sha256": "a" * 64},
        {"nonce": bytes(32)},
    ):
        with pytest.raises(ValueError):
            replace(identity, **changes)


def test_sequential_threads_cannot_reuse_session_even_if_thread_id_is_recycled():
    import threading

    cursor = Cursor()
    captured = {}

    def create():
        session = TdsCoordinatorSession(cursor)
        captured["session"] = session
        captured["identity"] = session.initialize(NONCE)

    def borrow():
        try:
            captured["session"].require_same(captured["identity"])
        except TdsSessionObservationError:
            captured["refused"] = True

    creator = threading.Thread(target=create)
    creator.start()
    creator.join()
    assert "identity" in captured
    before = len(cursor.calls)
    borrower = threading.Thread(target=borrow)
    borrower.start()
    borrower.join()
    assert captured.get("refused") is True
    assert len(cursor.calls) == before


def test_committable_autocommit_request_is_not_an_explicit_transaction():
    cursor = Cursor()
    cursor.row[10:13] = [0, 1, 0]
    session = TdsCoordinatorSession(cursor)
    identity = session.initialize(NONCE)
    session.require_same(identity)


@pytest.mark.parametrize("transaction", [(0, -1, 0), (1, 1, 0), (2, 1, 0), (0, 1, 2)])
def test_initialization_observation_rejects_doomed_explicit_or_implicit_transactions(transaction):
    cursor = Cursor()
    cursor.row[10:13] = transaction
    with pytest.raises(TdsSessionObservationError):
        TdsCoordinatorSession(cursor).initialize(NONCE)


@pytest.mark.parametrize("during", ["initialize", "require_same"])
def test_interrupt_permanently_poisons_session(during):
    cursor = Cursor()
    session = TdsCoordinatorSession(cursor)
    if during == "require_same":
        identity = session.initialize(NONCE)
    cursor.error = KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):
        if during == "initialize":
            session.initialize(NONCE)
        else:
            session.require_same(identity)
    cursor.error = None
    before = len(cursor.calls)
    with pytest.raises(TdsSessionObservationError):
        session.initialize(NONCE)
    assert len(cursor.calls) == before


@pytest.mark.parametrize("phase", ["initialize", "observe", "require_same"])
@pytest.mark.parametrize("completion", [True, 0, 1, [], RuntimeError("driver"), "missing"])
def test_unproved_result_completion_poison_without_following_sql(phase, completion):
    cursor = Cursor()
    session = TdsCoordinatorSession(cursor)
    identity = session.initialize(NONCE) if phase == "require_same" else None
    original_execute = cursor.execute

    def execute(sql, *parameters):
        original_execute(sql, *parameters)
        if phase != "initialize" and sql == _INITIALIZE:
            return
        if completion == "missing":
            cursor.nextset = None
        else:
            cursor.completion = completion

    cursor.execute = execute
    before = len(cursor.calls)
    with pytest.raises(TdsSessionObservationError):
        if identity is None:
            session.initialize(NONCE)
        else:
            session.require_same(identity)
    assert len(cursor.calls) - before == (2 if phase == "observe" else 1)
    stopped = len(cursor.calls)
    with pytest.raises(TdsSessionObservationError):
        session.initialize(NONCE)
    assert len(cursor.calls) == stopped


@pytest.mark.parametrize("phase", ["initialize", "observe"])
def test_wrong_description_rejects_before_cursor_reuse(phase):
    cursor = Cursor()
    original_execute = cursor.execute

    def execute(sql, *parameters):
        cursor.bad_description = (sql == _INITIALIZE) == (phase == "initialize")
        original_execute(sql, *parameters)

    cursor.execute = execute
    with pytest.raises(TdsSessionObservationError):
        TdsCoordinatorSession(cursor).initialize(NONCE)
    assert len(cursor.calls) == (1 if phase == "initialize" else 2)


@pytest.mark.parametrize("completion", [None, False])
def test_exact_terminal_nextset_values_are_supported(completion):
    cursor = Cursor()
    cursor.completion = completion
    session = TdsCoordinatorSession(cursor)
    session.require_same(session.initialize(NONCE))


@pytest.mark.parametrize("attribute", ["description", "nextset"])
@pytest.mark.parametrize("error", [AttributeError, RuntimeError])
def test_missing_or_raising_completion_attribute_stops_initialization(attribute, error):
    class BrokenCursor(Cursor):
        def __getattribute__(self, name):
            if name == attribute:
                raise error("unavailable")
            return super().__getattribute__(name)

    cursor = BrokenCursor()
    session = TdsCoordinatorSession(cursor)
    with pytest.raises(TdsSessionObservationError):
        session.initialize(NONCE)
    assert len(cursor.calls) == 1
    with pytest.raises(TdsSessionObservationError):
        session.initialize(NONCE)
    assert len(cursor.calls) == 1
