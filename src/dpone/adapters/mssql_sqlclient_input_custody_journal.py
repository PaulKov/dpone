"""Durable observe-or-advance journal for parent input custody release."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Any

from dpone.ports.mssql_native_route_backend import NativeInputCustodyReceipt

_OUTCOME_UNKNOWN = "mssql_native.input_custody_outcome_unknown"
_RECEIPT_INVALID = "mssql_native.input_custody_receipt_invalid"
_REQUEST_INVALID = "mssql_native.input_custody_request_invalid"


class SQLiteSqlClientInputCustodyJournal:
    """Persist intent before release and return only its exact durable receipt.

    An ``intent`` row deliberately has no automatic recovery transition. If the
    process stops after recording intent, a caller cannot know whether the
    external release ran, so replay fails closed instead of repeating it.
    """

    def __init__(
        self,
        path: Path,
        *,
        reconciliation_observer: Callable[[str], NativeInputCustodyReceipt] | None = None,
    ) -> None:
        if not isinstance(path, Path) or (
            reconciliation_observer is not None and not callable(reconciliation_observer)
        ):
            raise ValueError(_REQUEST_INVALID)
        self._path = path
        self._reconciliation_observer = reconciliation_observer
        path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute(
                """
                CREATE TABLE IF NOT EXISTS sqlclient_input_custody (
                    request_sha256 TEXT PRIMARY KEY,
                    phase TEXT NOT NULL CHECK (phase IN ('intent', 'receipt')),
                    receipt_json TEXT,
                    CHECK (
                        (phase = 'intent' AND receipt_json IS NULL)
                        OR (phase = 'receipt' AND receipt_json IS NOT NULL)
                    )
                )
                """
            )

    def observe_or_advance(
        self,
        request_sha256: str,
        advance: Callable[[], NativeInputCustodyReceipt],
    ) -> NativeInputCustodyReceipt:
        """Observe a receipt or durably reserve one irreversible release."""
        self._validate_request(request_sha256)
        if not callable(advance):
            raise ValueError(_REQUEST_INVALID)

        observed = self._claim_or_observe(request_sha256)
        if observed is not None:
            return observed

        receipt = advance()
        self._validate_receipt(receipt, request_sha256)
        payload = self._encode_receipt(receipt)

        with self._transaction() as db:
            cursor = db.execute(
                """
                UPDATE sqlclient_input_custody
                SET phase = 'receipt', receipt_json = ?
                WHERE request_sha256 = ? AND phase = 'intent' AND receipt_json IS NULL
                """,
                (payload, request_sha256),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(_OUTCOME_UNKNOWN)
        return receipt

    def reconcile_unknown(
        self,
        request_sha256: str,
    ) -> NativeInputCustodyReceipt:
        """Resolve unknown intent through the injected source-of-truth observer.

        This method records evidence only. It has no effect callback and cannot
        create, reset, or delete an intent, so an operator cannot accidentally
        replay the irreversible release while resolving a lost acknowledgement.
        """
        self._validate_request(request_sha256)
        if self._reconciliation_observer is None:
            raise RuntimeError(_OUTCOME_UNKNOWN)
        with self._transaction() as db:
            row = db.execute(
                "SELECT phase, receipt_json FROM sqlclient_input_custody WHERE request_sha256 = ?",
                (request_sha256,),
            ).fetchone()
            if row is None:
                raise RuntimeError(_OUTCOME_UNKNOWN)
            phase, durable_payload = row
            if phase == "receipt" and type(durable_payload) is str:
                return self._decode_receipt(durable_payload, request_sha256)
            if phase != "intent" or durable_payload is not None:
                raise ValueError(_RECEIPT_INVALID)

        observed_receipt = self._reconciliation_observer(request_sha256)
        self._validate_receipt(observed_receipt, request_sha256)
        payload = self._encode_receipt(observed_receipt)

        with self._transaction() as db:
            row = db.execute(
                "SELECT phase, receipt_json FROM sqlclient_input_custody WHERE request_sha256 = ?",
                (request_sha256,),
            ).fetchone()
            if row is None:
                raise RuntimeError(_OUTCOME_UNKNOWN)
            phase, durable_payload = row
            if phase == "receipt" and type(durable_payload) is str:
                durable_receipt = self._decode_receipt(durable_payload, request_sha256)
                if durable_receipt != observed_receipt:
                    raise ValueError(_RECEIPT_INVALID)
                return durable_receipt
            if phase != "intent" or durable_payload is not None:
                raise ValueError(_RECEIPT_INVALID)
            cursor = db.execute(
                """
                UPDATE sqlclient_input_custody
                SET phase = 'receipt', receipt_json = ?
                WHERE request_sha256 = ? AND phase = 'intent' AND receipt_json IS NULL
                """,
                (payload, request_sha256),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(_OUTCOME_UNKNOWN)
        return observed_receipt

    def _claim_or_observe(self, request_sha256: str) -> NativeInputCustodyReceipt | None:
        with self._transaction() as db:
            row = db.execute(
                "SELECT phase, receipt_json FROM sqlclient_input_custody WHERE request_sha256 = ?",
                (request_sha256,),
            ).fetchone()
            if row is None:
                db.execute(
                    "INSERT INTO sqlclient_input_custody(request_sha256, phase, receipt_json) "
                    "VALUES (?, 'intent', NULL)",
                    (request_sha256,),
                )
                return None
            phase, payload = row
            if phase == "intent" and payload is None:
                raise RuntimeError(_OUTCOME_UNKNOWN)
            if phase != "receipt" or type(payload) is not str:
                raise ValueError(_RECEIPT_INVALID)
            return self._decode_receipt(payload, request_sha256)

    @staticmethod
    def _decode_receipt(payload: str, request_sha256: str) -> NativeInputCustodyReceipt:
        try:
            facts: Any = json.loads(payload)
            if type(facts) is not dict or set(facts) != {"request_sha256", "release_sha256"}:
                raise ValueError(_RECEIPT_INVALID)
            receipt = NativeInputCustodyReceipt(**facts)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError(_RECEIPT_INVALID) from exc
        SQLiteSqlClientInputCustodyJournal._validate_receipt(receipt, request_sha256)
        return receipt

    @staticmethod
    def _encode_receipt(receipt: NativeInputCustodyReceipt) -> str:
        return json.dumps(asdict(receipt), sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _validate_request(request_sha256: str) -> None:
        if (
            type(request_sha256) is not str
            or len(request_sha256) != 64
            or any(char not in "0123456789abcdef" for char in request_sha256)
        ):
            raise ValueError(_REQUEST_INVALID)

    @staticmethod
    def _validate_receipt(receipt: object, request_sha256: str) -> None:
        if type(receipt) is not NativeInputCustodyReceipt or receipt.request_sha256 != request_sha256:
            raise ValueError(_RECEIPT_INVALID)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self._path, timeout=30, isolation_level=None)
        try:
            db.execute("PRAGMA synchronous=FULL")
            yield db
        finally:
            db.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise


__all__ = ("SQLiteSqlClientInputCustodyJournal",)
