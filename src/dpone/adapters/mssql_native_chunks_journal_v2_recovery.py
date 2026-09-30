"""Observation-only BCP recovery and verified-stage retirement for v2 journals."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any

from dpone.contracts.bounded_window import WindowContractError
from dpone.contracts.mssql_native_chunks import NativeChunkReceipt
from dpone.contracts.mssql_native_verification import is_sha256_digest, matches_native_receipt


def observe_bcp_recovery(
    journal: Any,
    ordinal: int,
    attempt_id: str,
    *,
    barrier: Callable[[], AbstractContextManager[Any]],
    observe: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    """Append only a proved observation after durable positive BCP terminal."""
    data = journal._staging(allow_recovery=True)
    events = data["events"].get(attempt_id, [])
    if (
        journal.identity.import_backend != "bcp"
        or not events
        or events[-1]["event"] not in {"UNKNOWN", "WRITER_TERMINAL"}
        or events[-1]["observation"]["writer_outcome"] != "success"
        or not any(
            event["event"] == "WRITER_TERMINAL" and event["observation"]["writer_outcome"] == "success"
            for event in events
        )
    ):
        raise WindowContractError("mssql_native.bcp_recovery_proof_missing")
    with barrier():
        observation = observe()
        if observation.get("writer_outcome") != "success" or observation.get("quiescence") != "proved":
            raise WindowContractError("mssql_native.bcp_recovery_proof_missing")
        prior = events[-1]
        return journal._append(
            data,
            ordinal,
            attempt_id,
            "QUIESCENT",
            prior["artifact_binding"],
            prior["stage_binding"],
            prior["writer_binding"],
            observation,
            allow_recovery=True,
        )


def recover_bcp_verified(
    journal: Any,
    ordinal: int,
    attempt_id: str,
    *,
    barrier: Callable[[], AbstractContextManager[Any]],
    observe: Callable[[], tuple[dict[str, Any], NativeChunkReceipt]],
) -> NativeChunkReceipt:
    """Reobserve a positive terminal attempt and commit its receipt under one barrier."""
    data = journal._staging(allow_recovery=True)
    events = data["events"].get(attempt_id, [])
    if (
        journal.identity.import_backend != "bcp"
        or not events
        or events[-1]["event"] not in {"WRITER_TERMINAL", "UNKNOWN", "QUIESCENT", "VERIFIED"}
        or (
            events[-1]["event"] in {"WRITER_TERMINAL", "UNKNOWN"}
            and events[-1]["observation"]["writer_outcome"] != "success"
        )
        or not any(
            event["event"] == "WRITER_TERMINAL" and event["observation"]["writer_outcome"] == "success"
            for event in events
        )
    ):
        raise WindowContractError("mssql_native.bcp_recovery_proof_missing")
    with barrier():
        observation, receipt = observe()
        if (
            observation.get("writer_outcome") != "success"
            or observation.get("quiescence") != "proved"
            or not matches_native_receipt(journal.identity, receipt, events[-1])
        ):
            raise WindowContractError("mssql_native.bcp_recovery_proof_missing")
        previous = events[-1]
        if previous["event"] in {"WRITER_TERMINAL", "UNKNOWN"}:
            journal._append(
                data,
                ordinal,
                attempt_id,
                "QUIESCENT",
                previous["artifact_binding"],
                previous["stage_binding"],
                previous["writer_binding"],
                observation,
                allow_recovery=True,
            )
            data = journal._staging(allow_recovery=True)
            previous = data["events"][attempt_id][-1]
        if previous["event"] == "QUIESCENT":
            journal._append(
                data,
                ordinal,
                attempt_id,
                "VERIFIED",
                previous["artifact_binding"],
                previous["stage_binding"],
                previous["writer_binding"],
                observation,
                allow_recovery=True,
            )
        journal._commit_verified_receipt(receipt, allow_recovery=True)
        return receipt


def recover_sqlclient_verified(
    journal: Any,
    ordinal: int,
    attempt_id: str,
    *,
    barrier: Callable[[], AbstractContextManager[Any]],
    observe: Callable[[], tuple[dict[str, Any], NativeChunkReceipt]],
) -> NativeChunkReceipt:
    """Reconcile an uncertain SqlClient attempt only under its session lock."""
    data = journal._staging(allow_recovery=True)
    events = data["events"].get(attempt_id, [])
    if (
        journal.identity.import_backend != "mssql_sqlclient"
        or not events
        or events[-1]["event"] not in {"WRITER_TERMINAL", "UNKNOWN", "QUIESCENT", "VERIFIED"}
        or not events[-1]["writer_binding"]
    ):
        raise WindowContractError("mssql_native.sqlclient_recovery_proof_missing")
    with barrier():
        observation, receipt = observe()
        if (
            observation.get("writer_outcome") != "success"
            or observation.get("quiescence") != "proved"
            or observation.get("row_count") != events[-1]["artifact_binding"]["rows"]
            or observation.get("count_overflow") is not False
            or not matches_native_receipt(journal.identity, receipt, events[-1])
        ):
            raise WindowContractError("mssql_native.sqlclient_recovery_proof_missing")
        previous = events[-1]
        if previous["event"] in {"WRITER_TERMINAL", "UNKNOWN"}:
            journal._append(
                data,
                ordinal,
                attempt_id,
                "QUIESCENT",
                previous["artifact_binding"],
                previous["stage_binding"],
                previous["writer_binding"],
                observation,
                allow_recovery=True,
            )
            data = journal._staging(allow_recovery=True)
            previous = data["events"][attempt_id][-1]
        if previous["event"] == "QUIESCENT":
            journal._append(
                data,
                ordinal,
                attempt_id,
                "VERIFIED",
                previous["artifact_binding"],
                previous["stage_binding"],
                previous["writer_binding"],
                observation,
                allow_recovery=True,
            )
        journal._commit_verified_receipt(receipt, allow_recovery=True)
        return receipt


def observe_sqlclient_partial(
    journal: Any,
    ordinal: int,
    attempt_id: str,
    *,
    barrier: Callable[[], AbstractContextManager[Any]],
    observe: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    """Persist a bounded partial-stage proof after excluding the writer session."""
    data = journal._staging(allow_recovery=True)
    events = data["events"].get(attempt_id, [])
    if (
        journal.identity.import_backend != "mssql_sqlclient"
        or not events
        or events[-1]["event"] != "UNKNOWN"
        or not events[-1]["writer_binding"]
    ):
        raise WindowContractError("mssql_native.sqlclient_recovery_proof_missing")
    with barrier():
        observation = observe()
        expected = events[-1]["artifact_binding"]["rows"]
        rows = observation.get("row_count")
        if (
            observe() != observation
            or observation.get("quiescence") != "proved"
            or type(rows) is not int
            or not 0 <= rows < expected
            or observation.get("count_overflow") is not False
            or observation.get("limbs") is None
        ):
            raise WindowContractError("mssql_native.sqlclient_partial_proof_missing")
        previous = events[-1]
        return journal._append(
            data,
            ordinal,
            attempt_id,
            "PARTIAL_PROVED",
            previous["artifact_binding"],
            previous["stage_binding"],
            previous["writer_binding"],
            observation,
            allow_recovery=True,
        )


def retain_bcp_incident(journal: Any, ordinal: int, attempt_id: str) -> dict[str, Any]:
    """Retain an ambiguous BCP attempt without stage classification or replay."""
    data = journal._staging(allow_recovery=True)
    events = data["events"].get(attempt_id, [])
    if journal.identity.import_backend != "bcp" or not events or events[-1]["event"] != "UNKNOWN":
        raise WindowContractError("mssql_native.bcp_incident_required")
    previous = events[-1]
    return journal._append(
        data,
        ordinal,
        attempt_id,
        "INCIDENT_RETAINED",
        previous["artifact_binding"],
        previous["stage_binding"],
        previous["writer_binding"],
        previous["observation"],
        allow_recovery=True,
    )


def record_nonpublication(journal: Any, proof_sha256: str, *, assert_nonpublication: Callable[[], None]) -> None:
    """Persist invocation-level pre-EOF exclusion before any verified retirement."""
    data = journal._staging(allow_recovery=True)
    if (
        not is_sha256_digest(proof_sha256)
        or data["publication"] is not None
        or data["complete"] is not None
        or (not data["chunks"] and (data["events"] or data["nonces"]))
        or any(
            data["events"][chunk["attempt_id"]][-1]["event"]
            not in {"VERIFIED", "PARTIAL_PROVED", "FAILED_RETIRABLE", "RETIRED"}
            for chunk in data["chunks"].values()
        )
    ):
        raise WindowContractError("mssql_native.nonpublication_proof_missing")
    proof = {"kind": "pre_eof_nonpublication", "proof_sha256": proof_sha256}
    existing = [item for item in data["rollback_history"] if item.get("kind") == "pre_eof_nonpublication"]
    if existing:
        if existing != [proof]:
            raise WindowContractError("mssql_native.nonpublication_proof_changed")
        return
    assert_nonpublication()
    data["rollback_history"].append(proof)
    journal._save(data)


def retire_verified(journal: Any, ordinal: int, attempt_id: str, *, drop_exact_owned: Callable[[], None]) -> None:
    """Retire one proved stage after durable nonpublication and exact owner drop."""
    data = journal._staging(allow_recovery=True)
    if not any(item.get("kind") == "pre_eof_nonpublication" for item in data["rollback_history"]):
        raise WindowContractError("mssql_native.nonpublication_proof_missing")
    chunk = data["chunks"].get(str(ordinal))
    if chunk is None or chunk["attempt_id"] != attempt_id or chunk["phase"] not in {"staging", "verified"}:
        raise WindowContractError("mssql_native.verified_stage_required")
    previous = data["events"][attempt_id][-1]
    if previous["event"] in {"VERIFIED", "PARTIAL_PROVED"}:
        journal._append(
            data,
            ordinal,
            attempt_id,
            "FAILED_RETIRABLE",
            previous["artifact_binding"],
            previous["stage_binding"],
            previous["writer_binding"],
            previous["observation"],
            allow_recovery=True,
        )
        data = journal._staging(allow_recovery=True)
        previous = data["events"][attempt_id][-1]
    if previous["event"] != "FAILED_RETIRABLE":
        raise WindowContractError("mssql_native.verified_stage_required")
    drop_exact_owned()
    journal._append(
        data,
        ordinal,
        attempt_id,
        "RETIRED",
        previous["artifact_binding"],
        previous["stage_binding"],
        previous["writer_binding"],
        previous["observation"],
        allow_recovery=True,
    )


class BcpRecoveryMixin:
    """Serialize recovery methods against the journal's durable projection."""

    _lock: Any

    @staticmethod
    def _assert_no_nonpublication(data: dict[str, Any]) -> None:
        if any(item.get("kind") == "pre_eof_nonpublication" for item in data["rollback_history"]):
            raise WindowContractError("mssql_native.nonpublication_frozen")

    def observe_bcp_recovery(
        self,
        ordinal: int,
        attempt_id: str,
        *,
        barrier: Callable[[], AbstractContextManager[Any]],
        observe: Callable[[], dict[str, Any]],
    ) -> dict[str, Any]:
        """Observe an already authorized BCP attempt under an exact-stage barrier."""
        with self._lock:
            return observe_bcp_recovery(self, ordinal, attempt_id, barrier=barrier, observe=observe)

    def recover_bcp_verified(
        self,
        ordinal: int,
        attempt_id: str,
        *,
        barrier: Callable[[], AbstractContextManager[Any]],
        observe: Callable[[], tuple[dict[str, Any], NativeChunkReceipt]],
    ) -> NativeChunkReceipt:
        """Commit a recovered receipt while the exact-stage barrier is held."""
        with self._lock:
            return recover_bcp_verified(self, ordinal, attempt_id, barrier=barrier, observe=observe)

    def retain_bcp_incident(self, ordinal: int, attempt_id: str) -> dict[str, Any]:
        """Retain an ambiguous BCP attempt without classifying its stage."""
        with self._lock:
            return retain_bcp_incident(self, ordinal, attempt_id)

    def recover_sqlclient_verified(
        self,
        ordinal: int,
        attempt_id: str,
        *,
        barrier: Callable[[], AbstractContextManager[Any]],
        observe: Callable[[], tuple[dict[str, Any], NativeChunkReceipt]],
    ) -> NativeChunkReceipt:
        """Reconcile content only after acquiring the grant-bound session lock."""
        with self._lock:
            return recover_sqlclient_verified(self, ordinal, attempt_id, barrier=barrier, observe=observe)

    def observe_sqlclient_partial(
        self,
        ordinal: int,
        attempt_id: str,
        *,
        barrier: Callable[[], AbstractContextManager[Any]],
        observe: Callable[[], dict[str, Any]],
    ) -> dict[str, Any]:
        """Persist only a bounded partial result under the grant lock."""
        with self._lock:
            return observe_sqlclient_partial(self, ordinal, attempt_id, barrier=barrier, observe=observe)

    def record_nonpublication(self, proof_sha256: str, *, assert_nonpublication: Callable[[], None]) -> None:
        """Persist invocation-level exclusion proof before verified retirement."""
        with self._lock:
            record_nonpublication(self, proof_sha256, assert_nonpublication=assert_nonpublication)

    def retire_verified(self, ordinal: int, attempt_id: str, *, drop_exact_owned: Callable[[], None]) -> None:
        """Retire a writer-proved stage only after exact-owner drop."""
        with self._lock:
            retire_verified(self, ordinal, attempt_id, drop_exact_owned=drop_exact_owned)
