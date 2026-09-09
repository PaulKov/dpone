"""Lifecycle and integrity authority for one external-process input file."""

from __future__ import annotations

import os
from collections.abc import Callable
from threading import RLock
from typing import Protocol

from dpone.runtime.artifact_integrity import (
    ArtifactIntegrityError,
    FileArtifactReceipt,
    FileWireContract,
)


class PinnedFileIntegrityAuthority(Protocol):
    """Internal receipt boundary implemented by the held file identity."""

    def capture(
        self,
        wire_contract: FileWireContract,
        rows_exported: int | None,
    ) -> FileArtifactReceipt: ...

    def verify(
        self,
        receipt: FileArtifactReceipt,
        wire_contract: FileWireContract,
    ) -> None: ...


class PinnedFileConsumer:
    """Bind one source filename to a held OS file identity until cleanup.

    The consumer path may be a descriptor projection rather than the authored
    filename. External processes must receive ``inherited_file_descriptors``
    and call :meth:`prepare_process_input` immediately before spawning.
    """

    __slots__ = (
        "source_path",
        "consumer_path",
        "inherited_file_descriptors",
        "_prepare_callback",
        "_cleanup_callback",
        "_release_callback",
        "_integrity_authority",
        "_integrity_receipt",
        "_wire_contract",
        "_closed",
        "_lock",
    )

    def __init__(
        self,
        *,
        source_path: str,
        consumer_path: str,
        inherited_file_descriptors: tuple[int, ...],
        prepare_callback: Callable[[], None],
        cleanup_callback: Callable[[], None],
        release_callback: Callable[[], None],
        integrity_authority: PinnedFileIntegrityAuthority | None = None,
    ) -> None:
        self.source_path = source_path
        self.consumer_path = consumer_path
        self.inherited_file_descriptors = inherited_file_descriptors
        self._prepare_callback = prepare_callback
        self._cleanup_callback = cleanup_callback
        self._release_callback = release_callback
        self._integrity_authority = integrity_authority
        self._integrity_receipt: FileArtifactReceipt | None = None
        self._wire_contract: FileWireContract | None = None
        self._closed = False
        self._lock = RLock()

    def prepare_process_input(self, source_path: str) -> tuple[str, tuple[int, ...]]:
        """Return the immutable process path only for the bound source name."""

        with self._lock:
            self._require_open_locked()
            if _normalized_path(source_path) != _normalized_path(self.source_path):
                raise OSError("pinned_file_source_path_mismatch")
            self._prepare_callback()
            if self._integrity_receipt is not None and self._wire_contract is not None:
                self._verify_integrity_locked(
                    self._integrity_receipt,
                    wire_contract=self._wire_contract,
                )
            return self.consumer_path, self.inherited_file_descriptors

    def capture_integrity_receipt(
        self,
        wire_contract: FileWireContract,
        *,
        rows_exported: int | None,
    ) -> FileArtifactReceipt:
        """Capture bytes through the same held identity that BCP will consume."""

        with self._lock:
            self._require_open_locked()
            receipt = (
                self._integrity_authority.capture(wire_contract, rows_exported)
                if self._integrity_authority is not None
                else FileArtifactReceipt.capture(
                    self.source_path,
                    wire_contract=wire_contract,
                    rows_exported=rows_exported,
                )
            )
            if self._integrity_receipt is not None and receipt != self._integrity_receipt:
                raise ArtifactIntegrityError("artifact_integrity.receipt_authority_changed")
            if self._wire_contract is not None and wire_contract != self._wire_contract:
                raise ArtifactIntegrityError("artifact_integrity.wire_contract_mismatch")
            self._integrity_receipt = receipt
            self._wire_contract = wire_contract
            return receipt

    def verify_integrity_receipt(
        self,
        receipt: FileArtifactReceipt,
        *,
        wire_contract: FileWireContract,
    ) -> None:
        """Revalidate bytes through the exact held BCP input identity."""

        with self._lock:
            self._require_open_locked()
            self._verify_integrity_locked(receipt, wire_contract=wire_contract)

    def cleanup(self) -> None:
        """Release the exact file identity and remove only its owned name."""

        with self._lock:
            if self._closed:
                return
            self._closed = True
            primary_error: BaseException | None = None
            try:
                self._cleanup_callback()
            except BaseException as error:
                primary_error = error
            try:
                self._release_callback()
            except BaseException:
                if primary_error is None:
                    raise
                add_note = getattr(primary_error, "add_note", None)
                if callable(add_note):
                    add_note("pinned_file_consumer_release_failed")
            if primary_error is not None:
                raise primary_error

    def close(self) -> None:
        """Release the held identity without deleting the source name."""

        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._release_callback()

    def _require_open_locked(self) -> None:
        if self._closed:
            raise OSError("pinned_file_consumer_closed")

    def _verify_integrity_locked(
        self,
        receipt: FileArtifactReceipt,
        *,
        wire_contract: FileWireContract,
    ) -> None:
        if self._integrity_receipt is not None and receipt != self._integrity_receipt:
            raise ArtifactIntegrityError("artifact_integrity.receipt_authority_changed")
        if self._wire_contract is not None and wire_contract != self._wire_contract:
            raise ArtifactIntegrityError("artifact_integrity.wire_contract_mismatch")
        if self._integrity_authority is not None:
            self._integrity_authority.verify(receipt, wire_contract)
            return
        receipt.verify(self.source_path, wire_contract=wire_contract)


def _normalized_path(path: str) -> str:
    return os.path.normcase(os.path.abspath(path))


__all__ = ["PinnedFileConsumer", "PinnedFileIntegrityAuthority"]
