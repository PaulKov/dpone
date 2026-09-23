"""One-shot ordered evidence actor for P9a."""

import os
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Protocol
from uuid import uuid4

from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown, _ActorCommand, _ActorCore
from dpone.ports.evidence import CreateOnlyEvidenceWriterV1

ERROR = "mssql_native.sqlclient_restricted_writer_verify_invalid"


class RestrictedWriterVerifyEvidenceContract(Protocol):
    @property
    def observation_type(self) -> type: ...

    def observation(self, operation_id, receipts: tuple[object, ...]): ...
    def check_observation(self, value: object): ...
    def validate_record(self, value: object, operation_id, ordinal: int): ...
    def receipt(self, record: object): ...
    def receipts(self, observation: object) -> tuple[object, ...]: ...
    def write_parts(self, record: object, receipt: object) -> tuple[str, bytes]: ...


class RestrictedWriterVerifyCreateOnlyEvidenceWriter:
    """Atomically create each P9 record once; EEXIST is never reread or healed."""

    def __init__(self, root: Path) -> None:
        resolved = root.resolve(strict=True)
        if not resolved.is_dir() or resolved.is_symlink():
            raise ValueError(ERROR)
        self._root = resolved

    def write(self, relative_name: str, payload: bytes) -> None:
        if (
            type(relative_name) is not str
            or Path(relative_name).name != relative_name
            or relative_name in {"", ".", ".."}
            or type(payload) is not bytes
            or not payload
        ):
            raise ValueError(ERROR)
        directory = os.open(self._root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0))
        stage = f".{relative_name}.{uuid4().hex}.stage"
        descriptor = None
        close_error = None
        try:
            descriptor = os.open(
                stage,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=directory,
            )
            view = memoryview(payload)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise OSError(ERROR)
                view = view[written:]
            os.fsync(descriptor)
            closing, descriptor = descriptor, None
            os.close(closing)
            os.link(stage, relative_name, src_dir_fd=directory, dst_dir_fd=directory, follow_symlinks=False)
            os.fsync(directory)
        finally:
            if descriptor is not None:
                closing, descriptor = descriptor, None
                try:
                    os.close(closing)
                except BaseException as error:
                    close_error = error
            try:
                os.unlink(stage, dir_fd=directory)
            except FileNotFoundError:
                pass
            finally:
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            if close_error is not None:
                raise close_error


class RestrictedWriterVerifyEvidenceActor(_ActorCore[CreateOnlyEvidenceWriterV1, object]):
    def __init__(
        self,
        factory: Callable[[], AbstractContextManager[CreateOnlyEvidenceWriterV1]],
        operation_id,
        deadline,
        clock,
        contract: RestrictedWriterVerifyEvidenceContract,
    ):
        self._contract = contract
        self._operation_id = operation_id
        self._empty = contract.observation(operation_id, ())
        self._attempted = 0
        self._persisted = 0
        self._actor_receipts: tuple[object, ...] = ()
        self._writing = False
        super().__init__(factory, deadline, clock, contract.observation_type)

    @property
    def observation(self):
        return self.snapshot

    def _initial(self, backend: CreateOnlyEvidenceWriterV1):
        self._actor_receipts = ()
        return self._empty

    def _checked(self, value: object):
        return self._contract.check_observation(value)

    def _validate(self, value: object, ordinal: int):
        return self._contract.validate_record(value, self._operation_id, ordinal)

    def write(self, record: object, *, deadline: float):
        self._owned()
        if self._writing:
            self._shutdown()
            raise TdsJournalActorUnknown(self)
        self._writing = True
        previous = self.snapshot
        try:
            record = self._validate(record, self._attempted)
            self._attempted += 1
            expected = self._contract.receipt(record)
            observed = self._call(_ActorCommand("evidence", deadline, record))
            checked = self._checked(observed)
            if (
                self._contract.receipts(checked) != self._contract.receipts(previous) + (expected,)
                or self._checked(self.snapshot) != checked
            ):
                raise TdsJournalActorUnknown(self)
            return self._contract.receipts(checked)[-1]
        except BaseException:
            self._snapshot = previous
            self._shutdown()
            raise
        finally:
            self._writing = False

    def _dispatch(self, backend: CreateOnlyEvidenceWriterV1, command: _ActorCommand):
        if command.kind != "evidence":
            raise TdsJournalActorUnknown(self)
        record = self._validate(command.event, self._persisted)
        receipt = self._contract.receipt(record)
        self._persisted += 1
        relative_name, payload = self._contract.write_parts(record, receipt)
        backend.write(relative_name, payload)
        self._actor_receipts += (receipt,)
        return self._contract.observation(self._operation_id, self._actor_receipts)
