"""Streaming native frames with reusable byte reservations and unchanged IPC bounds."""

from __future__ import annotations

import pickle
from collections.abc import Callable, Generator, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeAlias, overload

from dpone.contracts.bounded_window import WindowContractError
from dpone.runtime.mssql_native_encoder import MssqlNativeEncoder
from dpone.runtime.native_wire_models import SourceNativeWireContract

if TYPE_CHECKING:
    from dpone.contracts.mssql_native_chunks import NativeChunkLimits

NativeRow: TypeAlias = Sequence[object] | Mapping[str, object]


@dataclass(frozen=True)
class _SizedNativeRow(Sequence[object]):
    """Private source reservation; only matching consumers may reuse its size.

    Values have passed source adaptation and sizing, including rejection of
    mutable binary inputs. The plain tuple is the sole worker/IPC representation.
    Contract identity and the row limit bind this accounting shortcut; workers
    still validate scalar domains and actual encoded bytes independently.
    """

    values: tuple[object, ...]
    contract: SourceNativeWireContract
    max_row_bytes: int
    encoded_bytes: int

    def __len__(self) -> int:
        return len(self.values)

    @overload
    def __getitem__(self, index: int) -> object: ...

    @overload
    def __getitem__(self, index: slice) -> tuple[object, ...]: ...

    def __getitem__(self, index: int | slice) -> object:
        return self.values[index]


@dataclass(frozen=True)
class SizedNativeFrame:
    """Frozen envelope holding detached rows and their exact native byte sum.

    The producer copies driver containers and binary buffers before sizing.
    Mapping snapshots remain plain dictionaries for identical pickle boundaries;
    consumers must not mutate them. This reservation is not value-validation or
    retained-file evidence. Workers still validate values and report actual size.
    """

    rows: tuple[NativeRow, ...]
    encoded_bytes: int


def sized_native_frames(
    rows: Iterator[NativeRow],
    contract: SourceNativeWireContract,
    limits: NativeChunkLimits,
    check: Callable[[], None] | None = None,
    ipc_overhead: int = 0,
) -> Generator[SizedNativeFrame, None, None]:
    """Consume once, retaining the native byte sum already used to split frames.

    Native bytes and conservative per-row pickle lengths bound frames separately.
    The scheduler must also check exact aggregate-frame and submitted-task pickle
    lengths. Limits do not bound Python heap/RSS. One row is sized ahead of each
    yielded boundary; checks run after pulling row indices 0, 1024, and so on.
    Empty input yields one zero-byte frame. The caller owns source closure, even
    on cancellation, early close or failure.
    """
    encoder = MssqlNativeEncoder(contract, max_row_bytes=limits.max_row_bytes)
    frame: list[NativeRow] = []
    native_bytes, ipc_bytes = 0, 64 + ipc_overhead
    emitted = False
    for index, source_row in enumerate(rows):
        if check is not None and index % 1024 == 0:
            check()

        reservation = source_row if type(source_row) is _SizedNativeRow else None
        if reservation is not None:
            source_row = reservation.values

        # Drivers may reuse a row container or binary buffer between fetches.
        def freeze(value: object) -> object:
            return bytes(value) if isinstance(value, (bytearray, memoryview)) else value

        row: NativeRow = (
            {key: freeze(value) for key, value in source_row.items()}
            if isinstance(source_row, Mapping)
            else tuple(freeze(value) for value in source_row)
        )
        size = (
            reservation.encoded_bytes
            if reservation is not None
            and reservation.contract is contract
            and reservation.max_row_bytes == limits.max_row_bytes
            else encoder.encoded_row_size(row)
        )
        ipc_size = len(pickle.dumps(row, protocol=5)) + 16
        if size > limits.max_row_bytes or ipc_size + 64 + ipc_overhead > limits.max_bytes:
            raise WindowContractError("mssql_native.row_exceeds_frame_limit")
        if frame and (
            native_bytes + size > limits.max_bytes
            or ipc_bytes + ipc_size > limits.max_bytes
            or len(frame) >= limits.max_rows
        ):
            yield SizedNativeFrame(tuple(frame), native_bytes)
            emitted = True
            frame, native_bytes, ipc_bytes = [], 0, 64 + ipc_overhead
        frame.append(row)
        native_bytes += size
        ipc_bytes += ipc_size
    if frame or not emitted:
        yield SizedNativeFrame(tuple(frame), native_bytes)
