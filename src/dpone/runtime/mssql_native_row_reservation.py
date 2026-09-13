"""Private immutable source-size handoff shared by adaptation and framing."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, overload

if TYPE_CHECKING:
    from dpone.runtime.native_wire_models import SourceNativeWireContract


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
