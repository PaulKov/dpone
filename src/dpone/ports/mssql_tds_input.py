"""Decoded-row boundary between native value policy and SDK input infrastructure."""

from collections.abc import Callable, Iterator
from typing import Any, BinaryIO, Protocol

from dpone.contracts.mssql_tds_api import TdsInputReceipt


class TdsInputColumn(Protocol):
    """Immutable admitted column metadata needed to construct an Arrow schema."""

    @property
    def name(self) -> str: ...

    @property
    def storage_type(self) -> str: ...

    @property
    def nullable(self) -> bool: ...


class TdsRowDecoder(Protocol):
    """Runtime-owned admission and exact scalar decoding, injected by composition."""

    @property
    def columns(self) -> tuple[TdsInputColumn, ...]: ...

    @property
    def input_mode(self) -> str: ...

    def iter_rows(
        self, stream: BinaryIO, *, max_row_bytes: int, on_bytes: Callable[[bytes], None]
    ) -> Iterator[tuple[Any, ...]]:
        """Decode to natural EOF, enforce row bounds and observe all consumed bytes."""


class TdsBulkInput(Protocol):
    """SDK-independent input authority supplied by the runtime composition.

    Consumption is one-shot. The SDK adapter must call require_complete after
    its copy call, even when the driver reports success or the input is empty.
    """

    def iter_rows(self) -> Iterator[tuple[Any, ...]]:
        """Yield strict native scalar values and establish natural EOF."""

    def iter_arrow_batches(self) -> Iterator[Any]:
        """Yield bounded explicitly typed Arrow batches; SDK imports stay optional."""

    def require_complete(self) -> TdsInputReceipt:
        """Raise unless full EOF, consumed count and sealed-file identity agree."""
