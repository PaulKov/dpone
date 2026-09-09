"""Protocols for CDC runtime orchestration."""

from __future__ import annotations

from typing import Protocol

from dpone.readiness.cdc import CDCOffset
from dpone.runtime.cdc.base import CDCBatch
from dpone.runtime.cdc.runtime_models import CdcApplyReceipt, CdcRuntimeStream


class CdcOffsetStore(Protocol):
    """Durable offset store used by the CDC runtime loop."""

    def load_offset(self, stream: CdcRuntimeStream) -> CDCOffset | None:
        """Return the last committed offset for this stream."""

    def save_offset(self, stream: CdcRuntimeStream, offset: CDCOffset) -> None:
        """Persist the next durable offset for this stream."""


class CdcSinkApplier(Protocol):
    """Apply a bounded CDC batch to a sink."""

    def apply(self, *, stream: CdcRuntimeStream, batch: CDCBatch) -> CdcApplyReceipt:
        """Return a durable sink-side receipt."""
