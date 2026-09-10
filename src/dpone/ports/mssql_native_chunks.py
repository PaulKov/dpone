"""Independent target-session boundary for bounded native chunk import."""

from typing import Protocol

from dpone.contracts.bounded_window import WindowLease
from dpone.contracts.mssql_native_chunks import EncodedNativeFile, NativeChunkPlan, NativeChunkReceipt


class NativeChunkImporter(Protocol):
    """Implementations own their connection and must fence mutation/settlement."""

    def import_file(
        self, plan: NativeChunkPlan, file: EncodedNativeFile, attempt_id: str, lease: WindowLease
    ) -> NativeChunkReceipt: ...

    def inspect(self, plan: NativeChunkPlan, receipt: NativeChunkReceipt, lease: WindowLease) -> NativeChunkReceipt: ...

    def settle(self, plan: NativeChunkPlan, attempt_id: str, lease: WindowLease) -> None: ...

    def allocated_bytes(self) -> int: ...
