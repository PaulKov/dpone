"""Compatibility facade for bounded-window ports and records."""

from dpone.ports.bounded_window_contracts import (
    ChunkReceipt as ChunkReceipt,
)
from dpone.ports.bounded_window_contracts import (
    ExclusiveWindowWriterGuard as ExclusiveWindowWriterGuard,
)
from dpone.ports.bounded_window_contracts import (
    PublicationStatus as PublicationStatus,
)
from dpone.ports.bounded_window_contracts import (
    WindowBinaryIngest as WindowBinaryIngest,
)
from dpone.ports.bounded_window_contracts import (
    WindowChunk as WindowChunk,
)
from dpone.ports.bounded_window_contracts import (
    WindowContractError as WindowContractError,
)
from dpone.ports.bounded_window_contracts import (
    WindowExecutor as WindowExecutor,
)
from dpone.ports.bounded_window_contracts import (
    WindowLease as WindowLease,
)
from dpone.ports.bounded_window_contracts import (
    WindowMetadataStore as WindowMetadataStore,
)
from dpone.ports.bounded_window_contracts import (
    WindowPlan as WindowPlan,
)
from dpone.ports.bounded_window_contracts import (
    WindowProgressJournal as WindowProgressJournal,
)
from dpone.ports.bounded_window_contracts import (
    WindowRecord as WindowRecord,
)
from dpone.ports.bounded_window_contracts import (
    WindowResult as WindowResult,
)
from dpone.ports.bounded_window_contracts import (
    WindowSource as WindowSource,
)
from dpone.ports.bounded_window_contracts import (
    WindowStore as WindowStore,
)
from dpone.ports.bounded_window_contracts import (
    WindowTarget as WindowTarget,
)

__all__ = [
    "ChunkReceipt",
    "ExclusiveWindowWriterGuard",
    "PublicationStatus",
    "WindowBinaryIngest",
    "WindowChunk",
    "WindowContractError",
    "WindowExecutor",
    "WindowLease",
    "WindowMetadataStore",
    "WindowPlan",
    "WindowProgressJournal",
    "WindowRecord",
    "WindowResult",
    "WindowSource",
    "WindowStore",
    "WindowTarget",
]
