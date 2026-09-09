"""Parent control-plane timeout capability for MSSQL backfill state."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from typing import TYPE_CHECKING, Any

_SCOPE_ERROR = "mssql_transaction.process_lane_control_timeout_scope_required"


class MssqlBackfillProcessLaneControlMixin:
    """Keep ledger and private campaign-fence timeout ownership encapsulated."""

    if TYPE_CHECKING:
        connector: Any
        _campaign_lock_connector: Any

    @contextmanager
    def bounded_process_lane_control_timeout(self, seconds: int) -> Iterator[None]:
        """Bound all store-owned parent sessions and restore them together."""

        connectors = {id(connector): connector for connector in (self.connector, self._campaign_lock_connector)}
        with ExitStack() as stack:
            for connector in connectors.values():
                scope = getattr(connector, "bounded_query_timeout", None)
                if not callable(scope):
                    raise RuntimeError(_SCOPE_ERROR)
                stack.enter_context(scope(seconds))
            yield


__all__ = ["MssqlBackfillProcessLaneControlMixin"]
