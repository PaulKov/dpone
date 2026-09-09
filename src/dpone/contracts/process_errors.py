"""Runtime process error contracts."""

from __future__ import annotations


class ETLProcessError(Exception):
    """Raised when an ETL process fails during runtime execution."""


class WindowContractError(RuntimeError):
    """Terminal invalid capability, identity, reconciliation, or state."""


class WindowTransientError(RuntimeError):
    """Adapter-classified transient fault; reconciliation is still mandatory."""


class WindowOutcomeUnknown(WindowContractError):
    """An operation may have committed; automatic replay is forbidden."""


class WindowLeaseLost(WindowContractError):
    """Writer lease expired or was replaced by a newer fencing token."""


__all__ = ["ETLProcessError", "WindowContractError", "WindowTransientError", "WindowOutcomeUnknown", "WindowLeaseLost"]
