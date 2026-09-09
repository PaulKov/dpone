"""Runtime connector base.

This module is kept for backward compatibility.

New code should import the interface from :mod:`dpone.ports.db_connector`.
"""

from __future__ import annotations

from dpone.ports.db_connector import AbstractConnector

__all__ = ["AbstractConnector"]
