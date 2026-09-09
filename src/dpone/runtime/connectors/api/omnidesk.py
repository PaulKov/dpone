"""Compatibility facade for the Omnidesk API connector."""

from __future__ import annotations

from dpone.runtime.connectors.api.omnidesk_connector import OmnideskConnector
from dpone.runtime.connectors.api.omnidesk_support import OmnideskCredentials, OmnideskPagination, get_default_manager

__all__ = ["OmnideskCredentials", "OmnideskPagination", "OmnideskConnector", "get_default_manager"]
