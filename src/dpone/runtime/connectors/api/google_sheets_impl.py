"""Deprecated compatibility shim for Google Sheets connector imports.

Use ``dpone.runtime.connectors.api.google_sheets`` for public imports or
``dpone.runtime.connectors.api.google_sheets_connector`` for focused connector imports.
"""

from __future__ import annotations

from dpone.runtime.connectors.api.google_sheets_connector import (
    GoogleSheetsConnector,
    GoogleSheetsCredentials,
    _load_google_auth_modules,
    _optional_int,
    _optional_str,
    get_default_manager,
    logger,
)

__all__ = [
    "GoogleSheetsConnector",
    "GoogleSheetsCredentials",
    "_load_google_auth_modules",
    "_optional_int",
    "_optional_str",
    "get_default_manager",
    "logger",
]
