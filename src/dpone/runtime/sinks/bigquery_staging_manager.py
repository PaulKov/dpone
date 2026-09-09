"""Compatibility shim for BigQuery staging manager.

Canonical implementation lives in
``dpone.runtime.sinks.staging_managers.bigquery``.
"""

from __future__ import annotations

from dpone.runtime.sinks.staging_managers.bigquery import BigQueryStagingManager

__all__ = ["BigQueryStagingManager"]
