"""Compatibility facade for BigQuery-backed reconciliation tech tables.

New code should depend on ``dpone.runtime.reconciliation.bigquery.BigQueryReconciliationStore``
or the generic reconciliation protocols. ``BigQueryTechTables`` remains for one
deprecation window so older imports keep working.
"""

from __future__ import annotations

from dpone.runtime.reconciliation.bigquery import BigQueryReconciliationStore


class BigQueryTechTables(BigQueryReconciliationStore):
    """Deprecated compatibility shim for the BigQuery reconciliation store."""


__all__ = ["BigQueryTechTables"]
