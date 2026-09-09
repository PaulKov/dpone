"""dpone runtime layer.

This package contains components required to *execute* ETL:

- sources / sinks
- connectors / credentials
- state / reconciliation
- airflow integration

It is intentionally separated from manifest compilation & DAG analysis utilities.
"""

from __future__ import annotations
