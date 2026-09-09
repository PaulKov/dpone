"""Soft Delete модуль для reconciliation.

Поддерживает soft delete для разных target БД:
- PostgreSQL: через psycopg
- BigQuery: через google-cloud-bigquery
- ClickHouse: через clickhouse-driver
"""

from dpone.runtime.reconciliation.soft_delete.registry import SoftDeleteRegistry

__all__ = [
    "SoftDeleteRegistry",
]
