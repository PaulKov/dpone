"""SQL-observed disposable layout authority, independent of invocation names."""

from __future__ import annotations

import hashlib
from types import SimpleNamespace

from tools.native_delivery_live_support.profiles import PROFILES, Dataset

from dpone.runtime.sinks.strategies.mssql.mssql_native_lineage import resolve_mssql_native_lineage_columns

from .inventory import canonical


def business_definitions(dataset):
    """Preserve the declared wire nullability when creating the synthetic target."""
    return [
        f"[{column['name']}] {column['target']} {'NULL' if column['source'].startswith('Nullable(') else 'NOT NULL'}"
        for column in dataset.schema()
    ]


def target_definitions(dataset):
    config = SimpleNamespace(options={"lineage": {"preset": "bulk_standard"}})
    lineage = resolve_mssql_native_lineage_columns(config)
    return business_definitions(dataset) + [
        f"[{name}] {dtype} {'NULL' if nullable else 'NOT NULL'}" for name, dtype, nullable, _, _ in lineage
    ]


def observed_layout(connector, qualified, *, temporary=False):
    """Read physical columns and indexes; omit randomized object identifiers."""
    catalog = "tempdb.sys" if temporary else "sys"
    columns = connector.get_records(
        f"SELECT c.column_id,c.name,t.name,c.max_length,c.precision,c.scale,c.is_nullable,"
        f"c.collation_name,c.is_identity,c.is_computed FROM {catalog}.columns c "
        f"JOIN {catalog}.types t ON c.user_type_id=t.user_type_id "
        "WHERE c.object_id=OBJECT_ID(?) ORDER BY c.column_id",
        (qualified,),
    )
    indexes = connector.get_records(
        f"SELECT index_id,type,is_unique,is_primary_key,is_disabled FROM {catalog}.indexes "
        "WHERE object_id=OBJECT_ID(?) ORDER BY index_id",
        (qualified,),
    )
    if not columns or not indexes:
        raise ValueError("local_fixture.target_layout_unavailable")
    return {"columns": [list(row) for row in columns], "indexes": [list(row) for row in indexes]}


def template_layouts(connector):
    """Observe each actual SQL heap shape in connection-local temporary tables.

    SQL Server releases these tables when the describe connection closes, even
    when introspection fails. Actual invocation tables are checked against them.
    """
    layouts = {}
    for profile in PROFILES:
        name = "#dda_layout_" + profile
        connector.execute_query(f"CREATE TABLE [{name}] ({', '.join(target_definitions(Dataset(profile, 0)))})")
        layouts[profile] = observed_layout(connector, "tempdb.." + name, temporary=True)
    return layouts


def layout_digest(layouts):
    return hashlib.sha256(canonical(layouts)).hexdigest()
