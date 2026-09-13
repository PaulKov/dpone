"""Owned-object cleanup only after an acknowledged outcome, under current fencing."""

from __future__ import annotations

from dpone.runtime.state.mssql_route_preflight import require_atomic_mssql_route, resolve_atomic_mssql_target

from .bindings import requests, storage_binding
from .cleanup_progress import drop_owned
from .source_guard import ddl_lock


def cleanup(session):
    if not session.faults.known:
        raise RuntimeError("local_fixture.unknown_outcome_retained")
    lease = session.store.acquire(session.plan.target_id, "cleanup:" + session.ownership_id, 60)
    try:
        value = session.inventory_store.load(session.ownership_id)
        if value != session.inventory or session.inventory_store.provisioned(session.ownership_id) != session.results:
            raise ValueError("local_fixture.inventory_changed")
        data = session.journal_data()
        if data and data.get("publication") and data["publication"]["phase"] not in {"succeeded", "prepared"}:
            raise RuntimeError("local_fixture.unresolved_publication_retained")
        if (
            data
            and data.get("publication")
            and data["publication"]["phase"] == "prepared"
            and not data.get("rollback_history")
        ):
            raise RuntimeError("local_fixture.prepared_publication_retained")
        with (
            ddl_lock(session.directory, writer=True),
            session.environment.sql_scope() as target,
            session.environment.sql_scope() as state,
        ):
            storage = storage_binding(session.environment, value, session.results, state)
            require_atomic_mssql_route(target, storage)

            def target_id():
                return target.get_records("SELECT OBJECT_ID(?)", (f"[{value.schema}].[business]",))[0][0]

            if target_id() is not None:
                identity = resolve_atomic_mssql_target(
                    target, storage, database=value.target_database, schema=value.schema, table=value.table
                )
                attempt, _ = requests(session.results)
                if identity.digest != attempt.target_identity:
                    raise ValueError("local_fixture.target_identity_changed")
            connector = session.environment.clickhouse()
            try:

                def source_id():
                    rows = connector.get_records(
                        "SELECT toString(uuid) FROM system.tables WHERE database=%(database)s AND name=%(table)s",
                        {"database": value.source_database, "table": value.schema},
                    )
                    if len(rows) > 1:
                        raise ValueError("local_fixture.source_identity_ambiguous")
                    return rows[0][0] if rows else None

                prefix = "local-cleanup/" + session.ownership_id + "/"
                drop_owned(
                    session.store,
                    prefix + "source",
                    session.results["source_uuid"],
                    source_id,
                    lambda: connector.execute_query(f"DROP TABLE `{value.source_database}`.`{value.schema}`"),
                    lease,
                )
                drop_owned(
                    session.store,
                    prefix + "target",
                    session.results["target_object_id"],
                    target_id,
                    lambda: target.execute_query(f"DROP TABLE [{value.schema}].[business]"),
                    lease,
                )
            finally:
                connector.close()
        # The immutable SQL identity registry and generic audit catalog remain.
        # Native service owns its stages; unresolved ones are never dropped here.
    finally:
        session.store.release(lease)
