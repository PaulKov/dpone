"""Independent business/metadata/catalog reads, excluded from delivery counters."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import datetime

from tools.native_delivery_live_support.execution import Snapshot
from tools.native_delivery_live_support.profiles import (
    WINDOW_END,
    WINDOW_START,
    Dataset,
    exact_multiset,
    multiset_summary,
)

from dpone.runtime.state.mssql_generic_transaction import MssqlGenericTransactionState

from .bindings import requests
from .inventory import canonical
from .provisioning import json_value


def _hash(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def _metadata_hash(rows):
    return str(multiset_summary(exact_multiset(rows))["sha256"])


def snapshot(session):
    value = session.inventory
    columns = [column["name"] for column in Dataset(value.profile, value.rows, value.seed).schema()]
    metadata = ["__dpone__run_id", "__dpone__load_id", "__dpone__loaded_at", "__dpone__extracted_at"]
    with session.environment.sql_scope() as connector:
        names = columns + metadata
        observed = connector.get_records(
            f"SELECT {','.join('[' + name + ']' for name in names)} FROM [{value.schema}].[business]", as_dict=True
        )
        inside = [
            row for row in observed if value.strategy == "full_refresh" or WINDOW_START <= row["event_at"] < WINDOW_END
        ]
        outside = [
            row
            for row in observed
            if value.strategy == "partition_replace" and not WINDOW_START <= row["event_at"] < WINDOW_END
        ]
        attempt, operation = requests(session.results)
        state = MssqlGenericTransactionState(connector, database=value.state_database, schema=value.schema)
        receipt = state.operations.receipt_by_key(operation.operation_key(attempt), connector=connector)
    expected_record = session.store.load(session.expectation_key)
    expected_receipt = json.loads(expected_record.payload) if expected_record else None
    expected_metadata = None
    if expected_receipt is not None:
        expected_metadata = {
            "__dpone__run_id": session.ownership_id[:26],
            "__dpone__load_id": session.ownership_id[:26],
            "__dpone__loaded_at": datetime.fromisoformat(expected_receipt["loaded_at_utc"]).replace(tzinfo=None),
            "__dpone__extracted_at": datetime.fromisoformat(
                expected_receipt["source_lifecycle"]["extraction_started_at_utc"]
            ).replace(tzinfo=None),
        }
    data = session.journal_data()
    complete = data and data.get("complete")
    encoded = (
        sum(chunk["receipt"]["encoded_bytes"] for chunk in data["chunks"].values() if chunk["phase"] == "verified")
        if complete
        else None
    )
    publication = data and data.get("publication")
    pipeline = bool(
        publication
        and publication["phase"] == "succeeded"
        and session.store.load("local-checkpoint/" + session.ownership_id)
    )
    return Snapshot(
        rows=tuple({name: row[name] for name in columns} for row in inside),
        outside_rows=tuple({name: row[name] for name in columns} for row in outside),
        metadata_expected=None
        if expected_metadata is None
        else _metadata_hash(expected_metadata for _ in range(value.rows)),
        metadata_observed=_metadata_hash({name: row[name] for name in metadata} for row in inside),
        receipt_expected=_hash(expected_receipt) if expected_receipt is not None else None,
        receipt_observed=_hash(json_value(asdict(receipt))) if receipt is not None else None,
        source_queries=session.faults.source_queries,
        publications=session.faults.publications,
        stage_reads=session.faults.stage_reads,
        commit_known=session.faults.known,
        encoded_bytes=encoded,
        fault_events=tuple(session.faults.events),
        receipt_probes=session.faults.probes,
        pipeline_complete=pipeline,
    )
