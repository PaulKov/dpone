"""Snapshot deletions must not be projected as incoming PostgreSQL rows."""

import pytest

from dpone.config import LoadStrategy
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.etl.result_metrics import populate_success_result
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.postgres import PostgresSink
from tests.test_postgres_strategy_preservation import RecordingConnector, config
from tests.test_runtime_postgres_strategy_split import StubLogger


@pytest.mark.parametrize("incoming,missing,matched", [(2, 3, 0), (0, 3, 0), (2, 0, 2)])
def test_snapshot_diff_reports_deletions_separately_after_commit(incoming, missing, matched):
    class Connector(RecordingConnector):
        def copy_from_iter(self, _schema, _table, _columns, rows):
            return len(list(rows))

        def execute_query(self, query, params=None):
            super().execute_query(query, params)
            statement = self.operations[-1][0].strip()
            if statement.startswith("DELETE FROM"):
                return missing if "NOT EXISTS" in statement else matched
            return incoming

        def get_records(self, query, params=None, as_dict=False):
            super().get_records(query, params, as_dict)
            statement = self.operations[-1][0]
            if "HAVING COUNT" in statement:
                return []
            if "COUNT(*)" in statement:
                return [(matched if "WHERE EXISTS" in statement else incoming,)]
            return [(True,)]

    connector = Connector()
    cfg = config(load_strategy=LoadStrategy.SNAPSHOT_DIFF, unique_key=["id"])
    batch = LoadPayload(InMemoryRowsArtifact([{"id": index} for index in range(incoming)]), [("id", "integer")])
    result = PostgresSink(connector, None, StubLogger()).load(cfg, batch)
    public = {}
    populate_success_result(public, result, validation_info=None, reconciliation_metrics=None)
    assert connector.operations[-1][0] == "COMMIT"
    assert public["loaded_rows"] == public["inserted_rows"] == incoming - matched
    assert public["updated_rows"] == matched
    assert public["hard_deleted_rows"] == missing
    assert public["replaced_rows"] == 0
    assert public["final_rows"] == public["staging_rows"] == incoming
