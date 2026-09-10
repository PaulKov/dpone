"""Real-row coverage of PostgreSQL snapshot deletion metrics and replay."""

import csv

import pytest

from dpone.config import LoadStrategy
from dpone.runtime.artifacts import FileExportArtifact, InMemoryRowsArtifact
from dpone.runtime.etl.result_metrics import populate_success_result
from dpone.runtime.sinks.load_payload import LoadPayload
from tests.integration.postgres.strategy_preservation_support import BUSINESS_SCHEMA
from tests.integration.postgres.test_postgres_strategy_preservation_live import lab  # noqa: F401

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_postgres]


@pytest.mark.parametrize("artifact_kind", ["memory", "file"])
def test_snapshot_diff_rows_and_metrics_on_change_empty_and_replay(lab, tmp_path, artifact_kind):  # noqa: F811
    lab.constrained_target()
    lab.execute(f"INSERT INTO {lab.schema}.target VALUES (91,'old','2026-01-01'),(92,'old','2026-01-01')")
    before = lab.snapshot("before")
    rows = [(1, "new", "2026-01-01"), (2, "new", "2026-01-02")]
    cfg = lab.config(load_strategy=LoadStrategy.SNAPSHOT_DIFF, unique_key=["id"])
    for attempt, (incoming, deleted, inserted, updated) in enumerate(
        [(rows, 3, 2, 0), (rows, 0, 0, 2), ([], 2, 0, 0), ([], 0, 0, 0)]
    ):
        if artifact_kind == "memory":
            artifact = InMemoryRowsArtifact(
                [dict(zip([name for name, _ in BUSINESS_SCHEMA], row, strict=True)) for row in incoming]
            )
        else:
            path = tmp_path / f"snapshot-{attempt}.csv"
            with path.open("w", newline="") as stream:
                csv.writer(stream).writerows(incoming)
            artifact = FileExportArtifact(
                str(path), columns=[name for name, _ in BUSINESS_SCHEMA], compressed=False, format="csv"
            )
        result = lab.sink.load(cfg, LoadPayload(artifact, BUSINESS_SCHEMA))
        public = {}
        populate_success_result(public, result, validation_info=None, reconciliation_metrics=None)
        assert public["loaded_rows"] == public["inserted_rows"] == inserted
        assert public["updated_rows"] == updated
        assert public["hard_deleted_rows"] == deleted
        assert public["replaced_rows"] == 0
        assert public["final_rows"] == public["staging_rows"] == len(incoming)
        after = lab.snapshot(f"snapshot-{attempt}")
        assert after["rows"] == incoming and after["metadata"] == before["metadata"]
        lab.observations.append({"label": f"metrics-{attempt}", "public_result": public})
        assert lab.connector.events[-1]["operation"] == "commit"
