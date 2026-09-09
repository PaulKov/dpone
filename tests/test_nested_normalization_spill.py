from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

from dpone.config import LoadConfig, LoadStrategy
from dpone.governance.quality import QualityGatePolicy, QualityGateRunner
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.governance.service import LoadGovernanceService
from dpone.runtime.sinks.base import LoadResult
from dpone.runtime.sources.base import ExtractResult


class _RecordingQualityRunner(QualityGateRunner):
    def __init__(self) -> None:
        self.non_empty_policies: list[QualityGatePolicy] = []

    def run(self, policy, *, source, target):
        if policy.gates:
            self.non_empty_policies.append(policy)
        return super().run(policy, source=source, target=target)


@dataclass
class _Source:
    def get_incremental_state(self, _load_config):
        return None

    def extract(self, _load_config, _last_state):
        return ExtractResult(
            artifact=InMemoryRowsArtifact([{"order_id": 1, "items": [{"sku": "A"}]}]),
            schema=[("order_id", "bigint"), ("items", "json")],
            state=SimpleNamespace(cursor="next"),
        )


class _Sink:
    def __init__(self) -> None:
        self.loaded_tables: list[str] = []
        self.aborted_tables: list[str] = []

    def stage_payload(self, load_config, payload):
        rows = list(payload.artifact._iterator)
        return SimpleNamespace(
            staged_rows=len(rows),
            target_table=load_config.target_table,
            payload_schema=tuple(payload.schema or ()),
            rows=rows,
        )

    def finalize_staged_load(self, load_config, handle):
        self.loaded_tables.append(load_config.target_table)
        rows = int(handle.staged_rows)
        return LoadResult(inserted_rows=rows, updated_rows=0, total_rows=rows, staging_rows=rows)

    def abort_staged_load(self, handle) -> None:
        self.aborted_tables.append(str(handle.target_table))

    def cleanup_staged_load(self, handle) -> None:
        del handle

    def load(self, load_config, payload):
        handle = self.stage_payload(load_config, payload)
        try:
            return self.finalize_staged_load(load_config, handle)
        except Exception:
            self.abort_staged_load(handle)
            raise


def test_spilled_nested_package_evaluates_non_empty_quality_once_on_aggregate(tmp_path: Path) -> None:
    runner = _RecordingQualityRunner()
    sink = _Sink()
    config = LoadConfig(
        source_conn_id="src",
        target_conn_id="sink",
        source_schema="api",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        unique_key=["order_id"],
        options={
            "schema_evolution": {"enabled": False},
            "lineage": {"preset": "hierarchical"},
            "normalization": {
                "nested": {
                    "enabled": True,
                    "materialization": "spill_to_disk",
                    "spill_output_dir": str(tmp_path),
                    "spill_output_format": "jsonl",
                }
            },
            "quality": {
                "gates": [
                    {
                        "id": "aggregate_rows",
                        "type": "min_rows",
                        "side": "target",
                        "threshold": 2,
                    }
                ]
            },
        },
    )

    result = ETLProcessor(
        _Source(),
        sink,
        load_governance_service=LoadGovernanceService(quality_runner=runner),
    ).run(config)

    assert result["status"] == "success"
    assert result["loaded_rows"] == 2
    assert sink.loaded_tables == ["orders__items", "orders"]
    assert len(runner.non_empty_policies) == 1
    assert result["reconciliation_metrics"]["quality_gates"]["passed"] is True
