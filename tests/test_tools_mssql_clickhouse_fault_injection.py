from __future__ import annotations

import importlib.util
import sys
from argparse import Namespace
from decimal import Decimal
from pathlib import Path

from dpone.runtime.lineage.partition_checkpoint import PartitionCheckpointStatus


def _load_tool_module():
    path = Path("tools/mssql_clickhouse_fault_injection.py")
    spec = importlib.util.spec_from_file_location("dpone_tools_mssql_clickhouse_fault_injection", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.path.insert(0, str(Path("tools").resolve()))
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_committed_checkpoints_cover_every_partition() -> None:
    module = _load_tool_module()
    module.stress.CURRENT_ARGS = Namespace(num_partitions=4, lower_bound=1, upper_bound=10000)

    checkpoints = module._committed_checkpoints(
        rows=10000,
        source_table="dbo.bench_orders",
        target_table="dpone_it.bench_orders_ch",
    )

    assert len(checkpoints) == 4
    assert {checkpoint.status for checkpoint in checkpoints} == {PartitionCheckpointStatus.COMMITTED}
    assert checkpoints[0].partition_bounds == {"lower": 1, "upper": 2500}
    assert checkpoints[-1].partition_bounds == {"lower": 7501, "upper": 10000}


def test_row_hash_normalizes_equivalent_numeric_representations() -> None:
    module = _load_tool_module()

    source_hash = module._rows_hash([(1, 10, Decimal("0.10"), "order-1")])
    target_hash = module._rows_hash([(1, 10, 0.1, "order-1")])
    whole_decimal_hash = module._rows_hash([(100, 10, Decimal("1.00"), "order-100")])
    integer_amount_hash = module._rows_hash([(100, 10, 1, "order-100")])
    decimal_hash = module._rows_hash([(114, 10, Decimal("1.14"), "order-114")])
    float_noise_hash = module._rows_hash([(114, 10, 1.1400000000000001, "order-114")])

    assert source_hash == target_hash
    assert whole_decimal_hash == integer_amount_hash
    assert decimal_hash == float_noise_hash


def test_typed_hash_uses_schema_and_policy_for_binary_time_columns() -> None:
    module = _load_tool_module()
    policy = module.MssqlClickHouseTypePolicy(binary_encoding="hex", time_encoding="seconds_since_midnight")
    schema = [
        ("payload", "varbinary(max)"),
        ("business_time", "time(7)"),
    ]

    source_hash = module._typed_hash([(b"\x00\xff", "01:02:03.0000000")], schema, policy)
    target_hash = module._typed_hash([("00ff", 3723)], schema, policy)

    assert source_hash == target_hash
