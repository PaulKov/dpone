"""Exact descriptor limits; metadata checks are not large-byte delivery proof."""

import pytest

from dpone.contracts.dbt_runtime_payloads import (
    DBT_RUNTIME_WIRE_V2,
    MAX_DBT_RUNTIME_PAYLOAD_BYTES,
    MAX_DBT_RUNTIME_PAYLOAD_TOTAL_BYTES,
    MAX_DBT_RUNTIME_PAYLOADS,
    dbt_runtime_payload_reference,
    validate_dbt_runtime_payload_descriptor,
)
from dpone.contracts.dbt_runtime_release_binding import dbt_runtime_payload_inventory


def descriptor(index, size):
    digest = f"{index:064x}"
    reference = dbt_runtime_payload_reference("dbt_project_sha256_" + digest, wire_contract=DBT_RUNTIME_WIRE_V2)
    return {
        "id": reference.id,
        "kind": reference.kind,
        "path": reference.path,
        "media_type": reference.media_type,
        "sha256": "sha256:" + digest,
        "bytes": size,
    }


@pytest.mark.parametrize(
    "size, valid", [(MAX_DBT_RUNTIME_PAYLOAD_BYTES, True), (MAX_DBT_RUNTIME_PAYLOAD_BYTES + 1, False)]
)
def test_exact_individual_descriptor_boundary(size, valid):
    if valid:
        validate_dbt_runtime_payload_descriptor(descriptor(1, size), wire_contract=DBT_RUNTIME_WIRE_V2)
    else:
        with pytest.raises(ValueError):
            validate_dbt_runtime_payload_descriptor(descriptor(1, size), wire_contract=DBT_RUNTIME_WIRE_V2)


@pytest.mark.parametrize("count", [MAX_DBT_RUNTIME_PAYLOADS, MAX_DBT_RUNTIME_PAYLOADS + 1])
def test_exact_runtime_inventory_count_boundary(count):
    rows = [descriptor(index, 1) for index in range(count)]
    if count == MAX_DBT_RUNTIME_PAYLOADS:
        assert len(dbt_runtime_payload_inventory(rows)) == count
    else:
        with pytest.raises(ValueError):
            dbt_runtime_payload_inventory(rows)


@pytest.mark.parametrize("overflow", [False, True])
def test_exact_runtime_inventory_aggregate_boundary(overflow):
    rows = [descriptor(1, MAX_DBT_RUNTIME_PAYLOAD_BYTES), descriptor(2, MAX_DBT_RUNTIME_PAYLOAD_BYTES)]
    assert sum(row["bytes"] for row in rows) == MAX_DBT_RUNTIME_PAYLOAD_TOTAL_BYTES
    if overflow:
        rows.append(descriptor(3, 1))
        with pytest.raises(ValueError, match="aggregate"):
            dbt_runtime_payload_inventory(rows)
    else:
        assert len(dbt_runtime_payload_inventory(rows)) == 2
