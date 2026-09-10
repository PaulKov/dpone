"""Reject native dbt authority before a descriptor-less legacy publication."""

from collections.abc import Mapping
from typing import Any

from dpone.contracts.dbt_execution_pack import DBT_EXECUTION_PACK_SCHEMA_V2
from dpone.contracts.dbt_release_workload_binding import runtime_payload_member
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V2, dbt_runtime_payload_reference
from dpone.contracts.strict_json import strict_json_object


def require_legacy_dbt_authority(pack: Mapping[str, Any]) -> None:
    """Validate legacy compatibility without manufacturing a native producer.

    Missing producer always means wire v1. Native CAS references and embedded
    execution-pack.v2 require a complete source-authorized native descriptor.
    This prepublication guard supplements the launcher's independent check.
    """
    refs = pack.get("runtime_payload_ids", [])
    if isinstance(refs, list):
        for reference in refs:
            try:
                dbt_runtime_payload_reference(reference, wire_contract=DBT_RUNTIME_WIRE_V2)
            except (ValueError, TypeError):
                continue  # Legacy validation retains its existing failure vocabulary.
            raise ValueError("native dbt payloads require complete workspace authority")
    workload = pack.get("workload")
    if not isinstance(workload, Mapping):
        return
    path = workload.get("manifest")
    if path != "runtime/dbt-execution-pack.json":
        return
    body = runtime_payload_member(
        pack,
        expected_path=path,
        max_archive_bytes=8 * 1024 * 1024,
        max_member_bytes=1024 * 1024,
        label="dbt execution pack",
    )
    if strict_json_object(body).get("schema") == DBT_EXECUTION_PACK_SCHEMA_V2:
        raise ValueError("native dbt execution requires complete workspace authority")
