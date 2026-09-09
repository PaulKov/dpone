"""Closed JSON Schema projection of the canonical multi-project source inventory.

Sorting, NFC paths, reference membership and fingerprints are additionally checked
by DbtSourceInventory and its release-binding contract; schema shape alone is not
source authority.
"""

from __future__ import annotations

from typing import Any

from dpone.contracts.dbt_publish_schema_contract_common import DIGEST, RELATIVE, TOKEN, object_schema
from dpone.contracts.dbt_workspace_paths import DBT_PROJECT_NAME_PATTERN


def dbt_source_inventory_schema() -> dict[str, Any]:
    workflow = object_schema(
        ("workflow_id", "dag_id", "workload_id", "runtime_payload_ids", "selection_lock_sha256"),
        {
            "workflow_id": {"type": "string", "pattern": "^[a-z][a-z0-9_]{0,63}$"},
            "dag_id": {**TOKEN, "maxLength": 250},
            "workload_id": {"type": "string", "pattern": "^dbt__[a-z][a-z0-9_]{0,63}$"},
            "runtime_payload_ids": {
                "type": "array",
                "minItems": 3,
                "maxItems": 3,
                "uniqueItems": True,
                "prefixItems": [
                    {"type": "string", "pattern": f"^{prefix}_sha256_[0-9a-f]{{64}}$"}
                    for prefix in ("dbt_project", "dbt_manifest", "dbt_selection")
                ],
                "items": False,
            },
            "selection_lock_sha256": DIGEST,
        },
    )
    project = object_schema(
        ("project_path", "project_name", "project_bundle_sha256", "manifest_sha256", "toolchain_sha256", "workflows"),
        {
            "project_path": RELATIVE,
            "project_name": {"type": "string", "pattern": f"^{DBT_PROJECT_NAME_PATTERN}$"},
            "project_bundle_sha256": DIGEST,
            "manifest_sha256": DIGEST,
            "toolchain_sha256": DIGEST,
            "workflows": {"type": "array", "minItems": 1, "maxItems": 64, "uniqueItems": True, "items": workflow},
        },
    )
    return object_schema(
        ("schema", "projects", "snapshot_sha256"),
        {
            "schema": {"const": "dpone.dbt-source-snapshot.v2"},
            "projects": {"type": "array", "minItems": 1, "maxItems": 64, "uniqueItems": True, "items": project},
            "snapshot_sha256": DIGEST,
        },
    )
