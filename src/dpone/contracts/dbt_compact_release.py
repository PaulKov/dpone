"""Pure identity and preservation policy for a complete compact workspace."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from typing import Any

from dpone.contracts.airflow_deployment import release_id
from dpone.contracts.dbt_contract_validation import sha256_bytes
from dpone.contracts.dbt_release import dbt_release_runtime_wire_contract
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V2
from dpone.contracts.dbt_runtime_release_binding import DbtReleaseArtifactIndex
from dpone.contracts.strict_json import strict_json_object

COMPACT_OUTPUT_INVALID = "DPONE_COMPACT_PACK_RELEASE_OUTPUT_INVALID"


class CompactWorkspaceReleasePlan:
    """Bind native metadata and preserve source identity during transport rewriting.

    This policy has no filesystem or provider dependencies. Construction proves
    metadata identity only; the caller must validate the public schema and capture
    verified source bytes before deriving a release, then verify the derived tree.
    """

    def __init__(self, payload: bytes, *, dag_ids: Sequence[str] | None = None) -> None:
        release = strict_json_object(payload)
        if (
            release.get("schema") != "dpone.release-set.v2"
            or dbt_release_runtime_wire_contract(release) != DBT_RUNTIME_WIRE_V2
        ):
            raise ValueError("native compact input requires a complete workspace wire-v2 release")
        self.expected_release_id = release_id(release)
        if release.get("release_id") != self.expected_release_id:
            raise ValueError("workspace release identity differs from its content")
        index = DbtReleaseArtifactIndex(release)
        if not index.dags or (dag_ids is not None and set(dag_ids) != set(index.dags)):
            raise ValueError("native compact delivery requires the complete workspace DAG inventory")
        self._release = release

    @property
    def release(self) -> dict[str, Any]:
        """Return detached metadata for mandatory external schema validation."""
        return deepcopy(self._release)

    def derive(
        self,
        captured: Mapping[str, bytes],
        *,
        rewrite_dag: Callable[[Mapping[str, Any]], dict[str, Any]],
        rewrite_pack: Callable[[Mapping[str, Any]], dict[str, Any]],
        promotion: Mapping[str, str],
    ) -> tuple[dict[str, Any], dict[str, bytes]]:
        """Preserve all non-transport fields and refresh only derived identities."""
        files = dict(captured)
        derived = self.release
        for section, rewrite in (("dag_specs", rewrite_dag), ("workload_packs", rewrite_pack)):
            for descriptor in derived["artifacts"][section]:
                path = descriptor["path"]
                original = strict_json_object(files[path])
                rewritten = rewrite(deepcopy(original))
                if section == "workload_packs":
                    _require_preserved_source(original, rewritten)
                    descriptor["pack_fingerprint"] = rewritten["pack_fingerprint"]
                files[path] = _json_bytes(rewritten)
                descriptor.update(sha256=sha256_bytes(files[path]), bytes=len(files[path]))
        derived["promotion"] = dict(promotion)
        derived["release_id"] = release_id(derived)
        files["release-set.json"] = _json_bytes(derived)
        files.pop("release-subjects.sha256")
        return derived, files


def _require_preserved_source(original: Mapping[str, Any], rewritten: Mapping[str, Any]) -> None:
    transport = {"airflow", "connection_projection", "xcom", "provider_execution", "pack_fingerprint"}
    if {k: v for k, v in original.items() if k not in transport} != {
        k: v for k, v in rewritten.items() if k not in transport
    }:
        raise ValueError("compact rewrite changed workload source identity")


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
