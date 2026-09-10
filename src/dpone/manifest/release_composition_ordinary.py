"""Bounded, no-follow capture of an ordinary producer root for composition."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone_airflow_pack.dag_spec_validation import validate_dag_spec_payload
from dpone_airflow_pack.pack_identity import parse_pack_json, verify_pack_fingerprint
from dpone_airflow_pack.strict_json import loads_strict_json_object

from dpone.contracts.configuration_errors import ETLConfigurationError
from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_relation_writes import require_distinct_logical_writes
from dpone.contracts.release_composition_ordinary import (
    OrdinaryReleaseCapture,
    OrdinaryReleaseInventoryError,
)
from dpone.manifest.release_composition_ordinary_closure import OrdinaryPackClosureVerifier
from dpone.ports.dbt_release_files import ConfinedReleaseFileReader
from dpone.readiness.airflow_compact_pack_release_helpers import rewrite_strict_init_fetch_dag_spec
from dpone.readiness.dbt_airflow_execution_pack import strict_transfer_pack

_MAX_SOURCE_FILES = 10_000
_MAX_SOURCE_FILE_BYTES = 8 * 1024 * 1024
_MAX_SOURCE_TOTAL_BYTES = 512 * 1024 * 1024
_ID = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,249}\Z")


class OrdinaryReleaseInventoryReader:
    """Capture exact source bytes and validate a deterministic strict projection.

    The source root contains only ``_dags/<id>.dag-spec.json`` and referenced
    ``<workload>/airflow-pack.json`` files. Each workload belongs to exactly one
    DAG. Source counts and bytes are bounded before acquisition; archive limits
    are enforced independently by the standard runtime extractor. The enclosing
    composition service additionally enforces complete-parent aggregate limits.
    """

    def __init__(
        self,
        *,
        read_file: ConfinedReleaseFileReader,
        closure: OrdinaryPackClosureVerifier,
    ) -> None:
        self._read_file = read_file
        self._closure = closure

    def capture(self, root: Path, *, xcom_sidecar_image: str) -> OrdinaryReleaseCapture:
        """Return detached source evidence or a sanitized fail-closed error."""
        try:
            return self._capture(Path(root).absolute(), xcom_sidecar_image=xcom_sidecar_image)
        except OrdinaryReleaseInventoryError:
            raise
        except (
            OSError,
            ValueError,
            TypeError,
            KeyError,
            RecursionError,
            DbtPublishingError,
            ETLConfigurationError,
        ) as exc:
            raise OrdinaryReleaseInventoryError(
                "ordinary sources are invalid or unsupported; regenerate a complete root of plain transfer packs"
            ) from exc

    def _capture(self, root: Path, *, xcom_sidecar_image: str) -> OrdinaryReleaseCapture:
        source_paths = _source_paths(root)
        dag_paths = [path for path in source_paths if path.startswith("_dags/")]
        if not dag_paths:
            raise OrdinaryReleaseInventoryError("ordinary source root has no DAG specifications")
        files: dict[str, bytes] = {}
        dags: dict[str, bytes] = {}
        packs: dict[str, bytes] = {}
        dag_rows: list[dict[str, Any]] = []
        pack_rows: list[dict[str, Any]] = []
        writes = []
        owners: set[str] = set()
        for path in dag_paths:
            body = self._read_file(root, path, max_bytes=_MAX_SOURCE_FILE_BYTES)
            _retain(files, path, body)
            spec = loads_strict_json_object(body.decode("utf-8"))
            dag_id = path.removeprefix("_dags/").removesuffix(".dag-spec.json")
            if spec.get("dag_id") != dag_id or validate_dag_spec_payload(spec, "ordinary DAG"):
                raise OrdinaryReleaseInventoryError("ordinary DAG schema or fingerprint is invalid")
            ids = []
            for node in spec["nodes"]:
                workload_id = node.get("workload_id")
                if not isinstance(workload_id, str) or not _ID.fullmatch(workload_id) or workload_id in owners:
                    raise OrdinaryReleaseInventoryError("ordinary workload membership is invalid or duplicated")
                if node.get("selector") is not None or node.get("pack_ref") != f"cached://workloads/{workload_id}":
                    raise OrdinaryReleaseInventoryError("ordinary DAG must reference an entire plain transfer workload")
                owners.add(workload_id)
                ids.append(workload_id)
                pack_path = f"{workload_id}/airflow-pack.json"
                if pack_path not in source_paths:
                    raise OrdinaryReleaseInventoryError("ordinary DAG references a missing workload pack")
                raw = self._read_file(root, pack_path, max_bytes=_MAX_SOURCE_FILE_BYTES)
                _retain(files, pack_path, raw)
                pack = parse_pack_json(raw)
                fingerprint = verify_pack_fingerprint(pack)
                writes.append(self._closure.verify(pack, workload_id=workload_id, dag_id=dag_id))
                rewritten = strict_transfer_pack(pack, xcom_sidecar_image=xcom_sidecar_image)
                packs[f"packs/{workload_id}.airflow-pack.json"] = _json_bytes(rewritten)
                pack_rows.append({**_descriptor(workload_id, pack_path, raw), "pack_fingerprint": fingerprint})
            if not ids:
                raise OrdinaryReleaseInventoryError("ordinary DAG has no workload membership")
            dags[f"dags/{dag_id}.dag-spec.json"] = _json_bytes(
                rewrite_strict_init_fetch_dag_spec(spec, workload_ids=ids)
            )
            dag_rows.append(_descriptor(dag_id, path, body))
        if set(files) != set(source_paths) or _source_paths(root) != source_paths:
            raise OrdinaryReleaseInventoryError("ordinary source root contains orphan or changing artifacts")
        require_distinct_logical_writes(writes)
        return OrdinaryReleaseCapture(
            inventory={
                "schema": "dpone.workload-inventory.v1",
                "dag_specs": dag_rows,
                "workload_packs": sorted(pack_rows, key=lambda row: row["id"]),
            },
            files=files,
            dag_files=dags,
            pack_files=packs,
            relation_writes=tuple(writes),
        )


def _source_paths(root: Path) -> tuple[str, ...]:
    if root.is_symlink() or not stat.S_ISDIR(root.lstat().st_mode):
        raise OrdinaryReleaseInventoryError("ordinary source root must be a regular directory")
    paths = []
    pending = [root]
    total = entries = 0
    while pending:
        directory = pending.pop()
        with os.scandir(directory) as children:
            for child in children:
                entries += 1
                if entries > 2 * _MAX_SOURCE_FILES + 1:
                    raise OrdinaryReleaseInventoryError("ordinary source directory count exceeds its limit")
                relative = Path(child.path).relative_to(root).as_posix()
                if child.is_symlink():
                    raise OrdinaryReleaseInventoryError("ordinary source root cannot contain symlinks")
                if child.is_dir(follow_symlinks=False):
                    if directory != root or not _ID.fullmatch(child.name):
                        raise OrdinaryReleaseInventoryError("ordinary source directory layout is unsupported")
                    pending.append(Path(child.path))
                    continue
                if not child.is_file(follow_symlinks=False) or directory == root:
                    raise OrdinaryReleaseInventoryError("ordinary source root contains an unsupported entry")
                parent = directory.name
                valid = (
                    parent == "_dags"
                    and child.name.endswith(".dag-spec.json")
                    and _ID.fullmatch(child.name.removesuffix(".dag-spec.json"))
                ) or (parent != "_dags" and child.name == "airflow-pack.json")
                if not valid:
                    raise OrdinaryReleaseInventoryError("ordinary source root contains an undeclared artifact")
                size = child.stat(follow_symlinks=False).st_size
                total += size
                paths.append(relative)
                if size > _MAX_SOURCE_FILE_BYTES or total > _MAX_SOURCE_TOTAL_BYTES or len(paths) > _MAX_SOURCE_FILES:
                    raise OrdinaryReleaseInventoryError("ordinary source inventory exceeds its count or byte limit")
    # Empty workload directories cannot silently disappear from the inventory.
    actual_dirs = {path.name for path in root.iterdir() if path.is_dir()}
    expected_dirs = {path.split("/")[0] for path in paths}
    if actual_dirs != expected_dirs:
        raise OrdinaryReleaseInventoryError("ordinary source root contains an orphan directory")
    return tuple(sorted(paths))


def _retain(files: dict[str, bytes], path: str, body: bytes) -> None:
    if len(body) > _MAX_SOURCE_FILE_BYTES or sum(map(len, files.values())) + len(body) > _MAX_SOURCE_TOTAL_BYTES:
        raise OrdinaryReleaseInventoryError("ordinary captured bytes exceed the inventory limit")
    files[path] = body


def _descriptor(identity: str, path: str, body: bytes) -> dict[str, Any]:
    return {"id": identity, "path": path, "sha256": "sha256:" + hashlib.sha256(body).hexdigest(), "bytes": len(body)}


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n").encode("utf-8")
