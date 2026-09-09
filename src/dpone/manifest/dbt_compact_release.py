"""Transform a complete verified workspace into a strict compact release tree."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory

from dpone.contracts.airflow_deployment import release_id
from dpone.contracts.dbt_contract_validation import sha256_bytes
from dpone.contracts.dbt_release import dbt_release_runtime_wire_contract
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V2
from dpone.contracts.dbt_runtime_release_binding import DbtReleaseArtifactIndex
from dpone.contracts.strict_json import strict_json_object
from dpone.gitops.release_set_validation import validate_release_set
from dpone.ports.dbt_compact_release import CompactWorkspaceRewriter, ReleaseIntegrityWriter
from dpone.ports.dbt_release_files import ConfinedReleaseFileReader, VerifiedWorkspaceReleaseCapture


class CompactWorkspaceReleaseBuilder:
    """Validate a frozen input, rewrite transport, then independently recapture it.

    All I/O and framework policy are injected at composition. Neither validation
    pass is optional. Publication belongs to the caller and starts only after the
    private stage has been verified and cleaned up.
    """

    def __init__(
        self,
        *,
        read_file: ConfinedReleaseFileReader,
        capture: VerifiedWorkspaceReleaseCapture,
        integrity: ReleaseIntegrityWriter,
        rewriter: CompactWorkspaceRewriter,
        promotion: Mapping[str, str],
    ) -> None:
        self._read = read_file
        self._capture = capture
        self._integrity = integrity
        self._rewriter = rewriter
        self._promotion = dict(promotion)

    def build(
        self, root: Path, *, xcom_sidecar_image: str, dag_ids: Sequence[str] | None = None
    ) -> Mapping[str, bytes]:
        """Return a complete immutable-publication input or fail without output."""
        payload = self._read(root, "release-set.json", max_bytes=8 * 1024 * 1024)
        release = strict_json_object(payload)
        if (
            release.get("schema") != "dpone.release-set.v2"
            or dbt_release_runtime_wire_contract(release) != DBT_RUNTIME_WIRE_V2
        ):
            raise ValueError("native compact input requires a complete workspace wire-v2 release")
        if validate_release_set(release).failure is not None:
            raise ValueError("workspace release violates its public contract")
        expected = release_id(release)
        if release.get("release_id") != expected:
            raise ValueError("workspace release identity differs from its content")
        captured = self._capture.capture_verified_files(root, release_payload=payload, expected_release_id=expected)
        index = DbtReleaseArtifactIndex(release)
        if not index.dags or (dag_ids is not None and set(dag_ids) != set(index.dags)):
            raise ValueError("native compact delivery requires the complete workspace DAG inventory")
        files = dict(captured)
        derived = deepcopy(release)
        artifacts = derived["artifacts"]
        for descriptor in artifacts["dag_specs"]:
            path = descriptor["path"]
            dag = strict_json_object(files[path])
            workloads = tuple(node["workload_id"] for node in dag["nodes"] if "workload_id" in node)
            rewritten = self._rewriter.dag(dag, workload_ids=workloads)
            files[path] = _json_bytes(rewritten)
            _refresh_descriptor(descriptor, files[path])
        for descriptor in artifacts["workload_packs"]:
            path = descriptor["path"]
            pack = strict_json_object(files[path])
            rewritten = self._rewriter.pack(pack, xcom_sidecar_image=xcom_sidecar_image)
            # The existing rewrite owns only transport fields. Freeze every other
            # field, including archive bytes, selectors and ordered payload refs.
            transport = {"airflow", "connection_projection", "xcom", "provider_execution", "pack_fingerprint"}
            if {k: v for k, v in pack.items() if k not in transport} != {
                k: v for k, v in rewritten.items() if k not in transport
            }:
                raise ValueError("compact rewrite changed workload source identity")
            files[path] = _json_bytes(rewritten)
            _refresh_descriptor(descriptor, files[path])
            descriptor["pack_fingerprint"] = rewritten["pack_fingerprint"]
        derived["promotion"] = dict(self._promotion)
        derived["release_id"] = release_id(derived)
        if validate_release_set(derived).failure is not None:
            raise ValueError("derived workspace release violates its public contract")
        files["release-set.json"] = _json_bytes(derived)
        files.pop("release-subjects.sha256")
        with TemporaryDirectory(prefix="dpone-compact-workspace-") as temporary:
            stage = Path(temporary)
            for path, body in files.items():
                destination = stage / path
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(body)
            self._integrity.write(stage)
            verified = self._capture.capture_verified_files(
                stage, release_payload=files["release-set.json"], expected_release_id=derived["release_id"]
            )
        return verified


def _refresh_descriptor(descriptor: dict[str, object], body: bytes) -> None:
    descriptor.update(sha256=sha256_bytes(body), bytes=len(body))


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def compact_report_output_is_safe(repo_root: Path, raw_output: object, *, pack_root: Path, cache_root: Path) -> bool:
    """A console mirror cannot overwrite a captured input or immutable cache tree."""
    from dpone.gitops.paths import confined_repo_file_path

    if not raw_output:
        return True
    try:
        _, destination = confined_repo_file_path(repo_root, str(raw_output), source="--output")
        if destination.exists() and destination.stat().st_nlink > 1:
            return False
        return all(not destination.is_relative_to(root.resolve()) for root in (pack_root, cache_root))
    except (ValueError, OSError, RuntimeError):
        return False
