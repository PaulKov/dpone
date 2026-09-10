"""Independent source admission for an exact composed release tree."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from dpone.contracts.dbt_relation_writes import require_distinct_logical_writes
from dpone.contracts.release_composition_policy import composition_native_release
from dpone.contracts.strict_json import strict_json_object
from dpone.manifest.release_composition_files import (
    NATIVE_SIDECARS,
    verify_composition_transport_files,
    write_private_files,
)
from dpone.ports.dbt_release_files import ConfinedReleaseFileReader
from dpone.ports.release_composition import CompositionIntegrity, CompositionNativeSourceReader
from dpone.ports.release_composition_ordinary import OrdinaryReleaseInventoryReaderPort


class VerifiedCompositionReleaseCapture:
    """Recapture every source and verify deterministic transport before admission.

    This is a build-plane capability. Downstream cache checks establish transport
    integrity, not a replacement for this complete source admission algorithm.
    """

    def __init__(
        self,
        *,
        native: CompositionNativeSourceReader,
        ordinary: OrdinaryReleaseInventoryReaderPort,
        integrity: CompositionIntegrity,
        read_file: ConfinedReleaseFileReader,
    ) -> None:
        self._native = native
        self._ordinary = ordinary
        self._integrity = integrity
        self._read = read_file

    def capture_verified_files(
        self, root: Path, *, release_payload: bytes, expected_release_id: str
    ) -> Mapping[str, bytes]:
        release = strict_json_object(release_payload)
        if release.get("release_id") != expected_release_id or len(release_payload) > 8 * 1024 * 1024:
            raise ValueError("composition release identity or metadata budget differs")
        files = verify_composition_transport_files(root, release, read_file=self._read)
        if self._read(root, "release-set.json", max_bytes=len(release_payload)) != release_payload:
            raise ValueError("composition release descriptor changed during capture")
        files["release-set.json"] = release_payload
        files["release-subjects.sha256"] = self._read(root, "release-subjects.sha256", max_bytes=8 * 1024 * 1024)
        self._integrity.require_capture_budget(len(body) for body in files.values())
        with TemporaryDirectory(prefix="dpone-composition-recheck-") as temporary:
            frozen = Path(temporary) / "parent"
            frozen.mkdir()
            write_private_files(frozen, files)
            self._integrity.verify(frozen)
            self._verify_sources(Path(temporary), release, files)
        return files

    def _verify_sources(self, stage: Path, release: Mapping[str, Any], files: Mapping[str, bytes]) -> None:
        native = composition_native_release(release)
        native_files = {row["path"]: files[row["path"]] for rows in native["artifacts"].values() for row in rows}
        native_files.update({original: files[source] for original, source in NATIVE_SIDECARS.items()})
        native_root = stage / "native"
        native_root.mkdir()
        write_private_files(native_root, native_files)
        self._native.capture_verified_files(
            native_root, release_payload=native_files["release-set.json"], expected_release_id=native["release_id"]
        )
        source = self._native.read(native_root, expected_release_id=native["release_id"])
        ordinary_root = stage / "ordinary"
        ordinary_root.mkdir()
        prefix = "_composition/standalone/"
        write_private_files(
            ordinary_root, {path.removeprefix(prefix): body for path, body in files.items() if path.startswith(prefix)}
        )
        sidecars = {
            strict_json_object(files[row["path"]]).get("xcom", {}).get("sidecar_image")
            for row in release["artifacts"]["workload_packs"]
        }
        if len(sidecars) != 1 or not isinstance(next(iter(sidecars)), str):
            raise ValueError("composition transport sidecar must match every workload")
        ordinary = self._ordinary.capture(ordinary_root, xcom_sidecar_image=next(iter(sidecars)))
        constituent = next(item for item in release["constituents"] if item["id"] == "standalone")
        if (
            ordinary.inventory_dict() != constituent["inventory"]
            or ordinary.inventory_sha256 != constituent["inventory_sha256"]
        ):
            raise ValueError("composition ordinary source inventory differs")
        for path, body in {**ordinary.dag_files, **ordinary.pack_files}.items():
            if files.get(path) != body:
                raise ValueError("composition ordinary transport differs from verified source")
        require_distinct_logical_writes((*source.relation_writes, *ordinary.relation_writes))
