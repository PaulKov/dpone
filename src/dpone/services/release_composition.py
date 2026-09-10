"""Coordinate explicit source admission and one immutable release publication."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Literal

from dpone.contracts.dbt_relation_writes import require_distinct_logical_writes
from dpone.contracts.release_composition import (
    COMPOSITION_PROFILE,
    NATIVE_SIDECARS,
    ReleaseCompositionReport,
    ReleaseCompositionRequest,
)
from dpone.contracts.release_composition_policy import assemble_composition_files, composition_native_release
from dpone.contracts.strict_json import strict_json_object
from dpone.gitops.release_set_validation import validate_release_set
from dpone.manifest.release_composition_files import (
    require_composition_root,
    verify_composition_transport_files,
    write_private_files,
)
from dpone.ports.dbt_release_files import ConfinedReleaseFileReader, VerifiedWorkspaceReleaseCapture
from dpone.ports.release_composition import CompositionIntegrity, CompositionNativeSourceReader, CompositionPublisher
from dpone.ports.release_composition_ordinary import OrdinaryReleaseInventoryReaderPort


class ReleaseCompositionService:
    """Use mandatory verifiers and an injected writer; never activate or execute."""

    def __init__(
        self,
        *,
        native: CompositionNativeSourceReader,
        ordinary: OrdinaryReleaseInventoryReaderPort,
        integrity: CompositionIntegrity,
        publisher: CompositionPublisher,
        capture: VerifiedWorkspaceReleaseCapture,
        read_file: ConfinedReleaseFileReader,
        producer_version: str,
        durability_error: type[Exception],
    ) -> None:
        self._native, self._ordinary = native, ordinary
        self._integrity, self._publish, self._read = integrity, publisher, read_file
        self._version, self._durability_error = producer_version, durability_error
        self._capture = capture

    def inventory(self, root: Path, *, xcom_sidecar_image: str) -> Mapping[str, Any]:
        """Produce a verified source-only digest without publication or source writes."""
        require_composition_root(root)
        result = self._ordinary.capture(root, xcom_sidecar_image=xcom_sidecar_image)
        return {
            "schema": "dpone.workload-inventory-report.v1",
            "passed": True,
            "inventory_sha256": result.inventory_sha256,
            "inventory": result.inventory_dict(),
        }

    def compose(self, request: ReleaseCompositionRequest) -> ReleaseCompositionReport:
        """Return a stable sanitized outcome, retaining visible publication identity."""
        identity = ""
        try:
            files = self._build(request)
            identity = strict_json_object(files["release-set.json"])["release_id"]
            self._publish(request.output_dir, files)
        except self._durability_error:
            return self._report(
                request,
                "durability_uncertain",
                identity,
                (
                    "DPONE_COMPOSITION_DURABILITY_UNCERTAIN: release is visible; recover storage and retry identical inputs",
                ),
            )
        except (ValueError, OSError, RuntimeError, TypeError, KeyError) as exc:
            # Detailed source payloads and credentials must never escape in reports.
            code = getattr(exc, "code", "DPONE_COMPOSITION_REJECTED")
            return self._report(
                request,
                "rejected",
                identity,
                (f"{code}: source, transport or immutable output validation failed; verify complete inputs and retry",),
            )
        return self._report(request, "passed", identity, ())

    def install(self, root: Path, *, cache_root: Path) -> ReleaseCompositionReport:
        """Independently readmit an existing composition before installing its exact ID."""
        root, cache_root = root.absolute(), cache_root.absolute()
        require_composition_root(root)
        require_composition_root(cache_root)
        if cache_root.resolve().is_relative_to(root.resolve()) or root.resolve().is_relative_to(cache_root.resolve()):
            raise ValueError("composition source and destination cache must be disjoint")
        payload = self._read(root, "release-set.json", max_bytes=8 * 1024 * 1024)
        release = strict_json_object(payload)
        files = self._capture.capture_verified_files(
            root, release_payload=payload, expected_release_id=release["release_id"]
        )
        destination = cache_root / "releases" / release["release_id"].replace(":", "-", 1)
        native = next(row for row in release["constituents"] if row["id"] == "native")
        ordinary = next(row for row in release["constituents"] if row["id"] == "standalone")
        request = ReleaseCompositionRequest(
            native_root=root,
            expected_release_id=native["release"]["release_id"],
            standalone_root=root,
            expected_inventory_sha256=ordinary["inventory_sha256"],
            output_dir=destination,
            xcom_sidecar_image="",
        )
        try:
            self._publish(destination, files)
        except self._durability_error:
            return self._report(
                request,
                "durability_uncertain",
                release["release_id"],
                (
                    "DPONE_COMPOSITION_DURABILITY_UNCERTAIN: release is visible; recover storage and retry identical inputs",
                ),
            )
        return self._report(request, "passed", release["release_id"], ())

    def _build(self, request: ReleaseCompositionRequest) -> Mapping[str, bytes]:
        native_root, ordinary_root, destination = (
            request.native_root.absolute(),
            request.standalone_root.absolute(),
            request.output_dir.absolute(),
        )
        for root in (native_root, ordinary_root, destination):
            require_composition_root(root)
        roots = [native_root.resolve(), ordinary_root.resolve(), destination.resolve()]
        if any(
            left.is_relative_to(right) or right.is_relative_to(left)
            for i, left in enumerate(roots)
            for right in roots[i + 1 :]
        ):
            raise ValueError("composition roots must be disjoint")
        payload = self._read(native_root, "release-set.json", max_bytes=8 * 1024 * 1024)
        native = strict_json_object(payload)
        promotion = {"schema": "dpone.compact-pack-release-promotion.v1", "profile": COMPOSITION_PROFILE}
        if request.profile != COMPOSITION_PROFILE or native.get("promotion") != promotion:
            raise ValueError("native input must already use the supported compact transport")
        if validate_release_set(native).failure is not None:
            raise ValueError("native input violates its public schema")
        captured = self._native.capture_verified_files(
            native_root, release_payload=payload, expected_release_id=request.expected_release_id
        )
        ordinary = self._ordinary.capture(ordinary_root, xcom_sidecar_image=request.xcom_sidecar_image)
        if ordinary.inventory_sha256 != request.expected_inventory_sha256:
            raise ValueError("ordinary source inventory differs from expected identity")
        files = assemble_composition_files(native, captured, ordinary, producer_version=self._version)
        release = strict_json_object(files["release-set.json"])
        if validate_release_set(release).failure is not None:
            raise ValueError("composed release violates its public schema")
        self._integrity.require_capture_budget(len(body) for body in files.values())
        with TemporaryDirectory(prefix="dpone-composition-stage-") as temporary:
            stage = Path(temporary)
            write_private_files(stage, files)
            self._integrity.write(stage)
            return self._capture.capture_verified_files(
                stage, release_payload=files["release-set.json"], expected_release_id=release["release_id"]
            )

    @staticmethod
    def _report(
        request: ReleaseCompositionRequest,
        status: Literal["passed", "rejected", "durability_uncertain"],
        identity: str,
        blockers: tuple[str, ...],
    ) -> ReleaseCompositionReport:
        return ReleaseCompositionReport(
            status=status,
            release_id=identity,
            output_dir=request.output_dir,
            source_release_id=request.expected_release_id,
            inventory_sha256=request.expected_inventory_sha256,
            blockers=blockers,
        )


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
