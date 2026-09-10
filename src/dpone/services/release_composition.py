"""Coordinate explicit source admission and one immutable release publication."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Literal

from dpone.contracts.airflow_deployment import release_id
from dpone.contracts.dbt_contract_validation import artifact_json_bytes, sha256_bytes
from dpone.contracts.release_composition import (
    COMPOSITION_PRODUCER,
    COMPOSITION_SCHEMA,
    ReleaseCompositionReport,
    ReleaseCompositionRequest,
)
from dpone.contracts.strict_json import strict_json_object
from dpone.gitops.release_set_validation import validate_release_set
from dpone.gitops.schema_release_set_promotion import COMPACT_PROMOTION_PROFILE, COMPACT_PROMOTION_SCHEMA
from dpone.manifest.release_composition_capture import VerifiedCompositionReleaseCapture
from dpone.manifest.release_composition_files import NATIVE_SIDECARS, require_composition_root, write_private_files
from dpone.ports.dbt_release_files import ConfinedReleaseFileReader
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
        read_file: ConfinedReleaseFileReader,
        producer_version: str,
        durability_error: type[Exception],
    ) -> None:
        self._native, self._ordinary = native, ordinary
        self._integrity, self._publish, self._read = integrity, publisher, read_file
        self._version, self._durability_error = producer_version, durability_error
        self._capture = VerifiedCompositionReleaseCapture(
            native=native, ordinary=ordinary, integrity=integrity, read_file=read_file
        )

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
        promotion = {"schema": COMPACT_PROMOTION_SCHEMA, "profile": COMPACT_PROMOTION_PROFILE}
        if request.profile != COMPACT_PROMOTION_PROFILE or native.get("promotion") != promotion:
            raise ValueError("native input must already use the supported compact transport")
        if validate_release_set(native).failure is not None:
            raise ValueError("native input violates its public schema")
        captured = self._native.capture_verified_files(
            native_root, release_payload=payload, expected_release_id=request.expected_release_id
        )
        ordinary = self._ordinary.capture(ordinary_root, xcom_sidecar_image=request.xcom_sidecar_image)
        if ordinary.inventory_sha256 != request.expected_inventory_sha256:
            raise ValueError("ordinary source inventory differs from expected identity")
        files = {path: body for path, body in captured.items() if path not in NATIVE_SIDECARS}
        files.update({target: captured[source] for source, target in NATIVE_SIDECARS.items()})
        for path, body in {**ordinary.dag_files, **ordinary.pack_files}.items():
            if path in files:
                raise ValueError("composition artifact path collision")
            files[path] = body
        files.update({f"_composition/standalone/{path}": body for path, body in ordinary.files.items()})
        artifacts = {section: [dict(row) for row in rows] for section, rows in native["artifacts"].items()}
        for section, values in (("dag_specs", ordinary.dag_files), ("workload_packs", ordinary.pack_files)):
            for path, body in sorted(values.items()):
                parsed = strict_json_object(body)
                key = parsed["dag_id"] if section == "dag_specs" else parsed["workload"]["workload_id"]
                row = _descriptor(key, path, body)
                if section == "workload_packs":
                    row["pack_fingerprint"] = parsed["pack_fingerprint"]
                artifacts[section].append(row)
        artifacts["composition_sources"] = [
            _descriptor(path, path, body) for path, body in sorted(files.items()) if path.startswith("_composition/")
        ]
        release: dict[str, Any] = {
            "schema": COMPOSITION_SCHEMA,
            "release_id": "",
            "producer": {"name": COMPOSITION_PRODUCER, "version": self._version},
            "promotion": promotion,
            "artifacts": artifacts,
            "constituents": [
                {"id": "native", "kind": "dbt_workspace", "release": native},
                {
                    "id": "standalone",
                    "kind": "workload_inventory",
                    "inventory": ordinary.inventory_dict(),
                    "inventory_sha256": ordinary.inventory_sha256,
                },
            ],
        }
        for rows in artifacts.values():
            rows.sort(key=lambda row: row["id"])
        release["release_id"] = release_id(release)
        if validate_release_set(release).failure is not None:
            raise ValueError("composed release violates its public schema")
        files["release-set.json"] = artifact_json_bytes(release)
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


def _descriptor(key: str, path: str, body: bytes) -> dict[str, Any]:
    return {"id": key, "path": path, "sha256": sha256_bytes(body), "bytes": len(body)}
