"""Explicit construction of source admission, transport and immutable publication."""

from dpone.app.dbt_promotion_composition import build_dbt_release_source_reader
from dpone.contracts.release_composition_ordinary import OrdinaryReleaseInventoryError
from dpone.gitops.airflow_compact_pack import AirflowCompactPackBuilder
from dpone.gitops.workload_dependencies import WorkloadDependencyResolver
from dpone.manifest.confined_files import read_confined_file
from dpone.manifest.loader import SingleYamlManifestLoader
from dpone.manifest.release_composition_ordinary import OrdinaryReleaseInventoryReader
from dpone.manifest.release_composition_ordinary_closure import OrdinaryPackClosureVerifier
from dpone.runtime.immutable_local_tree import (
    ImmutableLocalTreeDurabilityError,
    materialize_immutable_local_tree,
)
from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_payload_archive import (
    extract_runtime_payload,
    runtime_payload_archive,
    verify_runtime_payload_tree,
)
from dpone.services.dbt_release_integrity import DbtReleaseIntegrityService
from dpone.services.release_composition import ReleaseCompositionService, VerifiedCompositionReleaseCapture
from dpone.version import installed_version


def build_release_composition_service() -> ReleaseCompositionService:
    """Construct the same mandatory verifiers for public CLI and Python callers."""
    native = build_dbt_release_source_reader()
    ordinary = build_ordinary_release_inventory_reader()
    integrity = DbtReleaseIntegrityService()
    return ReleaseCompositionService(
        native=native,
        ordinary=ordinary,
        integrity=integrity,
        capture=VerifiedCompositionReleaseCapture(
            native=native, ordinary=ordinary, integrity=integrity, read_file=read_confined_file
        ),
        publisher=_publish_composition,
        read_file=read_confined_file,
        producer_version=installed_version(),
        durability_error=ImmutableLocalTreeDurabilityError,
    )


def _publish_composition(destination, files):
    return materialize_immutable_local_tree(
        destination, files, allowed_parent=destination.parent, root=destination.parent
    )


def build_ordinary_release_inventory_reader(*, read_file=read_confined_file) -> OrdinaryReleaseInventoryReader:
    """Wire the mandatory plain-transfer closure policy explicitly."""
    return OrdinaryReleaseInventoryReader(
        read_file=read_file,
        closure=OrdinaryPackClosureVerifier(
            dependencies=WorkloadDependencyResolver(),
            builder=AirflowCompactPackBuilder(),
            manifest_loader=SingleYamlManifestLoader(),
            unpack_verified=_unpack_verified,
        ),
    )


def _unpack_verified(pack, root):
    """Adapt runtime archive extraction errors to the build-plane source contract."""
    try:
        archive = runtime_payload_archive(pack)
        extract_runtime_payload(archive, root)
        verify_runtime_payload_tree(archive, root)
    except InitFetchError as exc:
        raise OrdinaryReleaseInventoryError("ordinary archive is invalid; regenerate the source pack") from exc
