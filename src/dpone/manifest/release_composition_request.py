"""Bounded closed local composition manifest acquisition."""

from pathlib import Path

from dpone.contracts.release_composition import COMPOSITION_PRODUCER, ReleaseCompositionRequest
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.manifest.bounded_yaml import load_bounded_yaml
from dpone.manifest.confined_files import read_confined_file


def read_release_composition_request(manifest: Path, *, output_dir: Path) -> ReleaseCompositionRequest:
    """Resolve local roots from one strict manifest; never discover remote sources."""
    manifest = manifest.absolute()
    value = load_bounded_yaml(read_confined_file(manifest.parent, manifest.name, max_bytes=1024 * 1024))
    if GitOpsSchemaValidator().validate(value, expected_kind=COMPOSITION_PRODUCER):
        raise ValueError("composition manifest violates its closed public schema")
    assert isinstance(value, dict)
    native, ordinary, transport = value["native_workspace"], value["standalone"], value["transport"]
    for raw in (native["root"], ordinary["root"]):
        if "\x00" in raw or "://" in raw:
            raise ValueError("composition input roots must be local filesystem paths")
    return ReleaseCompositionRequest(
        native_root=manifest.parent / native["root"],
        expected_release_id=native["expected_release_id"],
        standalone_root=manifest.parent / ordinary["root"],
        expected_inventory_sha256=ordinary["expected_inventory_sha256"],
        output_dir=output_dir.absolute(),
        xcom_sidecar_image=transport["xcom_sidecar_image"],
        profile=transport["profile"],
    )
