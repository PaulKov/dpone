"""Deterministic fixtures shared by PyPI prepublication boundary tests."""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import sys
import tarfile
import zipfile
from pathlib import Path
from types import ModuleType
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
VERSION = "0.74.0"
NAMES = (
    "apache_airflow_providers_dpone-0.74.0-py3-none-any.whl",
    "apache_airflow_providers_dpone-0.74.0.tar.gz",
    "dpone-0.74.0-py3-none-any.whl",
    "dpone-0.74.0.tar.gz",
    "dpone_airflow_pack-0.74.0-py3-none-any.whl",
    "dpone_airflow_pack-0.74.0.tar.gz",
    "dpone_native_accel-0.74.0-py3-none-any.whl",
    "dpone_native_accel-0.74.0.tar.gz",
)


def load_gate() -> ModuleType:
    """Load the CLI through its package-aware import path."""

    path = ROOT / "tools" / "pypi_prepublication_gate.py"
    spec = importlib.util.spec_from_file_location("tools.pypi_prepublication_gate", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def publication_context(module: ModuleType):
    """Return one valid immutable release-run context."""

    return module.PublicationContext(
        repository="PaulKov/dpone",
        commit_sha="a" * 40,
        release=f"v{VERSION}",
        workflow_path=".github/workflows/release.yml",
        workflow_run_id=123,
        workflow_run_attempt=1,
    )


def github_environment() -> dict[str, str]:
    """Return the exact GitHub defaults corresponding to the fixture context."""

    return {
        "GITHUB_EVENT_NAME": "push",
        "GITHUB_REF": f"refs/tags/v{VERSION}",
        "GITHUB_REF_NAME": f"v{VERSION}",
        "GITHUB_REF_TYPE": "tag",
        "GITHUB_REPOSITORY": "PaulKov/dpone",
        "GITHUB_RUN_ATTEMPT": "1",
        "GITHUB_RUN_ID": "123",
        "GITHUB_SHA": "a" * 40,
        "GITHUB_WORKFLOW_REF": f"PaulKov/dpone/.github/workflows/release.yml@refs/tags/v{VERSION}",
    }


def write_inventory(root: Path) -> tuple[Path, Path, list[dict[str, Any]]]:
    """Write one valid closed inventory and its exact local byte set."""

    dist = root / "dist"
    dist.mkdir(parents=True)
    artifacts: list[dict[str, Any]] = []
    for name in NAMES:
        package = package_for(name)
        content = candidate_bytes(name, metadata_name=package, metadata_version=VERSION)
        (dist / name).write_bytes(content)
        artifacts.append(
            {
                "artifact_type": "wheel" if name.endswith(".whl") else "sdist",
                "filename": name,
                "package": package,
                "sha256": hashlib.sha256(content).hexdigest(),
                "size_bytes": len(content),
                "version": VERSION,
            }
        )
    payload = {
        "artifacts": artifacts,
        "blockers": [],
        "decision": "GO",
        "expected_version": VERSION,
        "schema_version": 1,
        "status": "passed",
        "summary": {
            "artifact_count": 8,
            "distribution_count": 4,
            "expected_artifact_count": 8,
            "expected_distribution_count": 4,
        },
    }
    inventory = root / "candidate-inventory.json"
    inventory.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return inventory, dist, artifacts


def candidate_bytes(
    filename: str,
    *,
    metadata_name: str,
    metadata_version: str,
    metadata: bytes | None = None,
) -> bytes:
    """Build one deterministic candidate with explicit internal Core Metadata."""

    core = metadata or (f"Metadata-Version: 2.4\nName: {metadata_name}\nVersion: {metadata_version}\n\n".encode())
    buffer = io.BytesIO()
    if filename.endswith(".whl"):
        distribution = filename.split("-", 1)[0]
        parent = f"{distribution}-{VERSION}.dist-info"
        with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(f"{parent}/METADATA", core)
            archive.writestr("candidate/__init__.py", b"")
    else:
        parent = filename[:-7]
        with tarfile.open(fileobj=buffer, mode="w:gz", format=tarfile.PAX_FORMAT) as archive:
            directory = tarfile.TarInfo(parent)
            directory.type = tarfile.DIRTYPE
            directory.mtime = 0
            archive.addfile(directory)
            info = tarfile.TarInfo(f"{parent}/PKG-INFO")
            info.size = len(core)
            info.mtime = 0
            archive.addfile(info, io.BytesIO(core))
    return buffer.getvalue()


def replace_candidate(
    inventory: Path,
    dist: Path,
    filename: str,
    content: bytes,
) -> None:
    """Replace candidate bytes and keep the otherwise-valid inventory exact."""

    (dist / filename).write_bytes(content)
    payload = json.loads(inventory.read_text(encoding="utf-8"))
    artifact = next(item for item in payload["artifacts"] if item["filename"] == filename)
    artifact["sha256"] = hashlib.sha256(content).hexdigest()
    artifact["size_bytes"] = len(content)
    inventory.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def package_for(filename: str) -> str:
    """Map fixture artifact names to canonical PyPI projects."""

    if filename.startswith("apache_airflow"):
        return "apache-airflow-providers-dpone"
    if filename.startswith("dpone_airflow"):
        return "dpone-airflow-pack"
    if filename.startswith("dpone_native"):
        return "dpone-native-accel"
    return "dpone"


def public_row(artifact: dict[str, Any]) -> dict[str, Any]:
    """Project a candidate into the relevant PyPI JSON file fields."""

    return {
        "digests": {"sha256": artifact["sha256"]},
        "filename": artifact["filename"],
        "size": artifact["size_bytes"],
        "yanked": False,
    }


def public_fetcher(
    artifacts: list[dict[str, Any]],
    visible_names: set[str],
    *,
    mutations: dict[str, dict[str, Any]] | None = None,
):
    """Return a deterministic exact-version PyPI adapter stub."""

    by_package: dict[str, list[dict[str, Any]]] = {}
    for artifact in artifacts:
        if artifact["filename"] in visible_names:
            row = public_row(artifact)
            row.update((mutations or {}).get(artifact["filename"], {}))
            by_package.setdefault(artifact["package"], []).append(row)

    def fetch(package: str, version: str):
        rows = by_package.get(package)
        if rows is None:
            return None
        return {"info": {"name": package, "version": version}, "urls": rows}

    return fetch
