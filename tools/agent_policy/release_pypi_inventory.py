"""Read-only PyPI inventory classification (no upload, no Trusted Publisher)."""

from __future__ import annotations

import importlib.util
import json
import sys
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

HttpGet = Callable[[str], tuple[int, dict[str, Any] | None, str]]


def _load_sibling(module_name: str, filename: str) -> Any:
    if module_name in sys.modules:
        return sys.modules[module_name]
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


canonical = _load_sibling("dpone_agent_release_canonical", "release_canonical.py")
stream = _load_sibling("dpone_agent_release_stream_service", "release_stream_service.py")

StreamPrerequisiteError = stream.StreamPrerequisiteError

DEFAULT_PROJECTS = (
    "dpone",
    "dpone-native-accel",
    "dpone-airflow-pack",
    "apache-airflow-providers-dpone",
)


class InventoryError(RuntimeError):
    """PyPI inventory prerequisites failed."""


def fetch_project_release_files(
    project: str,
    *,
    http_get: HttpGet | None = None,
    index_url: str = "https://pypi.org/",
) -> dict[str, Any]:
    """Return normalized files from the PyPI JSON project endpoint."""

    base = index_url.rstrip("/") + "/"
    url = f"{base}pypi/{project}/json"
    getter = http_get or _default_http_get
    status, payload, text = getter(url)
    if status == 404:
        return {"project": project, "reachable": True, "exists": False, "files": [], "latest_version": None}
    if status >= 400 or payload is None:
        raise InventoryError(f"PYPI_HTTP_{status}:{project}:{text[:200]}")
    releases = payload.get("releases") or {}
    latest = str((payload.get("info") or {}).get("version") or "") or None
    files: list[dict[str, Any]] = []
    for version, rows in releases.items():
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            digests = row.get("digests") or {}
            files.append(
                {
                    "version": str(version),
                    "filename": str(row.get("filename") or ""),
                    "size": int(row.get("size") or 0),
                    "sha256": str(digests.get("sha256") or ""),
                    "yanked": bool(row.get("yanked")),
                }
            )
    return {
        "project": project,
        "reachable": True,
        "exists": True,
        "files": files,
        "latest_version": latest,
    }


def classify_expected_file(
    expected: Mapping[str, Any],
    project_files: list[dict[str, Any]],
) -> dict[str, Any]:
    """Classify one expected distribution against observed PyPI files."""

    filename = str(expected["filename"])
    project = str(expected["project"])
    sha256 = str(expected["sha256"])
    size = int(expected["size"])
    if not filename.endswith((".whl", ".tar.gz")):
        return {
            "project": project,
            "filename": filename,
            "classification": "NOT_PYPI_ARTIFACT",
        }
    matches = [row for row in project_files if row.get("filename") == filename]
    if not matches:
        return {
            "project": project,
            "filename": filename,
            "classification": "PENDING_UPLOAD",
            "sha256": sha256,
            "size": size,
        }
    exact = [
        row
        for row in matches
        if str(row.get("sha256")) == sha256 and int(row.get("size") or 0) == size and not bool(row.get("yanked"))
    ]
    if exact:
        return {
            "project": project,
            "filename": filename,
            "classification": "ALREADY_PUBLISHED_EXACT",
            "sha256": sha256,
            "size": size,
            "version": exact[0].get("version"),
            "integrity": "UNVERIFIED",
            "note": "Integrity API provenance not verified in bootstrap observe path",
        }
    return {
        "project": project,
        "filename": filename,
        "classification": "CONFLICT",
        "sha256": sha256,
        "size": size,
        "observed": [
            {"sha256": row.get("sha256"), "size": row.get("size"), "yanked": row.get("yanked")} for row in matches
        ],
    }


def run_pypi_inventory_observe(
    store: Any,
    *,
    release_identity_id: str,
    release_authority_id: str,
    repository_id: int,
    tag_ref: str,
    producer: Mapping[str, Any],
    now_utc: str,
    expected_distributions: list[dict[str, Any]] | None = None,
    projects: tuple[str, ...] = DEFAULT_PROJECTS,
    retention_days: int = 365,
    http_get: HttpGet | None = None,
    index_url: str = "https://pypi.org/",
) -> dict[str, Any]:
    """Observe PyPI under an active lease / AUTHORIZED attempt; never upload."""

    receipts = store.list_receipts(release_identity_id)
    authorized = _latest_kind(receipts, "AUTHORIZED")
    if authorized is None:
        raise InventoryError("AUTHORIZED_REQUIRED")
    authorization_id = str(authorized["payload"]["authorization_id"])
    expected = list(expected_distributions or [])
    project_rows: list[dict[str, Any]] = []
    files_by_project: dict[str, list[dict[str, Any]]] = {}
    for project in projects:
        row = fetch_project_release_files(project, http_get=http_get, index_url=index_url)
        project_rows.append(
            {
                "project": row["project"],
                "reachable": row["reachable"],
                "exists": row["exists"],
                "latest_version": row["latest_version"],
                "file_count": len(row["files"]),
            }
        )
        files_by_project[project] = list(row["files"])
    classifications = [
        classify_expected_file(item, files_by_project.get(str(item["project"]), [])) for item in expected
    ]
    conflicts = [row for row in classifications if row.get("classification") == "CONFLICT"]
    upload_subset = [row for row in classifications if row.get("classification") == "PENDING_UPLOAD"]
    inventory_id = canonical.sha256_id(
        "dpone.release.pypi-inventory.v2",
        {
            "authorization_id": authorization_id,
            "projects": project_rows,
            "classifications": classifications,
        },
    )
    receipt = stream.append_stream_receipt(
        store,
        release_identity_id=release_identity_id,
        release_authority_id=release_authority_id,
        repository_id=repository_id,
        tag_ref=tag_ref,
        producer=producer,
        receipt_type="pypi_inventory",
        payload={
            "kind": "PYPI_INVENTORY",
            "mode": "OBSERVE",
            "authorization_id": authorization_id,
            "inventory_id": inventory_id,
            "projects": project_rows,
            "classifications": classifications,
            "conflicts": conflicts,
            "upload_subset": upload_subset,
            "upload_attempted": False,
        },
        now_utc=now_utc,
        retention_days=retention_days,
        scope={"kind": "release", "release_identity_id": release_identity_id},
    )
    return {
        "status": "PYPI_INVENTORY",
        "mode": "OBSERVE",
        "inventory_id": inventory_id,
        "conflicts": conflicts,
        "upload_subset": upload_subset,
        "receipt": receipt,
    }


def _latest_kind(receipts: list[dict[str, Any]], kind: str) -> dict[str, Any] | None:
    for receipt in reversed(receipts):
        if str((receipt.get("payload") or {}).get("kind")) == kind:
            return receipt
    return None


def _default_http_get(url: str) -> tuple[int, dict[str, Any] | None, str]:
    request = urllib.request.Request(url, headers={"User-Agent": "dpone-release-controller"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310
            raw = response.read()
            text = raw.decode("utf-8") if raw else ""
            loaded = json.loads(text) if text else None
            payload = loaded if isinstance(loaded, dict) else None
            return int(response.status), payload, text
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        text = raw.decode("utf-8") if raw else str(exc)
        return int(exc.code), None, text
