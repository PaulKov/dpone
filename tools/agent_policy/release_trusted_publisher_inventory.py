"""Observe Trusted Publisher / Integrity publisher claims (never rebind)."""

from __future__ import annotations

import importlib.util
import json
import sys
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any
from urllib.parse import quote

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
pypi = _load_sibling("dpone_agent_release_pypi_inventory", "release_pypi_inventory.py")

StreamPrerequisiteError = stream.StreamPrerequisiteError

DEFAULT_PROJECTS = pypi.DEFAULT_PROJECTS

EXPECTED_CONTROLLER_PUBLISHER = {
    "kind": "GitHub",
    "repository_owner": "PaulKov",
    "repository": "dpone-release-controller",
    "workflow": "release-controller.yml",
    "environment": "pypi",
}

LEGACY_CANDIDATE_PUBLISHER = {
    "kind": "GitHub",
    "repository_owner": "PaulKov",
    "repository": "dpone",
    "workflow": "release.yml",
    "environment": "",
}


class InventoryError(RuntimeError):
    """Trusted Publisher inventory prerequisites failed."""


def fetch_file_provenance(
    project: str,
    version: str,
    filename: str,
    *,
    http_get: HttpGet | None = None,
    index_url: str = "https://pypi.org/",
) -> dict[str, Any]:
    """Return Integrity API provenance observation for one distribution file."""

    base = index_url.rstrip("/") + "/"
    encoded = quote(filename, safe="")
    url = f"{base}integrity/{project}/{version}/{encoded}/provenance"
    getter = http_get or _default_integrity_http_get
    status, payload, text = getter(url)
    if status == 404:
        return {
            "project": project,
            "version": version,
            "filename": filename,
            "provenance": "MISSING",
            "publishers": [],
        }
    if status >= 400 or payload is None:
        return {
            "project": project,
            "version": version,
            "filename": filename,
            "provenance": "UNVERIFIED",
            "reason": f"HTTP_{status}",
            "detail": text[:200],
            "publishers": [],
        }
    publishers = []
    for bundle in payload.get("attestation_bundles") or []:
        if not isinstance(bundle, dict):
            continue
        publisher = bundle.get("publisher")
        if isinstance(publisher, dict):
            publishers.append(
                {
                    "kind": str(publisher.get("kind") or ""),
                    "repository": str(publisher.get("repository") or ""),
                    "workflow": str(publisher.get("workflow") or ""),
                    "environment": str(publisher.get("environment") or ""),
                }
            )
    return {
        "project": project,
        "version": version,
        "filename": filename,
        "provenance": "PRESENT",
        "publishers": publishers,
        "signature_verified": False,
        "note": "Bootstrap observe records publisher claims only; DSSE not verified here",
    }


def classify_publisher_claims(
    publishers: list[dict[str, Any]],
    *,
    expected: Mapping[str, Any] = EXPECTED_CONTROLLER_PUBLISHER,
    legacy: Mapping[str, Any] = LEGACY_CANDIDATE_PUBLISHER,
) -> str:
    """Classify observed Integrity publisher claims against expected bindings."""

    if not publishers:
        return "PROVENANCE_MISSING"
    for publisher in publishers:
        if _publisher_matches(publisher, expected):
            return "PUBLISHER_CONTROLLER"
    for publisher in publishers:
        if _publisher_matches(publisher, legacy):
            return "PUBLISHER_CANDIDATE_REPO"
    return "PUBLISHER_OTHER"


def observe_project_trusted_publisher(
    project: str,
    *,
    http_get_json: HttpGet | None = None,
    http_get_integrity: HttpGet | None = None,
    index_url: str = "https://pypi.org/",
    max_files: int = 4,
) -> dict[str, Any]:
    """Observe latest-version Integrity publisher claims for one project."""

    row = pypi.fetch_project_release_files(project, http_get=http_get_json, index_url=index_url)
    if not row["exists"] or not row["latest_version"]:
        return {
            "project": project,
            "exists": bool(row["exists"]),
            "latest_version": row.get("latest_version"),
            "config_binding": "UNVERIFIED",
            "config_reason": "NO_PUBLIC_TRUSTED_PUBLISHER_MANAGEMENT_API",
            "classification": "PROJECT_MISSING" if not row["exists"] else "VERSION_MISSING",
            "files": [],
            "rebind_attempted": False,
        }
    version = str(row["latest_version"])
    latest_files = [f for f in row["files"] if str(f.get("version")) == version and f.get("filename")]
    latest_files = sorted(latest_files, key=lambda item: str(item["filename"]))[:max_files]
    file_rows: list[dict[str, Any]] = []
    classifications: list[str] = []
    for item in latest_files:
        provenance = fetch_file_provenance(
            project,
            version,
            str(item["filename"]),
            http_get=http_get_integrity,
            index_url=index_url,
        )
        classification = classify_publisher_claims(list(provenance.get("publishers") or []))
        if provenance.get("provenance") == "UNVERIFIED":
            classification = "PROVENANCE_UNVERIFIED"
        classifications.append(classification)
        file_rows.append({**provenance, "classification": classification})
    project_classification = _rollup(classifications)
    return {
        "project": project,
        "exists": True,
        "latest_version": version,
        "config_binding": "UNVERIFIED",
        "config_reason": "NO_PUBLIC_TRUSTED_PUBLISHER_MANAGEMENT_API",
        "classification": project_classification,
        "files": file_rows,
        "rebind_attempted": False,
        "expected_controller": dict(EXPECTED_CONTROLLER_PUBLISHER),
        "legacy_candidate": dict(LEGACY_CANDIDATE_PUBLISHER),
    }


def run_trusted_publisher_inventory(
    store: Any,
    *,
    release_identity_id: str,
    release_authority_id: str,
    repository_id: int,
    tag_ref: str,
    producer: Mapping[str, Any],
    now_utc: str,
    projects: tuple[str, ...] = DEFAULT_PROJECTS,
    retention_days: int = 365,
    http_get_json: HttpGet | None = None,
    http_get_integrity: HttpGet | None = None,
    index_url: str = "https://pypi.org/",
) -> dict[str, Any]:
    """Append TRUSTED_PUBLISHER_INVENTORY under AUTHORIZED; never rebind."""

    receipts = store.list_receipts(release_identity_id)
    authorized = None
    for receipt in reversed(receipts):
        if str((receipt.get("payload") or {}).get("kind")) == "AUTHORIZED":
            authorized = receipt
            break
    if authorized is None:
        raise InventoryError("AUTHORIZED_REQUIRED")
    authorization_id = str(authorized["payload"]["authorization_id"])
    project_rows = [
        observe_project_trusted_publisher(
            project,
            http_get_json=http_get_json,
            http_get_integrity=http_get_integrity,
            index_url=index_url,
        )
        for project in projects
    ]
    inventory_id = canonical.sha256_id(
        "dpone.release.trusted-publisher-inventory.v2",
        {
            "authorization_id": authorization_id,
            "expected_controller": EXPECTED_CONTROLLER_PUBLISHER,
            "projects": project_rows,
        },
    )
    receipt = stream.append_stream_receipt(
        store,
        release_identity_id=release_identity_id,
        release_authority_id=release_authority_id,
        repository_id=repository_id,
        tag_ref=tag_ref,
        producer=producer,
        receipt_type="trusted_publisher_inventory",
        payload={
            "kind": "TRUSTED_PUBLISHER_INVENTORY",
            "mode": "OBSERVE",
            "authorization_id": authorization_id,
            "inventory_id": inventory_id,
            "expected_controller": dict(EXPECTED_CONTROLLER_PUBLISHER),
            "legacy_candidate": dict(LEGACY_CANDIDATE_PUBLISHER),
            "projects": project_rows,
            "rebind_attempted": False,
            "config_binding": "UNVERIFIED",
            "config_reason": "NO_PUBLIC_TRUSTED_PUBLISHER_MANAGEMENT_API",
        },
        now_utc=now_utc,
        retention_days=retention_days,
        scope={"kind": "release", "release_identity_id": release_identity_id},
    )
    return {
        "status": "TRUSTED_PUBLISHER_INVENTORY",
        "mode": "OBSERVE",
        "inventory_id": inventory_id,
        "projects": project_rows,
        "rebind_attempted": False,
        "receipt": receipt,
    }


def _publisher_matches(observed: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    repo = str(observed.get("repository") or "")
    owner = str(expected.get("repository_owner") or "")
    name = str(expected.get("repository") or "")
    expected_repo = f"{owner}/{name}" if owner and name else name
    if repo != expected_repo:
        return False
    if str(observed.get("workflow") or "") != str(expected.get("workflow") or ""):
        return False
    expected_env = str(expected.get("environment") or "")
    observed_env = str(observed.get("environment") or "")
    if expected_env and observed_env != expected_env:
        return False
    return True


def _rollup(classifications: list[str]) -> str:
    if not classifications:
        return "PROVENANCE_MISSING"
    order = (
        "PROVENANCE_UNVERIFIED",
        "PUBLISHER_OTHER",
        "PUBLISHER_CANDIDATE_REPO",
        "PUBLISHER_CONTROLLER",
        "PROVENANCE_MISSING",
    )
    for label in order:
        if label in classifications:
            return label
    return classifications[0]


def _default_integrity_http_get(url: str) -> tuple[int, dict[str, Any] | None, str]:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "dpone-release-controller",
            "Accept": "application/vnd.pypi.integrity.v1+json",
        },
    )
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
