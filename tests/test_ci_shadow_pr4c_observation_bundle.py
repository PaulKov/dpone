from __future__ import annotations

import pytest

from dpone.services.ci.shadow_observation_bundle import (
    ObservationBundleEntry,
    ObservationBundleError,
    entries_from_manifest,
    observation_bundle_digest,
)


def _entry(path: str, value: str) -> ObservationBundleEntry:
    return ObservationBundleEntry(path, "100644", "sha256:" + value * 64)


def test_observation_bundle_digest_is_deterministic_and_domain_separated() -> None:
    entries = (_entry("pyproject.toml", "a"), _entry("src/dpone/services/ci/shadow_capacity.py", "b"))

    digest = observation_bundle_digest(entries, manifest_sha256="sha256:" + "c" * 64)

    assert digest.startswith("sha256:")
    assert digest != "sha256:" + "a" * 64
    assert digest != observation_bundle_digest(entries, manifest_sha256="sha256:" + "d" * 64)


@pytest.mark.parametrize(
    "entries",
    [
        (_entry("src/b.py", "a"), _entry("src/a.py", "b")),
        (_entry("src/a.py", "a"), _entry("src/a.py", "b")),
        (_entry("../unsafe.py", "a"),),
        (ObservationBundleEntry("src/a.py", "120000", "sha256:" + "a" * 64),),
    ],
)
def test_observation_bundle_digest_rejects_noncanonical_or_unsafe_entries(
    entries: tuple[ObservationBundleEntry, ...],
) -> None:
    with pytest.raises(ObservationBundleError):
        observation_bundle_digest(entries, manifest_sha256="sha256:" + "c" * 64)


def test_manifest_parser_rejects_unknown_fields_and_validates_before_returning_entries() -> None:
    manifest = {
        "schema": "dpone.ci-shadow-reconciliation-observation-bundle.v1",
        "domain": "dpone.ci-shadow-reconciliation-observation-bundle.v1",
        "entries": [
            {"path": "src/a.py", "mode": "100644", "blob_sha256": "sha256:" + "a" * 64},
        ],
    }

    assert entries_from_manifest(manifest) == (_entry("src/a.py", "a"),)
    manifest["unexpected"] = True
    with pytest.raises(ObservationBundleError):
        entries_from_manifest(manifest)
