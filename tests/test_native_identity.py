"""Native references validate identity shape without conferring authority."""

from dataclasses import FrozenInstanceError

import pytest

from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.native_identity import OriginalRef

DIGEST = "sha256:" + "a" * 64


def test_reference_preserves_exact_unicode_and_is_immutable():
    reference = OriginalRef("generation/данные.json", DIGEST)
    assert reference.locator == "generation/данные.json"
    assert reference.sha256 == DIGEST
    with pytest.raises(FrozenInstanceError):
        reference.locator = "replacement"  # type: ignore[misc]


@pytest.mark.parametrize(
    "locator",
    [
        "",
        ".",
        "..",
        "/root",
        "a/../b",
        "a//b",
        "a/",
        "a\\b",
        "a\x00b",
        "a\nb",
        "a\x7fb",
        "a\x85b",
        "\ud800",
        "é" * 2049,
        None,
        4,
    ],
)
def test_reference_rejects_unsafe_or_oversize_locator(locator):
    with pytest.raises(DbtPublishingError):
        OriginalRef(locator, DIGEST)


@pytest.mark.parametrize("digest", ["a" * 64, "sha256:" + "A" * 64, "sha256:" + "a" * 63, None, False])
def test_reference_requires_canonical_digest(digest):
    with pytest.raises(DbtPublishingError):
        OriginalRef("generation/receipt", digest)


def test_reference_locator_bound_counts_utf8_bytes():
    assert len(OriginalRef("é" * 2048, DIGEST).locator.encode()) == 4096
