from __future__ import annotations

import hashlib
import io
import stat
import zipfile

import pytest

from dpone.services.ci.shadow_capacity_archive import (
    CapacityArchiveError,
    extract_single_json_payload,
    verify_provider_archive,
)


def _archive(entries: dict[str, bytes], *, symlink: bool = False) -> bytes:
    destination = io.BytesIO()
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_STORED) as output:
        for name, content in entries.items():
            info = zipfile.ZipInfo(name)
            if symlink:
                info.external_attr = (stat.S_IFLNK | 0o777) << 16
            output.writestr(info, content)
    return destination.getvalue()


def _encrypted_archive() -> bytes:
    """Set ZIP's encrypted flag after creation; zipfile never writes encrypted data."""

    raw = bytearray(_archive({"capacity.json": b"{}"}))
    for signature, flag_offset in ((b"PK\x03\x04", 6), (b"PK\x01\x02", 8)):
        position = raw.index(signature)
        raw[position + flag_offset] |= 0x1
    return bytes(raw)


def test_provider_archive_and_exact_json_payload_have_separate_identities() -> None:
    payload = b'{"schema":"dpone.ci-shadow-reconciliation-capacity.v1"}'
    archive = _archive({"capacity.json": payload})

    archive_identity = verify_provider_archive(
        archive,
        provider_size=len(archive),
        provider_digest="sha256:" + hashlib.sha256(archive).hexdigest(),
        max_archive_bytes=len(archive),
    )
    extracted = extract_single_json_payload(archive, expected_name="capacity.json", max_payload_bytes=len(payload))

    assert archive_identity.size_in_bytes == len(archive)
    assert archive_identity.sha256 != "sha256:" + hashlib.sha256(payload).hexdigest()
    assert extracted.payload == payload
    assert extracted.sha256 == "sha256:" + hashlib.sha256(payload).hexdigest()


@pytest.mark.parametrize(
    ("entries", "expected_name", "kwargs"),
    [
        ({"capacity.json": b"{}", "extra.txt": b"x"}, "capacity.json", {}),
        ({"../capacity.json": b"{}"}, "capacity.json", {}),
        ({"capacity.json": b"{}"}, "other.json", {}),
        ({"capacity.json": b"{" * 9}, "capacity.json", {"max_payload_bytes": 8}),
    ],
)
def test_archive_extraction_rejects_ambiguous_or_unsafe_members(
    entries: dict[str, bytes], expected_name: str, kwargs: dict[str, int]
) -> None:
    with pytest.raises(CapacityArchiveError):
        extract_single_json_payload(
            _archive(entries), expected_name=expected_name, max_payload_bytes=kwargs.get("max_payload_bytes", 100)
        )


@pytest.mark.parametrize("archive", [_archive({"capacity.json": b"{}"}, symlink=True), _encrypted_archive()])
def test_archive_extraction_rejects_symlink_and_encryption(archive: bytes) -> None:
    with pytest.raises(CapacityArchiveError):
        extract_single_json_payload(
            archive,
            expected_name="capacity.json",
            max_payload_bytes=100,
        )


@pytest.mark.parametrize(
    ("provider_size", "provider_digest", "max_archive_bytes"),
    [
        (1, "sha256:" + "0" * 64, 100),
        (100, "sha256:" + "0" * 64, 2),
        (1, "invalid", 100),
    ],
)
def test_provider_archive_verification_fails_closed_on_transport_mismatch(
    provider_size: int, provider_digest: str, max_archive_bytes: int
) -> None:
    with pytest.raises(CapacityArchiveError):
        verify_provider_archive(
            b"x",
            provider_size=provider_size,
            provider_digest=provider_digest,
            max_archive_bytes=max_archive_bytes,
        )
