from __future__ import annotations

import os
from pathlib import Path

import pytest

from dpone.readiness.route_attestation_files import (
    RouteAttestationFileError,
    read_bounded_file,
    read_strict_json_mapping,
    write_create_only,
)


def test_bounded_reader_rejects_final_symlink_and_oversized_input(tmp_path: Path) -> None:
    outside = tmp_path / "outside.json"
    outside.write_bytes(b"{}")
    alias = tmp_path / "alias.json"
    alias.symlink_to(outside)

    with pytest.raises(RouteAttestationFileError) as symlink_error:
        read_bounded_file(alias, max_bytes=1024, label="attestation")
    assert symlink_error.value.code == "DPONE_ROUTE_ATTESTATION_INPUT_UNAVAILABLE"

    oversized = tmp_path / "oversized.json"
    oversized.write_bytes(b"x" * 1025)
    with pytest.raises(RouteAttestationFileError) as size_error:
        read_bounded_file(oversized, max_bytes=1024, label="attestation")
    assert size_error.value.code == "DPONE_ROUTE_ATTESTATION_INPUT_INVALID"


def test_bounded_reader_rejects_symlinked_parent_directory(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "attestation.json").write_bytes(b"{}")
    alias = tmp_path / "alias"
    alias.symlink_to(outside, target_is_directory=True)

    with pytest.raises(RouteAttestationFileError) as exc:
        read_bounded_file(alias / "attestation.json", max_bytes=1024, label="attestation")

    assert exc.value.code == "DPONE_ROUTE_ATTESTATION_INPUT_UNAVAILABLE"


def test_bounded_reader_rejects_parent_swap_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trusted = tmp_path / "trusted"
    trusted.mkdir()
    (trusted / "attestation.json").write_bytes(b"trusted")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "attestation.json").write_bytes(b"attacker")
    moved = tmp_path / "trusted-open"
    real_open = os.open
    swapped = False

    def swapping_open(path: object, flags: int, mode: int = 0o777, *, dir_fd: int | None = None) -> int:
        nonlocal swapped
        if path == "attestation.json" and dir_fd is not None and not swapped:
            trusted.rename(moved)
            trusted.symlink_to(outside, target_is_directory=True)
            swapped = True
        return real_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr("dpone.readiness.route_attestation_files.os.open", swapping_open)

    with pytest.raises(RouteAttestationFileError) as exc:
        read_bounded_file(trusted / "attestation.json", max_bytes=1024, label="attestation")

    assert exc.value.code == "DPONE_ROUTE_ATTESTATION_INPUT_UNAVAILABLE"


@pytest.mark.parametrize(
    "raw",
    [
        b'{"schema":"one","schema":"two"}',
        b'{"value":NaN}',
    ],
)
def test_strict_json_reader_rejects_duplicate_keys_and_non_finite_values(tmp_path: Path, raw: bytes) -> None:
    path = tmp_path / "input.json"
    path.write_bytes(raw)

    with pytest.raises(RouteAttestationFileError) as exc:
        read_strict_json_mapping(path, max_bytes=1024, label="attestation")

    assert exc.value.code == "DPONE_ROUTE_ATTESTATION_INPUT_INVALID"


def test_create_only_writer_never_replaces_existing_evidence(tmp_path: Path) -> None:
    output = tmp_path / "receipt.json"
    write_create_only(output, b"first")

    with pytest.raises(FileExistsError):
        write_create_only(output, b"second")

    assert output.read_bytes() == b"first"
    assert os.stat(output).st_mode & 0o777 == 0o644


def test_create_only_writer_never_follows_existing_final_symlink(tmp_path: Path) -> None:
    outside = tmp_path / "outside.json"
    outside.write_bytes(b"outside")
    output = tmp_path / "receipt.json"
    output.symlink_to(outside)

    with pytest.raises(FileExistsError):
        write_create_only(output, b"replacement")

    assert outside.read_bytes() == b"outside"
    assert output.is_symlink()


def test_create_only_writer_rejects_symlinked_parent_directory(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(outside, target_is_directory=True)

    with pytest.raises(RouteAttestationFileError) as exc:
        write_create_only(alias / "receipt.json", b"trusted")

    assert exc.value.code == "DPONE_ROUTE_ATTESTATION_OUTPUT_UNSAFE"
    assert not (outside / "receipt.json").exists()


def test_create_only_writer_rejects_parent_traversal(tmp_path: Path) -> None:
    output = tmp_path / "evidence" / ".." / "receipt.json"

    with pytest.raises(RouteAttestationFileError) as exc:
        write_create_only(output, b"trusted")

    assert exc.value.code == "DPONE_ROUTE_ATTESTATION_OUTPUT_UNSAFE"
    assert not (tmp_path / "receipt.json").exists()


def test_create_only_writer_rejects_parent_swap_before_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = tmp_path / "evidence"
    evidence.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    moved = tmp_path / "evidence-open"
    real_link = os.link
    swapped = False

    def swapping_link(
        source: object,
        destination: object,
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> None:
        nonlocal swapped
        if not swapped:
            evidence.rename(moved)
            evidence.symlink_to(outside, target_is_directory=True)
            swapped = True
        real_link(
            source,
            destination,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
            follow_symlinks=follow_symlinks,
        )

    monkeypatch.setattr("dpone.readiness.route_attestation_files.os.link", swapping_link)

    with pytest.raises(RouteAttestationFileError) as exc:
        write_create_only(evidence / "receipt.json", b"trusted")

    assert exc.value.code == "DPONE_ROUTE_ATTESTATION_OUTPUT_UNSAFE"
    assert not (outside / "receipt.json").exists()
    assert not (moved / "receipt.json").exists()
