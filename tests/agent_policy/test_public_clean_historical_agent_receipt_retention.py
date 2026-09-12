from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import NoReturn

import pytest

ROOT = Path(__file__).resolve().parents[2]
RETENTION_RECORD = ROOT / "test_artifacts/delivery-acceleration/hygiene-retention/agent-pr-receipt-retention-v1.json"
RETENTION_INDEX = ROOT / "test_artifacts/delivery-acceleration/hygiene-retention/index.json"
RECEIPT_ROOT = "test_artifacts/delivery-acceleration/dda-06/remediation-ci-7c25ce9/agent-pr-receipt"
DOWNLOAD_PATH = "test_artifacts/delivery-acceleration/dda-06/remediation-ci-7c25ce9/download.json"
DOWNLOAD_MANIFEST = ROOT / DOWNLOAD_PATH

EXPECTED_FILES = (
    (
        f"{RECEIPT_ROOT}/agent_audit_manifest.json",
        "100644",
        67354,
        "6aed59df5f6d337c488cd657065c80685873211e",
        "5586482c52d7d07dd1cc09ca3007604ec5f1cfb60d0911b784ced27b220190bd",
    ),
    (
        f"{RECEIPT_ROOT}/agent_pr_receipt.json",
        "100644",
        154640,
        "cdd0e5072428eecd95de5037eb2489103bb1c775",
        "897e685b74f8fbf70f0ced6089fc007b1973570c935157625070e150df96d1de",
    ),
    (
        f"{RECEIPT_ROOT}/pr-body.md",
        "100644",
        2734,
        "ef733464442aec062bea482a4bbb11c08eab05a8",
        "2738d3f28fdf9cb9dba0ad03951425863bcc4f604c86b5f9653f77b6908aa906",
    ),
    (
        f"{RECEIPT_ROOT}/pr-changed-paths.txt",
        "100644",
        51237,
        "f6ad629e0f69a76dc0c8a63c7e5e6f742a6542a6",
        "446c054414bc56e3de6f66c4c50c595cdf5fea71e6476834a669472c1fec0506",
    ),
    (
        f"{RECEIPT_ROOT}/pr-head-sha.txt",
        "100644",
        41,
        "4a0264a42075cc0d564a62e1f02bad8647dc1622",
        "d25c7b29af7ab46b91fd967f0687396a222ba547f487977ea7bc441246b0b7aa",
    ),
    (
        f"{RECEIPT_ROOT}/pr-receipt-exit-code.txt",
        "100644",
        2,
        "573541ac9702dd3969c9bc859d2b91ec1f7e6e56",
        "9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa",
    ),
)


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_non_finite(value: str) -> NoReturn:
    raise ValueError(f"non-finite JSON number: {value}")


def _read_regular_file_nofollow(path: Path) -> bytes:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise AssertionError(f"{path} must be a non-symlink regular file") from exc

    with os.fdopen(descriptor, "rb") as stream:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise AssertionError(f"{path} must be a non-symlink regular file")
        return stream.read()


def _sha256_regular_file_nofollow(path: Path) -> str:
    return hashlib.sha256(_read_regular_file_nofollow(path)).hexdigest()


def _is_lexically_absent(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return True
    return False


def _load_strict_json(path: Path) -> object:
    return json.loads(
        _read_regular_file_nofollow(path).decode("utf-8"),
        object_pairs_hook=_reject_duplicate_keys,
        parse_constant=_reject_non_finite,
    )


def _all_strings(value: object) -> Iterator[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for key, nested in value.items():
            yield from _all_strings(key)
            yield from _all_strings(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from _all_strings(nested)


def _expected_retained_files() -> list[dict[str, object]]:
    return [
        {
            "path": path,
            "mode": mode,
            "size_bytes": size_bytes,
            "git_blob": git_blob,
            "sha256": sha256,
        }
        for path, mode, size_bytes, git_blob, sha256 in EXPECTED_FILES
    ]


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ('{"schema":"expected","schema":"drift"}', "duplicate JSON key"),
        ('{"value":NaN}', "non-finite JSON number"),
        ('{"value":Infinity}', "non-finite JSON number"),
        ('{"value":-Infinity}', "non-finite JSON number"),
    ],
)
def test_strict_json_rejects_duplicate_keys_and_non_finite_values(
    tmp_path: Path,
    raw: str,
    message: str,
) -> None:
    candidate = tmp_path / "candidate.json"
    candidate.write_text(raw, encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        _load_strict_json(candidate)


@pytest.mark.parametrize(
    ("authority_name", "reader"),
    [
        ("record.json", _load_strict_json),
        ("index.json", _sha256_regular_file_nofollow),
        ("download.json", _sha256_regular_file_nofollow),
    ],
)
def test_authority_reads_reject_symlinked_regular_files(
    tmp_path: Path,
    authority_name: str,
    reader: Callable[[Path], object],
) -> None:
    source = tmp_path / "source.json"
    source.write_text("{}\n", encoding="utf-8")
    authority = tmp_path / authority_name
    authority.symlink_to(source)

    with pytest.raises(AssertionError, match="non-symlink regular file"):
        reader(authority)


@pytest.mark.parametrize("target_exists", [True, False], ids=["valid", "broken"])
def test_deleted_path_lexical_absence_rejects_symlinks(tmp_path: Path, target_exists: bool) -> None:
    target = tmp_path / "target"
    if target_exists:
        target.write_text("retained", encoding="utf-8")
    deleted_path = tmp_path / "deleted-receipt"
    deleted_path.symlink_to(target)

    assert not _is_lexically_absent(deleted_path)


def test_public_retention_record_is_exact_authority() -> None:
    assert not _is_lexically_absent(RETENTION_RECORD), (
        "append-only historical agent receipt retention record is missing"
    )
    record = _load_strict_json(RETENTION_RECORD)
    assert isinstance(record, dict)

    producer_sha256 = record.get("producer_sha256")
    assert isinstance(producer_sha256, str)
    assert re.fullmatch(r"[0-9a-f]{64}", producer_sha256)

    assert record == {
        "schema": "dpone.public-clean-historical-receipt-retention.v1",
        "status": "N/A",
        "scope": ("Historical location and integrity only; no execution, privacy, release, or live certification"),
        "source_commit": "6a69861832ffc3dea86fb05d0dd882645399f21a",
        "source_tree": "ea2153662106bbd58b27e3ca963425796145fdc9",
        "download_manifest": {
            "path": DOWNLOAD_PATH,
            "git_blob": "0a0b569abf11e8d526047450a34b8f5fbb628215",
            "sha256": ("e9ea9e06d09bf9a9d742a8b519d42430bb022b9066c927aa99fa5aacda5f801d"),
            "size_bytes": 1431,
        },
        "original_retention": {
            "archive_sha256": ("b4c1d695b5aae8a61d23755142514adb0527d81b69397a253a79e76005028832"),
            "inventory_sha256": ("42264395c79af082c108261b566ff7f1a6e42df8f7d263c271c2dcac4f2f497b"),
            "files": 796,
        },
        "producer_sha256": producer_sha256,
        "retained_files": _expected_retained_files(),
        "retrieval": (
            "Recover all six files together from the pinned source commit or verified complete external archive."
        ),
        "limitations": [
            "Reachable-history privacy scan remains UNVERIFIED.",
            "This record is not execution or release evidence.",
        ],
    }

    private_locator_fragments = (
        "/.codex/",
        "/.cursor/",
        "private_retention_dir",
        "retention-test-worktree",
    )
    for value in _all_strings(record):
        normalized = value.casefold()
        assert not re.search(r"(?:^|[\s('\"=])/(?:\S+)", value)
        assert not re.search(r"(?:^|[\s('\"=])[a-zA-Z]:[\\/]", value)
        assert "file://" not in normalized
        assert "~/" not in value
        assert all(fragment not in normalized for fragment in private_locator_fragments)


def test_current_tree_removes_only_bound_receipts_and_preserves_index() -> None:
    assert _sha256_regular_file_nofollow(RETENTION_INDEX) == (
        "7cc4a46fb0fd7c930fef93ab63923be5d261eddf20f145d3a25095136bbb5e2e"
    )
    assert _sha256_regular_file_nofollow(DOWNLOAD_MANIFEST) == (
        "e9ea9e06d09bf9a9d742a8b519d42430bb022b9066c927aa99fa5aacda5f801d"
    )

    retained_paths = [path for path, *_ in EXPECTED_FILES]
    remaining_paths = [path for path in retained_paths if not _is_lexically_absent(ROOT / path)]
    assert remaining_paths == [], f"bound historical agent receipt copies remain in the current tree: {remaining_paths}"
