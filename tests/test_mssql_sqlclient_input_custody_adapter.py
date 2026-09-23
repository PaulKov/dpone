import os
from dataclasses import asdict
from hashlib import sha256
from pathlib import Path

import pytest

from dpone.adapters.mssql_sqlclient_input_custody import FileSqlClientInputCustody
from dpone.contracts.mssql_native_chunks import NativeChunkReceipt
from dpone.contracts.mssql_native_parent_journal import (
    NativeChunkRetirementReceipt,
    NativeParentRetirementReceipt,
    canonical_digest,
)

H = "a" * 64


def _parent(receipt, attempt_id: str = "run-0-0") -> NativeChunkReceipt:
    return NativeChunkReceipt(
        0,
        attempt_id,
        H,
        receipt.rows,
        receipt.encoded_bytes,
        receipt.file_sha256,
        receipt.typed_digest,
        {"input_custody": asdict(receipt)},
    )


def _retirement(parent: NativeChunkReceipt) -> NativeParentRetirementReceipt:
    chunk = NativeChunkRetirementReceipt(0, parent.attempt_id, H, H, canonical_digest(asdict(parent)), H, H, H, H, H, H)
    return NativeParentRetirementReceipt(H, (chunk,))


def _retain(store: FileSqlClientInputCustody, data: bytes = b"exact"):
    return store.retain(
        data,
        plan_sha256=H,
        target_id="target",
        run_id="run",
        window_fingerprint="window",
        attempt_id="run-0-0",
        ordinal=0,
        rows=1,
        typed_digest=H,
    )


def _retain_file(
    store: FileSqlClientInputCustody,
    source: Path,
    *,
    expected_sha256: str | None = None,
):
    return store.retain_file(
        source,
        expected_size=6,
        expected_sha256=expected_sha256 or sha256(b"sealed").hexdigest(),
        plan_sha256=H,
        target_id="target",
        run_id="run",
        window_fingerprint="window",
        attempt_id="run-0-0",
        ordinal=0,
        rows=1,
        typed_digest=H,
    )


def test_retain_observe_and_release_only_after_exact_parent_retirement(tmp_path: Path) -> None:
    store = FileSqlClientInputCustody(tmp_path)
    receipt = _retain(store)
    parent = _parent(receipt)
    assert store.observe(receipt) == b"exact"
    with pytest.raises(ValueError, match="retirement_mismatch"):
        wrong = _parent(receipt, "other")
        store.release(receipt, parent, _retirement(wrong))
    assert store.observe(receipt) == b"exact"
    with pytest.raises(ValueError, match="retirement_mismatch"):
        store.release(
            receipt,
            parent,
            NativeParentRetirementReceipt(
                H, (NativeChunkRetirementReceipt(0, parent.attempt_id, H, H, H, H, H, H, H, H, H),)
            ),
        )
    retirement = _retirement(parent)
    store.release(receipt, parent, retirement)
    store.release(receipt, parent, retirement)
    with pytest.raises(RuntimeError, match="unavailable"):
        store.observe(receipt)


def test_replay_is_idempotent_and_drift_fails_closed(tmp_path: Path) -> None:
    store = FileSqlClientInputCustody(tmp_path)
    receipt = _retain(store)
    assert _retain(store) == receipt
    path = next(tmp_path.glob("*.bin"))
    path.write_bytes(b"drift")
    with pytest.raises(RuntimeError, match="drift"):
        store.observe(receipt)
    with pytest.raises(ValueError, match="conflict"):
        _retain(store)


def test_full_identity_has_independent_custody_and_invalid_metadata_writes_nothing(tmp_path: Path) -> None:
    store = FileSqlClientInputCustody(tmp_path)
    first = _retain(store)
    first_parent = _parent(first)
    second = store.retain(
        b"exact",
        plan_sha256=H,
        target_id="other-target",
        run_id="run",
        window_fingerprint="window",
        attempt_id="run-0-0",
        ordinal=0,
        rows=1,
        typed_digest=H,
    )
    assert first.durable_object_id != second.durable_object_id
    store.release(first, first_parent, _retirement(first_parent))
    assert store.observe(second) == b"exact"
    before = set(tmp_path.iterdir())
    with pytest.raises(ValueError, match="receipt_invalid"):
        store.retain(
            b"orphan",
            plan_sha256="invalid",
            target_id="target",
            run_id="run",
            window_fingerprint="window",
            attempt_id="run-1-0",
            ordinal=1,
            rows=1,
            typed_digest=H,
        )
    assert set(tmp_path.iterdir()) == before


def test_symlink_replacement_is_rejected(tmp_path: Path) -> None:
    store = FileSqlClientInputCustody(tmp_path)
    receipt = _retain(store)
    path = next(tmp_path.glob("*.bin"))
    outside = tmp_path / "outside"
    outside.write_bytes(b"exact")
    path.unlink()
    path.symlink_to(outside)
    with pytest.raises(RuntimeError, match="unavailable"):
        store.observe(receipt)


def test_unicode_identity_uses_contract_canonical_bytes(tmp_path: Path) -> None:
    store = FileSqlClientInputCustody(tmp_path)
    receipt = store.retain(
        b"exact",
        plan_sha256=H,
        target_id="цель",
        run_id="запуск",
        window_fingerprint="окно",
        attempt_id="попытка",
        ordinal=0,
        rows=1,
        typed_digest=H,
    )
    assert store.observe(receipt) == b"exact"


def test_open_pinned_keeps_verified_descriptor_at_start(tmp_path: Path) -> None:
    store = FileSqlClientInputCustody(tmp_path)
    receipt = _retain(store, b"pinned-input")

    with store.open_pinned(receipt) as descriptor:
        assert os.read(descriptor, len(b"pinned-input")) == b"pinned-input"
        next(tmp_path.glob("*.bin")).unlink()
        os.lseek(descriptor, 0, os.SEEK_SET)
        assert os.read(descriptor, len(b"pinned-input")) == b"pinned-input"

    with pytest.raises(OSError):
        os.fstat(descriptor)


def test_retain_file_streams_bounded_reads_and_preserves_bytes(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "source.bin"
    payload = b"bounded-stream" * 200_000
    source.write_bytes(payload)
    store = FileSqlClientInputCustody(tmp_path / "custody")
    requested: list[int] = []
    original_read = os.read

    def bounded_read(descriptor: int, count: int) -> bytes:
        requested.append(count)
        assert count <= 1024 * 1024
        return original_read(descriptor, count)

    monkeypatch.setattr(os, "read", bounded_read)
    receipt = store.retain_file(
        source,
        expected_size=len(payload),
        expected_sha256=sha256(payload).hexdigest(),
        plan_sha256=H,
        target_id="target",
        run_id="run",
        window_fingerprint="window",
        attempt_id="run-0-0",
        ordinal=0,
        rows=1,
        typed_digest=H,
    )

    assert requested and len(requested) >= 3
    assert store.observe(receipt) == payload
    assert receipt == FileSqlClientInputCustody(tmp_path / "bytes-custody").retain(
        payload,
        plan_sha256=H,
        target_id="target",
        run_id="run",
        window_fingerprint="window",
        attempt_id="run-0-0",
        ordinal=0,
        rows=1,
        typed_digest=H,
    )
    assert (
        store.retain_file(
            source,
            expected_size=len(payload),
            expected_sha256=sha256(payload).hexdigest(),
            plan_sha256=H,
            target_id="target",
            run_id="run",
            window_fingerprint="window",
            attempt_id="run-0-0",
            ordinal=0,
            rows=1,
            typed_digest=H,
        )
        == receipt
    )


def test_retain_file_rejects_symlink_and_digest_drift_without_orphans(tmp_path: Path) -> None:
    source = tmp_path / "source.bin"
    source.write_bytes(b"sealed")
    link = tmp_path / "source-link.bin"
    link.symlink_to(source)
    custody_root = tmp_path / "custody"
    store = FileSqlClientInputCustody(custody_root)
    with pytest.raises(RuntimeError, match="unavailable"):
        _retain_file(store, link)
    with pytest.raises(RuntimeError, match="drift"):
        _retain_file(store, source, expected_sha256=H)
    assert tuple(custody_root.iterdir()) == ()

    receipt = _retain_file(store, source)
    retained = next(custody_root.glob("*.bin"))
    retained.write_bytes(b"broken")
    with pytest.raises(ValueError, match="conflict"):
        _retain_file(store, source)
    assert receipt.durable_object_id in retained.name
