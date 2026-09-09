"""Immutable file-receipt and SQL Server staging authority contracts."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.runtime.artifact_integrity import (
    ArtifactIntegrityError,
    CompletedFileWrite,
    FileArtifactReceipt,
    FileOwnedScope,
    FileWireContract,
)
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.bulk_options import BulkOptionsResolver
from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sinks.staging_managers.mssql import MSSQLStagingManager
from dpone.runtime.sources.strategies.postgres.postgres_file_export_mixin import PostgresFileExportMixin


def _artifact(path: Path, *, rows: int = 1) -> FileExportArtifact:
    return FileExportArtifact(
        str(path),
        ["value"],
        format="mssql-delimited",
        rows_exported=rows,
        bulk_text_codec=BulkTextCodec(),
    )


def _wire_contract() -> FileWireContract:
    return FileWireContract.resolve(
        columns=("value",),
        format="mssql-delimited",
        compressed=False,
        has_header=False,
        bulk_text_codec=BulkTextCodec(),
    )


def _staging(manager: MSSQLStagingManager, *, error_file: Path | None = None) -> StagingTableArtifact:
    options: dict[str, object] = {}
    if error_file is not None:
        options = {"bulk": {"mode": "bcp", "bcp": {"error_file": str(error_file)}}}
    return StagingTableArtifact(
        schema="staging",
        table="raw",
        columns=["value"],
        staging_manager=manager,
        column_types={"value": "nvarchar(max)"},
        bulk_text_codec=BulkTextCodec(),
        bulk_options=BulkOptionsResolver.resolve(options),
    )


class _Connector:
    bcp_path = "bcp"
    trust_server_certificate = "yes"

    def __init__(self, counts: list[int], *, on_import=None) -> None:
        self._counts = iter(counts)
        self._on_import = on_import
        self.import_calls = 0

    @staticmethod
    def qualified_name(schema: str, table: str, *, database=None) -> str:
        prefix = f"[{database}]." if database else ""
        return f"{prefix}[{schema}].[{table}]"

    def get_records(self, query, *args, **kwargs):
        del query, args, kwargs
        return [(next(self._counts),)]

    def bcp_import(self, schema, table, file_path, *, options, database=None) -> int:
        del schema, table, database
        self.import_calls += 1
        if self._on_import is not None:
            self._on_import(Path(file_path), options)
        return 1


class _ReportedConnector(_Connector):
    def __init__(self, counts: list[int], reported) -> None:
        super().__init__(counts)
        self._reported = reported

    def bcp_import(self, schema, table, file_path, *, options, database=None):
        del schema, table, file_path, options, database
        self.import_calls += 1
        return self._reported


def test_same_size_tamper_is_rejected_before_bcp(tmp_path: Path) -> None:
    path = tmp_path / "wire.bcp"
    path.write_bytes(b"one\n")
    artifact = _artifact(path)
    path.write_bytes(b"two\n")
    connector = _Connector([0, 1])
    manager = MSSQLStagingManager(connector)

    with pytest.raises(ArtifactIntegrityError) as raised:
        manager.load_from_file(_staging(manager), artifact)

    assert raised.value.code == "artifact_integrity.sha256_mismatch"
    assert connector.import_calls == 0


def test_same_bytes_inode_replacement_is_rejected_before_bcp(tmp_path: Path) -> None:
    path = tmp_path / "wire.bcp"
    path.write_bytes(b"one\n")
    artifact = _artifact(path)
    captured = artifact.require_integrity_receipt().identity
    replacement = tmp_path / "replacement.bcp"
    replacement.write_bytes(path.read_bytes())
    os.utime(replacement, ns=(path.stat().st_atime_ns, captured.mtime_ns))
    os.replace(replacement, path)
    connector = _Connector([0, 1])
    manager = MSSQLStagingManager(connector)

    with pytest.raises(ArtifactIntegrityError) as raised:
        manager.load_from_file(_staging(manager), artifact)

    assert raised.value.code == "artifact_integrity.file_identity_mismatch"
    assert path.stat().st_ino != captured.inode
    assert connector.import_calls == 0


def test_same_object_mtime_change_is_rejected_before_bcp(tmp_path: Path) -> None:
    path = tmp_path / "wire.bcp"
    path.write_bytes(b"one\n")
    artifact = _artifact(path)
    captured = artifact.require_integrity_receipt().identity
    os.utime(path, ns=(path.stat().st_atime_ns, captured.mtime_ns + 1_000_000))
    connector = _Connector([0, 1])
    manager = MSSQLStagingManager(connector)

    with pytest.raises(ArtifactIntegrityError) as raised:
        manager.load_from_file(_staging(manager), artifact)

    assert raised.value.code == "artifact_integrity.file_identity_mismatch"
    assert connector.import_calls == 0


def test_path_outside_captured_owned_scope_is_rejected_before_identity_hash(tmp_path: Path) -> None:
    owned = tmp_path / "owned"
    owned.mkdir()
    path = owned / "wire.bcp"
    path.write_bytes(b"one\n")
    artifact = _artifact(path)
    escaped = tmp_path / "escaped.bcp"
    escaped.write_bytes(path.read_bytes())
    os.utime(escaped, ns=(path.stat().st_atime_ns, path.stat().st_mtime_ns))
    artifact.file_path = str(escaped)
    connector = _Connector([0, 1])
    manager = MSSQLStagingManager(connector)

    with pytest.raises(ArtifactIntegrityError) as raised:
        manager.load_from_file(_staging(manager), artifact)

    receipt = artifact.integrity_receipt
    assert receipt is not None
    assert receipt.owned_scope.path == str(owned.resolve())
    assert raised.value.code == "artifact_integrity.owned_scope_mismatch"
    assert connector.import_calls == 0


def test_missing_and_symlink_paths_keep_stable_pre_bcp_diagnostics(tmp_path: Path) -> None:
    missing = tmp_path / "missing.bcp"
    missing.write_bytes(b"one\n")
    missing_artifact = _artifact(missing)
    missing.unlink()
    connector = _Connector([0, 1])
    manager = MSSQLStagingManager(connector)

    with pytest.raises(ArtifactIntegrityError) as missing_raised:
        manager.load_from_file(_staging(manager), missing_artifact)

    original = tmp_path / "original.bcp"
    original.write_bytes(b"one\n")
    alias_artifact = _artifact(original)
    alias = tmp_path / "alias.bcp"
    alias.symlink_to(original)
    alias_artifact.file_path = str(alias)
    with pytest.raises(ArtifactIntegrityError) as alias_raised:
        manager.load_from_file(_staging(manager), alias_artifact)

    assert missing_raised.value.code == "artifact_integrity.file_unavailable"
    assert alias_raised.value.code == "artifact_integrity.regular_file_required"
    assert connector.import_calls == 0


def test_path_replacement_during_descriptor_hash_keeps_race_diagnostic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "wire.bcp"
    path.write_bytes(b"one\n")
    replacement = tmp_path / "replacement.bcp"
    replacement.write_bytes(b"one\n")
    real_fstat = os.fstat
    calls = 0

    def replacing_fstat(descriptor: int):
        nonlocal calls
        calls += 1
        if calls == 2:
            os.replace(replacement, path)
        return real_fstat(descriptor)

    monkeypatch.setattr(os, "fstat", replacing_fstat)

    with pytest.raises(ArtifactIntegrityError) as raised:
        _artifact(path)

    assert raised.value.code == "artifact_integrity.file_replaced_during_hash"


@pytest.mark.skipif(not hasattr(os, "pread"), reason="requires POSIX descriptor hashing")
def test_descriptor_receipt_rejects_a_different_inode_with_the_same_bytes(tmp_path: Path) -> None:
    original = tmp_path / "original.bcp"
    replacement = tmp_path / "replacement.bcp"
    original.write_bytes(b"one\n")
    replacement.write_bytes(b"one\n")
    original_descriptor = os.open(original, os.O_RDONLY)
    replacement_descriptor = os.open(replacement, os.O_RDONLY)
    try:
        receipt = FileArtifactReceipt.capture_descriptor(
            original_descriptor,
            owned_scope=FileOwnedScope.capture(str(original)),
            wire_contract=_wire_contract(),
            rows_exported=1,
        )

        receipt.verify_descriptor(original_descriptor, wire_contract=_wire_contract())
        with pytest.raises(ArtifactIntegrityError) as raised:
            receipt.verify_descriptor(replacement_descriptor, wire_contract=_wire_contract())
    finally:
        os.close(replacement_descriptor)
        os.close(original_descriptor)

    assert raised.value.code == "artifact_integrity.file_identity_mismatch"


@pytest.mark.skipif(not hasattr(os, "pread") or not hasattr(os, "pwrite"), reason="requires POSIX descriptor I/O")
def test_descriptor_receipt_detects_same_inode_byte_mutation(tmp_path: Path) -> None:
    path = tmp_path / "wire.bcp"
    path.write_bytes(b"one\n")
    original_stat = path.stat()
    descriptor = os.open(path, os.O_RDWR)
    try:
        receipt = FileArtifactReceipt.capture_descriptor(
            descriptor,
            owned_scope=FileOwnedScope.capture(str(path)),
            wire_contract=_wire_contract(),
            rows_exported=1,
        )
        os.pwrite(descriptor, b"two\n", 0)
        os.fsync(descriptor)
        os.utime(path, ns=(original_stat.st_atime_ns, receipt.identity.mtime_ns))

        with pytest.raises(ArtifactIntegrityError) as raised:
            receipt.verify_descriptor(descriptor, wire_contract=_wire_contract())
    finally:
        os.close(descriptor)

    assert raised.value.code == "artifact_integrity.sha256_mismatch"


@pytest.mark.parametrize(
    "mutate",
    (
        lambda artifact: setattr(artifact, "columns", ["different"]),
        lambda artifact: setattr(artifact, "format", "csv"),
        lambda artifact: setattr(artifact, "compressed", True),
        lambda artifact: setattr(artifact, "has_header", True),
        lambda artifact: setattr(
            artifact,
            "bulk_text_codec",
            BulkTextCodec(marker_prefix="\x1e", empty_string_marker="\x1eE"),
        ),
    ),
    ids=("columns", "format", "compression", "header", "codec"),
)
def test_wire_interpretation_mutation_is_rejected_before_bcp(tmp_path: Path, mutate) -> None:
    path = tmp_path / "wire.bcp"
    path.write_bytes(b"one\n")
    artifact = _artifact(path)
    mutate(artifact)
    connector = _Connector([0, 1])
    manager = MSSQLStagingManager(connector)

    with pytest.raises(ArtifactIntegrityError) as raised:
        manager.load_from_file(_staging(manager), artifact)

    assert raised.value.code == "artifact_integrity.wire_contract_mismatch"
    assert connector.import_calls == 0


def test_mutation_during_bcp_is_rejected_by_post_import_hash(tmp_path: Path) -> None:
    path = tmp_path / "wire.bcp"
    path.write_bytes(b"one\n")
    artifact = _artifact(path)

    def mutate(file_path: Path, _options) -> None:
        file_path.write_bytes(b"two\n")

    connector = _Connector([0, 1], on_import=mutate)
    manager = MSSQLStagingManager(connector)
    with pytest.raises(ArtifactIntegrityError) as raised:
        manager.load_from_file(_staging(manager), artifact)

    assert raised.value.code == "artifact_integrity.sha256_mismatch"


def test_actual_staging_count_must_match_receipt(tmp_path: Path) -> None:
    path = tmp_path / "wire.bcp"
    path.write_bytes(b"one\n")
    connector = _Connector([0, 0])
    manager = MSSQLStagingManager(connector)

    with pytest.raises(ArtifactIntegrityError) as raised:
        manager.load_from_file(_staging(manager), _artifact(path))

    assert raised.value.code == "artifact_integrity.staging_row_count_mismatch"


def test_streaming_rows_are_spooled_once_and_imported_by_one_bcp_process() -> None:
    connector = _ReportedConnector([0, 5], 5)
    manager = MSSQLStagingManager(connector)
    staging = _staging(manager)
    yielded: list[int] = []

    def rows():
        for value in range(5):
            yielded.append(value)
            yield {"value": f"row-{value}"}

    inserted = manager.insert_streaming_rows(staging, rows())

    assert inserted == 5
    assert yielded == list(range(5))
    assert connector.import_calls == 1
    assert staging.consumed_payload_evidence is not None
    assert staging.consumed_payload_evidence.actual_raw_rows == 5


@pytest.mark.parametrize("reported", (None, 0, False), ids=("no_count", "reported_zero", "boolean"))
def test_missing_or_non_authoritative_bcp_count_is_rejected(tmp_path: Path, reported) -> None:
    path = tmp_path / "wire.bcp"
    path.write_bytes(b"one\n")
    connector = _ReportedConnector([0, 1], reported)
    manager = MSSQLStagingManager(connector)

    with pytest.raises(ArtifactIntegrityError) as raised:
        manager.load_from_file(_staging(manager), _artifact(path))

    assert raised.value.code == "artifact_integrity.bcp_row_count_mismatch"


def test_verified_bcp_attaches_exact_consumed_payload_evidence(tmp_path: Path) -> None:
    path = tmp_path / "wire.bcp"
    path.write_bytes(b"one\n")
    connector = _Connector([0, 1])
    manager = MSSQLStagingManager(connector)
    staging = _staging(manager)

    assert manager.load_from_file(staging, _artifact(path)) == 1

    evidence = staging.consumed_payload_evidence.require_complete(native=False)
    assert evidence.declared_rows == evidence.actual_raw_rows == 1
    assert evidence.actual_native_rows is None
    assert len(evidence.manifest_sha256) == 64
    assert staging.wire_schema == (("value", "nvarchar(max)"),)


def test_verified_empty_file_binds_evidence_without_invoking_bcp(tmp_path: Path) -> None:
    path = tmp_path / "empty.bcp"
    path.write_bytes(b"")
    connector = _Connector([0, 0])
    manager = MSSQLStagingManager(connector)
    staging = _staging(manager)

    assert manager.load_from_file(staging, _artifact(path, rows=0)) == 0

    evidence = staging.consumed_payload_evidence.require_complete(native=False)
    assert evidence.declared_rows == evidence.actual_raw_rows == 0
    assert len(evidence.parts) == 1
    assert evidence.parts[0].artifact_size_bytes == 0
    assert connector.import_calls == 0


def test_nonempty_bcp_error_file_is_a_rejected_row(tmp_path: Path) -> None:
    path = tmp_path / "wire.bcp"
    path.write_bytes(b"one\n")
    error_path = tmp_path / "rejected.err"

    def reject(_file_path: Path, options) -> None:
        Path(options.error_file).write_text("vendor rejected row", encoding="utf-8")

    connector = _Connector([0, 1], on_import=reject)
    manager = MSSQLStagingManager(connector)
    with pytest.raises(ArtifactIntegrityError) as raised:
        manager.load_from_file(_staging(manager, error_file=error_path), _artifact(path))

    assert raised.value.code == "artifact_integrity.bcp_rejected_rows"


def test_mssql_delimited_record_receipt_counts_null_empty_and_all_null_rows(tmp_path: Path) -> None:
    cases = (
        (b"", 0),
        (b"\n", 1),  # one-column SQL NULL record
        (b"\x1dE\n", 1),  # one-column encoded empty string
        (b"\t\n", 1),  # two-column all-NULL record
        (b"value", 1),  # final record without a row terminator
        (b"\n\x1dE\n\t\nvalue", 4),
    )
    for index, (payload, expected_rows) in enumerate(cases):
        path = tmp_path / f"boundary_{index}.bcp"
        path.write_bytes(payload)
        artifact = FileExportArtifact(
            str(path),
            ["value"],
            format="mssql-delimited",
            bulk_text_codec=BulkTextCodec(),
        )

        PostgresFileExportMixin._attach_rows_exported(artifact, str(path), compressed=False)

        assert artifact.rows_exported == expected_rows
        receipt = artifact.require_integrity_receipt()
        assert receipt.size_bytes == path.stat().st_size
        assert len(receipt.sha256) == 64


def test_wire_receipt_binds_explicit_codec_identity_and_version(tmp_path: Path) -> None:
    path = tmp_path / "wire.bcp"
    path.write_bytes(b"one\n")
    artifact = _artifact(path)

    class FutureCodec(BulkTextCodec):
        codec_version = 3

    artifact.bulk_text_codec = FutureCodec()
    connector = _Connector([0, 1])
    manager = MSSQLStagingManager(connector)

    with pytest.raises(ArtifactIntegrityError) as raised:
        manager.load_from_file(_staging(manager), artifact)

    assert raised.value.code == "artifact_integrity.wire_contract_mismatch"
    assert connector.import_calls == 0


def test_receipt_row_count_is_immutable(tmp_path: Path) -> None:
    path = tmp_path / "wire.bcp"
    path.write_bytes(b"one\n")
    artifact = _artifact(path)

    with pytest.raises(ArtifactIntegrityError) as raised:
        artifact.rows_exported = 2

    assert raised.value.code == "artifact_integrity.rows_exported_immutable"


def test_completed_write_receipt_is_reverified_at_consumer_boundary(tmp_path: Path) -> None:
    path = tmp_path / "completed.bcp"
    payload = b"one\n"
    path.write_bytes(payload)
    artifact = FileExportArtifact(
        str(path),
        ("value",),
        format="mssql-delimited",
        bulk_text_codec=BulkTextCodec(),
        _completed_write=CompletedFileWrite(
            sha256=hashlib.sha256(payload).hexdigest(),
            size_bytes=len(payload),
            rows_exported=1,
        ),
    )
    path.write_bytes(b"two\n")

    with pytest.raises(ArtifactIntegrityError) as raised:
        artifact.require_integrity_receipt()

    assert raised.value.code in {
        "artifact_integrity.file_identity_mismatch",
        "artifact_integrity.sha256_mismatch",
    }


def test_file_materialization_failure_cleans_staging_and_preserves_error(tmp_path: Path) -> None:
    path = tmp_path / "wire.bcp"
    path.write_bytes(b"one\n")
    cleaned: list[bool] = []
    handle = SimpleNamespace(row_count=0, cleanup=lambda: cleaned.append(True))
    manager = SimpleNamespace(
        create=lambda _config, _schema: handle,
        load_from_file=lambda _handle, _artifact: (_ for _ in ()).throw(
            ArtifactIntegrityError("artifact_integrity.sha256_mismatch")
        ),
    )

    with pytest.raises(ArtifactIntegrityError) as raised:
        _artifact(path).materialize(manager, SimpleNamespace(), [("value", "nvarchar(max)")])

    assert raised.value.code == "artifact_integrity.sha256_mismatch"
    assert cleaned == [True]
