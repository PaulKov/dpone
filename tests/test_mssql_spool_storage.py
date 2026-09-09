"""Fail-closed local-spool contracts for character-mode MSSQL BCP."""

from __future__ import annotations

import hashlib
import os
import stat
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.source_physical_identity import SourcePhysicalIdentity
from dpone.runtime.artifact_integrity import ArtifactIntegrityError
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.bulk_options import BulkOptionsResolver
from dpone.runtime.connectors.mssql_bulk import BcpCredentials, BcpOptions, BcpRunner
from dpone.runtime.credentials.factory import SinkFactory
from dpone.runtime.etl.mssql_transaction_route_identity import invocation_route_fingerprint
from dpone.runtime.mssql_spool_route import bind_mssql_character_spool_preflight
from dpone.runtime.pinned_directory import PinnedDirectory, PinnedFileConsumer
from dpone.runtime.pinned_directory_win32_api import Win32DirectoryApi
from dpone.runtime.pinned_directory_windows import (
    WindowsDirectoryIdentity,
    WindowsPinnedDirectoryLease,
)
from dpone.runtime.sinks.mssql import MSSQLSink
from dpone.runtime.sinks.staging_managers.mssql import MSSQLStagingManager
from dpone.runtime.sources.clickhouse import ClickHouseSource
from dpone.runtime.storage_policy import (
    RuntimeStorageAdmissionError,
    RuntimeStoragePolicy,
    StoragePreflightService,
)
from dpone.runtime.support.bulk_text_codec import BulkTextCodec
from dpone.runtime.support.mssql_bcp_values import DelimitedBulkFile, MssqlSpoolCapacityError


class _Connector:
    bcp_path = "bcp"
    trust_server_certificate = "yes"

    def __init__(self, *, on_import=None, fail_import: bool = False) -> None:
        self._counts = iter((0, 1))
        self._on_import = on_import
        self._fail_import = fail_import
        self.import_calls = 0
        self.import_options: BcpOptions | None = None
        self.import_source_path: Path | None = None

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
        self.import_options = options
        self.import_source_path = Path(file_path)
        process_path = file_path
        authority = options.input_file_authority
        if authority is not None:
            process_path, _descriptors = authority.prepare_process_input(file_path)
        if self._on_import is not None:
            self._on_import(Path(process_path))
        if self._fail_import:
            raise RuntimeError("synthetic_bcp_failure")
        return 1


class _FakeWin32DirectoryApi:
    def __init__(self) -> None:
        self.opened: list[Path] = []
        self.paths: dict[int, Path] = {}
        self.identities: dict[int, WindowsDirectoryIdentity] = {}
        self.descriptor_paths: dict[int, Path] = {}
        self.closed: list[int] = []
        self.deleted: list[Path] = []

    def open_directory(self, path: Path) -> int:
        handle = len(self.opened) + 100
        self.opened.append(path)
        self.paths[handle] = path
        self.identities[handle] = WindowsDirectoryIdentity(volume=7, file_index=handle, attributes=0x10)
        return handle

    def identity(self, handle: int) -> WindowsDirectoryIdentity:
        return self.identities[handle]

    def require_final_path(self, handle: int, expected: Path) -> None:
        assert self.paths[handle] == expected

    def create_exclusive_file(self, path: Path) -> int:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        self.descriptor_paths[descriptor] = path
        return descriptor

    @staticmethod
    def descriptor_identity(descriptor: int) -> WindowsDirectoryIdentity:
        identity = os.fstat(descriptor)
        return WindowsDirectoryIdentity(
            volume=int(identity.st_dev),
            file_index=int(identity.st_ino),
            attributes=0,
        )

    def duplicate_read_lock(self, descriptor: int) -> int:
        source = self.descriptor_paths[descriptor]
        identity = self.descriptor_identity(descriptor)
        handle = max((*self.paths, 999)) + 1
        self.paths[handle] = source
        self.identities[handle] = identity
        return handle

    def delete_exact_file(self, path: Path, *, expected: WindowsDirectoryIdentity) -> None:
        descriptor = os.open(path, os.O_RDONLY)
        try:
            observed = self.descriptor_identity(descriptor)
            if not observed.same_file(expected):
                raise OSError("win32_cleanup_target_identity_changed")
        finally:
            os.close(descriptor)
        os.unlink(path)
        self.deleted.append(path)

    def close(self, handle: int) -> None:
        self.closed.append(handle)


def _service(*, free_bytes: int) -> StoragePreflightService:
    return StoragePreflightService(
        disk_usage_provider=lambda _path: SimpleNamespace(free=free_bytes),
        spool_free_bytes_provider=lambda _directory: free_bytes,
    )


def _policy(work_dir: Path, *, min_free_bytes: int = 10) -> RuntimeStoragePolicy:
    return RuntimeStoragePolicy.from_sources(
        runtime={
            "storage": {
                "work_dir": str(work_dir),
                "min_free_bytes": min_free_bytes,
            }
        },
        env={},
    )


def _staging(manager: MSSQLStagingManager) -> StagingTableArtifact:
    return StagingTableArtifact(
        schema="staging",
        table="raw",
        columns=["value"],
        staging_manager=manager,
        column_types={"value": "nvarchar(max)"},
        bulk_text_codec=BulkTextCodec(),
        bulk_options=BulkOptionsResolver.resolve({}),
    )


def test_spool_admission_provisions_the_exact_work_dir_and_reserves_headroom(tmp_path: Path) -> None:
    work_dir = tmp_path / "mounted" / "spool"

    admission = _service(free_bytes=100).require_spool_admission(_policy(work_dir))

    assert admission.work_dir == work_dir.resolve()
    assert admission.free_bytes == 100
    assert admission.max_spool_bytes == 90
    assert admission.min_free_bytes == 10
    assert work_dir.is_dir()
    admission.close()


def test_runtime_storage_projection_remains_authoritative_without_raw_runtime_block(tmp_path: Path) -> None:
    projected = tmp_path / "projected"

    policy = RuntimeStoragePolicy.from_sources(
        runtime={},
        source_options={"runtime_storage": {"work_dir": str(projected), "min_free_bytes": "2MiB"}},
        env={},
    )

    assert policy.work_dir == projected
    assert policy.min_free_bytes == 2 * 1024 * 1024
    assert policy.warnings == ()


def test_unrelated_raw_runtime_keys_do_not_shadow_storage_projection(tmp_path: Path) -> None:
    projected = tmp_path / "projected"

    policy = RuntimeStoragePolicy.from_sources(
        runtime={"execution": {"workers": 2}},
        source_options={"runtime_storage": {"work_dir": str(projected), "min_free_bytes": "2MiB"}},
        env={},
    )

    assert policy.work_dir == projected
    assert policy.min_free_bytes == 2 * 1024 * 1024
    assert policy.warnings == ()


def test_empty_raw_storage_block_does_not_shadow_storage_projection(tmp_path: Path) -> None:
    projected = tmp_path / "projected"

    policy = RuntimeStoragePolicy.from_sources(
        runtime={"storage": {}, "execution": {"workers": 2}},
        source_options={"runtime_storage": {"work_dir": str(projected), "min_free_bytes": "2MiB"}},
        env={},
    )

    assert policy.work_dir == projected
    assert policy.min_free_bytes == 2 * 1024 * 1024
    assert policy.warnings == ()


def test_spool_preflight_fails_closed_on_low_space_before_rows_are_consumed(tmp_path: Path) -> None:
    yielded = False
    manager = MSSQLStagingManager(
        _Connector(),
        storage_policy=_policy(tmp_path / "spool", min_free_bytes=11),
        storage_preflight_service=_service(free_bytes=10),
    )

    def rows():
        nonlocal yielded
        yielded = True
        yield {"value": "must-not-be-read"}

    with pytest.raises(RuntimeStorageAdmissionError) as raised:
        manager.insert_streaming_rows(_staging(manager), rows())

    assert raised.value.code == "mssql_spool_work_dir_low_space"
    assert yielded is False


def test_spool_preflight_error_does_not_disclose_the_configured_path(tmp_path: Path) -> None:
    secret_path = tmp_path / "tenant-sensitive-segment" / "missing"
    policy = RuntimeStoragePolicy.from_sources(
        runtime={
            "storage": {
                "work_dir": str(secret_path),
                "create_dirs": False,
                "min_free_bytes": 0,
            }
        },
        env={},
    )

    with pytest.raises(RuntimeStorageAdmissionError) as raised:
        _service(free_bytes=100).require_spool_admission(policy)

    assert raised.value.code == "mssql_spool_work_dir_not_writable"
    assert str(raised.value) == "mssql_spool_work_dir_not_writable"
    assert raised.value.__cause__ is None
    assert "tenant-sensitive-segment" not in str(raised.value)


def test_spool_admission_rejects_directory_replacement(tmp_path: Path) -> None:
    work_dir = tmp_path / "spool"
    service = _service(free_bytes=100)
    admission = service.require_spool_admission(_policy(work_dir))
    displaced = tmp_path / "displaced"
    work_dir.rename(displaced)
    work_dir.mkdir()

    with pytest.raises(RuntimeStorageAdmissionError) as raised:
        service.refresh_spool_admission(admission)

    assert raised.value.code == "mssql_spool_work_dir_identity_changed"
    admission.close()


def test_spool_writer_uses_pinned_directory_after_path_swap(
    tmp_path: Path,
) -> None:
    work_dir = tmp_path / "spool"
    displaced = tmp_path / "displaced"
    service = _service(free_bytes=100)
    admission = service.require_spool_admission(_policy(work_dir, min_free_bytes=0))
    refreshed = service.refresh_spool_admission(admission)
    work_dir.rename(displaced)
    work_dir.mkdir()
    writer = DelimitedBulkFile(
        directory=str(refreshed.work_dir),
        pinned_directory=refreshed.directory,
        max_bytes=100,
    )

    try:
        path, count = writer.write_rows([{"value": "pinned"}], ["value"])
        name = Path(path).name

        assert count == 1
        assert list(work_dir.iterdir()) == []
        assert (displaced / name).read_bytes() == b"pinned\n"
    finally:
        if "name" in locals():
            os.unlink(name, dir_fd=refreshed.directory_fd)
        refreshed.close()


def test_pinned_directory_rejects_entry_path_traversal(tmp_path: Path) -> None:
    work_dir = tmp_path / "spool"
    service = _service(free_bytes=100)
    admission = service.require_spool_admission(_policy(work_dir, min_free_bytes=0))

    try:
        with pytest.raises(ValueError, match="pinned_directory_entry_name_invalid"):
            admission.directory.create_file(prefix="../escaped_", suffix=".bcp")

        assert list(tmp_path.glob("escaped_*.bcp")) == []
        assert list(work_dir.iterdir()) == []
    finally:
        admission.close()


def test_windows_directory_lease_locks_the_complete_chain_and_releases_in_reverse(tmp_path: Path) -> None:
    api = _FakeWin32DirectoryApi()
    lease = WindowsPinnedDirectoryLease.open(tmp_path, api=api)

    descriptor, path = lease.create_file("attempt.bcp")
    os.write(descriptor, b"locked")
    os.close(descriptor)
    lease.require_identity()
    lease.unlink("attempt.bcp", missing_ok=False)
    handles = tuple(api.paths)
    lease.close()

    assert api.opened == [*reversed(tmp_path.absolute().parents), tmp_path.absolute()]
    assert api.closed == list(reversed(handles))
    assert not Path(path).exists()


def test_win32_directory_open_participates_in_delete_sharing(monkeypatch: pytest.MonkeyPatch) -> None:
    observed: dict[str, int] = {}

    def create_file(_path: Path, **options: int | bool) -> int:
        observed.update({key: int(value) for key, value in options.items()})
        return 123

    monkeypatch.setattr(Win32DirectoryApi, "_create_file", staticmethod(create_file))

    assert Win32DirectoryApi().open_directory(Path("spool")) == 123
    assert observed["access"] == 0x00000020 | 0x00000080
    assert observed["sharing"] & 0x00000004 == 0


def test_win32_exact_cleanup_opens_and_deletes_only_the_verified_handle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}
    expected = WindowsDirectoryIdentity(volume=7, file_index=11, attributes=0)

    def create_file(path: Path, **options: int | bool) -> int:
        observed["path"] = path
        observed.update(options)
        return 123

    monkeypatch.setattr(Win32DirectoryApi, "_create_file", staticmethod(create_file))
    monkeypatch.setattr(Win32DirectoryApi, "identity", lambda _self, _handle: expected)
    monkeypatch.setattr(
        Win32DirectoryApi,
        "require_final_path",
        lambda _self, handle, path: observed.update(final_path=(handle, path)),
    )
    monkeypatch.setattr(
        Win32DirectoryApi,
        "_mark_delete_pending",
        staticmethod(lambda handle: observed.update(deleted_handle=handle)),
    )
    monkeypatch.setattr(
        Win32DirectoryApi,
        "close",
        lambda _self, handle: observed.update(closed_handle=handle),
    )

    path = Path("spool") / "attempt.bcp"
    Win32DirectoryApi().delete_exact_file(path, expected=expected)

    assert observed["path"] == path
    assert observed["access"] == 0x00010000 | 0x00000080
    assert int(observed["sharing"]) & 0x00000004 == 0
    assert observed["final_path"] == (123, path)
    assert observed["deleted_handle"] == 123
    assert observed["closed_handle"] == 123


def test_windows_directory_lease_rejects_changed_leaf_identity(tmp_path: Path) -> None:
    api = _FakeWin32DirectoryApi()
    lease = WindowsPinnedDirectoryLease.open(tmp_path, api=api)
    leaf_handle = tuple(api.paths)[-1]
    api.identities[leaf_handle] = WindowsDirectoryIdentity(volume=7, file_index=999, attributes=0x10)

    try:
        with pytest.raises(OSError, match="win32_pinned_directory_identity_changed"):
            lease.require_identity()
    finally:
        lease.close()


def test_windows_file_consumer_holds_written_identity_through_cleanup(tmp_path: Path) -> None:
    api = _FakeWin32DirectoryApi()
    lease = WindowsPinnedDirectoryLease.open(tmp_path, api=api)
    descriptor, path = lease.create_file("attempt.bcp")
    os.write(descriptor, b"held")
    consumer = lease.pin_file_for_consumer("attempt.bcp", writer_descriptor=descriptor)
    os.close(descriptor)

    try:
        process_path, inherited = consumer.prepare_process_input(path)
        assert inherited == ()
        assert Path(process_path).read_bytes() == b"held"
        consumer.cleanup()
        assert not Path(path).exists()
        assert api.deleted == [Path(path)]
    finally:
        consumer.close()
        lease.close()


def test_windows_file_consumer_cleanup_never_deletes_a_replacement(tmp_path: Path) -> None:
    api = _FakeWin32DirectoryApi()
    lease = WindowsPinnedDirectoryLease.open(tmp_path, api=api)
    descriptor, path_value = lease.create_file("attempt.bcp")
    path = Path(path_value)
    os.write(descriptor, b"held")
    consumer = lease.pin_file_for_consumer(path.name, writer_descriptor=descriptor)
    os.close(descriptor)
    displaced = tmp_path / "displaced.bcp"
    path.rename(displaced)
    path.write_bytes(b"replacement")

    try:
        with pytest.raises(OSError, match="win32_cleanup_target_identity_changed"):
            consumer.cleanup()

        assert path.read_bytes() == b"replacement"
        assert displaced.read_bytes() == b"held"
        assert api.deleted == []
    finally:
        consumer.close()
        path.unlink(missing_ok=True)
        displaced.unlink(missing_ok=True)
        lease.close()


def test_windows_preconsumer_failure_cleanup_never_deletes_a_replacement(tmp_path: Path) -> None:
    api = _FakeWin32DirectoryApi()
    lease = WindowsPinnedDirectoryLease.open(tmp_path, api=api)
    directory = PinnedDirectory(
        tmp_path,
        None,
        device=lease.device,
        inode=lease.inode,
        windows_lease=lease,
    )
    writer = DelimitedBulkFile(
        directory=str(tmp_path),
        pinned_directory=directory,
        max_bytes=1024,
    )
    original_delete = api.delete_exact_file
    replacement_paths: list[Path] = []
    displaced_paths: list[Path] = []

    def replace_before_delete(path: Path, *, expected: WindowsDirectoryIdentity) -> None:
        displaced = path.with_name("displaced.bcp")
        path.rename(displaced)
        path.write_bytes(b"replacement\n")
        replacement_paths.append(path)
        displaced_paths.append(displaced)
        original_delete(path, expected=expected)

    api.delete_exact_file = replace_before_delete  # type: ignore[method-assign]

    def fail_during_serialization():
        yield {"value": "original"}
        raise RuntimeError("synthetic_source_failure")

    try:
        with pytest.raises(RuntimeError, match="synthetic_source_failure") as raised:
            writer.write_rows_for_consumer(fail_during_serialization(), ["value"])

        assert raised.value.cleanup_error_code == "mssql_spool_cleanup_failed"
        assert raised.value.residue_possible is True
        assert len(replacement_paths) == len(displaced_paths) == 1
        assert replacement_paths[0].read_bytes() == b"replacement\n"
        assert displaced_paths[0].read_bytes() == b"original\n"
        assert api.deleted == []
    finally:
        for path in (*replacement_paths, *displaced_paths):
            path.unlink(missing_ok=True)
        directory.close()


@pytest.mark.skipif(os.name != "nt", reason="requires Win32 directory sharing semantics")
def test_windows_directory_lease_blocks_path_swap_until_cleanup(tmp_path: Path) -> None:
    work_dir = tmp_path / "spool"
    service = _service(free_bytes=1024)
    admission = service.require_spool_admission(_policy(work_dir, min_free_bytes=0))

    try:
        with pytest.raises(OSError):
            work_dir.rename(tmp_path / "displaced")
    finally:
        admission.close()

    work_dir.rename(tmp_path / "displaced")


def test_bcp_consumer_reads_held_identity_after_final_directory_refresh(tmp_path: Path) -> None:
    work_dir = tmp_path / "spool"
    displaced = tmp_path / "displaced"
    service = _service(free_bytes=1024)
    admission = service.require_spool_admission(_policy(work_dir, min_free_bytes=0))
    writer = DelimitedBulkFile(
        directory=str(admission.work_dir),
        pinned_directory=admission.directory,
        max_bytes=1024,
    )
    path, _count, consumer = writer.write_rows_for_consumer([{"value": "authority"}], ["value"])
    service.refresh_spool_admission(admission)
    replacement: Path | None = None

    if os.name == "nt":
        with pytest.raises(OSError):
            work_dir.rename(displaced)
        with pytest.raises(OSError):
            Path(path).write_text("replacement\n", encoding="utf-8")
        with pytest.raises(OSError):
            Path(path).unlink()
    else:
        work_dir.rename(displaced)
        work_dir.mkdir()
        replacement = work_dir / Path(path).name
        replacement.write_text("replacement\n", encoding="utf-8")

    def run_stub(command, **kwargs):  # noqa: ANN001
        child = [
            sys.executable,
            "-c",
            "from pathlib import Path; import sys; "
            "print(Path(sys.argv[1]).read_text(encoding='utf-8').strip()); "
            "print('1 rows copied.')",
            command[3],
        ]
        return subprocess.run(child, **kwargs)  # noqa: S603 - exact interpreter and test program.

    runner = BcpRunner(
        BcpCredentials(host="localhost", port=1433, database="db", trusted_connection=True),
        BcpOptions(input_file_authority=consumer),
        run=run_stub,
    )
    try:
        result = runner.import_file("[db].[dbo].[target]", path)
        assert result.rows_copied == 1
        assert "authority" in result.stdout
        assert "replacement" not in result.stdout
    finally:
        consumer.cleanup()
        admission.close()

    consumed_path = Path(path) if os.name == "nt" else displaced / Path(path).name
    assert not consumed_path.exists()
    if replacement is not None:
        assert replacement.read_text(encoding="utf-8") == "replacement\n"


@pytest.mark.skipif(os.name == "nt", reason="POSIX descriptor projection owns the unlinked spool")
def test_posix_spool_bcp_and_evidence_share_one_unlinked_descriptor(tmp_path: Path) -> None:
    observed: dict[str, object] = {}

    class ReplacingConnector(_Connector):
        def bcp_import(self, schema, table, file_path, *, options, database=None) -> int:  # noqa: ANN001
            del schema, table, database
            source_name = Path(file_path)
            source_name.unlink(missing_ok=True)
            source_name.write_bytes(b"replacement\n")
            authority = options.input_file_authority
            assert authority is not None
            process_path, inherited = authority.prepare_process_input(file_path)
            observed["payload"] = Path(process_path).read_bytes()
            observed["inherited"] = inherited
            observed["replacement"] = source_name
            return 1

    connector = ReplacingConnector()
    manager = MSSQLStagingManager(
        connector,
        storage_policy=_policy(tmp_path / "spool", min_free_bytes=0),
        storage_preflight_service=_service(free_bytes=1024),
    )

    inserted = manager.insert_rows(_staging(manager), [{"value": "authority"}])

    replacement = observed["replacement"]
    assert inserted == 1
    assert observed["payload"] == b"authority\n"
    assert observed["inherited"]
    assert isinstance(replacement, Path)
    assert replacement.read_bytes() == b"replacement\n"


@pytest.mark.skipif(os.name == "nt", reason="POSIX descriptor identity is mutated with dup2")
def test_posix_consumer_rejects_descriptor_rebinding_before_bcp(tmp_path: Path) -> None:
    replacement = tmp_path / "descriptor-replacement.bcp"
    replacement.write_bytes(b"replacement\n")

    class RebindingConnector(_Connector):
        def bcp_import(self, schema, table, file_path, *, options, database=None) -> int:  # noqa: ANN001
            del schema, table, database
            authority = options.input_file_authority
            assert authority is not None
            descriptor = authority.inherited_file_descriptors[0]
            os.close(descriptor)
            replacement_descriptor = os.open(replacement, os.O_RDONLY)
            if replacement_descriptor != descriptor:
                os.dup2(replacement_descriptor, descriptor)
                os.close(replacement_descriptor)
            authority.prepare_process_input(file_path)
            return 1

    manager = MSSQLStagingManager(
        RebindingConnector(),
        storage_policy=_policy(tmp_path / "spool", min_free_bytes=0),
        storage_preflight_service=_service(free_bytes=1024),
    )

    with pytest.raises(ArtifactIntegrityError) as raised:
        manager.insert_rows(_staging(manager), [{"value": "authority"}])

    assert raised.value.code == "artifact_integrity.file_identity_mismatch"


@pytest.mark.skipif(os.name == "nt", reason="Windows evidence uses its held path/handle contract")
def test_posix_preflight_fails_closed_without_descriptor_pread_before_extract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _Connector()
    sink = MSSQLSink(
        connector,
        state_storage=_TargetAtomicState(),
        runtime_storage_policy=_policy(tmp_path / "spool", min_free_bytes=0),
        storage_preflight_service=_service(free_bytes=1024),
    )
    config = bind_mssql_character_spool_preflight(
        _mssql_load_config(options={}),
        source=_CharacterSpoolSource(),
    )
    monkeypatch.setattr(os, "pread", None)

    with pytest.raises(ArtifactIntegrityError) as raised:
        sink.preflight_before_extract(load_config=config)

    assert raised.value.code == "artifact_integrity.descriptor_read_unsupported"
    assert connector.import_calls == 0
    assert list((tmp_path / "spool").iterdir()) == []


@pytest.mark.skipif(os.name == "nt", reason="Windows pins the consumer with a kernel handle")
def test_posix_consumer_fails_closed_without_a_verified_descriptor_projection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    admission = _service(free_bytes=1024).require_spool_admission(_policy(tmp_path / "spool", min_free_bytes=0))
    writer = DelimitedBulkFile(
        directory=str(admission.work_dir),
        pinned_directory=admission.directory,
        max_bytes=1024,
    )
    original_open = os.open

    def reject_descriptor_projection(path, *args, **kwargs):  # noqa: ANN001
        if os.fspath(path).startswith(("/proc/self/fd/", "/dev/fd/")):
            raise OSError("descriptor projection unavailable")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(os, "open", reject_descriptor_projection)
    try:
        with pytest.raises(OSError, match="pinned_file_descriptor_path_unsupported"):
            writer.write_rows_for_consumer([{"value": "must-not-reach-bcp"}], ["value"])

        assert list(admission.work_dir.iterdir()) == []
    finally:
        admission.close()


def test_bcp_consumer_failure_redacts_private_process_path() -> None:
    sensitive_path = "/tenant-sensitive-segment/spool.bcp"

    class Authority:
        @staticmethod
        def prepare_process_input(_source_path: str) -> tuple[str, tuple[int, ...]]:
            return sensitive_path, ()

    def fail_run(command, **_kwargs):  # noqa: ANN001
        return SimpleNamespace(
            returncode=1,
            stdout="",
            stderr=f"cannot open {command[3]}",
        )

    runner = BcpRunner(
        BcpCredentials(host="localhost", port=1433, database="db", trusted_connection=True),
        BcpOptions(input_file_authority=Authority()),
        run=fail_run,
    )

    with pytest.raises(RuntimeError) as raised:
        runner.import_file("[db].[dbo].[target]", "source.bcp")

    assert sensitive_path not in str(raised.value)
    assert "<pinned-input>" in str(raised.value)


def test_manager_rejects_swap_after_admission_and_cleans_through_pinned_fd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    work_dir = tmp_path / "spool"
    displaced = tmp_path / "displaced"
    connector = _Connector()
    manager = MSSQLStagingManager(
        connector,
        storage_policy=_policy(work_dir, min_free_bytes=0),
        storage_preflight_service=_service(free_bytes=1024),
    )
    original_write = DelimitedBulkFile.write_rows_for_consumer

    def swap_then_write(writer, *args, **kwargs):  # noqa: ANN001
        work_dir.rename(displaced)
        work_dir.mkdir()
        return original_write(writer, *args, **kwargs)

    monkeypatch.setattr(DelimitedBulkFile, "write_rows_for_consumer", swap_then_write)

    with pytest.raises(RuntimeStorageAdmissionError) as raised:
        manager.insert_streaming_rows(_staging(manager), iter(({"value": "one"},)))

    assert raised.value.code == "mssql_spool_work_dir_identity_changed"
    assert connector.import_calls == 0
    assert list(work_dir.iterdir()) == []
    assert list(displaced.iterdir()) == []


def test_spool_admission_pins_a_resolved_symlink_target(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    alias = tmp_path / "current"
    alias.symlink_to(first, target_is_directory=True)
    service = _service(free_bytes=100)

    admission = service.require_spool_admission(_policy(alias))
    alias.unlink()
    alias.symlink_to(second, target_is_directory=True)

    refreshed = service.refresh_spool_admission(admission)
    assert refreshed.work_dir == first.resolve()
    refreshed.close()


def test_delimited_writer_enforces_exact_utf8_byte_cap_and_removes_partial_file(tmp_path: Path) -> None:
    writer = DelimitedBulkFile(directory=str(tmp_path), max_bytes=6)

    with pytest.raises(MssqlSpoolCapacityError) as raised:
        writer.write_rows([{"value": "abcdef"}], ["value"])

    assert raised.value.code == "mssql_spool_byte_limit_exceeded"
    assert list(tmp_path.iterdir()) == []


def test_delimited_writer_without_storage_policy_preserves_system_temp_fallback() -> None:
    writer = DelimitedBulkFile()
    path, count = writer.write_rows([{"value": "legacy"}], ["value"])

    try:
        assert count == 1
        assert Path(path).parent.resolve() == Path(tempfile.gettempdir()).resolve()
    finally:
        Path(path).unlink(missing_ok=True)


def test_partial_write_unlink_failure_emits_stable_residue_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service(free_bytes=100)
    admission = service.require_spool_admission(_policy(tmp_path / "spool", min_free_bytes=0))
    writer = DelimitedBulkFile(
        directory=str(admission.work_dir),
        pinned_directory=admission.directory,
        max_bytes=3,
    )
    original_unlink = PinnedDirectory.unlink

    def fail_spool_unlink(directory, entry_name, *, missing_ok=False):  # noqa: ANN001
        if str(entry_name).endswith(".bcp") and directory is admission.directory:
            raise OSError("sensitive path must not escape")
        return original_unlink(directory, entry_name, missing_ok=missing_ok)

    monkeypatch.setattr(PinnedDirectory, "unlink", fail_spool_unlink)
    try:
        with pytest.raises(MssqlSpoolCapacityError) as raised:
            writer.write_rows(
                ({"value": "a"}, {"value": "too-large"}),
                ["value"],
            )

        assert raised.value.code == "mssql_spool_byte_limit_exceeded"
        assert raised.value.cleanup_error_code == "mssql_spool_cleanup_failed"
        assert raised.value.residue_possible is True
        assert getattr(raised.value, "__notes__", ()) == ["mssql_spool_cleanup_failed"]
        assert "sensitive" not in str(raised.value)
        residue = next(admission.work_dir.glob("*.bcp"))
    finally:
        monkeypatch.setattr(PinnedDirectory, "unlink", original_unlink)
        if "residue" in locals():
            original_unlink(admission.directory, residue.name)
        admission.close()


def test_spool_write_os_error_is_stable_and_path_free(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = MSSQLStagingManager(
        _Connector(),
        storage_policy=_policy(tmp_path / "tenant-sensitive-segment", min_free_bytes=0),
        storage_preflight_service=_service(free_bytes=1024),
    )

    def fail_write(*_args, **_kwargs):
        raise OSError("disk failure at /tenant-sensitive-segment/spool.bcp")

    monkeypatch.setattr(DelimitedBulkFile, "write_rows_for_consumer", fail_write)

    with pytest.raises(RuntimeStorageAdmissionError) as raised:
        manager.insert_streaming_rows(_staging(manager), iter(({"value": "one"},)))

    assert raised.value.code == "mssql_spool_write_failed"
    assert str(raised.value) == "mssql_spool_write_failed"
    assert raised.value.__cause__ is None
    assert "tenant-sensitive-segment" not in str(raised.value)


@pytest.mark.parametrize("fail_import", (False, True), ids=("success", "failure"))
def test_manager_spools_only_inside_policy_work_dir_and_eagerly_cleans(
    tmp_path: Path,
    fail_import: bool,
) -> None:
    work_dir = tmp_path / "mounted" / "spool"
    observed: list[Path] = []

    def observe(path: Path) -> None:
        assert path.exists()
        if os.name != "nt":
            assert stat.S_IMODE(path.stat().st_mode) == 0o400
        observed.append(path)

    connector = _Connector(on_import=observe, fail_import=fail_import)
    manager = MSSQLStagingManager(
        connector,
        storage_policy=_policy(work_dir, min_free_bytes=0),
        storage_preflight_service=_service(free_bytes=1024),
    )

    if fail_import:
        with pytest.raises(RuntimeError, match="synthetic_bcp_failure"):
            manager.insert_streaming_rows(_staging(manager), iter(({"value": "one"},)))
    else:
        assert manager.insert_streaming_rows(_staging(manager), iter(({"value": "one"},))) == 1

    assert len(observed) == 1
    assert connector.import_options is not None
    assert connector.import_options.input_file_authority is not None
    assert connector.import_source_path is not None
    assert connector.import_source_path.parent == work_dir.resolve()
    assert not connector.import_source_path.exists()
    with pytest.raises(OSError, match="pinned_file_consumer_closed"):
        connector.import_options.input_file_authority.prepare_process_input(str(connector.import_source_path))
    assert not observed[0].exists()
    assert list(work_dir.iterdir()) == []


@pytest.mark.skipif(os.name == "nt", reason="POSIX removes the authored basename before BCP")
def test_posix_path_replacement_cannot_redirect_bcp_evidence_or_cleanup(tmp_path: Path) -> None:
    work_dir = tmp_path / "spool"
    replacement_payload = b"replacement\n"
    consumed_payloads: list[bytes] = []

    def replace_authored_path(process_path: Path) -> None:
        authored_path = connector.import_source_path
        assert authored_path is not None
        assert not authored_path.exists()
        authored_path.write_bytes(replacement_payload)
        consumed_payloads.append(process_path.read_bytes())

    connector = _Connector(on_import=replace_authored_path)
    manager = MSSQLStagingManager(
        connector,
        storage_policy=_policy(work_dir, min_free_bytes=0),
        storage_preflight_service=_service(free_bytes=1024),
    )
    staging = _staging(manager)

    assert manager.insert_streaming_rows(staging, iter(({"value": "one"},))) == 1

    authored_path = connector.import_source_path
    assert authored_path is not None
    assert consumed_payloads == [b"one\n"]
    assert authored_path.read_bytes() == replacement_payload
    evidence = staging.consumed_payload_evidence
    assert evidence is not None
    assert len(evidence.parts) == 1
    assert evidence.parts[0].artifact_sha256 == hashlib.sha256(consumed_payloads[0]).hexdigest()
    assert evidence.parts[0].artifact_sha256 != hashlib.sha256(replacement_payload).hexdigest()

    authored_path.unlink()
    assert list(work_dir.iterdir()) == []


@pytest.mark.skipif(os.name == "nt", reason="POSIX consumer files are anonymous before serialization")
def test_posix_consumer_file_has_no_basename_during_source_iteration(tmp_path: Path) -> None:
    admission = _service(free_bytes=1024).require_spool_admission(_policy(tmp_path / "spool", min_free_bytes=0))
    writer = DelimitedBulkFile(
        directory=str(admission.work_dir),
        pinned_directory=admission.directory,
        max_bytes=1024,
    )
    directory_snapshots: list[list[Path]] = []

    def rows():
        directory_snapshots.append(list(admission.work_dir.iterdir()))
        yield {"value": "original"}

    path, count, consumer = writer.write_rows_for_consumer(rows(), ["value"])
    try:
        import fcntl

        process_path, descriptors = consumer.prepare_process_input(path)
        assert count == 1
        assert directory_snapshots == [[]]
        assert len(descriptors) == 1
        assert int(fcntl.fcntl(descriptors[0], fcntl.F_GETFL)) & os.O_ACCMODE == os.O_RDONLY
        assert Path(process_path).read_bytes() == b"original\n"
        assert not Path(path).exists()
    finally:
        consumer.cleanup()
        admission.close()

    assert list(Path(path).parent.iterdir()) == []


@pytest.mark.skipif(os.name == "nt", reason="POSIX pinning must never unlink a rebound basename")
def test_posix_replacement_created_before_pin_survives_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    admission = _service(free_bytes=1024).require_spool_admission(_policy(tmp_path / "spool", min_free_bytes=0))
    writer = DelimitedBulkFile(
        directory=str(admission.work_dir),
        pinned_directory=admission.directory,
        max_bytes=1024,
    )
    original_pin = PinnedDirectory.pin_file_for_consumer
    replacement_paths: list[Path] = []

    def replace_then_pin(directory, entry_name, **kwargs):  # noqa: ANN001
        replacement = directory.path / entry_name
        assert not replacement.exists()
        replacement.write_bytes(b"replacement\n")
        replacement_paths.append(replacement)
        return original_pin(directory, entry_name, **kwargs)

    monkeypatch.setattr(PinnedDirectory, "pin_file_for_consumer", replace_then_pin)
    path, _count, consumer = writer.write_rows_for_consumer([{"value": "original"}], ["value"])
    try:
        process_path, _descriptors = consumer.prepare_process_input(path)
        assert Path(process_path).read_bytes() == b"original\n"
    finally:
        consumer.cleanup()
        admission.close()

    assert replacement_paths == [Path(path)]
    assert Path(path).read_bytes() == b"replacement\n"
    Path(path).unlink()


def test_cleanup_failure_does_not_replace_the_primary_bcp_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    work_dir = tmp_path / "spool"
    observed: list[Path] = []
    connector = _Connector(on_import=observed.append, fail_import=True)
    manager = MSSQLStagingManager(
        connector,
        storage_policy=_policy(work_dir, min_free_bytes=0),
        storage_preflight_service=_service(free_bytes=1024),
    )
    original_cleanup = PinnedFileConsumer.cleanup

    def cleanup_then_fail(consumer: PinnedFileConsumer) -> None:
        original_cleanup(consumer)
        raise OSError("sensitive cleanup detail")

    monkeypatch.setattr(PinnedFileConsumer, "cleanup", cleanup_then_fail)
    with pytest.raises(RuntimeError, match="synthetic_bcp_failure") as raised:
        manager.insert_streaming_rows(_staging(manager), iter(({"value": "one"},)))

    assert getattr(raised.value, "__notes__", ()) == ["mssql_spool_cleanup_failed"]
    assert raised.value.cleanup_error_code == "mssql_spool_cleanup_failed"
    assert raised.value.residue_possible is True
    assert len(observed) == 1
    assert not observed[0].exists()
    assert list(work_dir.iterdir()) == []


def test_cleanup_failure_after_success_is_stable_and_path_free(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    work_dir = tmp_path / "spool"
    observed: list[Path] = []
    connector = _Connector(on_import=observed.append)
    manager = MSSQLStagingManager(
        connector,
        storage_policy=_policy(work_dir, min_free_bytes=0),
        storage_preflight_service=_service(free_bytes=1024),
    )
    original_cleanup = PinnedFileConsumer.cleanup

    def cleanup_then_fail(consumer: PinnedFileConsumer) -> None:
        original_cleanup(consumer)
        raise OSError("sensitive cleanup detail")

    monkeypatch.setattr(PinnedFileConsumer, "cleanup", cleanup_then_fail)
    with pytest.raises(RuntimeStorageAdmissionError) as raised:
        manager.insert_streaming_rows(_staging(manager), iter(({"value": "one"},)))

    assert raised.value.code == "mssql_spool_cleanup_failed"
    assert raised.value.cleanup_error_code == "mssql_spool_cleanup_failed"
    assert raised.value.residue_possible is True
    assert str(raised.value) == "mssql_spool_cleanup_failed"
    assert raised.value.__cause__ is None
    assert len(observed) == 1
    assert not observed[0].exists()
    assert list(work_dir.iterdir()) == []


def test_directory_release_failure_after_success_is_stable_and_path_free(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    work_dir = tmp_path / "spool"
    manager = MSSQLStagingManager(
        _Connector(),
        storage_policy=_policy(work_dir, min_free_bytes=0),
        storage_preflight_service=_service(free_bytes=1024),
    )
    original_close = PinnedDirectory.close

    def close_then_fail(directory):  # noqa: ANN001
        original_close(directory)
        raise OSError("sensitive release detail")

    monkeypatch.setattr(PinnedDirectory, "close", close_then_fail)

    with pytest.raises(RuntimeStorageAdmissionError) as raised:
        manager.insert_streaming_rows(_staging(manager), iter(({"value": "one"},)))

    assert raised.value.code == "mssql_spool_directory_release_failed"
    assert str(raised.value) == "mssql_spool_directory_release_failed"
    assert "sensitive" not in str(raised.value)
    assert list(work_dir.iterdir()) == []


def test_directory_release_failure_does_not_replace_primary_bcp_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    work_dir = tmp_path / "spool"
    manager = MSSQLStagingManager(
        _Connector(fail_import=True),
        storage_policy=_policy(work_dir, min_free_bytes=0),
        storage_preflight_service=_service(free_bytes=1024),
    )
    original_close = PinnedDirectory.close

    def close_then_fail(directory):  # noqa: ANN001
        original_close(directory)
        raise OSError("sensitive release detail")

    monkeypatch.setattr(PinnedDirectory, "close", close_then_fail)

    with pytest.raises(RuntimeError, match="synthetic_bcp_failure") as raised:
        manager.insert_streaming_rows(_staging(manager), iter(({"value": "one"},)))

    assert raised.value.directory_release_error_code == "mssql_spool_directory_release_failed"
    assert getattr(raised.value, "__notes__", ()) == ["mssql_spool_directory_release_failed"]
    assert "sensitive" not in str(raised.value)
    assert list(work_dir.iterdir()) == []


def test_manager_without_explicit_policy_keeps_system_temp_fallback() -> None:
    manager = MSSQLStagingManager(_Connector())

    assert manager.storage_policy.work_dir == RuntimeStoragePolicy.from_options({}).work_dir
    assert any("system temp" in warning for warning in manager.storage_policy.warnings)


def test_sink_factory_injects_runtime_storage_policy_into_mssql_spool(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = _policy(tmp_path / "injected", min_free_bytes=0)
    connector = _Connector()
    monkeypatch.setattr(SinkFactory, "_create_mssql_connector", lambda *_args, **_kwargs: connector)

    sink = SinkFactory.create(
        connection_id="mssql-target",
        state_storage=None,
        credentials_source="params",
        connection_type="mssql",
        runtime_storage_policy=policy,
    )

    assert isinstance(sink, MSSQLSink)
    assert sink.staging_manager.storage_policy is policy


class _TargetAtomicState:
    atomicity = "target_atomic"
    provisioning = "external"


class _CharacterSpoolSource:
    @staticmethod
    def mssql_character_spool_preflight_requirement(_load_config):
        return "local_character_spool_v1"


class _NonSpoolSource:
    pass


def _mssql_load_config(*, options: dict[str, object]) -> LoadConfig:
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="source",
        source_table="events",
        target_schema="dbo",
        target_table="events",
        target_database="DWH_Dev",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={"source_type": "postgres", "sink_type": "mssql", **options},
    )


@pytest.mark.parametrize(
    "route_options",
    (
        {"__dpone_mssql_native_staging": True},
        {"__dpone_mssql_typed_file_staging_v1": True},
        {"internal_query": True},
        {"batch_commit_mode": "whole"},
        {"__dpone_mssql_character_spool_preflight_v1": True},
    ),
    ids=("native", "typed-file", "internal-query", "source-file", "forged-proof"),
)
def test_sink_preflight_does_not_apply_character_spool_floor_without_route_proof(
    tmp_path: Path,
    route_options: dict[str, object],
) -> None:
    sink = MSSQLSink(
        _Connector(),
        state_storage=_TargetAtomicState(),
        runtime_storage_policy=_policy(tmp_path / "spool", min_free_bytes=1024),
        storage_preflight_service=_service(free_bytes=1),
    )

    config = bind_mssql_character_spool_preflight(
        _mssql_load_config(options=route_options),
        source=_NonSpoolSource(),
    )

    sink.preflight_before_extract(load_config=config)


def test_sink_preflight_applies_floor_only_with_runtime_issued_character_spool_proof(
    tmp_path: Path,
) -> None:
    sink = MSSQLSink(
        _Connector(),
        state_storage=_TargetAtomicState(),
        runtime_storage_policy=_policy(tmp_path / "spool", min_free_bytes=1024),
        storage_preflight_service=_service(free_bytes=1),
    )
    config = bind_mssql_character_spool_preflight(
        _mssql_load_config(options={}),
        source=_CharacterSpoolSource(),
    )

    with pytest.raises(RuntimeStorageAdmissionError) as raised:
        sink.preflight_before_extract(load_config=config)

    assert raised.value.code == "mssql_spool_work_dir_low_space"


def test_runtime_spool_proof_is_excluded_from_persisted_route_identity() -> None:
    config = _mssql_load_config(options={"source_type": "clickhouse"})
    bound = bind_mssql_character_spool_preflight(config, source=_CharacterSpoolSource())
    source_identity = SourcePhysicalIdentity(
        dialect="clickhouse",
        cluster_identifier="cluster-a",
        database="marketing",
        effective_principal="reader",
        session_principal="reader",
    )

    expected = invocation_route_fingerprint(
        config,
        target_identity=b"t" * 32,
        source_identity=source_identity,
    )

    assert (
        invocation_route_fingerprint(
            bound,
            target_identity=b"t" * 32,
            source_identity=source_identity,
        )
        == expected
    )


@pytest.mark.parametrize(
    ("options", "expected"),
    (
        ({}, "local_character_spool_v1"),
        ({"query": {"sql": "SELECT 1"}}, None),
        ({"export_to_gcs": True}, None),
    ),
    ids=("row-stream", "query", "gcs"),
)
def test_clickhouse_issues_preflight_proof_only_for_deterministic_row_stream(
    options: dict[str, object],
    expected: str | None,
) -> None:
    source = ClickHouseSource(object(), logger=object())
    config = _mssql_load_config(options={"source_type": "clickhouse", **options})

    assert source.mssql_character_spool_preflight_requirement(config) == expected


@pytest.mark.parametrize(
    "options",
    (
        {"query": {"sql": "SELECT 1"}},
        {"export_to_gcs": True},
    ),
    ids=("query", "gcs"),
)
def test_clickhouse_non_spool_route_binding_does_not_apply_storage_floor(
    tmp_path: Path,
    options: dict[str, object],
) -> None:
    source = ClickHouseSource(object(), logger=object())
    config = bind_mssql_character_spool_preflight(
        _mssql_load_config(options={"source_type": "clickhouse", **options}),
        source=source,
    )
    sink = MSSQLSink(
        _Connector(),
        state_storage=_TargetAtomicState(),
        runtime_storage_policy=_policy(tmp_path / "spool", min_free_bytes=1024),
        storage_preflight_service=_service(free_bytes=1),
    )

    sink.preflight_before_extract(load_config=config)
