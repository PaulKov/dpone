"""Capacity-bound local spool lifecycle for character-mode MSSQL BCP."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from dpone.runtime.artifact_integrity import FileWireContract
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.bulk_options import BulkOptionsResolver
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.pinned_file_consumer import PinnedFileConsumer
from dpone.runtime.storage_policy import (
    MSSQL_SPOOL_DIRECTORY_RELEASE_ERROR_CODE,
    RuntimeSpoolAdmission,
    RuntimeStorageAdmissionError,
    RuntimeStoragePolicy,
    StoragePreflightService,
    mark_mssql_spool_directory_release_failure,
)
from dpone.runtime.support.bulk_text_codec import BulkTextCodec
from dpone.runtime.support.mssql_bcp_values import (
    DelimitedBulkFile,
    mark_mssql_spool_cleanup_failure,
)


@dataclass(frozen=True, slots=True)
class MssqlSpoolWrite:
    """One complete spool file and its immutable row/codec interpretation."""

    path: str
    rows: int
    text_codec: BulkTextCodec
    admission: RuntimeSpoolAdmission
    input_file_authority: PinnedFileConsumer


class MssqlCharacterSpool:
    """Provision, revalidate, bound, and eagerly clean one BCP spool file."""

    def __init__(
        self,
        policy: RuntimeStoragePolicy,
        preflight_service: StoragePreflightService,
    ) -> None:
        self.policy = policy
        self._preflight_service = preflight_service

    def preflight(self) -> None:
        """Pin the work-directory identity before source extraction starts."""

        admission = self._preflight_service.require_spool_admission(self.policy)
        try:
            admission.directory.probe_consumer_file()
        except OSError:
            error = RuntimeStorageAdmissionError("mssql_spool_work_dir_not_writable")
            try:
                admission.close()
            except OSError:
                mark_mssql_spool_directory_release_failure(error)
            raise error from None
        except BaseException as error:
            try:
                admission.close()
            except OSError:
                mark_mssql_spool_directory_release_failure(error)
            raise
        try:
            admission.close()
        except OSError:
            raise RuntimeStorageAdmissionError(MSSQL_SPOOL_DIRECTORY_RELEASE_ERROR_CODE) from None

    def import_rows(
        self,
        artifact: StagingTableArtifact,
        rows: Iterable[Mapping[str, object]],
        *,
        connector: Any,
        importer: Callable[..., int],
    ) -> int:
        """Materialize, import, and clean one row stream through BCP."""

        with self.materialize(rows, artifact.columns, column_types=artifact.column_types) as spool:
            self._preflight_service.refresh_spool_admission(spool.admission)
            wire_contract = FileWireContract.resolve(
                columns=tuple(str(column) for column in artifact.columns),
                format="mssql-delimited",
                compressed=False,
                has_header=False,
                bulk_text_codec=spool.text_codec,
            )
            integrity_receipt = spool.input_file_authority.capture_integrity_receipt(
                wire_contract,
                rows_exported=spool.rows,
            )
            file_artifact = FileExportArtifact(
                spool.path,
                artifact.columns,
                format="mssql-delimited",
                bulk_text_codec=spool.text_codec,
                rows_exported=spool.rows,
                _integrity_receipt=integrity_receipt,
            )
            artifact.bulk_text_codec = spool.text_codec
            bulk = artifact.bulk_options or BulkOptionsResolver.resolve({})
            options = bulk.bcp.to_bcp_options(
                bcp_path=getattr(connector, "bcp_path", "bcp"),
                trust_server_certificate=connector.trust_server_certificate == "yes",
            )
            options = replace(options, input_file_authority=spool.input_file_authority)
            # The surrounding materialization context is the single physical
            # lifecycle owner.  It preserves a primary BCP failure if unlink
            # also fails, instead of letting a nested artifact cleanup replace
            # the authoritative import error.
            return importer(artifact, file_artifact, options=options)

    @contextmanager
    def materialize(
        self,
        rows: Iterable[Mapping[str, object]],
        columns: Sequence[str],
        *,
        column_types: Mapping[str, str],
    ) -> Iterator[MssqlSpoolWrite]:
        """Yield one complete bounded spool and delete it for every outcome."""

        admission = self._preflight_service.require_spool_admission(self.policy)
        text_codec = BulkTextCodec()
        writer = DelimitedBulkFile(
            field_terminator="\t",
            row_terminator="\n",
            unsafe_text_policy="fail",
            text_codec=text_codec,
            directory=str(admission.work_dir),
            pinned_directory=admission.directory,
            max_bytes=admission.max_spool_bytes,
        )
        path: str | None = None
        consumer: PinnedFileConsumer | None = None
        primary_error: BaseException | None = None
        try:
            try:
                path, count, consumer = writer.write_rows_for_consumer(
                    rows,
                    columns,
                    column_types=column_types,
                )
            except OSError as exc:
                # OSError text commonly embeds the configured work path.  It
                # is operationally secondary to the stable spool contract and
                # must not enter public run evidence.
                stable = RuntimeStorageAdmissionError("mssql_spool_write_failed")
                if bool(getattr(exc, "residue_possible", False)):
                    mark_mssql_spool_cleanup_failure(stable)
                raise stable from None
            admission = self._preflight_service.refresh_spool_admission(admission)
            yield MssqlSpoolWrite(
                path=path,
                rows=count,
                text_codec=text_codec,
                admission=admission,
                input_file_authority=consumer,
            )
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            secondary_error: RuntimeStorageAdmissionError | None = None
            cleanup_failed = False
            try:
                if consumer is not None:
                    consumer.cleanup()
                elif path is not None:  # pragma: no cover - atomic writer return contract.
                    admission.directory.unlink(Path(path).name, missing_ok=True)
            except OSError:
                cleanup_failed = True
            if cleanup_failed:
                if primary_error is None:
                    secondary_error = RuntimeStorageAdmissionError("mssql_spool_cleanup_failed")
                    mark_mssql_spool_cleanup_failure(secondary_error)
                else:
                    mark_mssql_spool_cleanup_failure(primary_error)
            try:
                admission.close()
            except OSError:
                release_owner = primary_error or secondary_error
                if release_owner is None:
                    secondary_error = RuntimeStorageAdmissionError(MSSQL_SPOOL_DIRECTORY_RELEASE_ERROR_CODE)
                else:
                    mark_mssql_spool_directory_release_failure(release_owner)
            if primary_error is None and secondary_error is not None:
                raise secondary_error from None


__all__ = ["MssqlCharacterSpool", "MssqlSpoolWrite"]
