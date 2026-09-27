"""Opt-in BCP attempt coordinator with durable stage and writer boundaries."""

from __future__ import annotations

import secrets
from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from dpone.ports.mssql_native_writer import NativeStageWriteGrant
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.mssql_native_chunks_files import verify_native_file


class NativeTargetLocalAttempt:
    """Bind one sealed attempt to supervised BCP and an exact-stage aggregate."""

    def __init__(
        self, journal: Any, custody: Any, writer: Any, barrier: Callable[..., Any], unknown_error: type[Exception]
    ) -> None:
        self.journal, self.custody, self.writer, self.barrier = journal, custody, writer, barrier
        self.unknown_error = unknown_error

    @staticmethod
    def _observation(
        outcome: str,
        consumed: int | None,
        *,
        quiescence: str = "unverified",
        row_count: int | None = None,
        limbs: list[str] | None = None,
        diagnostic: str = "mssql_native.writer_observed",
    ) -> dict[str, Any]:
        return dict(
            writer_outcome=outcome,
            input_rows_consumed=consumed,
            row_count=row_count,
            count_overflow=False if row_count is not None else None,
            limbs=limbs,
            quiescence=quiescence,
            diagnostic_code=diagnostic,
        )

    def import_file(self, importer: Any, plan: Any, file: Any, attempt_id: str, lease: Any, artifact: Any) -> Any:
        """Append proof only after its real stage, process, and SQL boundary."""
        if (
            self.journal.attempt_id(file.ordinal, self.journal.data["chunks"][str(file.ordinal)]["attempt"])
            != attempt_id
        ):
            raise ValueError("mssql_native.attempt_identity_changed")
        identity = self.journal.identity
        table = importer.table_name(plan, attempt_id)
        qualified = importer.qualified(table)
        with importer._mutation_scope(plan, attempt_id, lease):
            importer._assert_lease(lease)
            importer._create_owned_stage(plan, attempt_id, table)
            object_id = importer._object_id(table)
            importer._assert_stage_identity(plan, attempt_id, table, object_id)
            stage = dict(
                stage_id=self.journal.opaque_stage_id(qualified),
                owner_binding_sha256=importer._ownership(plan, attempt_id)["binding"],
                object_id=object_id,
                schema_sha256=identity.capability_layout_sha256,
            )
            self.journal.append_event(file.ordinal, attempt_id, "STAGE_OWNED", stage_binding=stage)
            self.custody.reassert_grant(lease, identity.invocation_key)
            token = secrets.token_hex(32)
            grant = NativeStageWriteGrant(
                attempt_id,
                qualified,
                file.path,
                file.rows,
                file.encoded_bytes,
                file.file_sha256,
                token,
                identity.writer_proof_capability,
            )
            writer_binding = dict(
                import_backend=identity.import_backend,
                writer_proof_capability=identity.writer_proof_capability,
                protocol_sha256=identity.companion_protocol_sha256,
                package_sha256=identity.companion_package_sha256,
                capability_sha256=identity.capability_layout_sha256,
                grant_token_sha256=grant.grant_token_sha256,
                timeout_policy_sha256=identity.timeout_policy_sha256,
            )
            self.journal.append_event(file.ordinal, attempt_id, "GRANTED", writer_binding=writer_binding)
            with TemporaryDirectory(prefix="dpone-native-rejects-", dir=file.path.parent) as directory:
                rejects = Path(directory) / "rejects.txt"
                self.journal.append_event(file.ordinal, attempt_id, "WRITING")
                try:
                    outcome = self.writer.write(grant, rejects_path=rejects)
                except Exception:
                    self.journal.append_event(
                        file.ordinal,
                        attempt_id,
                        "UNKNOWN",
                        observation=self._observation("custody_lost", None, diagnostic="mssql_native.writer_uncertain"),
                    )
                    raise
            observed = self._observation(outcome.classification, outcome.rows_consumed)
            if outcome.classification == "failure":
                self.journal.append_event(file.ordinal, attempt_id, "WRITER_TERMINAL", observation=observed)
            if not outcome.positive_terminal:
                self.journal.append_event(file.ordinal, attempt_id, "UNKNOWN", observation=observed)
                raise self.unknown_error("mssql_native.writer_outcome_unknown")
            self.journal.append_event(file.ordinal, attempt_id, "WRITER_TERMINAL", observation=observed)
            try:
                with self.barrier(
                    qualified, lambda: importer._assert_stage_identity(plan, attempt_id, table, object_id)
                ):
                    digest, aggregate = importer._target_digest(table, file)
            except Exception:
                self.journal.append_event(
                    file.ordinal,
                    attempt_id,
                    "UNKNOWN",
                    observation=self._observation(
                        "success", file.rows, quiescence="failed", diagnostic="mssql_native.stage_observation_failed"
                    ),
                )
                raise
            self.journal.append_event(
                file.ordinal,
                attempt_id,
                "QUIESCENT",
                observation=self._observation("success", file.rows, quiescence="proved"),
            )
            self.journal.append_event(
                file.ordinal,
                attempt_id,
                "VERIFIED",
                observation=self._observation(
                    "success",
                    file.rows,
                    quiescence="proved",
                    row_count=digest.rows,
                    limbs=[str(value) for value in aggregate[2:]],
                    diagnostic="mssql_native.verified",
                ),
            )
            return importer._receipt(plan, file, attempt_id, object_id, digest.typed_sum, artifact)

    def inspect(self, importer: Any, plan: Any, receipt: Any, lease: Any) -> Any:
        """Repeat exact-stage aggregate verification without business-row reads."""
        table = importer.table_name(plan, receipt.attempt_id)
        part = receipt.consumed_part_evidence
        object_id = part.get("native_object_id")
        if type(object_id) is not int or object_id < 1:
            raise ValueError("mssql_native.stage_identity_mismatch")
        importer._assert_lease(lease)
        with self.barrier(
            receipt.stage_id,
            lambda: importer._assert_stage_identity(plan, receipt.attempt_id, table, object_id),
        ):
            observed = importer._verify_contents(table, receipt.rows, receipt.typed_digest)
            if part.get("native_typed_sum") != observed:
                raise ValueError("mssql_native.typed_digest_mismatch")
        return receipt

    def recover_positive(self, importer: Any, plan: Any, file: Any, attempt_id: str, lease: Any) -> Any:
        """Observe a durable positive terminal under a fresh barrier; never relaunch."""
        importer._assert_lease(lease)
        verify_native_file(file)
        events = self.journal.data["events"][attempt_id]
        object_id = events[-1]["stage_binding"]["object_id"]
        table = importer.table_name(plan, attempt_id)
        qualified = importer.qualified(table)
        artifact = FileExportArtifact(
            str(file.path),
            tuple(column.name for column in importer.columns),
            format="mssql-native",
            rows_exported=file.rows,
        )
        integrity = artifact.require_integrity_receipt()
        if integrity.sha256 != file.file_sha256 or integrity.size_bytes != file.encoded_bytes:
            raise ValueError("mssql_native.file_identity_changed")

        def assert_identity() -> None:
            importer._assert_stage_identity(plan, attempt_id, table, object_id)

        def observe() -> tuple[dict[str, Any], Any]:
            digest, aggregate = importer._target_digest(table, file)
            observation = self._observation(
                "success",
                file.rows,
                quiescence="proved",
                row_count=digest.rows,
                limbs=[str(value) for value in aggregate[2:]],
                diagnostic="mssql_native.verified",
            )
            receipt = importer._receipt(plan, file, attempt_id, object_id, digest.typed_sum, artifact)
            return observation, receipt

        return self.journal.recover_bcp_verified(
            file.ordinal,
            attempt_id,
            barrier=lambda: self.barrier(qualified, assert_identity),
            observe=observe,
        )

    def settle_published(self, importer: Any, plan: Any, receipt: Any, lease: Any) -> None:
        """Drop only a completed receipt after durable publication and state success."""
        publication = self.journal.publication.state()
        complete = self.journal.completed()
        if (
            publication is None
            or publication["phase"] != "succeeded"
            or complete is None
            or receipt not in complete.receipts
        ):
            raise ValueError("mssql_native.published_cleanup_publication_required")
        self.drop_exact_owned(importer, plan, receipt, lease)

    @staticmethod
    def drop_exact_owned(importer: Any, plan: Any, receipt: Any, lease: Any) -> None:
        """Perform an exact-owner drop only after the caller's durable authority."""
        table = importer.table_name(plan, receipt.attempt_id)
        if receipt.stage_id != importer.qualified(table):
            raise ValueError("mssql_native.stage_identity_mismatch")
        object_id = receipt.consumed_part_evidence.get("native_object_id")
        if type(object_id) is not int or object_id < 1:
            raise ValueError("mssql_native.stage_identity_mismatch")
        with importer._mutation_scope(plan, receipt.attempt_id, lease):
            importer._assert_lease(lease)
            importer._assert_stage_identity(plan, receipt.attempt_id, table, object_id)
            importer.connector.execute_query(f"DROP TABLE {receipt.stage_id}")
            importer._assert_lease(lease)
