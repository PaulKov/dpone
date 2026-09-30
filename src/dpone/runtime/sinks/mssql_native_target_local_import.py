"""Opt-in BCP attempt coordinator with durable stage and writer boundaries."""

from __future__ import annotations

import secrets
import time
from collections.abc import Callable
from functools import partial
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from dpone.ports.mssql_native import (
    SQLCLIENT_SESSION_PROOF,
    NativeStageColumnMapping,
    NativeStageWriteGrant,
    NativeStageWriteRequest,
    OperationDeadline,
)
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.mssql_native_target_local_recovery import retire_exact_owned_stage
from dpone.runtime.sinks.mssql_native_target_local_evidence import (
    aggregate_limbs,
    verified_receipt,
    writer_observation,
)


class NativeTargetLocalAttempt:
    """Bind one sealed attempt to supervised BCP and an exact-stage aggregate."""

    _observation = staticmethod(writer_observation)

    def __init__(
        self,
        journal: Any,
        custody: Any,
        writer: Any,
        barrier: Callable[..., Any],
        unknown_error: type[Exception],
        verify_file: Callable[[Any], None],
        retirement_timeout_seconds: int = 3600,
        *,
        timeout_seconds: int | None = None,
        max_row_bytes: int | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.journal, self.custody, self.writer, self.barrier = journal, custody, writer, barrier
        self.unknown_error = unknown_error
        self.verify_file = verify_file
        self.retirement_timeout_seconds = retirement_timeout_seconds
        self.timeout_seconds = retirement_timeout_seconds if timeout_seconds is None else timeout_seconds
        self.max_row_bytes = max_row_bytes
        self.clock = clock
        if type(self.timeout_seconds) is not int or self.timeout_seconds < 1:
            raise ValueError("mssql_native.invalid_writer_timeout")

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
            grant_token_sha256 = sha256(secrets.token_bytes(32)).hexdigest()
            grant = self._grant(importer, file, attempt_id, qualified, stage, grant_token_sha256)
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
                    if identity.writer_proof_capability == SQLCLIENT_SESSION_PROOF:
                        outcome = self.writer.write(
                            grant,
                            deadline=OperationDeadline(float(self.clock() + self.timeout_seconds), clock=self.clock),
                        )
                    else:
                        outcome = self.writer.write(grant, rejects_path=rejects)
                except Exception:
                    self.journal.append_event(
                        file.ordinal,
                        attempt_id,
                        "UNKNOWN",
                        observation=writer_observation(
                            "custody_lost", None, diagnostic="mssql_native.writer_uncertain"
                        ),
                    )
                    raise
            consumed = getattr(outcome, "input_rows_consumed", getattr(outcome, "rows_consumed", None))
            observed = writer_observation(outcome.classification, consumed)
            if outcome.classification == "failure":
                self.journal.append_event(file.ordinal, attempt_id, "WRITER_TERMINAL", observation=observed)
            if not outcome.positive_terminal:
                self.journal.append_event(file.ordinal, attempt_id, "UNKNOWN", observation=observed)
                raise self.unknown_error("mssql_native.writer_outcome_unknown")
            self.journal.append_event(file.ordinal, attempt_id, "WRITER_TERMINAL", observation=observed)
            try:
                barrier = (
                    self.barrier(
                        qualified,
                        grant_token_sha256,
                        lambda: importer._assert_stage_identity(plan, attempt_id, table, object_id),
                    )
                    if identity.writer_proof_capability == SQLCLIENT_SESSION_PROOF
                    else self.barrier(
                        qualified, lambda: importer._assert_stage_identity(plan, attempt_id, table, object_id)
                    )
                )
                with barrier:
                    digest, aggregate = importer._target_digest(table, file)
            except Exception:
                self.journal.append_event(
                    file.ordinal,
                    attempt_id,
                    "UNKNOWN",
                    observation=writer_observation(
                        "success", file.rows, quiescence="failed", diagnostic="mssql_native.stage_observation_failed"
                    ),
                )
                raise
            self.journal.append_event(
                file.ordinal,
                attempt_id,
                "QUIESCENT",
                observation=writer_observation("success", file.rows, quiescence="proved"),
            )
            self.journal.append_event(
                file.ordinal,
                attempt_id,
                "VERIFIED",
                observation=writer_observation(
                    "success",
                    file.rows,
                    quiescence="proved",
                    row_count=digest.rows,
                    limbs=aggregate_limbs(digest, aggregate),
                    diagnostic="mssql_native.verified",
                ),
            )
            return verified_receipt(importer, plan, file, attempt_id, object_id, digest, artifact)

    def _grant(
        self,
        importer: Any,
        file: Any,
        attempt_id: str,
        qualified: str,
        stage: dict[str, Any],
        grant_token_sha256: str,
    ) -> Any:
        identity = self.journal.identity
        if identity.writer_proof_capability != SQLCLIENT_SESSION_PROOF:
            return NativeStageWriteGrant(
                attempt_id,
                qualified,
                file.path,
                file.rows,
                file.encoded_bytes,
                file.file_sha256,
                grant_token_sha256,
                identity.writer_proof_capability,
            )
        if type(self.max_row_bytes) is not int or self.max_row_bytes < 1:
            raise ValueError("mssql_native.max_row_bytes_required")
        columns = tuple(
            NativeStageColumnMapping(index, column.name, dtype, column.nullable)
            for index, (column, dtype) in enumerate(zip(importer.columns, importer._types, strict=True))
        )
        return NativeStageWriteRequest(
            attempt_id=attempt_id,
            qualified_stage=qualified,
            stage_id_sha256=stage["stage_id"],
            owner_binding_sha256=stage["owner_binding_sha256"],
            object_id=stage["object_id"],
            schema_sha256=stage["schema_sha256"],
            file_path=file.path,
            expected_rows=file.rows,
            encoded_bytes=file.encoded_bytes,
            max_row_bytes=min(self.max_row_bytes, max(1, file.encoded_bytes)),
            file_sha256=file.file_sha256,
            grant_token_sha256=grant_token_sha256,
            proof_capability=identity.writer_proof_capability,
            wire_layout_sha256=identity.capability_layout_sha256,
            columns=columns,
            layout_version=2 if getattr(importer, "_persisted_hash_layout", False) else 1,
        )

    def inspect(self, importer: Any, plan: Any, receipt: Any, lease: Any) -> Any:
        """Repeat exact-stage aggregate verification without business-row reads."""
        table = importer.table_name(plan, receipt.attempt_id)
        part = receipt.consumed_part_evidence
        object_id = part.get("native_object_id")
        if type(object_id) is not int or object_id < 1:
            raise ValueError("mssql_native.stage_identity_mismatch")
        importer._assert_lease(lease)

        def check() -> None:
            importer._assert_stage_identity(plan, receipt.attempt_id, table, object_id)

        if self.journal.identity.writer_proof_capability == SQLCLIENT_SESSION_PROOF:
            events = self.journal.data["events"][receipt.attempt_id]
            boundary = self.barrier(
                receipt.stage_id,
                events[-1]["writer_binding"]["grant_token_sha256"],
                check,
            )
        else:
            boundary = self.barrier(receipt.stage_id, check)
        with boundary:
            if getattr(importer, "_persisted_hash_layout", False):
                observed = importer._repeat_mutation_watermark(table, receipt.rows)
                if (
                    part.get("native_stage_layout") != "mssql-native-persisted-hash-v2"
                    or part.get("native_mutation_watermark") != observed.mutation_watermark
                ):
                    raise ValueError("mssql_native.stage_mutation_detected")
            else:
                observed_sum = importer._verify_contents(table, receipt.rows, receipt.typed_digest)
                if part.get("native_typed_sum") != observed_sum:
                    raise ValueError("mssql_native.typed_digest_mismatch")
        return receipt

    def recover_positive(self, importer: Any, plan: Any, file: Any, attempt_id: str, lease: Any) -> Any:
        """Observe a durable positive terminal under a fresh barrier; never relaunch."""
        importer._assert_lease(lease)
        self.verify_file(file)
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
            observation = writer_observation(
                "success",
                file.rows,
                quiescence="proved",
                row_count=digest.rows,
                limbs=aggregate_limbs(digest, aggregate),
                diagnostic="mssql_native.verified",
            )
            receipt = verified_receipt(importer, plan, file, attempt_id, object_id, digest, artifact)
            return observation, receipt

        terminal = events[-1]
        grant_token_sha256 = terminal["writer_binding"]["grant_token_sha256"]
        if self.journal.identity.writer_proof_capability == SQLCLIENT_SESSION_PROOF:
            barrier = partial(self.barrier, qualified, grant_token_sha256, assert_identity)
            observed_digest: Any = None
            observed_aggregate: tuple[Any, ...] | None = None

            def observe_sqlclient() -> dict[str, Any]:
                nonlocal observed_digest, observed_aggregate
                observed_digest, observed_aggregate = importer._target_digest_observation(table, file.rows)
                return writer_observation(
                    "success" if observed_digest.rows == file.rows else "failure",
                    file.rows if observed_digest.rows == file.rows else None,
                    quiescence="proved",
                    row_count=observed_digest.rows,
                    limbs=aggregate_limbs(observed_digest, observed_aggregate),
                    diagnostic="mssql_native.sqlclient_reconciled",
                )

            def observe_sqlclient_verified() -> tuple[dict[str, Any], Any]:
                observation = observe_sqlclient()
                if observed_digest.typed_digest != file.typed_digest:
                    raise ValueError("mssql_native.typed_digest_mismatch")
                receipt = verified_receipt(importer, plan, file, attempt_id, object_id, observed_digest, artifact)
                return observation, receipt

            try:
                return self.journal.recover_sqlclient_verified(
                    file.ordinal,
                    attempt_id,
                    barrier=barrier,
                    observe=observe_sqlclient_verified,
                )
            except ValueError as error:
                if str(error) != "mssql_native.typed_digest_mismatch" or observed_digest is None:
                    raise
                if observed_digest.rows >= file.rows:
                    raise
            self.journal.observe_sqlclient_partial(
                file.ordinal,
                attempt_id,
                barrier=barrier,
                observe=observe_sqlclient,
            )
            return None
        else:
            barrier = partial(self.barrier, qualified, assert_identity)
            recover = self.journal.recover_bcp_verified
        return recover(
            file.ordinal,
            attempt_id,
            barrier=barrier,
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

    def drop_exact_owned(self, importer: Any, plan: Any, receipt: Any, lease: Any) -> None:
        """Perform an exact-owner drop only after the caller's durable authority."""
        retire_exact_owned_stage(importer, plan, receipt, lease, timeout_seconds=self.retirement_timeout_seconds)
