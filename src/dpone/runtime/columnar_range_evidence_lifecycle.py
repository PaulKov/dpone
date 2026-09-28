"""Measured lifecycle transitions for governed columnar range evidence."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from threading import Lock
from typing import Any

from dpone.ports.columnar_range_parallelism import RangeStageReceipt, columnar_range_fingerprint


def safe_stage_identity(config: Any) -> str:
    """Return a stable table identity without connector options or credentials."""

    parts = (
        getattr(config, "target_database", None),
        getattr(config, "target_schema", None),
        getattr(config, "target_table", None),
        getattr(config, "table", None),
    )
    identity = ".".join(str(part) for part in parts if part not in {None, ""})
    if not identity:
        raise ValueError("columnar_range_stage_identity_missing")
    return identity


class ColumnarRangeEvidenceLifecycle:
    """Serialize immutable evidence transitions owned by one extraction artifact."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._current: Any | None = None
        self._assembly_receipt: str | None = None

    @property
    def current(self) -> Any | None:
        with self._lock:
            return self._current

    def bind_extracted(self, evidence: Any) -> None:
        if evidence.outcome_status != "extracted":
            raise ValueError("columnar_range_initial_evidence_must_be_extracted")
        with self._lock:
            self._current = evidence

    def bind_terminal_failure(self, evidence: Any) -> None:
        if evidence.outcome_status not in {"failed", "cancelled"}:
            raise ValueError("columnar_range_terminal_source_evidence_must_be_failure")
        with self._lock:
            self._current = evidence

    def stage(
        self,
        *,
        groups: Sequence[Any],
        stage_configs: Sequence[Any],
        observed_load_concurrency: int,
        assembly_rows: int | None,
        authoritative_config: Any,
        observed_rows: Mapping[str, int],
    ) -> None:
        with self._lock:
            evidence = self._require("extracted")
            if len(groups) != len(stage_configs):
                raise ValueError("columnar_range_stage_config_coverage_mismatch")
            receipts: dict[str, RangeStageReceipt] = {}
            identities: list[str] = []
            for group, config in zip(groups, stage_configs, strict=True):
                identity = safe_stage_identity(config)
                identities.append(identity)
                range_id = str(getattr(group, "range_id", "") or "")
                rows = _nonnegative("staged rows", observed_rows.get(range_id))
                digest = columnar_range_fingerprint(
                    {
                        "schema": "dpone.native_transfer.columnar_range_stage_receipt.v1",
                        "execution_plan": evidence.execution_plan_fingerprint,
                        "range_id": range_id,
                        "stage_identity": identity,
                        "staged_rows": rows,
                    }
                )
                receipts[range_id] = RangeStageReceipt(range_id, identity, digest, rows)
            self._assembly_receipt = None
            if evidence.policy.staging_topology == "per_partition":
                self._assembly_receipt = columnar_range_fingerprint(
                    {
                        "schema": "dpone.native_transfer.columnar_range_assembly_receipt.v1",
                        "execution_plan": evidence.execution_plan_fingerprint,
                        "partition_stages": identities,
                        "authoritative_stage": safe_stage_identity(authoritative_config),
                        "assembled_rows": _nonnegative("assembled rows", assembly_rows),
                    }
                )
            self._current = evidence.with_stage_receipts(receipts, observed_load_concurrency=observed_load_concurrency)

    def quality_passed(self, *, config: Any, validation_receipt: object) -> None:
        self._quality_passed(config=config, authority={"sink_validation": validation_receipt})

    def governed_quality_passed(self, *, config: Any, quality_receipt: Mapping[str, object]) -> None:
        self._quality_passed(config=config, authority={"governed_quality": dict(quality_receipt)})

    def _quality_passed(self, *, config: Any, authority: Mapping[str, object]) -> None:
        with self._lock:
            evidence = self._require("staged")
            sink_receipt = authority.get("sink_validation")
            if sink_receipt is not None and not _canonical_digest(sink_receipt):
                raise ValueError("columnar_range_validation_receipt_invalid")
            receipt = columnar_range_fingerprint(
                {
                    "schema": "dpone.native_transfer.columnar_range_quality_receipt.v1",
                    "execution_plan": evidence.execution_plan_fingerprint,
                    "stage": safe_stage_identity(config),
                    "staged_rows": sum(item.stage.staged_rows for item in evidence.ranges if item.stage),
                    "authority": dict(authority),
                }
            )
            self._current = evidence.with_quality_receipt(
                quality_receipt_sha256=receipt,
                assembly_receipt_sha256=self._assembly_receipt,
            )

    def publication_unknown(self) -> None:
        with self._lock:
            self._current = self._require("quality_passed").publication_unknown(
                failure_code="columnar_range_publication_outcome_unknown"
            )

    def publication_confirmed(self, *, result: Any, config: Any) -> None:
        with self._lock:
            evidence = self._require("quality_passed")
            self._current = evidence.published(
                publication_receipt_sha256=_publication_receipt(evidence, result, config)
            )

    def cleanup_succeeded(self) -> None:
        with self._lock:
            if self._current is None or self._current.outcome_status != "published":
                return
            evidence = self._current
            self._current = evidence.complete(
                publication_receipt_sha256=evidence.publication_receipt_sha256,
                cleanup_status="completed",
            )

    def cleanup_failed(self) -> None:
        with self._lock:
            if self._current is None or self._current.outcome_status != "published":
                return
            evidence = self._current
            self._current = replace(
                evidence,
                outcome_status="failed",
                failure_code="columnar_range_post_commit_cleanup_failed",
                cleanup_status="failed",
                cleanup_failures=("columnar_range_cleanup_failed",),
            )

    def failed(
        self,
        *,
        failure_code: str,
        cleanup_status: str,
        cleanup_failures: Sequence[str] = (),
        publication_result: Any | None = None,
        config: Any | None = None,
    ) -> None:
        with self._lock:
            evidence = self._current
            if evidence is None or evidence.outcome_status == "publication_unknown":
                return
            if evidence.outcome_status in {"failed", "cancelled"}:
                cleanup_failures = tuple(dict.fromkeys((*evidence.cleanup_failures, *cleanup_failures)))
                if evidence.cleanup_status == "failed" or cleanup_failures:
                    cleanup_status = "failed"
                elif cleanup_status != "completed":
                    cleanup_status = evidence.cleanup_status
                failure_code = evidence.failure_code or failure_code
            publication = evidence.publication_receipt_sha256
            if publication_result is not None and config is not None:
                publication = columnar_range_fingerprint(
                    {
                        "schema": "dpone.native_transfer.columnar_range_publication_receipt.v1",
                        "execution_plan": evidence.execution_plan_fingerprint,
                        "target": safe_stage_identity(config),
                        "result": _public_result(publication_result),
                        "quality": evidence.quality_receipt_sha256,
                    }
                )
            self._current = replace(
                evidence,
                outcome_status="failed",
                failure_code=failure_code,
                cleanup_status=cleanup_status,
                cleanup_failures=tuple(cleanup_failures),
                publication_receipt_sha256=publication,
            )

    def to_dict(self) -> dict[str, Any]:
        current = self.current
        return current.to_dict() if current is not None else {}

    def _require(self, status: str) -> Any:
        if self._current is None or self._current.outcome_status != status:
            raise ValueError(f"columnar_range_evidence_requires_{status}")
        return self._current


def _public_result(result: Any) -> Mapping[str, object]:
    fields = ("inserted_rows", "updated_rows", "total_rows", "staging_rows", "replaced_rows")
    return {name: value for name in fields if isinstance((value := getattr(result, name, None)), int)}


def _publication_receipt(evidence: Any, result: Any, config: Any) -> str:
    return columnar_range_fingerprint(
        {
            "schema": "dpone.native_transfer.columnar_range_publication_receipt.v1",
            "execution_plan": evidence.execution_plan_fingerprint,
            "target": safe_stage_identity(config),
            "result": _public_result(result),
            "quality": evidence.quality_receipt_sha256,
        }
    )


def _nonnegative(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")
    return value


def _canonical_digest(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", value) is not None


def advance_range_quality(owner: Any, *, config: Any, validation_receipt: object) -> None:
    transition = getattr(owner, "mark_range_quality_passed", None)
    if callable(transition):
        transition(config=config, validation_receipt=validation_receipt)


def advance_range_publication_unknown(owner: Any) -> None:
    transition = getattr(owner, "mark_range_publication_unknown", None)
    if callable(transition):
        transition()


def advance_range_success(owner: Any, *, result: Any, config: Any, cleanup_status: str) -> None:
    transition = getattr(owner, "mark_range_succeeded", None)
    if callable(transition):
        transition(result=result, config=config, cleanup_status=cleanup_status)


def advance_range_failure(
    owner: Any,
    *,
    failure_code: str,
    cleanup_status: str,
    cleanup_failures: Sequence[str] = (),
    publication_result: Any | None = None,
    config: Any | None = None,
) -> None:
    transition = getattr(owner, "mark_range_failed", None)
    if callable(transition):
        transition(
            failure_code=failure_code,
            cleanup_status=cleanup_status,
            cleanup_failures=cleanup_failures,
            publication_result=publication_result,
            config=config,
        )


def resolve_range_staging_cleanup(
    owner: Any, *, primary: BaseException | None, cleanup_error: BaseException | None
) -> None:
    if primary is not None:
        advance_range_failure(
            owner,
            failure_code="columnar_range_stage_failed",
            cleanup_status="failed" if cleanup_error else "not_started",
            cleanup_failures=("columnar_range_partition_cleanup_failed",) if cleanup_error else (),
        )
        if cleanup_error is not None:
            primary.add_note(f"range staging cleanup failed: {type(cleanup_error).__name__}")
        raise primary
    if cleanup_error is not None:
        advance_range_failure(
            owner,
            failure_code="columnar_range_partition_cleanup_failed",
            cleanup_status="failed",
            cleanup_failures=("columnar_range_partition_cleanup_failed",),
        )
        raise cleanup_error


class RangeEvidenceTransitions:
    """Compact adapter-facing facade for governed lifecycle transitions."""

    @staticmethod
    def stage_failed(owner: Any, cleanup_succeeded: bool) -> None:
        advance_range_failure(
            owner,
            failure_code="columnar_range_stage_failed",
            cleanup_status="completed" if cleanup_succeeded else "failed",
            cleanup_failures=() if cleanup_succeeded else ("columnar_range_cleanup_failed",),
        )

    @staticmethod
    def validation_failed(owner: Any, cleanup_succeeded: bool) -> None:
        advance_range_failure(
            owner,
            failure_code="columnar_range_validation_failed",
            cleanup_status="completed" if cleanup_succeeded else "failed",
            cleanup_failures=() if cleanup_succeeded else ("columnar_range_cleanup_failed",),
        )

    @staticmethod
    def quality(owner: Any, config: Any, receipt: object) -> None:
        advance_range_quality(owner, config=config, validation_receipt=receipt)

    @staticmethod
    def publication_unknown(owner: Any) -> None:
        advance_range_publication_unknown(owner)

    @staticmethod
    def publication_confirmed(owner: Any, result: Any, config: Any) -> None:
        transition = getattr(owner, "mark_range_publication_confirmed", None)
        if callable(transition):
            transition(result=result, config=config)

    @staticmethod
    def cleanup_succeeded(owner: Any) -> None:
        transition = getattr(owner, "mark_range_cleanup_succeeded", None)
        if callable(transition):
            transition()

    def validated(
        self,
        owner: Any,
        *,
        load_config: Any,
        staging_config: Any,
        external_validation: object | None,
        validate: Callable[[], object],
        authority: Any,
    ) -> object:
        token = external_validation if external_validation is not None else validate()
        evidence = getattr(owner, "range_execution_evidence", None)
        if evidence is None or getattr(evidence, "outcome_status", None) == "quality_passed":
            return token
        reader = getattr(authority, "strategy_staging_validation_receipt", None)
        receipt = reader(token, load_config, staging_config) if callable(reader) else None
        self.quality(owner, staging_config, receipt)
        return token

    def published(self, owner: Any, publish: Callable[[], Any], config: Any) -> Any:
        try:
            result = publish()
        except Exception:
            self.publication_unknown(owner)
            raise
        self.publication_confirmed(owner, result, config)
        return result

    @staticmethod
    def prepublication_failed(owner: Any) -> None:
        advance_range_failure(
            owner,
            failure_code="columnar_range_publication_preparation_failed",
            cleanup_status="not_started",
        )

    def cleaned(self, owner: Any, cleanup: Callable[..., None], *args: Any) -> None:
        try:
            cleanup(*args)
        except Exception:
            self.cleanup_failed(owner)
            raise
        self.cleanup_succeeded(owner)

    def aborted(self, owner: Any, cleanup: Callable[[], None]) -> None:
        try:
            cleanup()
        except Exception:
            self.validation_failed(owner, False)
            raise
        self.validation_failed(owner, True)

    def cleaned_clickhouse(self, service: Any, handle: Any, cleanup: Callable[..., None]) -> None:
        self.cleaned(handle.sink_state, cleanup, service._sink, service._external, service._drop_configs, handle)

    def aborted_clickhouse(self, service: Any, handle: Any, cleanup: Callable[..., None]) -> None:
        def operation() -> None:
            if not service._external.cleanup(handle, abort=True):
                cleanup(service._sink, service._external, service._drop_configs, handle)

        self.aborted(handle.sink_state, operation)

    @staticmethod
    def cleanup_failed(owner: Any) -> None:
        transition = getattr(owner, "mark_range_cleanup_failed", None)
        if callable(transition):
            transition()


range_evidence_transitions = RangeEvidenceTransitions()
