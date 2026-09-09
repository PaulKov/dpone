"""CDC apply certification runner and strategy interfaces."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol

from dpone.ops.cdc.apply_models import (
    CdcApplyCertificationReport,
    CdcApplyEvent,
    CdcApplyFixture,
    CdcApplyResult,
    canonical_rows,
    row_key,
    rows_hash,
)
from dpone.ops.cdc.catalog import CdcHandoffCatalog
from dpone.ops.cdc.handoff import SnapshotCdcHandoffService
from dpone.ops.cdc.models import CdcStreamKey


class CdcApplyStrategy(Protocol):
    """Apply one CDC fixture without talking to live source or sink systems."""

    def apply(self, fixture: CdcApplyFixture) -> CdcApplyResult:
        """Return deterministic final rows, metrics, and blockers."""


class InMemoryCdcApplyStrategy:
    """Generic idempotent CDC apply strategy for certification fixtures."""

    def apply(self, fixture: CdcApplyFixture) -> CdcApplyResult:
        blockers: list[str] = []
        warnings: list[str] = []
        rows = {row_key(row, fixture.unique_key): dict(row) for row in fixture.initial_rows}
        seen_event_ids: set[str] = set()
        applied = 0
        duplicates = 0
        counts = {"insert": 0, "update": 0, "delete": 0}
        deleted_keys: list[tuple[str, ...]] = []

        for event in fixture.events:
            if event.event_id in seen_event_ids:
                duplicates += 1
                continue
            seen_event_ids.add(event.event_id)
            counts[event.operation] = counts.get(event.operation, 0) + 1
            applied += self._apply_event(rows=rows, event=event, fixture=fixture, blockers=blockers)
            if event.operation == "delete":
                deleted_keys.append(tuple(str(event.key.get(name, "")) for name in fixture.unique_key))

        actual_rows = canonical_rows(tuple(rows.values()), fixture.unique_key)
        expected_rows = canonical_rows(fixture.expected_rows, fixture.unique_key)
        source_hash = rows_hash(expected_rows, fixture.unique_key)
        sink_hash = rows_hash(actual_rows, fixture.unique_key)
        typed_hash_passed = source_hash == sink_hash
        deleted_keys_absent = all(key not in rows for key in deleted_keys)
        delete_semantics_passed = deleted_keys_absent
        if not typed_hash_passed:
            blockers.extend(("cdc_apply_correctness.mismatch", "typed_cdc_hash.mismatch"))
        if not delete_semantics_passed:
            blockers.append("delete_semantics.mismatch")

        metrics: dict[str, object] = {
            "events_seen": len(fixture.events),
            "events_applied": applied,
            "duplicate_events": duplicates,
            "insert_events": counts.get("insert", 0),
            "update_events": counts.get("update", 0),
            "delete_events": counts.get("delete", 0),
            "final_rows": len(actual_rows),
            "expected_rows": len(expected_rows),
            "source_hash": source_hash,
            "sink_hash": sink_hash,
            "deleted_keys_absent": deleted_keys_absent,
        }
        return CdcApplyResult(
            passed=not blockers,
            actual_rows=actual_rows,
            expected_rows=expected_rows,
            metrics=metrics,
            blockers=tuple(dict.fromkeys(blockers)),
            warnings=tuple(dict.fromkeys(warnings)),
            typed_hash_passed=typed_hash_passed,
            delete_semantics_passed=delete_semantics_passed,
        )

    @staticmethod
    def _apply_event(
        *,
        rows: dict[tuple[str, ...], dict[str, Any]],
        event: CdcApplyEvent,
        fixture: CdcApplyFixture,
        blockers: list[str],
    ) -> int:
        key = tuple(str(event.key.get(name, "")) for name in fixture.unique_key)
        if event.operation in {"insert", "update"}:
            if event.after is None:
                blockers.append(f"cdc_event.{event.operation}.after_missing")
                return 0
            rows[key] = dict(event.after)
            return 1
        if event.operation == "delete":
            rows.pop(key, None)
            return 1
        blockers.append(f"cdc_event.unsupported_operation:{event.operation}")
        return 0


class CdcApplyCertificationService:
    """Generate CDC apply evidence and embedded handoff reports."""

    def __init__(
        self,
        *,
        catalog: CdcHandoffCatalog | None = None,
        handoff_service: SnapshotCdcHandoffService | None = None,
        strategies: Mapping[str, CdcApplyStrategy] | None = None,
        default_strategy: CdcApplyStrategy | None = None,
    ) -> None:
        self._catalog = catalog or CdcHandoffCatalog.default()
        self._handoff_service = handoff_service or SnapshotCdcHandoffService(catalog=self._catalog)
        self._default_strategy = default_strategy or InMemoryCdcApplyStrategy()
        self._strategies = dict(strategies or {})

    def certify(
        self,
        *,
        output_dir: str | Path,
        source: str,
        sink: str,
        strategy: str = "cdc",
        source_dataset: str,
        target_dataset: str,
        fixture_json: str | Path,
    ) -> CdcApplyCertificationReport:
        directory = Path(output_dir)
        stream = CdcStreamKey.of(
            source=source,
            sink=sink,
            strategy=strategy,
            source_dataset=source_dataset,
            target_dataset=target_dataset,
        )
        profile = self._catalog.get(stream.route)
        if profile is None:
            return self._unsupported_report(directory=directory, stream=stream)

        fixture = CdcApplyFixture.from_path(fixture_json)
        result = self._strategy(profile.sink_apply_mode).apply(fixture)
        evidence = self._write_evidence(directory=directory / "evidence", fixture=fixture, result=result)
        handoff = self._handoff_service.evaluate(
            output_dir=directory / "handoff",
            source=source,
            sink=sink,
            strategy=strategy,
            source_dataset=source_dataset,
            target_dataset=target_dataset,
            artifacts=evidence,
        )
        blockers = tuple(dict.fromkeys((*result.blockers, *handoff.blockers)))
        warnings = tuple(dict.fromkeys((*result.warnings, *handoff.warnings)))
        report = CdcApplyCertificationReport(
            stream=stream,
            profile=profile,
            passed=not blockers,
            blockers=blockers,
            warnings=warnings,
            metrics=result.metrics,
            evidence_artifacts={name: str(path) for name, path in evidence.items()},
            output_dir=str(directory),
            json_path=str(directory / "cdc_apply_certification.json"),
            markdown_path=str(directory / "cdc_apply_certification.md"),
            handoff_json_path=handoff.json_path,
            handoff_markdown_path=handoff.markdown_path,
        )
        report.write()
        return report

    def _strategy(self, sink_apply_mode: str) -> CdcApplyStrategy:
        return self._strategies.get(sink_apply_mode, self._default_strategy)

    @staticmethod
    def _unsupported_report(*, directory: Path, stream: CdcStreamKey) -> CdcApplyCertificationReport:
        blocker = f"cdc.route_unsupported:{stream.route.colon_id}"
        report = CdcApplyCertificationReport(
            stream=stream,
            profile=None,
            passed=False,
            blockers=(blocker,),
            warnings=tuple(),
            metrics={},
            evidence_artifacts={},
            output_dir=str(directory),
            json_path=str(directory / "cdc_apply_certification.json"),
            markdown_path=str(directory / "cdc_apply_certification.md"),
            handoff_json_path="",
            handoff_markdown_path="",
        )
        report.write()
        return report

    def _write_evidence(
        self,
        *,
        directory: Path,
        fixture: CdcApplyFixture,
        result: CdcApplyResult,
    ) -> dict[str, Path]:
        directory.mkdir(parents=True, exist_ok=True)
        payloads = _evidence_payloads(fixture=fixture, result=result)
        artifacts: dict[str, Path] = {}
        for name, payload in payloads.items():
            path = directory / f"{name}.json"
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            artifacts[name] = path
        return artifacts


def _evidence_payloads(*, fixture: CdcApplyFixture, result: CdcApplyResult) -> dict[str, dict[str, object]]:
    schema_drift_passed = not fixture.schema_changes
    return {
        "cdc_snapshot_boundary": _passed_payload(
            passed=bool(fixture.snapshot_boundary),
            summary="snapshot boundary captured",
            blockers=() if fixture.snapshot_boundary else ("cdc_snapshot_boundary.missing",),
            snapshot_boundary=fixture.snapshot_boundary,
        ),
        "cdc_window": _passed_payload(
            passed=_window_passed(fixture),
            summary="CDC window is bounded after snapshot",
            blockers=() if _window_passed(fixture) else ("cdc_window.invalid",),
            window_start=fixture.window_start,
            window_end=fixture.window_end,
            snapshot_boundary=fixture.snapshot_boundary,
        ),
        "retention_preflight": _passed_payload(
            passed=_retention_passed(fixture),
            summary="source retention covers CDC window",
            blockers=() if _retention_passed(fixture) else ("retention_preflight.window_not_available",),
            retention_min=fixture.retention_min,
            window_start=fixture.window_start,
        ),
        "cdc_apply_correctness": _passed_payload(
            passed=result.passed,
            summary="CDC events apply to expected final rows",
            blockers=tuple(item for item in result.blockers if item.startswith("cdc_apply_correctness")),
            actual_row_count=len(result.actual_rows),
            expected_row_count=len(result.expected_rows),
            actual_rows=list(result.actual_rows),
            expected_rows=list(result.expected_rows),
            metrics=dict(result.metrics),
        ),
        "delete_semantics": _passed_payload(
            passed=result.delete_semantics_passed,
            summary="delete events remove or tombstone configured keys deterministically",
            blockers=() if result.delete_semantics_passed else ("delete_semantics.mismatch",),
            delete_events=result.metrics.get("delete_events", 0),
            deleted_keys_absent=result.metrics.get("deleted_keys_absent", False),
        ),
        "typed_cdc_hash": _passed_payload(
            passed=result.typed_hash_passed,
            summary="typed CDC source and sink hashes match",
            blockers=() if result.typed_hash_passed else ("typed_cdc_hash.mismatch",),
            source_hash=result.metrics.get("source_hash", ""),
            sink_hash=result.metrics.get("sink_hash", ""),
        ),
        "schema_drift_governance": _passed_payload(
            passed=schema_drift_passed,
            summary="no unapproved schema drift observed in CDC window",
            blockers=() if schema_drift_passed else ("schema_drift_governance.unapproved_change",),
            schema_changes=[dict(item) for item in fixture.schema_changes],
        ),
    }


def _passed_payload(*, passed: bool, summary: str, blockers: tuple[str, ...], **values: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "passed": passed,
        "summary": summary,
        "blockers": list(blockers),
    }
    payload.update(values)
    return payload


def _window_passed(fixture: CdcApplyFixture) -> bool:
    return bool(fixture.snapshot_boundary and fixture.window_start and fixture.window_end) and (
        _compare_token(fixture.snapshot_boundary, fixture.window_start) <= 0
        and _compare_token(fixture.window_start, fixture.window_end) <= 0
    )


def _retention_passed(fixture: CdcApplyFixture) -> bool:
    return (
        bool(fixture.retention_min and fixture.window_start)
        and _compare_token(fixture.retention_min, fixture.window_start) <= 0
    )


def _compare_token(left: str, right: str) -> int:
    left_value = _numeric_token(left)
    right_value = _numeric_token(right)
    if left_value is None or right_value is None:
        return (left > right) - (left < right)
    return (left_value > right_value) - (left_value < right_value)


def _numeric_token(value: str) -> int | None:
    token = value.strip()
    if not token:
        return None
    try:
        if "/" in token:
            high, low = token.split("/", 1)
            return (int(high, 16) << 32) + int(low, 16)
        if token.lower().startswith("0x"):
            return int(token, 16)
        return int(token)
    except ValueError:
        return None
