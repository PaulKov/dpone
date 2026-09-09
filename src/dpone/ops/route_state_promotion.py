"""Route state promotion service over ledger evidence and commit receipts."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from pathlib import Path

from dpone.ops.routes.models import RouteKey
from dpone.ops.routes.state_promotion_models import (
    RouteCommitReceipt,
    RouteStatePromotionReport,
    RouteStateRecord,
)
from dpone.ops.routes.state_promotion_policy import RouteStatePromotionPolicy
from dpone.ops.routes.state_store import LocalRouteStateStore, RouteStateStore

_UTC = timezone.utc  # noqa: UP017


class RouteStatePromotionService:
    """Promote source state only after verified route execution evidence."""

    def __init__(
        self,
        *,
        state_store: RouteStateStore | None = None,
        policy: RouteStatePromotionPolicy | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._state_store = state_store or LocalRouteStateStore()
        self._policy = policy or RouteStatePromotionPolicy()
        self._clock = clock or (lambda: datetime.now(_UTC))

    def promote(
        self,
        *,
        output_dir: str | Path,
        source: str,
        sink: str,
        strategy: str,
        dataset: str,
        run_id: str,
        ledger_json: str | Path,
        proposed_state: str,
        source_boundary: str,
        sink_boundary: str,
        idempotency_key: str,
        commit_token: str,
        target: str,
        fencing_token: str = "",
        rows_applied: int = 0,
        events_applied: int = 0,
    ) -> RouteStatePromotionReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        route = RouteKey.of(source, sink, strategy)
        now = self._clock()
        if now.tzinfo is None:
            now = now.replace(tzinfo=_UTC)

        receipt = RouteCommitReceipt(
            route=route,
            dataset=dataset,
            run_id=run_id,
            proposed_state=proposed_state,
            source_boundary=source_boundary,
            sink_boundary=sink_boundary,
            idempotency_key=idempotency_key,
            commit_token=commit_token,
            target=target,
            fencing_token=fencing_token,
            rows_applied=rows_applied,
            events_applied=events_applied,
            created_at=now.isoformat(),
        )
        ledger_path = Path(ledger_json)
        ledger_payload = _read_json(ledger_path)
        previous_state = self._state_store.read_state(output_dir=directory, route=route, dataset=dataset)
        replay_warnings, conflict_blockers = _idempotency_decision(previous_state, receipt)

        decision = self._policy.evaluate(
            route=route,
            dataset=dataset,
            run_id=run_id,
            ledger_payload=ledger_payload,
            receipt=receipt,
            blockers=conflict_blockers,
            warnings=replay_warnings,
        )
        state_path = self._state_store.state_path(output_dir=directory, route=route, dataset=dataset)
        promoted_state: RouteStateRecord | None = previous_state if replay_warnings and decision.passed else None

        if decision.passed and not replay_warnings:
            expected_version = previous_state.version if previous_state else 0
            candidate = _record_from_receipt(receipt=receipt, promoted_at=now.isoformat(), version=expected_version + 1)
            state_path, written = self._state_store.write_state_if_version(
                output_dir=directory,
                route=route,
                dataset=dataset,
                expected_version=expected_version,
                record=candidate,
            )
            if written:
                promoted_state = candidate
            else:
                latest_state = self._state_store.read_state(output_dir=directory, route=route, dataset=dataset)
                replay_warnings, conflict_blockers = _idempotency_decision(latest_state, receipt)
                if replay_warnings:
                    promoted_state = latest_state
                    decision = self._policy.evaluate(
                        route=route,
                        dataset=dataset,
                        run_id=run_id,
                        ledger_payload=ledger_payload,
                        receipt=receipt,
                        warnings=replay_warnings,
                    )
                else:
                    promoted_state = latest_state
                    decision = self._policy.evaluate(
                        route=route,
                        dataset=dataset,
                        run_id=run_id,
                        ledger_payload=ledger_payload,
                        receipt=receipt,
                        blockers=("state_promotion.concurrent_write_conflict", *conflict_blockers),
                    )

        report = RouteStatePromotionReport(
            route=route,
            dataset=dataset,
            run_id=run_id,
            passed=decision.passed,
            level=decision.level,
            blockers=decision.blockers,
            warnings=decision.warnings,
            next_actions=decision.next_actions,
            receipt=receipt,
            previous_state=previous_state,
            promoted_state=promoted_state,
            ledger_path=str(ledger_path),
            state_path=str(state_path),
            output_dir=str(directory),
            json_path=str(directory / "state_promotion.json"),
            markdown_path=str(directory / "state_promotion.md"),
            state_backend=self._state_store.backend_name,
        )
        report.write()
        return report


def _record_from_receipt(*, receipt: RouteCommitReceipt, promoted_at: str, version: int) -> RouteStateRecord:
    return RouteStateRecord(
        route=receipt.route,
        dataset=receipt.dataset,
        source_state=receipt.proposed_state,
        source_boundary=receipt.source_boundary,
        sink_boundary=receipt.sink_boundary,
        run_id=receipt.run_id,
        idempotency_key=receipt.idempotency_key,
        fencing_token=receipt.fencing_token,
        commit_token=receipt.commit_token,
        promoted_at=promoted_at,
        version=version,
    )


def _idempotency_decision(
    previous_state: RouteStateRecord | None,
    receipt: RouteCommitReceipt,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if previous_state is None or previous_state.idempotency_key != receipt.idempotency_key:
        return tuple(), tuple()
    if (
        previous_state.source_state == receipt.proposed_state
        and previous_state.source_boundary == receipt.source_boundary
        and previous_state.sink_boundary == receipt.sink_boundary
        and previous_state.commit_token == receipt.commit_token
    ):
        return ("state_promotion.idempotent_replay",), tuple()
    return tuple(), ("state_promotion.idempotency_conflict",)


def _read_json(path: Path) -> Mapping[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, Mapping) else {}
