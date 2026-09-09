"""Optional Airflow metadata publishing for scheduler-side diagnostics."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

_MAX_VARIABLE_BYTES = 1024 * 1024


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)  # noqa: UP017 - package supports Python 3.10.


@dataclass(frozen=True)
class AirflowVariableStatusPublisher:
    """Publish non-secret sync evidence into Airflow metadata when available."""

    variable_key: str | None = None
    clock: Callable[[], datetime] = _utc_now

    def publish(self, evidence: Mapping[str, Any]) -> dict[str, Any]:
        payload = dict(evidence)
        if not self.variable_key:
            return payload
        payload["airflow_variable_key"] = self.variable_key
        payload["airflow_variable_published"] = True
        payload["airflow_variable_published_at"] = (
            self.clock()
            .astimezone(timezone.utc)  # noqa: UP017 - package supports Python 3.10.
            .isoformat()
        )
        try:
            serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
            if len(serialized.encode("utf-8")) > _MAX_VARIABLE_BYTES:
                raise ValueError("Airflow Variable diagnostic exceeds 1 MiB")
            _airflow_variable_class().set(
                self.variable_key,
                serialized,
            )
        except Exception as exc:  # noqa: BLE001 - cache sync must stay fail-open.
            warnings = list(payload.get("warnings") or [])
            warnings.append(
                {
                    "code": "airflow_variable_publish_failed",
                    "message": "Airflow Variable diagnostic could not be published",
                    "error_type": exc.__class__.__name__,
                }
            )
            payload["warnings"] = warnings
            payload["status"] = "blocked" if payload.get("blockers") else "warning"
            payload["reason"] = "sync_completed_airflow_variable_publish_failed"
            payload["airflow_variable_published"] = False
            return payload
        return payload


def _airflow_variable_class() -> Any:
    try:
        from airflow.models.variable import Variable
    except Exception:  # noqa: BLE001 - Airflow compatibility path.
        try:
            from airflow.models import Variable
        except Exception:  # noqa: BLE001 - Airflow 3 task-runtime fallback.
            from airflow.sdk import Variable
    return Variable
