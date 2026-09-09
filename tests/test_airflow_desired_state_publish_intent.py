from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from dpone.adapters.airflow_desired_state_publish_intent import (
    FileDesiredStatePublishIntentStore,
)


def test_publish_intent_is_created_once_and_reused_across_process_retries(
    tmp_path: Path,
) -> None:
    path = tmp_path / "control" / "publish-intent.json"
    store = FileDesiredStatePublishIntentStore(path)
    calls = 0

    def occurrence() -> str:
        nonlocal calls
        calls += 1
        return "123e4567-e89b-42d3-a456-426614174000"

    first = store.resolve(
        candidate_sha256="sha256:" + "a" * 64,
        clock=lambda: datetime.fromisoformat("2026-07-28T10:00:00+00:00"),
        occurrence_id_factory=occurrence,
    )
    replay = store.resolve(
        candidate_sha256="sha256:" + "a" * 64,
        clock=lambda: datetime.fromisoformat("2026-07-29T10:00:00+00:00"),
        occurrence_id_factory=occurrence,
    )

    assert replay == first
    assert calls == 1
    assert path.read_bytes() == first.to_json_bytes()


def test_publish_intent_rejects_reuse_for_another_candidate(tmp_path: Path) -> None:
    store = FileDesiredStatePublishIntentStore(tmp_path / "intent.json")
    store.resolve(
        candidate_sha256="sha256:" + "a" * 64,
        clock=lambda: datetime.fromisoformat("2026-07-28T10:00:00+00:00"),
        occurrence_id_factory=lambda: "123e4567-e89b-42d3-a456-426614174000",
    )

    with pytest.raises(ValueError, match="another candidate"):
        store.resolve(
            candidate_sha256="sha256:" + "b" * 64,
            clock=lambda: datetime.fromisoformat("2026-07-28T10:00:00+00:00"),
            occurrence_id_factory=lambda: "223e4567-e89b-42d3-a456-426614174000",
        )
