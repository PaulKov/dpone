"""Shared-deadline process-lane shutdown contracts."""

from __future__ import annotations

from dpone.backfill import process_lane_processes
from dpone.backfill.process_lane_processes import ProcessLaneHandle, terminate_running_lanes


class _Process:
    def __init__(self) -> None:
        self.alive = True

    def is_alive(self) -> bool:
        return self.alive


def test_peer_quiescence_uses_one_shared_deadline_not_one_per_lane(monkeypatch) -> None:
    lanes = [
        ProcessLaneHandle(worker_id=index, process=_Process(), connection=object(), running=object())
        for index in range(4)
    ]
    signals: list[tuple[bool, tuple[int, ...]]] = []
    waits: list[tuple[int, ...]] = []

    def terminate(current, *, force):
        signals.append((force, tuple(lane.worker_id for lane in current)))

    def wait(current, *, deadline):
        del deadline
        waits.append(tuple(lane.worker_id for lane in current))
        if len(waits) == 2:
            for lane in current:
                lane.process.alive = False

    monkeypatch.setattr(process_lane_processes, "_terminate_lanes", terminate)
    monkeypatch.setattr(process_lane_processes, "_wait_for_process_trees", wait)

    terminated = terminate_running_lanes(lanes, timeout_seconds=0.25)

    assert terminated == lanes
    assert signals == [(False, (0, 1, 2, 3)), (True, (0, 1, 2, 3))]
    assert waits == [(0, 1, 2, 3), (0, 1, 2, 3)]
    assert all(lane.tree_quiesced for lane in lanes)
