"""Lease-level notification evidence, including restored values and failed feeds."""

from __future__ import annotations

import os
import struct
from pathlib import Path

import pytest
from tools.agent_policy.workflow_privilege_mutation_observer import MutationObserver


@pytest.mark.parametrize("mutation", ["bytes", "mode", "hardlink", "rename", "inventory"])
def test_observer_records_restored_mutations(tmp_path: Path, mutation: str) -> None:
    target = tmp_path / "workflow.yml"
    target.write_bytes(b"before")
    original_mode = target.stat().st_mode
    descriptors = [os.open(tmp_path, os.O_RDONLY), os.open(target, os.O_RDONLY)]
    observer = MutationObserver()
    try:
        for descriptor in descriptors:
            observer.watch(descriptor, scope="workflows", contents=True)
        assert observer.unchanged()
        temporary = tmp_path / "temporary"
        if mutation == "bytes":
            target.write_bytes(b"after!")
            target.write_bytes(b"before")
        elif mutation == "mode":
            target.chmod(0o600)
            target.chmod(original_mode)
        elif mutation == "hardlink":
            os.link(target, temporary)
            temporary.unlink()
        elif mutation == "rename":
            target.rename(temporary)
            temporary.rename(target)
        else:
            temporary.write_bytes(b"transient")
            temporary.unlink()
        assert not observer.unchanged()
        assert not observer.unchanged(), "draining must not erase recorded mutation evidence"
        assert observer.unchanged(policy_only=True)
    finally:
        observer.close()
        for descriptor in descriptors:
            os.close(descriptor)
    assert not observer.unchanged()
    observer.close()


@pytest.mark.parametrize("mask", [0x2000, 0x4000, 0x8000])
def test_lost_notification_stream_fails_closed(mask: int) -> None:
    observer = MutationObserver()
    try:
        observer._consume_inotify(struct.pack("iIII", -1, mask, 0, 0))
        assert not observer.unchanged(policy_only=True)
    finally:
        observer.close()


def test_unknown_watch_fails_closed() -> None:
    observer = MutationObserver()
    try:
        observer._record(123456)
        assert not observer.unchanged(policy_only=True)
    finally:
        observer.close()


def test_watch_setup_failure_stays_failed() -> None:
    observer = MutationObserver()
    try:
        with pytest.raises((OSError, ValueError)):
            observer.watch(999999, scope="policy", contents=True)
        assert not observer.unchanged(policy_only=True)
    finally:
        observer.close()


def test_notification_read_failure_is_sticky() -> None:
    class FailedQueue:
        def control(self, *_args: object) -> list[object]:
            raise OSError("injected notification read failure")

    observer = MutationObserver()
    queue = observer._queue
    try:
        observer._queue = FailedQueue()
        assert not observer.unchanged(policy_only=True)
        observer._queue = queue
        assert not observer.unchanged(policy_only=True)
    finally:
        observer._queue = queue
        observer.close()


def test_workflow_watch_loss_preserves_policy_scope() -> None:
    observer = MutationObserver()
    try:
        observer._scopes[123456] = {"workflows"}
        observer._consume_inotify(struct.pack("iIII", 123456, 0x8000, 0, 0))
        assert observer.unchanged(policy_only=True)
        assert not observer.unchanged()
    finally:
        observer.close()


def test_continuous_irrelevant_events_have_bounded_drain() -> None:
    class BusyQueue:
        calls = 0

        def control(self, *_args: object) -> list[object]:
            from types import SimpleNamespace

            self.calls += 1
            return [SimpleNamespace(ident=123456, flags=0)]

    observer = MutationObserver()
    original = observer._queue
    busy = BusyQueue()
    try:
        observer._queue = busy
        observer._scopes[123456] = {"workflows"}
        assert not observer.unchanged(policy_only=True)
        assert busy.calls == 256
    finally:
        observer._queue = original
        observer.close()
