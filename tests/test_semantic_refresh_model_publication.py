from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from dpone.ports.semantic_refresh_clickhouse_resources import (
    ClickHouseFailedScratchCleanupReceipt,
    ClickHouseScratchRelationAbsence,
)
from dpone.ports.semantic_refresh_mssql_cleanup_ack import MssqlFailedPrecommitCleanupAck
from dpone.runtime.semantic_refresh_model_publication import (
    SemanticRefreshFailedPrecommitCleanupService,
    SemanticRefreshModelPublicationService,
)


@dataclass(frozen=True)
class _Loaded:
    plan: object
    receipt: object


class _Recorder:
    def __init__(self, *, existing_prepared: _Loaded | None = None) -> None:
        self.events: list[tuple[str, object]] = []
        self.existing_prepared = existing_prepared

    def issuer(self) -> Any:
        recorder = self

        class Issuer:
            def issue(self, **identity: object) -> object:
                recorder.events.append(("issue", identity))
                return object()

        return Issuer()

    def artifact(self) -> Any:
        recorder = self

        class Artifact:
            def seal(self, **identity: object) -> object:
                recorder.events.append(("seal", identity))
                return "sealed-artifact"

        return Artifact()

    def plan_factory(self) -> Any:
        recorder = self

        class Factory:
            def build(self, **request: object) -> object:
                recorder.events.append(("plan", request))
                return "prepare-plan"

        return Factory()

    def prepared_loader(self) -> Any:
        recorder = self

        class Loader:
            def load_if_prepared(self, **identity: object) -> _Loaded | None:
                recorder.events.append(("find", identity))
                return recorder.existing_prepared

            def load(self, **identity: object) -> _Loaded:
                recorder.events.append(("load", identity))
                return _Loaded("durable-plan", "durable-receipt")

        return Loader()

    def authority(self) -> Any:
        recorder = self

        class Authority:
            def load(self, binding: str, operation_id: str) -> object:
                recorder.events.append(("authority", (binding, operation_id)))
                return "protected-authority"

        return Authority()

    def head_factory(self) -> Any:
        recorder = self

        class Factory:
            def build(self, authority: object, receipt: object) -> object:
                recorder.events.append(("heads", (authority, receipt)))
                return "head-plan"

        return Factory()

    def publication(self) -> Any:
        recorder = self

        class Publication:
            def prepare(self, plan: object) -> object:
                recorder.events.append(("prepare", plan))
                return "prepared-receipt"

            def commit(self, plan: object, *, prepared: object, heads: object) -> object:
                recorder.events.append(("commit", (plan, prepared, heads)))
                return "publication-receipt"

        return Publication()

    def resources(self) -> Any:
        recorder = self

        class Resources:
            def reserve_operation(self, **identity: object) -> None:
                recorder.events.append(("reserve", identity))

            def release_operation(self, **identity: object) -> None:
                recorder.events.append(("release", identity))

        return Resources()


def _service(recorder: _Recorder) -> SemanticRefreshModelPublicationService:
    return SemanticRefreshModelPublicationService(
        seal_issuer=recorder.issuer(),
        artifact_publisher=recorder.artifact(),
        prepare_plan_factory=recorder.plan_factory(),
        prepared_loader=recorder.prepared_loader(),
        publication_authority=recorder.authority(),
        publication=recorder.publication(),
        resources=recorder.resources(),
        head_plan_factory=recorder.head_factory(),
    )


def _cleanup_receipt() -> ClickHouseFailedScratchCleanupReceipt:
    return ClickHouseFailedScratchCleanupReceipt(
        workflow_execution_id="scheduled__2026-08-09T00:00:00+00:00",
        workflow_execution_binding_sha256="sha256:" + "a" * 64,
        operation_id="sha256:" + "b" * 64,
        operation_plan_sha256="sha256:" + "c" * 64,
        attempt_binding_sha256="sha256:" + "d" * 64,
        fencing_epoch=1,
        target_uuid="00000000-0000-0000-0000-000000000001",
        relations=(
            ClickHouseScratchRelationAbsence(
                kind="shadow",
                name="orders__dpone_shadow__bbbbbbbbbbbbbbbb",
                expected_uuid="00000000-0000-0000-0000-000000000002",
            ),
            ClickHouseScratchRelationAbsence(
                kind="staging",
                name="orders__dpone_staging__bbbbbbbbbbbbbbbb",
                expected_uuid="00000000-0000-0000-0000-000000000003",
            ),
        ),
    )


def test_prepare_uses_evidence_seal_before_protected_clickhouse_prepare() -> None:
    recorder = _Recorder()

    result = _service(recorder).prepare(
        workflow_execution_binding_sha256="sha256:" + "a" * 64,
        operation_id="sha256:" + "b" * 64,
    )

    assert result == "prepared-receipt"
    assert [name for name, _ in recorder.events] == [
        "find",
        "reserve",
        "issue",
        "seal",
        "plan",
        "prepare",
    ]
    assert recorder.events[4][1]["sealed_artifact"] == "sealed-artifact"  # type: ignore[index]


def test_prepare_retry_returns_durable_receipt_without_external_side_effects() -> None:
    recorder = _Recorder(existing_prepared=_Loaded("durable-plan", "durable-prepared-receipt"))

    result = _service(recorder).prepare(
        workflow_execution_binding_sha256="sha256:" + "a" * 64,
        operation_id="sha256:" + "b" * 64,
    )

    assert result == "durable-prepared-receipt"
    assert [name for name, _ in recorder.events] == ["find"]


def test_commit_uses_durable_prepared_documents_and_protected_heads() -> None:
    recorder = _Recorder()

    result = _service(recorder).commit(
        workflow_execution_binding_sha256="sha256:" + "a" * 64,
        operation_id="sha256:" + "b" * 64,
    )

    assert result == "publication-receipt"
    assert [name for name, _ in recorder.events] == ["load", "authority", "heads", "commit", "release"]
    assert recorder.events[-2][1] == ("durable-plan", "durable-receipt", "head-plan")


def test_failed_precommit_cleanup_releases_only_after_exact_scratch_cleanup() -> None:
    events: list[tuple[str, object]] = []
    authority = object()
    closure = object()
    acknowledgement = object()
    receipt = _cleanup_receipt()

    class Authority:
        def load(self, binding: str, operation_id: str) -> object:
            events.append(("load_failed", (binding, operation_id)))
            return authority

    class Cleaner:
        def cleanup(self, value: object) -> ClickHouseFailedScratchCleanupReceipt:
            assert value is authority
            events.append(("cleanup_failed", value))
            return receipt

    class Resources:
        def reserve_operation(self, **_identity: object) -> None:
            raise AssertionError("failed cleanup must not reserve")

        def release_operation(self, **identity: object) -> None:
            events.append(("release", identity))

    class ReleasedResources:
        def assert_released(self, **identity: object) -> object:
            events.append(("assert_released", identity))
            return closure

    class CleanupAck:
        def persist_exact(self, *, scratch: object, resources: object) -> object:
            assert scratch is receipt
            assert resources is closure
            events.append(("persist_ack", (scratch, resources)))
            return acknowledgement

    observed = SemanticRefreshFailedPrecommitCleanupService(
        authority=Authority(),  # type: ignore[arg-type]
        cleaner=Cleaner(),  # type: ignore[arg-type]
        resources=Resources(),
        released_resources=ReleasedResources(),  # type: ignore[arg-type]
        cleanup_ack=CleanupAck(),  # type: ignore[arg-type]
    ).cleanup(
        workflow_execution_binding_sha256="sha256:" + "a" * 64,
        operation_id="sha256:" + "b" * 64,
    )

    assert [name for name, _ in events] == [
        "load_failed",
        "cleanup_failed",
        "release",
        "assert_released",
        "persist_ack",
    ]
    assert observed is acknowledgement


def test_failed_precommit_cleanup_never_acknowledges_without_released_closure() -> None:
    events: list[str] = []

    class Authority:
        def load(self, _binding: str, _operation_id: str) -> object:
            return object()

    class Cleaner:
        def cleanup(self, _value: object) -> ClickHouseFailedScratchCleanupReceipt:
            events.append("cleanup")
            return _cleanup_receipt()

    class Resources:
        def reserve_operation(self, **_identity: object) -> None:
            raise AssertionError("failed cleanup must not reserve")

        def release_operation(self, **_identity: object) -> None:
            events.append("release")

    class ReleasedResources:
        def assert_released(self, **_identity: object) -> object:
            events.append("assert_released")
            raise RuntimeError("released closure unavailable")

    class CleanupAck:
        def persist_exact(self, **_proofs: object) -> MssqlFailedPrecommitCleanupAck:
            events.append("persist_ack")
            raise AssertionError("unproven cleanup must not persist acknowledgement")

    with pytest.raises(RuntimeError, match="released closure unavailable"):
        SemanticRefreshFailedPrecommitCleanupService(
            authority=Authority(),  # type: ignore[arg-type]
            cleaner=Cleaner(),  # type: ignore[arg-type]
            resources=Resources(),
            released_resources=ReleasedResources(),  # type: ignore[arg-type]
            cleanup_ack=CleanupAck(),
        ).cleanup(
            workflow_execution_binding_sha256="sha256:" + "a" * 64,
            operation_id="sha256:" + "b" * 64,
        )

    assert events == ["cleanup", "release", "assert_released"]


def test_failed_precommit_cleanup_retries_after_acknowledgement_loss() -> None:
    events: list[str] = []
    acknowledgement = object()
    calls = 0

    class Authority:
        def load(self, _binding: str, _operation_id: str) -> object:
            events.append("load")
            return object()

    class Cleaner:
        def cleanup(self, _value: object) -> ClickHouseFailedScratchCleanupReceipt:
            events.append("cleanup_absent")
            return _cleanup_receipt()

    class Resources:
        def reserve_operation(self, **_identity: object) -> None:
            raise AssertionError("failed cleanup must not reserve")

        def release_operation(self, **_identity: object) -> None:
            events.append("release_replay")

    class ReleasedResources:
        def assert_released(self, **_identity: object) -> object:
            events.append("assert_released")
            return object()

    class CleanupAck:
        def persist_exact(self, **_proofs: object) -> object:
            nonlocal calls
            calls += 1
            events.append("persist_ack")
            if calls == 1:
                raise RuntimeError("ack commit acknowledgement lost")
            return acknowledgement

    service = SemanticRefreshFailedPrecommitCleanupService(
        authority=Authority(),  # type: ignore[arg-type]
        cleaner=Cleaner(),  # type: ignore[arg-type]
        resources=Resources(),
        released_resources=ReleasedResources(),  # type: ignore[arg-type]
        cleanup_ack=CleanupAck(),  # type: ignore[arg-type]
    )
    identity = {
        "workflow_execution_binding_sha256": "sha256:" + "a" * 64,
        "operation_id": "sha256:" + "b" * 64,
    }

    with pytest.raises(RuntimeError, match="ack commit acknowledgement lost"):
        service.cleanup(**identity)
    observed = service.cleanup(**identity)

    assert observed is acknowledgement
    assert events == [
        "load",
        "cleanup_absent",
        "release_replay",
        "assert_released",
        "persist_ack",
        "load",
        "cleanup_absent",
        "release_replay",
        "assert_released",
        "persist_ack",
    ]


def test_failed_precommit_cleanup_never_releases_when_scratch_cleanup_fails() -> None:
    events: list[str] = []

    class Authority:
        def load(self, _binding: str, _operation_id: str) -> object:
            return object()

    class Cleaner:
        def cleanup(self, _value: object) -> None:
            events.append("cleanup")
            raise RuntimeError("scratch proof unavailable")

    class Resources:
        def reserve_operation(self, **_identity: object) -> None:
            raise AssertionError("failed cleanup must not reserve")

        def release_operation(self, **_identity: object) -> None:
            events.append("release")

    class UnreachableReleasedResources:
        def assert_released(self, **_identity: object) -> object:
            events.append("assert_released")
            return object()

    class UnreachableCleanupAck:
        def persist_exact(self, **_proofs: object) -> MssqlFailedPrecommitCleanupAck:
            events.append("persist_ack")
            raise AssertionError("failed cleanup must not persist acknowledgement")

    with pytest.raises(RuntimeError, match="scratch proof unavailable"):
        SemanticRefreshFailedPrecommitCleanupService(
            authority=Authority(),  # type: ignore[arg-type]
            cleaner=Cleaner(),  # type: ignore[arg-type]
            resources=Resources(),
            released_resources=UnreachableReleasedResources(),  # type: ignore[arg-type]
            cleanup_ack=UnreachableCleanupAck(),
        ).cleanup(
            workflow_execution_binding_sha256="sha256:" + "a" * 64,
            operation_id="sha256:" + "b" * 64,
        )

    assert events == ["cleanup"]
