from __future__ import annotations

import pytest

from dpone.services.safe_sample_policy import TemporaryTargetPlan


def _plan() -> TemporaryTargetPlan:
    return TemporaryTargetPlan(
        mode="temporary",
        pipeline_id="orders_daily",
        process="orders_daily",
        sink_type="clickhouse",
        connection_ref="clickhouse_dev",
        original_table={"schema": "analytics", "name": "orders"},
        temporary_table={"schema": "dpone_tmp_development", "name": "orders_daily_abc123"},
        ttl_seconds=86400,
        cleanup_required=True,
        pii_policy="masked",
    )


def test_temporary_target_lifecycle_prepares_and_cleans_with_secret_free_evidence() -> None:
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor

    class FakeAdapter:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, str]]] = []

        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            self.calls.append(("create", plan.temporary_table))
            return {"backend": "fake-clickhouse", "created": True}

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            self.calls.append(("drop", plan.temporary_table))
            return {"backend": "fake-clickhouse", "dropped": True}

    adapter = FakeAdapter()
    executor = TemporaryTargetLifecycleExecutor(adapter=adapter)

    prepared = executor.prepare(_plan())
    cleaned = executor.cleanup(_plan())

    assert adapter.calls == [
        ("create", {"schema": "dpone_tmp_development", "name": "orders_daily_abc123"}),
        ("drop", {"schema": "dpone_tmp_development", "name": "orders_daily_abc123"}),
    ]
    assert prepared.status == "prepared"
    assert cleaned.status == "cleaned"
    assert prepared.to_dict()["schema"] == "dpone.temporary-target-lifecycle.v1"
    assert prepared.to_dict()["temporary_table"] == {"schema": "dpone_tmp_development", "name": "orders_daily_abc123"}
    assert "password" not in repr(prepared.to_dict()).lower()


def test_temporary_target_lifecycle_redacts_nested_secret_metadata() -> None:
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor

    class FakeAdapter:
        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {
                "backend": "fake-clickhouse",
                "access_token": "must-not-leak",
                "session-token": "must-not-leak-either",
                "lease_id": "lease-must-not-leak",
                "nested": {
                    "safe_metric": 42,
                    "password": "nested-must-not-leak",
                    "vault_token": "nested-token-must-not-leak",
                },
            }

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {}

    result = TemporaryTargetLifecycleExecutor(adapter=FakeAdapter()).prepare(_plan()).to_dict()

    assert result["adapter_metadata"] == {
        "backend": "fake-clickhouse",
        "nested": {"safe_metric": 42},
    }
    serialized = repr(result)
    assert "must-not-leak" not in serialized
    assert "password" not in serialized.lower()
    assert "vault_token" not in serialized.lower()
    assert "lease_id" not in serialized.lower()


def test_temporary_target_lifecycle_wraps_adapter_failure() -> None:
    from dpone.services.safe_sample_target_lifecycle import (
        TemporaryTargetLifecycleError,
        TemporaryTargetLifecycleExecutor,
    )

    class FailingAdapter:
        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            raise RuntimeError("permission denied")

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {}

    with pytest.raises(TemporaryTargetLifecycleError) as exc:
        TemporaryTargetLifecycleExecutor(adapter=FailingAdapter()).prepare(_plan())

    assert exc.value.code == "DPONE_RUNTIME_TEMPORARY_TARGET_CREATE_FAILED"
    assert "permission denied" in str(exc.value)


def test_temporary_target_lifecycle_registry_selects_adapter_by_sink_type() -> None:
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetAdapterRegistry

    class FakeAdapter:
        def __init__(self) -> None:
            self.created = False

        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            self.created = True
            return {"backend": "fake-clickhouse"}

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {"backend": "fake-clickhouse"}

    adapters: list[FakeAdapter] = []

    def factory(plan: TemporaryTargetPlan) -> FakeAdapter:
        assert plan.sink_type == "clickhouse"
        adapter = FakeAdapter()
        adapters.append(adapter)
        return adapter

    executor = TemporaryTargetAdapterRegistry({"clickhouse": factory}).executor_for(_plan())
    result = executor.prepare(_plan())

    assert result.status == "prepared"
    assert result.adapter_metadata == {"backend": "fake-clickhouse"}
    assert adapters[0].created is True


def test_temporary_target_lifecycle_registry_rejects_missing_adapter() -> None:
    from dpone.services.safe_sample_target_lifecycle import (
        TemporaryTargetAdapterRegistry,
        TemporaryTargetLifecycleError,
    )

    with pytest.raises(TemporaryTargetLifecycleError) as exc:
        TemporaryTargetAdapterRegistry().executor_for(_plan())

    assert exc.value.code == "DPONE_RUNTIME_TEMPORARY_TARGET_ADAPTER_NOT_FOUND"
    assert "clickhouse" in str(exc.value)
