from __future__ import annotations

import json

import pytest

from dpone.backfill.mapping import build_airflow_mapping_plan
from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy


class _MappedOperator:
    def __init__(self, static_kwargs: dict[str, object], mapped_kwargs: dict[str, object]) -> None:
        self.static_kwargs = static_kwargs
        self.mapped_kwargs = mapped_kwargs

    def __rshift__(self, other):
        return other


class _PartialOperator:
    def __init__(self, kwargs: dict[str, object]) -> None:
        self.kwargs = kwargs

    def expand(self, **kwargs):
        return _MappedOperator(self.kwargs, kwargs)


class _Operator:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs

    @classmethod
    def partial(cls, **kwargs):
        return _PartialOperator(kwargs)

    def __rshift__(self, other):
        return other


def _config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="tgt",
        source_schema="dbo",
        source_table="orders",
        target_schema="analytics",
        target_table="orders",
        load_strategy=LoadStrategy.BACKFILL,
        options={
            "source_type": "mssql",
            "sink_type": "postgres",
            "backfill": {
                "inner_mode": "partition_replace",
                "parallel_workers": 1,
                "chunk": {"column": "d", "from": "2025-01-01", "to": "2025-01-04", "step": "1d"},
                "state": {"backend": "audit_schema", "require_distributed_lock": True},
            },
        },
    )


def _pack(mode: str = "summary") -> dict[str, object]:
    plan = build_airflow_mapping_plan(
        _config(),
        {"mode": mode, "max_items": 2 if mode == "summary" else 4, "max_active": 2, "pool": "history"},
    )
    return {
        "kind": "gitops.airflow_pack",
        "schema_version": "3",
        "workload": {"workload_id": "orders"},
        "kpo_kwargs": {
            "task_id": "orders__dpone_runtime",
            "image": "dpone:test",
            "env_vars": {"BASE": "one"},
        },
        "mapping_plan": plan.to_jsonable(),
        "steps": [],
        "outcome_gate": {"required_status": "passed"},
        "xcom": {},
    }


def test_provider_materializes_static_bounded_mapping_with_inline_outcome(monkeypatch) -> None:
    import dpone_airflow_pack.pack_tasks as pack_tasks

    monkeypatch.setattr(pack_tasks, "_operator_class", lambda pack: _Operator)

    tasks = pack_tasks._build_tasks_from_loaded_pack(
        _pack(),
        dag=object(),
        operator_overrides={"retries": 1},
        node=None,
        task_group=None,
    )

    assert sorted(tasks) == ["dpone_runtime"]
    runtime = tasks["dpone_runtime"]
    assert runtime.static_kwargs["pool"] == "history"
    assert runtime.static_kwargs["max_active_tis_per_dag"] == 2
    assert runtime.static_kwargs["inline_outcome_required_status"] == "passed"
    assert runtime.static_kwargs["retries"] == 1
    assert "env_vars" not in runtime.static_kwargs
    environments = runtime.mapped_kwargs["env_vars"]
    assert len(environments) == 2
    assert all(environment["BASE"] == "one" for environment in environments)
    payloads = [json.loads(environment["DPONE_AIRFLOW_MAPPING_ITEM"]) for environment in environments]
    assert [(item["first_chunk_index"], item["last_chunk_index"]) for item in payloads] == [(1, 2), (3, 4)]


def test_provider_keeps_legacy_internal_operator_behavior(monkeypatch) -> None:
    import dpone_airflow_pack.pack_tasks as pack_tasks

    monkeypatch.setattr(pack_tasks, "_operator_class", lambda pack: _Operator)
    pack = _pack()
    pack.pop("mapping_plan")
    pack.pop("outcome_gate")

    tasks = pack_tasks._build_tasks_from_loaded_pack(
        pack,
        dag=object(),
        operator_overrides=None,
        node=None,
        task_group=None,
    )

    assert isinstance(tasks["dpone_runtime"], _Operator)
    assert sorted(tasks) == ["dpone_runtime"]


def test_provider_rejects_mapping_fingerprint_drift_before_operator_creation(monkeypatch) -> None:
    import dpone_airflow_pack.pack_tasks as pack_tasks

    created: list[object] = []
    monkeypatch.setattr(pack_tasks, "_operator_class", lambda pack: created.append(pack) or _Operator)
    pack = _pack()
    pack["mapping_plan"]["items"][0]["last_chunk_index"] = 3

    with pytest.raises(ValueError, match="DPONE_AIRFLOW_MAPPING_PLAN_MISMATCH"):
        pack_tasks._build_tasks_from_loaded_pack(
            pack,
            dag=object(),
            operator_overrides=None,
            node=None,
            task_group=None,
        )

    assert created == []


def test_provider_rejects_pool_override_for_mapped_workload(monkeypatch) -> None:
    import dpone_airflow_pack.pack_tasks as pack_tasks

    monkeypatch.setattr(pack_tasks, "_operator_class", lambda pack: _Operator)

    with pytest.raises(ValueError, match="bounded mapping fields"):
        pack_tasks._build_tasks_from_loaded_pack(
            _pack(),
            dag=object(),
            operator_overrides={"pool": "other"},
            node=None,
            task_group=None,
        )


def test_mapped_pack_wire_guard_rejects_legacy_provider_projection() -> None:
    from dpone_airflow_pack.pack_validation import validate_airflow_pack_payload
    from dpone_airflow_pack.runtime_adapter import DponeAirflowContractError

    unsafe = _pack()
    with pytest.raises(DponeAirflowContractError) as raised:
        validate_airflow_pack_payload(unsafe, location="memory://unsafe-pack")

    assert raised.value.blockers[0]["code"] == "airflow_pack_mapping_provider_guard_invalid"

    guarded = _pack()
    guarded["mapped_kpo_kwargs"] = guarded.pop("kpo_kwargs")
    normalized = validate_airflow_pack_payload(guarded, location="memory://guarded-pack")

    assert "mapped_kpo_kwargs" not in normalized
    assert normalized["kpo_kwargs"]["task_id"] == "orders__dpone_runtime"
