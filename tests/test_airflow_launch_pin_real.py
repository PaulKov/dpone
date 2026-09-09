"""Installed-Airflow operator lifecycle tests for launch pins (authority C).

Hermetic store/CAS/envelope contracts live in ``test_airflow_launch_pin.py``.
This module requires a real Apache Airflow + cncf-kubernetes provider install
(the Airflow pack compatibility matrix).
"""

from __future__ import annotations

import types

import pytest

pytest.importorskip("airflow", reason="installed Airflow is required")
pytest.importorskip("airflow.providers.cncf.kubernetes")

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator  # noqa: E402
from dpone_airflow_pack.launch_pin import (  # noqa: E402
    AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY,
    commit_launch_pin_for_selected_pod,
)
from dpone_airflow_pack.launch_pin_pod import (  # noqa: E402
    InMemoryLaunchPinPodReader,
    set_launch_pin_pod_reader,
)
from dpone_airflow_pack.launch_pin_store import InMemoryLaunchPinStore, set_launch_pin_store  # noqa: E402
from dpone_airflow_pack.operators import PinnedXComSidecarKubernetesPodOperator  # noqa: E402
from dpone_airflow_pack.outcome import evaluate_pack_outcome  # noqa: E402

from tests.test_airflow_launch_pin import (  # noqa: E402
    _deployment_identity,
    _pod,
    _run_identity,
    _xcom_pull_with_locator,
    _xcom_summary,
    _XComStore,
)


@pytest.fixture(autouse=True)
def _isolated_launch_pin_authority() -> None:
    store = InMemoryLaunchPinStore()
    reader = InMemoryLaunchPinPodReader()
    set_launch_pin_store(store)
    set_launch_pin_pod_reader(reader)
    yield store, reader
    set_launch_pin_store(None)
    set_launch_pin_pod_reader(None)


def _runtime_operator(**overrides: object) -> PinnedXComSidecarKubernetesPodOperator:
    defaults: dict[str, object] = {
        "task_id": "orders__runtime",
        "name": "orders-runtime",
        "namespace": "airflow",
        "image": "dpone-runtime:test",
        "cmds": ["/bin/sh", "-ec"],
        "arguments": ["true"],
        "kubernetes_conn_id": "kubernetes_default",
        "do_xcom_push": True,
        "get_logs": False,
        "pin_deployment_identity_for_separate_outcome_gate": True,
        "launch_pin_store": {"kubernetes_conn_id": "kubernetes_default", "namespace": "airflow"},
        "params": {
            "dpone_deployment_identity": _deployment_identity(),
            "dpone_run_identity": _run_identity(),
        },
    }
    defaults.update(overrides)
    return PinnedXComSidecarKubernetesPodOperator(**defaults)  # type: ignore[arg-type]


def test_pre_hook_operator_does_not_pin_without_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    base_operator = PinnedXComSidecarKubernetesPodOperator.__mro__[1]
    pushed: list[object] = []

    monkeypatch.setattr(base_operator, "get_or_create_pod", lambda _self, pod, _context: pod)
    operator = PinnedXComSidecarKubernetesPodOperator(
        task_id="orders__pre_hook",
        name="orders-pre-hook",
        namespace="airflow",
        image="dpone-runtime:test",
        cmds=["/bin/sh", "-ec"],
        arguments=["true"],
        do_xcom_push=False,
        pin_deployment_identity_for_separate_outcome_gate=False,
        params={"dpone_deployment_identity": _deployment_identity()},
    )
    ti = types.SimpleNamespace(
        dag_id="dag",
        run_id="run",
        task_id="orders__pre_hook",
        map_index=-1,
        try_number=1,
        xcom_push=lambda **kwargs: pushed.append(kwargs),
        xcom_pull=lambda **_: None,
    )
    operator.get_or_create_pod(
        {
            "metadata": {
                "name": "hook",
                "namespace": "airflow",
                "uid": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            }
        },
        {"ti": ti},
    )
    assert pushed == []


def test_get_or_create_pod_hydrates_uid_before_commit(monkeypatch: pytest.MonkeyPatch) -> None:
    base_operator = PinnedXComSidecarKubernetesPodOperator.__mro__[1]
    remote = _pod(
        uid="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
        name="runtime-pod",
    )
    reader = InMemoryLaunchPinPodReader()
    reader.put(remote)
    set_launch_pin_pod_reader(reader)

    def _create(_self: object, pod: object, _context: object) -> object:
        assert isinstance(pod, dict)
        return {
            "metadata": {"name": "runtime-pod", "namespace": "airflow", "annotations": {}},
            "spec": pod.get("spec") if isinstance(pod, dict) else remote["spec"],
        }

    monkeypatch.setattr(base_operator, "get_or_create_pod", _create)
    operator = _runtime_operator(do_xcom_push=False)
    monkeypatch.setattr(
        "dpone_airflow_pack.launch_pin_pod._read_pod_via_operator_or_reader",
        lambda **kwargs: reader.read_pod(
            namespace=str(kwargs["namespace"]),
            name=str(kwargs["name"]),
            kubernetes_conn_id=kwargs.get("kubernetes_conn_id"),
        ),
    )
    ti = _XComStore()
    selected = operator.get_or_create_pod(remote, {"ti": ti})
    assert selected["metadata"]["uid"] == "cccccccc-cccc-4ccc-8ccc-cccccccccccc"  # type: ignore[index]
    assert ti.values[AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY]["pod_uid"] == "cccccccc-cccc-4ccc-8ccc-cccccccccccc"


def test_separate_outcome_gate_forces_keep_pod() -> None:
    operator = _runtime_operator(
        do_xcom_push=False,
        on_finish_action="delete_succeeded_pod",
    )
    assert getattr(operator, "on_finish_action", None) == "keep_pod" or (
        isinstance(getattr(operator, "kwargs", None), dict) and operator.kwargs.get("on_finish_action") == "keep_pod"
    )


def test_deferral_two_operator_instances_preserve_launch_pin_ref() -> None:
    """Instance A execute/defer → instance B trigger_reentry keeps closed ref → gate PRESENT."""

    from dpone_airflow_pack.launch_pin_commit import (
        embed_launch_pin_ref_in_summary,
        inject_launch_pin_ref_into_defer_kwargs,
        remember_committed_launch_pin_ref,
    )
    from dpone_airflow_pack.launch_pin_summary import launch_pin_ref_error

    ti = _XComStore()
    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
        evidence="sha256:" + "f" * 64,
    )
    retained = commit_launch_pin_for_selected_pod(context={"ti": ti}, pod=pod)
    assert launch_pin_ref_error(retained["launch_pin_ref"]) == ""

    op_a = _runtime_operator()
    remember_committed_launch_pin_ref(op_a, retained)
    defer_kwargs = inject_launch_pin_ref_into_defer_kwargs(op_a, kwargs=None)
    assert "launch_pin_ref" in defer_kwargs
    assert launch_pin_ref_error(defer_kwargs["launch_pin_ref"]) == ""

    class _ResumeOperator(PinnedXComSidecarKubernetesPodOperator):
        def trigger_reentry(self, context, event, **kwargs):  # type: ignore[no-untyped-def]
            from dpone_airflow_pack.launch_pin_commit import rehydrate_committed_launch_pin_ref

            rehydrate_committed_launch_pin_ref(self, context=context, defer_kwargs=kwargs)
            summary = _xcom_summary(
                deployment_identity=_deployment_identity(),
                run_identity=_run_identity(),
                pin=None,
            )
            summary.pop("launch_pin_ref", None)
            return embed_launch_pin_ref_in_summary(self, summary, context=context)

    op_b = _ResumeOperator(
        task_id="orders__runtime",
        name="orders-runtime",
        namespace="airflow",
        image="dpone-runtime:test",
        cmds=["/bin/sh", "-ec"],
        arguments=["true"],
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
        get_logs=False,
        pin_deployment_identity_for_separate_outcome_gate=True,
        launch_pin_store={"kubernetes_conn_id": "kubernetes_default", "namespace": "airflow"},
        params={
            "dpone_deployment_identity": _deployment_identity(),
            "dpone_run_identity": _run_identity(),
        },
    )
    assert getattr(op_b, "_committed_launch_pin_ref", None) is None
    context = {"ti": ti}
    result = op_b.trigger_reentry(context, {"status": "success"}, **defer_kwargs)
    assert isinstance(result, dict)
    assert launch_pin_ref_error(result.get("launch_pin_ref")) == ""
    assert result["launch_pin_ref"]["pod_uid"] == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    assert result["launch_pin_ref"]["pointer_resource_version"]

    payload = evaluate_pack_outcome(
        upstream_task_id="orders__runtime",
        ti=types.SimpleNamespace(
            dag_id="dag",
            run_id="run",
            xcom_pull=_xcom_pull_with_locator(summary=result, pin=retained),
            xcom_push=lambda **_: None,
        ),
        launch_pin_required=True,
    )
    assert payload["passed"] is True
    assert result["launch_pin_ref"]["pin_sha256"] == retained["pin_sha256"]
    assert result["launch_pin_ref"]["envelope_sha256"] == retained["envelope_sha256"]


def test_deferral_rehydrates_launch_pin_ref_from_locator_xcom() -> None:
    from dpone_airflow_pack.launch_pin_commit import embed_launch_pin_ref_in_summary, rehydrate_committed_launch_pin_ref
    from dpone_airflow_pack.launch_pin_summary import launch_pin_ref_error

    ti = _XComStore()
    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
        evidence="sha256:" + "f" * 64,
    )
    retained = commit_launch_pin_for_selected_pod(context={"ti": ti}, pod=pod)

    op_b = _runtime_operator()
    context = {
        "ti": types.SimpleNamespace(dag_id="dag", run_id="run", task_id="orders__runtime", xcom_pull=ti.xcom_pull)
    }
    ref = rehydrate_committed_launch_pin_ref(op_b, context=context, defer_kwargs=None)
    assert ref is not None
    assert launch_pin_ref_error(ref) == ""
    summary = {"status": "passed", "run_identity": _run_identity(), "deployment_identity": _deployment_identity()}
    embedded = embed_launch_pin_ref_in_summary(op_b, summary, context=context)
    assert embedded["launch_pin_ref"]["pin_sha256"] == retained["pin_sha256"]


def test_trigger_reentry_provider_1014_pulls_xcom_when_super_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Production trigger_reentry: super pushes return_value and returns None → gate PRESENT."""

    from dpone_airflow_pack.launch_pin_commit import (
        inject_launch_pin_ref_into_defer_kwargs,
        remember_committed_launch_pin_ref,
    )
    from dpone_airflow_pack.launch_pin_summary import launch_pin_ref_error

    ti = _XComStore()
    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
        evidence="sha256:" + "f" * 64,
    )
    retained = commit_launch_pin_for_selected_pod(context={"ti": ti}, pod=pod)
    summary_without_ref = _xcom_summary(
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
        pin=None,
    )
    summary_without_ref.pop("launch_pin_ref", None)

    op_a = _runtime_operator()
    remember_committed_launch_pin_ref(op_a, retained)
    defer_kwargs = inject_launch_pin_ref_into_defer_kwargs(op_a, kwargs=None)

    def _provider_trigger_reentry(self: object, context: object, event: object) -> None:
        del self, event
        ti_inner = context.get("ti") if isinstance(context, dict) else None
        if ti_inner is not None and hasattr(ti_inner, "xcom_push"):
            ti_inner.xcom_push(key="return_value", value=dict(summary_without_ref))
        return None

    monkeypatch.setattr(KubernetesPodOperator, "trigger_reentry", _provider_trigger_reentry)

    op_b = _runtime_operator()
    context = {"ti": ti}
    result = op_b.trigger_reentry(context, {"status": "success"}, **defer_kwargs)
    assert isinstance(result, dict)
    assert launch_pin_ref_error(result.get("launch_pin_ref")) == ""
    payload = evaluate_pack_outcome(
        upstream_task_id="orders__runtime",
        ti=types.SimpleNamespace(
            dag_id="dag",
            run_id="run",
            xcom_pull=_xcom_pull_with_locator(summary=result, pin=retained),
            xcom_push=lambda **_: None,
        ),
        launch_pin_required=True,
    )
    assert payload["passed"] is True
