"""Lifecycle contract tests for outcome_gate launch pins (authority C)."""

from __future__ import annotations

import json
import types
from collections.abc import Mapping

import pytest
from dpone_airflow_pack.deployment_identity import (
    AIRFLOW_DEPLOYMENT_IDENTITY_ENV,
    serialize_deployment_identity,
)
from dpone_airflow_pack.launch_pin import (
    AIRFLOW_EXPECTED_RUNTIME_EVIDENCE_SHA256_ENV,
    AIRFLOW_RUN_PINNED_DEPLOYMENT_IDENTITY_XCOM_KEY,
    AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY,
    PIN_INVALID,
    PIN_MISSING,
    PIN_STALE_WRITER,
    PIN_UNAVAILABLE,
    build_launch_pin,
    commit_launch_pin_for_selected_pod,
    ensure_launch_envelope_on_pod,
    resolve_launch_pin,
)
from dpone_airflow_pack.launch_pin_envelope import envelope_sha256
from dpone_airflow_pack.launch_pin_pod import (
    InMemoryLaunchPinPodReader,
    ensure_selected_pod_with_uid,
    set_launch_pin_pod_reader,
)
from dpone_airflow_pack.launch_pin_store import InMemoryLaunchPinStore, set_launch_pin_store
from dpone_airflow_pack.outcome import evaluate_pack_outcome
from dpone_airflow_pack.run_identity import AIRFLOW_RUN_IDENTITY_ENV, serialize_run_identity


@pytest.fixture(autouse=True)
def _isolated_launch_pin_authority() -> None:
    global _LAST_STORED_PIN
    store = InMemoryLaunchPinStore()
    reader = InMemoryLaunchPinPodReader()
    set_launch_pin_store(store)
    set_launch_pin_pod_reader(reader)
    _LAST_STORED_PIN = None
    yield store, reader
    _LAST_STORED_PIN = None
    set_launch_pin_store(None)
    set_launch_pin_pod_reader(None)


def _run_identity(*, deployment_digest: str = "b") -> dict[str, object]:
    return {
        "schema": "dpone.airflow-run-identity.v1",
        "release_id": "sha256:" + "a" * 64,
        "deployment_id": "sha256:" + deployment_digest * 64,
        "dag_spec": {"id": "orders_daily", "sha256": "sha256:" + "c" * 64},
        "workload_pack": {"id": "orders", "sha256": "sha256:" + "d" * 64},
        "runtime_image_digest": "sha256:" + "e" * 64,
        "binding_set_ref": "sha256:" + "1" * 64,
        "connection_registry_ref": "sha256:" + "2" * 64,
        "credential_runtime_ref": "sha256:" + "3" * 64,
        "airflow_bundle": {
            "backend": "git",
            "ref": "git:7ac31f2",
            "versioned": True,
            "version": "7ac31f2",
            "snapshot_ref": None,
        },
    }


def _deployment_identity(
    *,
    deployment_digest: str = "b",
    activation_id: str = "3f60628e-ef48-48b0-84c3-a9e27a82a7f2",
) -> dict[str, object]:
    return {
        "schema": "dpone.airflow-deployment-identity.v1",
        "release_id": "sha256:" + "a" * 64,
        "deployment_id": "sha256:" + deployment_digest * 64,
        "activation_id": activation_id,
    }


_LAST_STORED_PIN: dict[str, object] | None = None


def _xcom_summary(
    *,
    deployment_identity: dict[str, object],
    run_identity: dict[str, object],
    pin: dict[str, object] | None = None,
) -> dict[str, object]:
    from dpone_airflow_pack.launch_pin_commit import build_launch_pin_ref

    summary: dict[str, object] = {
        "kind": "gitops.airflow_xcom_summary",
        "schema_version": "1",
        "producer": "dpone gitops airflow run-spec-exec",
        "status": "passed",
        "runtime_profile_path": "runtime-profile.json",
        "run_spec_path": "run-spec.json",
        "runtime_evidence_path": "runtime-evidence.json",
        "runtime_evidence_sha256": "sha256:" + "f" * 64,
        "runtime_evidence": {
            "schema_version": "dpone.airflow.inline_runtime_evidence.v1",
            "status": "passed",
            "metrics": {"duration_seconds": 1.0, "step_count": 1},
        },
        "run_identity": run_identity,
        "deployment_identity": deployment_identity,
    }
    source = pin if pin is not None else _LAST_STORED_PIN
    if isinstance(source, dict) and source.get("pin_sha256"):
        closed_source = dict(source)
        if not closed_source.get("pointer_resource_version"):
            closed_source["pointer_resource_version"] = "test-rv"
        if not closed_source.get("envelope_sha256"):
            closed_source["envelope_sha256"] = envelope_sha256(
                run_identity=run_identity,
                deployment_identity=deployment_identity,
                expected_runtime_evidence_sha256=str(summary.get("runtime_evidence_sha256") or "") or None,
            )
        summary["launch_pin_ref"] = build_launch_pin_ref(closed_source)
    return summary


def _pod(
    *,
    uid: str,
    deployment_identity: dict[str, object],
    run_identity: dict[str, object],
    evidence: str | None = "sha256:" + "f" * 64,
    namespace: str = "airflow",
    name: str | None = None,
) -> dict[str, object]:
    pod: dict[str, object] = {
        "metadata": {
            "name": name or f"pod-{uid[:8]}",
            "namespace": namespace,
            "uid": uid,
            "annotations": {},
        },
        "spec": {
            "containers": [
                {
                    "name": "base",
                    "env": [
                        {
                            "name": AIRFLOW_DEPLOYMENT_IDENTITY_ENV,
                            "value": serialize_deployment_identity(deployment_identity),
                        },
                        {
                            "name": AIRFLOW_RUN_IDENTITY_ENV,
                            "value": serialize_run_identity(run_identity),
                        },
                        {
                            "name": AIRFLOW_EXPECTED_RUNTIME_EVIDENCE_SHA256_ENV,
                            "value": "" if evidence is None else evidence,
                        },
                    ],
                }
            ]
        },
    }
    ensure_launch_envelope_on_pod(
        pod,
        run_identity=run_identity,
        deployment_identity=deployment_identity,
        expected_runtime_evidence_sha256=evidence,
    )
    return pod


def _stamp_attempt_store_authority(
    pin: dict[str, object],
    *,
    kubernetes_conn_id: str | None = None,
    namespace: str | None = None,
) -> dict[str, object]:
    from dpone_airflow_pack.launch_pin_locator import store_authority_digest

    ns = namespace
    if ns is None and isinstance(pin.get("store_namespace"), str):
        ns = str(pin["store_namespace"])
    if ns is None and isinstance(pin.get("pod_namespace"), str):
        ns = str(pin["pod_namespace"])
    conn = kubernetes_conn_id
    if conn is None and isinstance(pin.get("kubernetes_conn_id"), str):
        conn = str(pin["kubernetes_conn_id"]).strip() or None
    stamped = dict(pin)
    if ns:
        stamped["store_namespace"] = ns
    stamped["store_authority_digest"] = store_authority_digest(kubernetes_conn_id=conn, namespace=ns)
    return stamped


def _enrich_locator_xcom(pin: Mapping[str, object]) -> dict[str, object]:
    from dpone_airflow_pack.launch_pin_commit import build_launch_pin_ref
    from dpone_airflow_pack.launch_pin_locator import LaunchPinStoreLocator, launch_pin_store_locator_to_mapping

    stamped = _stamp_attempt_store_authority(dict(pin))
    ref = build_launch_pin_ref(stamped)
    store_conn = stamped.get("kubernetes_conn_id")
    store_ns = stamped.get("store_namespace")
    closed_store = launch_pin_store_locator_to_mapping(
        LaunchPinStoreLocator(
            kubernetes_conn_id=(
                str(store_conn).strip() if isinstance(store_conn, str) and str(store_conn).strip() else None
            ),
            namespace=str(store_ns).strip() if isinstance(store_ns, str) and str(store_ns).strip() else None,
        )
    )
    return {
        **ref,
        **stamped,
        "launch_pin_ref": ref,
        "launch_pin_store": closed_store,
    }


def _store_pin_with_pod(
    *,
    pod: dict[str, object],
    attempt: dict[str, object] | None = None,
) -> dict[str, object]:
    global _LAST_STORED_PIN
    from dpone_airflow_pack.launch_pin_store import get_launch_pin_store

    meta = pod["metadata"]
    assert isinstance(meta, dict)
    # Override envelope from the pod itself for accuracy.
    from dpone_airflow_pack.launch_pin_envelope import launch_envelope_from_pod
    from dpone_airflow_pack.launch_pin_pod import remember_launch_pin_pod

    envelope = launch_envelope_from_pod(pod)
    pin = build_launch_pin(
        attempt=attempt
        or {
            "dag_id": "dag",
            "run_id": "run",
            "task_id": "orders__runtime",
            "map_index": -1,
            "try_number": 1,
        },
        pod_namespace=str(meta["namespace"]),
        pod_name=str(meta["name"]),
        pod_uid=str(meta["uid"]),
        run_identity=envelope["run_identity"],
        deployment_identity=envelope["deployment_identity"],
        expected_runtime_evidence_sha256=envelope["expected_runtime_evidence_sha256"],
    )
    remember_launch_pin_pod(pod)
    pin = _stamp_attempt_store_authority(pin, namespace=str(meta["namespace"]))
    store = get_launch_pin_store()
    assert store is not None
    retained = store.create_once(pin)
    _LAST_STORED_PIN = dict(retained)
    return retained


def _xcom_pull_with_locator(
    *,
    summary: object | None = None,
    pin: dict[str, object] | None = None,
    extras: dict[str, object] | None = None,
):
    from dpone_airflow_pack.launch_pin_commit import build_launch_pin_ref

    locator = pin if pin is not None else _LAST_STORED_PIN
    bound_summary = summary
    if isinstance(bound_summary, dict) and isinstance(locator, dict) and locator.get("pin_sha256"):
        bound_summary = dict(bound_summary)
        bound_summary["launch_pin_ref"] = build_launch_pin_ref(_stamp_attempt_store_authority(dict(locator)))
    enriched_locator = _enrich_locator_xcom(dict(locator)) if isinstance(locator, dict) else locator

    def _pull(*, task_ids: str, key: str = "return_value", **_: object) -> object:
        del task_ids
        if key == "return_value":
            return bound_summary
        if key == AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY:
            return enriched_locator
        if extras is not None and key in extras:
            return extras[key]
        return None

    return _pull


class _XComStore:
    def __init__(self, task_id: str = "orders__runtime") -> None:
        self.task_id = task_id
        self.dag_id = "dag"
        self.run_id = "run"
        self.map_index = -1
        self.try_number = 1
        self.values: dict[str, object] = {}

    def xcom_push(self, *, key: str, value: object) -> None:
        self.values[key] = value

    def xcom_pull(self, *, task_ids: str | None = None, key: str = "return_value", **_: object) -> object:
        del task_ids
        return self.values.get(key)


def test_reattach_keeps_existing_pin_when_operator_expects_new_tip() -> None:
    launch_deployment = _deployment_identity()
    launch_run = _run_identity()
    tip_b_deployment = _deployment_identity(
        deployment_digest="c",
        activation_id="4f60628e-ef48-48b0-84c3-a9e27a82a7f2",
    )
    tip_b_run = _run_identity(deployment_digest="c")
    ti = _XComStore()
    pod_a = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=launch_deployment,
        run_identity=launch_run,
        evidence="sha256:" + "f" * 64,
    )
    first = commit_launch_pin_for_selected_pod(context={"ti": ti}, pod=pod_a)
    assert first["deployment_identity"] == launch_deployment

    retained = commit_launch_pin_for_selected_pod(context={"ti": ti}, pod=pod_a)
    assert retained == first
    assert ti.values[AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY]["deployment_identity"] == launch_deployment
    del tip_b_deployment, tip_b_run

    summary = _xcom_summary(deployment_identity=launch_deployment, run_identity=launch_run, pin=first)
    ti.values["return_value"] = summary

    def _pull(*, task_ids: str, key: str = "return_value", **_: object) -> object:
        if key == "return_value":
            return summary
        return ti.values.get(key)

    payload = evaluate_pack_outcome(
        upstream_task_id="orders__runtime",
        ti=types.SimpleNamespace(dag_id="dag", run_id="run", xcom_pull=_pull),
        expected_deployment_identity=_deployment_identity(deployment_digest="c"),
        expected_run_identity=_run_identity(deployment_digest="c"),
        expected_runtime_evidence_sha256="sha256:" + "9" * 64,
        launch_pin_required=True,
    )
    assert payload["passed"] is True


def test_reattach_without_prior_pin_reads_identity_from_selected_pod() -> None:
    launch_deployment = _deployment_identity()
    launch_run = _run_identity()
    tip_b_deployment = _deployment_identity(
        deployment_digest="c",
        activation_id="4f60628e-ef48-48b0-84c3-a9e27a82a7f2",
    )
    tip_b_run = _run_identity(deployment_digest="c")
    ti = _XComStore()
    pod_a = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=launch_deployment,
        run_identity=launch_run,
        evidence="sha256:" + "f" * 64,
    )
    del tip_b_deployment, tip_b_run
    pinned = commit_launch_pin_for_selected_pod(context={"ti": ti}, pod=pod_a)
    assert pinned["deployment_identity"] == launch_deployment
    assert pinned["run_identity"] == launch_run
    assert pinned["expected_runtime_evidence_sha256"] == "sha256:" + "f" * 64


def test_present_pin_with_null_evidence_does_not_apply_parse_time_evidence() -> None:
    launch_deployment = _deployment_identity()
    launch_run = _run_identity()
    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=launch_deployment,
        run_identity=launch_run,
        evidence=None,
    )
    pin = _store_pin_with_pod(pod=pod)
    summary = _xcom_summary(deployment_identity=launch_deployment, run_identity=launch_run)
    payload = evaluate_pack_outcome(
        upstream_task_id="orders__runtime",
        ti=types.SimpleNamespace(
            dag_id="dag",
            run_id="run",
            xcom_pull=_xcom_pull_with_locator(summary=summary, pin=pin),
        ),
        expected_deployment_identity=launch_deployment,
        expected_run_identity=launch_run,
        expected_runtime_evidence_sha256="sha256:" + "9" * 64,
        launch_pin_required=True,
    )
    assert payload["passed"] is True
    assert payload.get("runtime_evidence_sha256") == "sha256:" + "f" * 64


def test_legacy_deployment_only_pin_with_required_true_is_blocker() -> None:
    summary = _xcom_summary(
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
    )

    def _xcom_pull(*, task_ids: str, key: str = "return_value", **_: object) -> object:
        if key == "return_value":
            return summary
        if key == AIRFLOW_RUN_PINNED_DEPLOYMENT_IDENTITY_XCOM_KEY:
            return _deployment_identity()
        return None

    with pytest.raises(RuntimeError, match=PIN_MISSING):
        evaluate_pack_outcome(
            upstream_task_id="orders__runtime",
            ti=types.SimpleNamespace(dag_id="dag", run_id="run", xcom_pull=_xcom_pull),
            expected_deployment_identity=_deployment_identity(),
            launch_pin_required=True,
        )


def test_legacy_deployment_only_pin_allowed_when_required_false() -> None:
    summary = _xcom_summary(
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
    )

    def _xcom_pull(*, task_ids: str, key: str = "return_value", **_: object) -> object:
        if key == "return_value":
            return summary
        if key == AIRFLOW_RUN_PINNED_DEPLOYMENT_IDENTITY_XCOM_KEY:
            return _deployment_identity()
        return None

    resolution = resolve_launch_pin(
        ti=types.SimpleNamespace(dag_id="dag", run_id="run", xcom_pull=_xcom_pull),
        upstream_task_id="orders__runtime",
        required=False,
    )
    assert resolution.status == "PRESENT_LEGACY_PARTIAL"


def test_malformed_pod_identities_are_blocker() -> None:
    ti = _XComStore()
    pod = {
        "metadata": {
            "name": "pod-bad",
            "namespace": "airflow",
            "uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        },
        "spec": {
            "containers": [
                {
                    "name": "base",
                    "env": [
                        {"name": AIRFLOW_RUN_IDENTITY_ENV, "value": "{not-json"},
                        {
                            "name": AIRFLOW_DEPLOYMENT_IDENTITY_ENV,
                            "value": serialize_deployment_identity(_deployment_identity()),
                        },
                        {"name": AIRFLOW_EXPECTED_RUNTIME_EVIDENCE_SHA256_ENV, "value": ""},
                        {
                            "name": "DPONE_AIRFLOW_ENVELOPE_SHA256",
                            "value": "sha256:" + "e" * 64,
                        },
                    ],
                }
            ]
        },
    }
    with pytest.raises(RuntimeError, match=PIN_INVALID):
        commit_launch_pin_for_selected_pod(context={"ti": ti}, pod=pod)


def test_missing_pod_envelope_is_blocker() -> None:
    ti = _XComStore()
    pod = {
        "metadata": {
            "name": "pod-empty",
            "namespace": "airflow",
            "uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        },
        "spec": {"containers": [{"name": "base", "env": []}]},
    }
    with pytest.raises(RuntimeError, match=PIN_MISSING):
        commit_launch_pin_for_selected_pod(context={"ti": ti}, pod=pod)


def test_malformed_pin_pointer_in_store_is_blocker_not_live_tip_fallback() -> None:
    from dpone_airflow_pack.launch_pin_store import get_launch_pin_store

    store = get_launch_pin_store()
    assert store is not None
    # Pod-ref pointer missing uid — cannot re-fetch authoritatively.
    bad = {
        "schema": AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY,
        "dag_id": "dag",
        "run_id": "run",
        "task_id": "orders__runtime",
        "map_index": -1,
        "try_number": 1,
        "pod_namespace": "airflow",
        "pod_name": "pod",
        "pod_uid": "",
        "run_identity": None,
        "deployment_identity": None,
        "expected_runtime_evidence_sha256": None,
        "envelope_sha256": "sha256:" + "e" * 64,
        "pin_sha256": "sha256:" + "0" * 64,
        "state": "ACTIVE",
    }
    store._rows[("dag", "run", "orders__runtime", -1, 1)] = bad  # type: ignore[attr-defined]
    store._subject_heads[("dag", "run", "orders__runtime", -1)] = 1  # type: ignore[attr-defined]
    summary = _xcom_summary(
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
    )
    with pytest.raises(RuntimeError, match=PIN_INVALID):
        evaluate_pack_outcome(
            upstream_task_id="orders__runtime",
            ti=types.SimpleNamespace(
                dag_id="dag",
                run_id="run",
                xcom_pull=_xcom_pull_with_locator(summary=summary, pin=bad),
            ),
            expected_deployment_identity=_deployment_identity(deployment_digest="c"),
            launch_pin_required=True,
        )


def test_required_true_store_miss_does_not_accept_xcom_as_present() -> None:
    launch_deployment = _deployment_identity()
    launch_run = _run_identity()
    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=launch_deployment,
        run_identity=launch_run,
    )
    pin = build_launch_pin(
        attempt={
            "dag_id": "dag",
            "run_id": "run",
            "task_id": "orders__runtime",
            "map_index": -1,
            "try_number": 1,
        },
        pod_namespace="airflow",
        pod_name=str(pod["metadata"]["name"]),  # type: ignore[index]
        pod_uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        run_identity=launch_run,
        deployment_identity=launch_deployment,
        expected_runtime_evidence_sha256="sha256:" + "f" * 64,
    )
    pin["pointer_resource_version"] = "xcom-only-rv"
    pin = _stamp_attempt_store_authority(pin, namespace="airflow")

    def _xcom_pull(*, task_ids: str, key: str = "return_value", **_: object) -> object:
        if key == "return_value":
            return _xcom_summary(deployment_identity=launch_deployment, run_identity=launch_run, pin=pin)
        if key == AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY:
            return _enrich_locator_xcom(pin)
        return None

    # Store is empty (autouse fixture); XCom has a full pin — must NOT be PRESENT.
    with pytest.raises(RuntimeError, match=PIN_MISSING):
        evaluate_pack_outcome(
            upstream_task_id="orders__runtime",
            ti=types.SimpleNamespace(dag_id="dag", run_id="run", xcom_pull=_xcom_pull),
            launch_pin_required=True,
        )


def test_unavailable_pin_read_is_blocker_not_live_tip_fallback() -> None:
    from dpone_airflow_pack.launch_pin_commit import build_launch_pin_ref

    locator = {
        "try_number": 1,
        "pod_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "pin_sha256": "sha256:" + "1" * 64,
        "envelope_sha256": "sha256:" + "2" * 64,
        "pointer_resource_version": "1",
    }
    summary = _xcom_summary(
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
    )
    summary["launch_pin_ref"] = build_launch_pin_ref(
        {
            **locator,
            "envelope_sha256": locator["envelope_sha256"],
        }
    )

    class _BoomStore:
        def get(self, **_: object) -> None:
            raise RuntimeError(f"{PIN_UNAVAILABLE}: metadata down")

        def create_once(self, pin: object) -> object:
            return pin

    set_launch_pin_store(_BoomStore())  # type: ignore[arg-type]

    def _xcom_pull(*, task_ids: str, key: str = "return_value", **_: object) -> object:
        if key == AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY:
            raise RuntimeError("xcom backend unavailable")
        if key == "return_value":
            return summary
        return None

    with pytest.raises(RuntimeError, match=PIN_UNAVAILABLE):
        evaluate_pack_outcome(
            upstream_task_id="orders__runtime",
            ti=types.SimpleNamespace(dag_id="dag", run_id="run", xcom_pull=_xcom_pull),
            expected_deployment_identity=_deployment_identity(deployment_digest="c"),
            launch_pin_required=True,
        )


def test_missing_required_pin_is_blocker() -> None:
    def _xcom_pull(*, task_ids: str, key: str = "return_value", **_: object) -> object:
        if key == "return_value":
            return _xcom_summary(
                deployment_identity=_deployment_identity(),
                run_identity=_run_identity(),
            )
        return None

    with pytest.raises(RuntimeError, match=PIN_MISSING):
        evaluate_pack_outcome(
            upstream_task_id="orders__runtime",
            ti=types.SimpleNamespace(dag_id="dag", run_id="run", xcom_pull=_xcom_pull),
            expected_deployment_identity=_deployment_identity(),
            launch_pin_required=True,
        )


def test_absent_legacy_pin_may_fallback() -> None:
    summary = _xcom_summary(
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
    )

    def _xcom_pull(*, task_ids: str, key: str = "return_value", **_: object) -> object:
        if key == "return_value":
            return summary
        return None

    payload = evaluate_pack_outcome(
        upstream_task_id="orders__runtime",
        ti=types.SimpleNamespace(dag_id="dag", run_id="run", xcom_pull=_xcom_pull),
        expected_deployment_identity=_deployment_identity(),
        expected_run_identity=_run_identity(),
        launch_pin_required=False,
    )
    assert payload["passed"] is True


def test_resolve_launch_pin_statuses() -> None:
    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
        evidence=None,
    )
    pin = _store_pin_with_pod(pod=pod)
    assert (
        resolve_launch_pin(
            ti=types.SimpleNamespace(dag_id="dag", run_id="run", xcom_pull=_xcom_pull_with_locator(pin=pin)),
            upstream_task_id="orders__runtime",
        ).status
        == "PRESENT"
    )

    class _BoomStore:
        def get(self, **_: object) -> None:
            raise RuntimeError(f"{PIN_UNAVAILABLE}: db down")

        def create_once(self, pin: object) -> object:
            return pin

    set_launch_pin_store(_BoomStore())  # type: ignore[arg-type]
    assert (
        resolve_launch_pin(
            ti=types.SimpleNamespace(
                dag_id="dag",
                run_id="run",
                xcom_pull=_xcom_pull_with_locator(pin=pin),
            ),
            upstream_task_id="orders__runtime",
            required=True,
        ).status
        == "UNAVAILABLE"
    )


def test_new_pod_without_request_uid_is_hydrated_via_read_pod() -> None:
    remote = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
        name="pod-new",
    )
    reader = InMemoryLaunchPinPodReader()
    reader.put(remote)
    set_launch_pin_pod_reader(reader)
    request = {
        "metadata": {"name": "pod-new", "namespace": "airflow", "annotations": {}},
        "spec": remote["spec"],
    }
    hydrated = ensure_selected_pod_with_uid(pod=request, operator=types.SimpleNamespace(namespace="airflow"))
    assert hydrated["metadata"]["uid"] == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"  # type: ignore[index]
    ti = _XComStore()
    pin = commit_launch_pin_for_selected_pod(context={"ti": ti}, pod=hydrated)
    assert pin["pod_uid"] == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


def test_reattach_pod_already_has_uid_skips_remote_read() -> None:
    calls: list[tuple[str, str]] = []

    class _CountingReader(InMemoryLaunchPinPodReader):
        def read_pod(self, *, namespace: str, name: str, kubernetes_conn_id: str | None = None) -> object:
            calls.append((namespace, name))
            return super().read_pod(namespace=namespace, name=name, kubernetes_conn_id=kubernetes_conn_id)

    reader = _CountingReader()
    set_launch_pin_pod_reader(reader)
    pod = _pod(
        uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
    )
    hydrated = ensure_selected_pod_with_uid(pod=pod, operator=None)
    assert hydrated is pod
    assert calls == []


def test_remote_read_failure_is_pin_unavailable() -> None:
    class _FailingReader:
        def read_pod(self, *, namespace: str, name: str, kubernetes_conn_id: str | None = None) -> object:
            del kubernetes_conn_id
            raise RuntimeError(f"{PIN_UNAVAILABLE}: boom {namespace}/{name}")

        def delete_pod(self, **_: object) -> None:
            raise RuntimeError(f"{PIN_UNAVAILABLE}: delete unused")

    set_launch_pin_pod_reader(_FailingReader())  # type: ignore[arg-type]
    request = {"metadata": {"name": "pod-x", "namespace": "airflow"}}
    with pytest.raises(RuntimeError, match=PIN_UNAVAILABLE):
        ensure_selected_pod_with_uid(pod=request, operator=None)


def test_gate_pod_deleted_is_pin_unavailable() -> None:
    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
    )
    pin = _store_pin_with_pod(pod=pod)
    set_launch_pin_pod_reader(InMemoryLaunchPinPodReader())  # empty registry → deleted
    summary = _xcom_summary(
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
    )
    with pytest.raises(RuntimeError, match=PIN_UNAVAILABLE):
        evaluate_pack_outcome(
            upstream_task_id="orders__runtime",
            ti=types.SimpleNamespace(
                dag_id="dag",
                run_id="run",
                xcom_pull=_xcom_pull_with_locator(summary=summary, pin=pin),
            ),
            launch_pin_required=True,
        )


def test_pin_conflict_when_same_try_selects_different_pod() -> None:
    ti = _XComStore()
    first_pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
    )
    commit_launch_pin_for_selected_pod(context={"ti": ti}, pod=first_pod)
    other_pod = _pod(
        uid="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
    )
    with pytest.raises(RuntimeError, match="PIN_CONFLICT"):
        commit_launch_pin_for_selected_pod(context={"ti": ti}, pod=other_pod)


def test_stale_lower_try_cannot_overwrite_newer_try() -> None:
    ti = _XComStore()
    ti.try_number = 2
    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
    )
    commit_launch_pin_for_selected_pod(context={"ti": ti}, pod=pod)
    ti.try_number = 1
    with pytest.raises(RuntimeError, match=PIN_STALE_WRITER):
        commit_launch_pin_for_selected_pod(context={"ti": ti}, pod=pod)


def test_concurrent_same_try_writers_are_idempotent_or_conflict() -> None:
    from dpone_airflow_pack.launch_pin_store import get_launch_pin_store

    store = get_launch_pin_store()
    assert store is not None
    pin_a = build_launch_pin(
        attempt={"dag_id": "dag", "run_id": "run", "task_id": "t", "map_index": -1, "try_number": 1},
        pod_namespace="airflow",
        pod_name="pod-a",
        pod_uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        run_identity=_run_identity(),
        deployment_identity=_deployment_identity(),
        expected_runtime_evidence_sha256=None,
    )
    retained = store.create_once(pin_a)
    assert retained["pin_sha256"] == pin_a["pin_sha256"]
    assert retained.get("state") == "ACTIVE"
    assert store.create_once(pin_a)["pin_sha256"] == pin_a["pin_sha256"]
    pin_b = build_launch_pin(
        attempt={"dag_id": "dag", "run_id": "run", "task_id": "t", "map_index": -1, "try_number": 1},
        pod_namespace="airflow",
        pod_name="pod-b",
        pod_uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        run_identity=_run_identity(),
        deployment_identity=_deployment_identity(),
        expected_runtime_evidence_sha256=None,
    )
    with pytest.raises(RuntimeError, match="PIN_CONFLICT"):
        store.create_once(pin_b)


def test_xcom_mirror_failure_after_active_is_recovery_required() -> None:
    from dpone_airflow_pack.launch_pin import PIN_RECOVERY_REQUIRED

    class _PushBoom(_XComStore):
        def xcom_push(self, *, key: str, value: object) -> None:
            raise RuntimeError("xcom down")

    ti = _PushBoom()
    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
    )
    with pytest.raises(RuntimeError, match=PIN_RECOVERY_REQUIRED):
        commit_launch_pin_for_selected_pod(context={"ti": ti}, pod=pod)


def test_launch_pin_required_defaults_from_exact_deployment_identity() -> None:
    from dpone_airflow_pack.launch_pin import launch_pin_required

    assert launch_pin_required({"_dpone_deployment_identity": _deployment_identity()}) is True
    assert launch_pin_required({"deployment_identity_pin": {"required": False}}) is False
    assert launch_pin_required({}) is False


def test_exact_activation_rejects_required_false() -> None:
    from dpone_airflow_pack.pack_task_runtime import with_run_identity

    pack_sha = "sha256:" + "d" * 64
    context = {
        "schema": "dpone.airflow-run-identity.v1",
        "release_id": "sha256:" + "a" * 64,
        "deployment_id": "sha256:" + "b" * 64,
        "dag_spec": {"id": "orders_daily", "sha256": "sha256:" + "c" * 64},
        "runtime_image_digest": "sha256:" + "e" * 64,
        "binding_set_ref": "sha256:" + "1" * 64,
        "connection_registry_ref": "sha256:" + "2" * 64,
        "credential_runtime_ref": "sha256:" + "3" * 64,
        "airflow_bundle": {
            "backend": "git",
            "ref": "git:7ac31f2",
            "versioned": True,
            "version": "7ac31f2",
            "snapshot_ref": None,
        },
        "_activation_id": "3f60628e-ef48-48b0-84c3-a9e27a82a7f2",
        "_workload_pack_sha256": {"orders": pack_sha},
    }
    with pytest.raises(ValueError, match="required=false"):
        with_run_identity(
            {"deployment_identity_pin": {"required": False}},
            provenance={"pack_sha256": pack_sha},
            dag=types.SimpleNamespace(),
            node=None,
            run_identity_context=context,
            workload_id="orders",
        )


def test_unavailable_payload_is_json_machine_readable() -> None:
    locator = {
        "try_number": 1,
        "pod_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "pin_sha256": "sha256:" + "1" * 64,
        "envelope_sha256": "sha256:" + "2" * 64,
        "pointer_resource_version": "1",
        "dag_id": "dag",
        "run_id": "run",
        "task_id": "orders__runtime",
        "map_index": -1,
    }
    summary = _xcom_summary(
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
        pin=locator,
    )

    class _BoomStore:
        def get(self, **_: object) -> None:
            raise RuntimeError(f"{PIN_UNAVAILABLE}: xcom backend unavailable")

        def create_once(self, pin: object) -> object:
            return pin

    set_launch_pin_store(_BoomStore())  # type: ignore[arg-type]

    with pytest.raises(RuntimeError) as raised:
        evaluate_pack_outcome(
            upstream_task_id="orders__runtime",
            ti=types.SimpleNamespace(
                dag_id="dag",
                run_id="run",
                xcom_pull=_xcom_pull_with_locator(summary=summary, pin=locator),
            ),
            launch_pin_required=True,
        )
    payload = json.loads(str(raised.value))
    assert payload["code"] == PIN_UNAVAILABLE
    assert payload["status"] == "UNAVAILABLE"


def test_get_launch_pin_store_has_no_production_inmemory_fallback() -> None:
    from dpone_airflow_pack.launch_pin import authoritative_launch_pin_store
    from dpone_airflow_pack.launch_pin_k8s_store import KubernetesConfigMapLaunchPinStore
    from dpone_airflow_pack.launch_pin_store import get_launch_pin_store

    set_launch_pin_store(None)
    assert get_launch_pin_store() is None
    store = authoritative_launch_pin_store()
    # Production prefers ConfigMap CAS when the kubernetes client is importable;
    # hermetic environments without that extra get a miss-only stand-in.
    assert type(store).__name__ in {
        KubernetesConfigMapLaunchPinStore.__name__,
        "_MissOnlyLaunchPinStore",
    }


def test_succeeded_pod_retained_through_gate_then_cleanup() -> None:
    from dpone_airflow_pack.launch_pin_cleanup import (
        AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_XCOM_KEY,
        cleanup_launch_pin_pod,
    )
    from dpone_airflow_pack.launch_pin_pod import get_launch_pin_pod_reader

    launch_deployment = _deployment_identity()
    launch_run = _run_identity()
    runtime_ti = _XComStore()
    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=launch_deployment,
        run_identity=launch_run,
        evidence="sha256:" + "f" * 64,
    )
    retained = commit_launch_pin_for_selected_pod(context={"ti": runtime_ti}, pod=pod)
    summary = _xcom_summary(deployment_identity=launch_deployment, run_identity=launch_run, pin=retained)
    gate_values: dict[str, object] = {}

    def _pull(*, task_ids: str, key: str = "return_value", **_: object) -> object:
        if key == "return_value":
            return summary
        if task_ids.endswith("outcome_gate") or task_ids == "orders__outcome_gate":
            return gate_values.get(key)
        return runtime_ti.values.get(key)

    def _push(*, key: str, value: object) -> None:
        gate_values[key] = value

    gate_ti = types.SimpleNamespace(dag_id="dag", run_id="run", xcom_pull=_pull, xcom_push=_push)
    payload = evaluate_pack_outcome(
        upstream_task_id="orders__runtime",
        ti=gate_ti,
        launch_pin_required=True,
    )
    assert payload["passed"] is True
    assert AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_XCOM_KEY in gate_values
    reader = get_launch_pin_pod_reader()
    assert isinstance(reader, InMemoryLaunchPinPodReader)
    # Pod still present for gate (retained).
    assert reader.read_pod(namespace="airflow", name=str(pod["metadata"]["name"])) is not None  # type: ignore[index]

    cleanup = cleanup_launch_pin_pod(
        upstream_task_id="orders__runtime",
        outcome_gate_task_id="orders__outcome_gate",
        ti=gate_ti,
    )
    assert cleanup["status"] == "deleted"
    assert reader.deleted == [("airflow", str(pod["metadata"]["name"]), "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")]  # type: ignore[index]


def test_gate_digest_mismatch_is_pin_invalid() -> None:
    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
    )
    pin = _store_pin_with_pod(pod=pod)
    # Tamper digest while keeping pod envelope intact.
    from dpone_airflow_pack.launch_pin_store import get_launch_pin_store

    store = get_launch_pin_store()
    assert store is not None
    tampered = dict(pin)
    tampered["pin_sha256"] = "sha256:" + "9" * 64
    key_pin = build_launch_pin(
        attempt={
            "dag_id": "dag",
            "run_id": "run",
            "task_id": "orders__runtime",
            "map_index": -1,
            "try_number": 2,
        },
        pod_namespace="airflow",
        pod_name="other",
        pod_uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        run_identity=_run_identity(),
        deployment_identity=_deployment_identity(),
        expected_runtime_evidence_sha256=None,
    )
    # Overwrite try-1 row by direct mutation of in-memory store internals.
    assert isinstance(store, InMemoryLaunchPinStore)
    store._rows[("dag", "run", "orders__runtime", -1, 1)] = tampered  # noqa: SLF001
    summary = _xcom_summary(
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
    )
    with pytest.raises(RuntimeError, match=PIN_INVALID):
        evaluate_pack_outcome(
            upstream_task_id="orders__runtime",
            ti=types.SimpleNamespace(
                dag_id="dag",
                run_id="run",
                xcom_pull=_xcom_pull_with_locator(summary=summary, pin=tampered),
            ),
            launch_pin_required=True,
        )
    del key_pin


def test_required_true_rejects_null_identities() -> None:
    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
        evidence=None,
    )
    # Build a pin with null identities directly into the store (tampered authority).
    from dpone_airflow_pack.launch_pin_pod import remember_launch_pin_pod
    from dpone_airflow_pack.launch_pin_store import get_launch_pin_store

    remember_launch_pin_pod(pod)
    # Break envelope identities on the live pod after storing a null-identity pointer.
    pin = build_launch_pin(
        attempt={
            "dag_id": "dag",
            "run_id": "run",
            "task_id": "orders__runtime",
            "map_index": -1,
            "try_number": 1,
        },
        pod_namespace="airflow",
        pod_name=str(pod["metadata"]["name"]),  # type: ignore[index]
        pod_uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        run_identity=None,
        deployment_identity=None,
        expected_runtime_evidence_sha256=None,
    )
    pin = _stamp_attempt_store_authority(dict(pin), namespace="airflow")
    store = get_launch_pin_store()
    assert store is not None
    retained = store.create_once(pin)
    # Also clear identities from the live pod envelope so rebuild stays null.
    null_envelope = {
        "run_identity": None,
        "deployment_identity": None,
        "expected_runtime_evidence_sha256": None,
        "envelope_sha256": envelope_sha256(
            run_identity=None,
            deployment_identity=None,
            expected_runtime_evidence_sha256=None,
        ),
    }
    pod["metadata"]["annotations"] = {  # type: ignore[index]
        "dpone.airflow/runtime-launch-envelope.v1": json.dumps(
            null_envelope,
            separators=(",", ":"),
            sort_keys=True,
        )
    }
    pod["spec"]["containers"][0]["env"] = [  # type: ignore[index]
        {"name": AIRFLOW_RUN_IDENTITY_ENV, "value": ""},
        {"name": AIRFLOW_DEPLOYMENT_IDENTITY_ENV, "value": ""},
        {"name": AIRFLOW_EXPECTED_RUNTIME_EVIDENCE_SHA256_ENV, "value": ""},
    ]
    remember_launch_pin_pod(pod)
    summary = _xcom_summary(
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
        pin=retained,
    )

    with pytest.raises(RuntimeError, match=PIN_MISSING):
        evaluate_pack_outcome(
            upstream_task_id="orders__runtime",
            ti=types.SimpleNamespace(
                dag_id="dag",
                run_id="run",
                xcom_pull=_xcom_pull_with_locator(summary=summary, pin=retained),
            ),
            launch_pin_required=True,
        )


def test_cleanup_failure_does_not_raise() -> None:
    from dpone_airflow_pack.launch_pin_cleanup import (
        AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_XCOM_KEY,
        AIRFLOW_OUTCOME_GATE_RESULT_XCOM_KEY,
        build_cleanup_handle_from_pin,
        cleanup_launch_pin_pod,
    )

    class _BoomReader(InMemoryLaunchPinPodReader):
        def delete_pod(self, **_: object) -> None:
            raise RuntimeError(f"{PIN_UNAVAILABLE}: delete denied")

    reader = _BoomReader()
    set_launch_pin_pod_reader(reader)
    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
    )
    pin = _store_pin_with_pod(pod=pod)
    handle = build_cleanup_handle_from_pin(pin)
    gate_values = {
        AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_XCOM_KEY: handle,
        AIRFLOW_OUTCOME_GATE_RESULT_XCOM_KEY: {"passed": True, "payload": {}},
    }

    def _pull(*, task_ids: str, key: str = "return_value", **_: object) -> object:
        del task_ids
        return gate_values.get(key)

    result = cleanup_launch_pin_pod(
        upstream_task_id="orders__runtime",
        outcome_gate_task_id="orders__outcome_gate",
        ti=types.SimpleNamespace(dag_id="dag", run_id="run", xcom_pull=_pull),
    )
    assert result["status"] == "delete_failed"


def test_configmap_store_create_once_cas() -> None:
    from dpone_airflow_pack.launch_pin_k8s_store import KubernetesConfigMapLaunchPinStore

    class _Api:
        def __init__(self) -> None:
            self.objects: dict[str, dict[str, object]] = {}
            self.rv = 0

        def create_namespaced_config_map(self, *, namespace: str, body: dict[str, object]) -> dict[str, object]:
            del namespace
            name = str(body["metadata"]["name"])  # type: ignore[index]
            if name in self.objects:
                exc = RuntimeError("already exists")
                setattr(exc, "status", 409)
                raise exc
            self.rv += 1
            stored = json.loads(json.dumps(body))
            stored["metadata"]["resourceVersion"] = str(self.rv)
            self.objects[name] = stored
            return stored

        def read_namespaced_config_map(self, *, name: str, namespace: str) -> dict[str, object]:
            del namespace
            if name not in self.objects:
                exc = RuntimeError("not found")
                setattr(exc, "status", 404)
                raise exc
            return self.objects[name]

        def replace_namespaced_config_map(
            self, *, name: str, namespace: str, body: dict[str, object]
        ) -> dict[str, object]:
            del namespace
            current = self.objects[name]
            if str(body["metadata"].get("resourceVersion")) != str(  # type: ignore[union-attr]
                current["metadata"]["resourceVersion"]  # type: ignore[index]
            ):
                exc = RuntimeError("conflict")
                setattr(exc, "status", 409)
                raise exc
            self.rv += 1
            stored = json.loads(json.dumps(body))
            stored["metadata"]["resourceVersion"] = str(self.rv)
            self.objects[name] = stored
            return stored

    api = _Api()
    store = KubernetesConfigMapLaunchPinStore(namespace="airflow", api=api)
    pin = build_launch_pin(
        attempt={"dag_id": "dag", "run_id": "run", "task_id": "t", "map_index": -1, "try_number": 1},
        pod_namespace="airflow",
        pod_name="pod-a",
        pod_uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        run_identity=_run_identity(),
        deployment_identity=_deployment_identity(),
        expected_runtime_evidence_sha256=None,
        kubernetes_conn_id="kubernetes_default",
    )
    assert store.create_once(pin)["pod_uid"] == pin["pod_uid"]
    assert store.create_once(pin)["pin_sha256"] == pin["pin_sha256"]
    other = build_launch_pin(
        attempt={"dag_id": "dag", "run_id": "run", "task_id": "t", "map_index": -1, "try_number": 1},
        pod_namespace="airflow",
        pod_name="pod-b",
        pod_uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        run_identity=_run_identity(),
        deployment_identity=_deployment_identity(),
        expected_runtime_evidence_sha256=None,
        kubernetes_conn_id="kubernetes_default",
    )
    with pytest.raises(RuntimeError, match="PIN_CONFLICT"):
        store.create_once(other)


def test_gate_failed_cleanup_rematerializes_failure() -> None:
    from dpone_airflow_pack.launch_pin_cleanup import (
        AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_XCOM_KEY,
        AIRFLOW_OUTCOME_GATE_RESULT_XCOM_KEY,
        build_cleanup_handle_from_pin,
        cleanup_launch_pin_pod,
    )

    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
    )
    pin = _store_pin_with_pod(pod=pod)
    gate_values = {
        AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_XCOM_KEY: build_cleanup_handle_from_pin(pin),
        AIRFLOW_OUTCOME_GATE_RESULT_XCOM_KEY: {
            "passed": False,
            "payload": {"code": "airflow_outcome_failed"},
        },
    }

    def _pull(*, task_ids: str, key: str = "return_value", **_: object) -> object:
        del task_ids
        return gate_values.get(key)

    with pytest.raises(RuntimeError, match="OUTCOME_GATE_REMATERIALIZED"):
        cleanup_launch_pin_pod(
            upstream_task_id="orders__runtime",
            outcome_gate_task_id="orders__outcome_gate",
            ti=types.SimpleNamespace(dag_id="dag", run_id="run", xcom_pull=_pull),
        )
    reader = __import__(
        "dpone_airflow_pack.launch_pin_pod", fromlist=["get_launch_pin_pod_reader"]
    ).get_launch_pin_pod_reader()
    assert isinstance(reader, InMemoryLaunchPinPodReader)
    assert reader.deleted == [("airflow", str(pod["metadata"]["name"]), "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")]  # type: ignore[index]


def test_cleanup_uses_only_gate_handle_not_mutable_head() -> None:
    from dpone_airflow_pack.launch_pin_cleanup import (
        AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_XCOM_KEY,
        AIRFLOW_OUTCOME_GATE_RESULT_XCOM_KEY,
        build_cleanup_handle_from_pin,
        cleanup_launch_pin_pod,
    )
    from dpone_airflow_pack.launch_pin_pod import get_launch_pin_pod_reader

    pod_try1 = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
        name="pod-try1",
    )
    pin_try1 = _store_pin_with_pod(
        pod=pod_try1,
        attempt={
            "dag_id": "dag",
            "run_id": "run",
            "task_id": "orders__runtime",
            "map_index": -1,
            "try_number": 1,
        },
    )
    pod_try2 = _pod(
        uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
        name="pod-try2",
    )
    # Plant try2 head directly; ACTIVE prior blocks successor CAS (P0.2).
    from dpone_airflow_pack.launch_pin_store import get_launch_pin_store

    store = get_launch_pin_store()
    assert isinstance(store, InMemoryLaunchPinStore)
    pin_try2 = build_launch_pin(
        attempt={
            "dag_id": "dag",
            "run_id": "run",
            "task_id": "orders__runtime",
            "map_index": -1,
            "try_number": 2,
        },
        pod_namespace="airflow",
        pod_name="pod-try2",
        pod_uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        run_identity=_run_identity(),
        deployment_identity=_deployment_identity(),
        expected_runtime_evidence_sha256="sha256:" + "f" * 64,
    )
    pin_try2["state"] = "ACTIVE"
    store._rows[("dag", "run", "orders__runtime", -1, 2)] = pin_try2  # noqa: SLF001
    store._subject_heads[("dag", "run", "orders__runtime", -1)] = 2  # noqa: SLF001
    from dpone_airflow_pack.launch_pin_pod import remember_launch_pin_pod

    remember_launch_pin_pod(pod_try2)
    gate_values = {
        AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_XCOM_KEY: build_cleanup_handle_from_pin(pin_try1),
        AIRFLOW_OUTCOME_GATE_RESULT_XCOM_KEY: {"passed": True, "payload": {}},
    }

    def _pull(*, task_ids: str, key: str = "return_value", **_: object) -> object:
        del task_ids
        return gate_values.get(key)

    result = cleanup_launch_pin_pod(
        upstream_task_id="orders__runtime",
        outcome_gate_task_id="orders__outcome_gate",
        ti=types.SimpleNamespace(dag_id="dag", run_id="run", xcom_pull=_pull),
    )
    assert result["status"] == "head_transition_failed"
    assert result["try_number"] == 1
    assert result["pod_uid"] == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    assert result["pod_delete"]["status"] == "deleted"
    assert result["per_try_delete"]["status"] == "skipped"
    assert result["per_try_delete"]["reason"] == "head_not_proven_closed"
    reader = get_launch_pin_pod_reader()
    assert isinstance(reader, InMemoryLaunchPinPodReader)
    assert reader.deleted == [("airflow", "pod-try1", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")]
    # Higher-try pod remains.
    assert reader.read_pod(namespace="airflow", name="pod-try2") is not None


def test_cleanup_skips_without_xcom_fallback_when_handle_absent() -> None:
    from dpone_airflow_pack.launch_pin_cleanup import cleanup_launch_pin_pod
    from dpone_airflow_pack.launch_pin_pod import get_launch_pin_pod_reader

    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
    )
    _store_pin_with_pod(pod=pod)
    # Runtime XCom mirror present, but cleanup must not use it.
    runtime_mirror = {
        AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY: {
            "pod_namespace": "airflow",
            "pod_name": str(pod["metadata"]["name"]),  # type: ignore[index]
            "pod_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        }
    }

    def _pull(*, task_ids: str, key: str = "return_value", **_: object) -> object:
        if task_ids == "orders__runtime":
            return runtime_mirror.get(key)
        return None

    result = cleanup_launch_pin_pod(
        upstream_task_id="orders__runtime",
        outcome_gate_task_id="orders__outcome_gate",
        ti=types.SimpleNamespace(dag_id="dag", run_id="run", xcom_pull=_pull),
    )
    assert result["status"] == "skipped"
    assert result["reason"] == "cleanup_handle_absent"
    reader = get_launch_pin_pod_reader()
    assert isinstance(reader, InMemoryLaunchPinPodReader)
    assert reader.deleted == []


def test_cas_failure_abandons_created_pod() -> None:
    from dpone_airflow_pack.launch_pin_pod import get_launch_pin_pod_reader
    from dpone_airflow_pack.launch_pin_store import set_launch_pin_store

    class _FailStore(InMemoryLaunchPinStore):
        def create_once(self, pin: object) -> object:
            del pin
            raise RuntimeError(f"{PIN_UNAVAILABLE}: simulated CAS failure")

    set_launch_pin_store(_FailStore())
    ti = _XComStore()
    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
    )
    # Register pod so abandon delete can find it.
    from dpone_airflow_pack.launch_pin_pod import remember_launch_pin_pod

    remember_launch_pin_pod(pod)
    with pytest.raises(RuntimeError, match="CAS failed before base authority"):
        commit_launch_pin_for_selected_pod(context={"ti": ti}, pod=pod)
    reader = get_launch_pin_pod_reader()
    assert isinstance(reader, InMemoryLaunchPinPodReader)
    assert reader.deleted == [("airflow", str(pod["metadata"]["name"]), "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")]  # type: ignore[index]


def test_post_active_locator_failure_does_not_abandon_winner_pod() -> None:
    """ACTIVE confirmation must never be reclassified as pre-authority abandon."""

    from dpone_airflow_pack.launch_pin import PIN_RECOVERY_REQUIRED
    from dpone_airflow_pack.launch_pin_pod import get_launch_pin_pod_reader, remember_launch_pin_pod

    class _PushBoom(_XComStore):
        def xcom_push(self, *, key: str, value: object) -> None:
            raise RuntimeError("locator xcom down after ACTIVE")

    ti = _PushBoom()
    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
    )
    remember_launch_pin_pod(pod)
    with pytest.raises(RuntimeError, match=PIN_RECOVERY_REQUIRED):
        commit_launch_pin_for_selected_pod(context={"ti": ti}, pod=pod)
    reader = get_launch_pin_pod_reader()
    assert isinstance(reader, InMemoryLaunchPinPodReader)
    assert reader.deleted == []
    assert reader.read_pod(namespace="airflow", name=str(pod["metadata"]["name"])) is not None  # type: ignore[index]


def test_launch_pin_barrier_init_injected_on_pod_request() -> None:
    from dpone_airflow_pack.launch_pin_barrier import LAUNCH_PIN_BARRIER_INIT_NAME, ensure_launch_pin_cas_barrier_on_pod

    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
    )
    # Clear uid to mimic pre-create request object.
    pod["metadata"]["uid"] = ""  # type: ignore[index]
    ensure_launch_pin_cas_barrier_on_pod(
        pod,
        dag_id="dag",
        run_id="run",
        task_id="orders__runtime",
        map_index=-1,
        try_number=1,
        store_namespace="airflow",
        envelope_sha256_digest=envelope_sha256(
            run_identity=_run_identity(),
            deployment_identity=_deployment_identity(),
            expected_runtime_evidence_sha256="sha256:" + "f" * 64,
        ),
    )
    inits = pod["spec"]["initContainers"]  # type: ignore[index]
    assert isinstance(inits, list)
    assert inits[0]["name"] == LAUNCH_PIN_BARRIER_INIT_NAME  # type: ignore[index]
    env = {item["name"]: item for item in inits[0]["env"]}  # type: ignore[index]
    assert "valueFrom" in env["POD_UID"]
    assert env["PIN_CM_NAMESPACE"]["value"] == "airflow"
    assert env["PIN_HEAD_CM_NAME"]["value"].startswith("dpone-lph-")
    assert env["PIN_HEAD_DATA_KEY"]["value"] == "head.json"


def test_launch_pin_barrier_attaches_after_strict_init_fetch_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression (0.73.38): CAS barrier adds a second init container; validate first."""

    from copy import deepcopy

    from dpone_airflow_pack import (
        launch_pin_barrier,
        launch_pin_operator,
        operators,
        xcom_sidecar,
    )
    from dpone_airflow_pack.init_fetch_pod_contract import INIT_CONTAINER_NAME
    from dpone_airflow_pack.launch_pin_barrier import LAUNCH_PIN_BARRIER_INIT_NAME
    from dpone_airflow_pack.operators import PinnedXComSidecarKubernetesPodOperator
    from dpone_airflow_pack.xcom_sidecar import XCOM_SIDECAR_CONTAINER_NAME, XComSidecarRuntimeConfig

    runtime_image = "registry.example/dpone@sha256:" + "a" * 64
    sidecar_image = "registry.example/xcom@sha256:" + "b" * 64
    pod = {
        "metadata": {"name": "orders-runtime", "namespace": "airflow", "annotations": {}},
        "spec": {
            "containers": [
                {"name": "base", "image": runtime_image},
                {"name": XCOM_SIDECAR_CONTAINER_NAME, "image": sidecar_image},
            ],
            "initContainers": [{"name": INIT_CONTAINER_NAME, "image": runtime_image}],
        },
    }
    base_pod = deepcopy(pod)
    call_order: list[str] = []

    real_validate = xcom_sidecar.validate_strict_xcom_pod
    real_attach = launch_pin_barrier.attach_launch_pin_cas_barrier

    def _validate_spy(*args: object, **kwargs: object) -> object:
        call_order.append("validate")
        return real_validate(*args, **kwargs)

    def _attach_spy(*args: object, **kwargs: object) -> object:
        call_order.append("attach")
        return real_attach(*args, **kwargs)

    monkeypatch.setattr(operators, "validate_strict_xcom_pod", _validate_spy)
    monkeypatch.setattr(
        launch_pin_operator,
        "attach_launch_pin_cas_barrier",
        _attach_spy,
    )
    base_operator = PinnedXComSidecarKubernetesPodOperator.__mro__[1]
    monkeypatch.setattr(
        base_operator,
        "build_pod_request_obj",
        lambda _self, context=None: deepcopy(base_pod),
    )

    try:
        from airflow.providers.cncf.kubernetes.operators.pod import (  # noqa: F401
            KubernetesPodOperator as _RealKubernetesPodOperator,
        )

        real_kubernetes_provider = issubclass(
            PinnedXComSidecarKubernetesPodOperator.__mro__[1],
            _RealKubernetesPodOperator,
        )
    except ImportError:
        real_kubernetes_provider = False

    if real_kubernetes_provider:
        launch_pin_store: dict[str, str] = {
            "kubernetes_conn_id": "kubernetes_default",
            "namespace": "airflow",
        }
        operator_kwargs: dict[str, object] = {"kubernetes_conn_id": "kubernetes_default"}
    else:
        # Dependency-light stub KPO is in-cluster (kubernetes_conn_id=None).
        launch_pin_store = {"namespace": "airflow"}
        operator_kwargs = {}

    operator = PinnedXComSidecarKubernetesPodOperator(
        task_id="orders__runtime",
        name="orders-runtime",
        namespace="airflow",
        image=runtime_image,
        cmds=["/bin/sh", "-ec"],
        arguments=["true"],
        do_xcom_push=True,
        get_logs=False,
        pin_deployment_identity_for_separate_outcome_gate=True,
        launch_pin_store=launch_pin_store,
        xcom_sidecar=XComSidecarRuntimeConfig(
            image=sidecar_image,
            strict_runtime_image=runtime_image,
        ),
        expected_runtime_evidence_sha256="sha256:" + "f" * 64,
        params={
            "dpone_deployment_identity": _deployment_identity(),
            "dpone_run_identity": _run_identity(),
        },
        **operator_kwargs,
    )

    built = operator.build_pod_request_obj(
        context={
            "ti": types.SimpleNamespace(
                dag_id="dag",
                run_id="run",
                task_id="orders__runtime",
                map_index=-1,
                try_number=1,
            )
        }
    )

    init_names = [item["name"] for item in built["spec"]["initContainers"]]  # type: ignore[index]
    assert init_names == [LAUNCH_PIN_BARRIER_INIT_NAME, INIT_CONTAINER_NAME]
    assert call_order == ["validate", "attach"]


def test_envelope_merge_preserves_value_from_env() -> None:
    pod: dict[str, object] = {
        "metadata": {"name": "pod-vf", "namespace": "airflow", "uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"},
        "spec": {
            "containers": [
                {
                    "name": "base",
                    "env": [
                        {
                            "name": "DPONE_TOKEN",
                            "valueFrom": {"secretKeyRef": {"name": "dpone-token", "key": "token"}},
                        }
                    ],
                }
            ]
        },
    }
    ensure_launch_envelope_on_pod(
        pod,
        run_identity=_run_identity(),
        deployment_identity=_deployment_identity(),
        expected_runtime_evidence_sha256=None,
    )
    env = pod["spec"]["containers"][0]["env"]  # type: ignore[index]
    by_name = {item["name"]: item for item in env}  # type: ignore[index]
    assert by_name["DPONE_TOKEN"]["valueFrom"]["secretKeyRef"]["name"] == "dpone-token"
    assert AIRFLOW_RUN_IDENTITY_ENV in by_name
    assert AIRFLOW_DEPLOYMENT_IDENTITY_ENV in by_name


def test_annotation_with_null_evidence_accepts_reconciled_env_without_optional_evidence_key() -> None:
    from dpone_airflow_pack.launch_pin_envelope import (
        AIRFLOW_RUNTIME_LAUNCH_ENVELOPE_ANNOTATION,
        launch_envelope_from_pod,
    )

    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
        evidence=None,
    )
    # Drop one env key while keeping annotation.
    pod["spec"]["containers"][0]["env"] = [  # type: ignore[index]
        item
        for item in pod["spec"]["containers"][0]["env"]  # type: ignore[index]
        if item["name"] != AIRFLOW_EXPECTED_RUNTIME_EVIDENCE_SHA256_ENV
    ]
    assert AIRFLOW_RUNTIME_LAUNCH_ENVELOPE_ANNOTATION in pod["metadata"]["annotations"]  # type: ignore[index]
    envelope = launch_envelope_from_pod(pod)

    assert envelope["run_identity"] == _run_identity()
    assert envelope["deployment_identity"] == _deployment_identity()
    assert envelope["expected_runtime_evidence_sha256"] is None


def test_annotation_with_evidence_requires_matching_evidence_env_key() -> None:
    from dpone_airflow_pack.launch_pin_envelope import launch_envelope_from_pod

    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
        evidence="sha256:" + "f" * 64,
    )
    pod["spec"]["containers"][0]["env"] = [  # type: ignore[index]
        item
        for item in pod["spec"]["containers"][0]["env"]  # type: ignore[index]
        if item["name"] != AIRFLOW_EXPECTED_RUNTIME_EVIDENCE_SHA256_ENV
    ]

    with pytest.raises(RuntimeError, match=PIN_INVALID):
        launch_envelope_from_pod(pod)


def test_annotation_with_null_evidence_requires_env_digest_to_bind_null() -> None:
    from dpone_airflow_pack.launch_pin_envelope import (
        AIRFLOW_ENVELOPE_SHA256_ENV,
        launch_envelope_from_pod,
    )

    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
        evidence=None,
    )
    pod["spec"]["containers"][0]["env"] = [  # type: ignore[index]
        item
        for item in pod["spec"]["containers"][0]["env"]  # type: ignore[index]
        if item["name"] != AIRFLOW_EXPECTED_RUNTIME_EVIDENCE_SHA256_ENV
    ]
    envelope_digest_env = next(
        item
        for item in pod["spec"]["containers"][0]["env"]  # type: ignore[index]
        if item["name"] == AIRFLOW_ENVELOPE_SHA256_ENV
    )
    envelope_digest_env["value"] = "sha256:" + "0" * 64

    with pytest.raises(RuntimeError, match=PIN_INVALID):
        launch_envelope_from_pod(pod)


def test_required_false_never_opens_kubernetes_store(monkeypatch: pytest.MonkeyPatch) -> None:
    from dpone_airflow_pack import launch_pin as launch_pin_module
    from dpone_airflow_pack.launch_pin_store import set_launch_pin_store

    set_launch_pin_store(None)

    def _boom(**_: object) -> object:
        raise AssertionError("remote store must not be constructed for required=false")

    monkeypatch.setattr(launch_pin_module, "authoritative_launch_pin_store", _boom)
    resolution = resolve_launch_pin(
        ti=types.SimpleNamespace(dag_id="dag", run_id="run", xcom_pull=lambda **_: None),
        upstream_task_id="orders__runtime",
        required=False,
    )
    assert resolution.status == "ABSENT_LEGACY"


def test_cleanup_task_marked_teardown(monkeypatch: pytest.MonkeyPatch) -> None:
    from dpone_airflow_pack import launch_pin_cleanup as cleanup_module

    class _Task:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            self.is_teardown = False
            self.on_failure_fail_dagrun = True

        def as_teardown(self, *, on_failure_fail_dagrun: bool = True) -> _Task:
            self.is_teardown = True
            self.on_failure_fail_dagrun = on_failure_fail_dagrun
            return self

    monkeypatch.setattr(cleanup_module, "python_operator_class", lambda: _Task)
    task = cleanup_module.build_pack_launch_pin_cleanup_task(
        pack={},
        dag=object(),
        upstream_task_id="orders__runtime",
        outcome_gate_task=types.SimpleNamespace(task_id="orders__outcome_gate"),
    )
    assert task.is_teardown is True
    assert task.on_failure_fail_dagrun is False


def test_configmap_store_typed_v1_models_create_read_replace() -> None:
    pytest.importorskip("kubernetes")
    from dpone_airflow_pack.launch_pin_cleanup import build_cleanup_handle_from_pin
    from dpone_airflow_pack.launch_pin_k8s_store import KubernetesConfigMapLaunchPinStore
    from kubernetes.client import V1ConfigMap, V1ObjectMeta

    class _TypedApi:
        def __init__(self) -> None:
            self.objects: dict[str, V1ConfigMap] = {}
            self.rv = 0

        def create_namespaced_config_map(self, *, namespace: str, body: dict[str, object]) -> V1ConfigMap:
            del namespace
            name = str(body["metadata"]["name"])  # type: ignore[index]
            if name in self.objects:
                exc = RuntimeError("already exists")
                setattr(exc, "status", 409)
                raise exc
            self.rv += 1
            cm = V1ConfigMap(
                metadata=V1ObjectMeta(
                    name=name,
                    namespace="airflow",
                    resource_version=str(self.rv),
                    labels=dict(body["metadata"]["labels"]),  # type: ignore[index]
                ),
                data=dict(body["data"]),  # type: ignore[arg-type]
            )
            self.objects[name] = cm
            return cm

        def read_namespaced_config_map(self, *, name: str, namespace: str) -> V1ConfigMap:
            del namespace
            if name not in self.objects:
                exc = RuntimeError("not found")
                setattr(exc, "status", 404)
                raise exc
            return self.objects[name]

        def replace_namespaced_config_map(self, *, name: str, namespace: str, body: dict[str, object]) -> V1ConfigMap:
            del namespace
            current = self.objects[name]
            if str(body["metadata"].get("resourceVersion")) != str(current.metadata.resource_version):  # type: ignore[union-attr]
                exc = RuntimeError("conflict")
                setattr(exc, "status", 409)
                raise exc
            self.rv += 1
            cm = V1ConfigMap(
                metadata=V1ObjectMeta(
                    name=name,
                    namespace="airflow",
                    resource_version=str(self.rv),
                    labels=dict(body["metadata"]["labels"]),  # type: ignore[index]
                ),
                data=dict(body["data"]),  # type: ignore[arg-type]
            )
            self.objects[name] = cm
            return cm

    api = _TypedApi()
    store = KubernetesConfigMapLaunchPinStore(namespace="airflow", api=api)
    pin = build_launch_pin(
        attempt={"dag_id": "dag", "run_id": "run", "task_id": "t", "map_index": -1, "try_number": 1},
        pod_namespace="airflow",
        pod_name="pod-a",
        pod_uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        run_identity=_run_identity(),
        deployment_identity=_deployment_identity(),
        expected_runtime_evidence_sha256=None,
        kubernetes_conn_id="kubernetes_default",
    )
    created = store.create_once(pin)
    assert created["state"] == "ACTIVE"
    assert created["pointer_resource_version"]
    read_back = store.get(dag_id="dag", run_id="run", task_id="t", map_index=-1)
    assert read_back is not None
    assert read_back["pointer_resource_version"] == created["pointer_resource_version"]
    handle = build_cleanup_handle_from_pin(created)
    assert handle["pointer_resource_version"] == created["pointer_resource_version"]


def test_configmap_ambiguous_create_reconciles_to_active() -> None:
    from dpone_airflow_pack.launch_pin_k8s_store import KubernetesConfigMapLaunchPinStore

    class _Api:
        def __init__(self) -> None:
            self.objects: dict[str, dict[str, object]] = {}
            self.rv = 0
            self.fail_create_response = True

        def create_namespaced_config_map(self, *, namespace: str, body: dict[str, object]) -> dict[str, object]:
            del namespace
            name = str(body["metadata"]["name"])  # type: ignore[index]
            self.rv += 1
            stored = json.loads(json.dumps(body))
            stored["metadata"]["resourceVersion"] = str(self.rv)
            self.objects[name] = stored
            if self.fail_create_response:
                self.fail_create_response = False
                raise TimeoutError("response lost after server commit")
            return stored

        def read_namespaced_config_map(self, *, name: str, namespace: str) -> dict[str, object]:
            del namespace
            return self.objects[name]

        def replace_namespaced_config_map(
            self, *, name: str, namespace: str, body: dict[str, object]
        ) -> dict[str, object]:
            del namespace
            self.rv += 1
            stored = json.loads(json.dumps(body))
            stored["metadata"]["resourceVersion"] = str(self.rv)
            self.objects[name] = stored
            return stored

    api = _Api()
    store = KubernetesConfigMapLaunchPinStore(namespace="airflow", api=api)
    pin = build_launch_pin(
        attempt={"dag_id": "dag", "run_id": "run", "task_id": "t", "map_index": -1, "try_number": 1},
        pod_namespace="airflow",
        pod_name="pod-a",
        pod_uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        run_identity=_run_identity(),
        deployment_identity=_deployment_identity(),
        expected_runtime_evidence_sha256=None,
    )
    retained = store.create_once(pin)
    assert retained["state"] == "ACTIVE"
    assert retained["pod_uid"] == pin["pod_uid"]


def test_configmap_malformed_existing_is_pin_invalid() -> None:
    from dpone_airflow_pack.launch_pin_k8s_store import (
        LAUNCH_PIN_CONFIGMAP_LABEL,
        LAUNCH_PIN_CONFIGMAP_LABEL_VALUE,
        KubernetesConfigMapLaunchPinStore,
    )

    class _Api:
        def create_namespaced_config_map(self, *, namespace: str, body: dict[str, object]) -> dict[str, object]:
            del namespace, body
            exc = RuntimeError("already exists")
            setattr(exc, "status", 409)
            raise exc

        def read_namespaced_config_map(self, *, name: str, namespace: str) -> dict[str, object]:
            del namespace
            return {
                "metadata": {
                    "name": name,
                    "namespace": "airflow",
                    "resourceVersion": "1",
                    "labels": {LAUNCH_PIN_CONFIGMAP_LABEL: LAUNCH_PIN_CONFIGMAP_LABEL_VALUE},
                },
                "data": {"pin.json": "{not-json"},
            }

        def replace_namespaced_config_map(self, **_: object) -> dict[str, object]:
            raise AssertionError("malformed ConfigMap must never be auto-replaced")

    store = KubernetesConfigMapLaunchPinStore(namespace="airflow", api=_Api())
    pin = build_launch_pin(
        attempt={"dag_id": "dag", "run_id": "run", "task_id": "t", "map_index": -1, "try_number": 1},
        pod_namespace="airflow",
        pod_name="pod-a",
        pod_uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        run_identity=_run_identity(),
        deployment_identity=_deployment_identity(),
        expected_runtime_evidence_sha256=None,
    )
    with pytest.raises(RuntimeError, match=PIN_INVALID):
        store.create_once(pin)


def test_higher_try_blocked_while_active_owner_pod_exists() -> None:
    from dpone_airflow_pack.launch_pin import PIN_RECOVERY_REQUIRED

    ti = _XComStore()
    pod1 = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
        name="pod-try1",
    )
    commit_launch_pin_for_selected_pod(context={"ti": ti}, pod=pod1)
    ti.try_number = 2
    pod2 = _pod(
        uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
        name="pod-try2",
    )
    with pytest.raises(RuntimeError, match=PIN_RECOVERY_REQUIRED):
        commit_launch_pin_for_selected_pod(context={"ti": ti}, pod=pod2)
    # Terminal/Failed is insufficient while prior pin remains ACTIVE.
    pod1["status"] = {"phase": "Failed"}
    with pytest.raises(RuntimeError, match=PIN_RECOVERY_REQUIRED):
        commit_launch_pin_for_selected_pod(context={"ti": ti}, pod=pod2)


def test_candidate_terminal_allows_successor_replace() -> None:
    from dpone_airflow_pack.launch_pin_codes import LAUNCH_PIN_STATE_CANDIDATE
    from dpone_airflow_pack.launch_pin_store import get_launch_pin_store

    store = get_launch_pin_store()
    assert store is not None
    pod1 = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
        name="pod-try1",
    )
    _store_pin_with_pod(
        pod=pod1,
        attempt={
            "dag_id": "dag",
            "run_id": "run",
            "task_id": "orders__runtime",
            "map_index": -1,
            "try_number": 1,
        },
    )
    # Force prior row back to CANDIDATE (pre-ACTIVE) and terminal pod.
    key = ("dag", "run", "orders__runtime", -1, 1)
    row = dict(store._rows[key])  # type: ignore[attr-defined]
    row["state"] = LAUNCH_PIN_STATE_CANDIDATE
    store._rows[key] = row  # type: ignore[attr-defined]
    pod1["status"] = {"phase": "Failed"}
    ti = _XComStore()
    ti.try_number = 2
    pod2 = _pod(
        uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
        name="pod-try2",
    )
    retained = commit_launch_pin_for_selected_pod(context={"ti": ti}, pod=pod2)
    assert retained["pod_uid"] == "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    assert retained.get("state") == "ACTIVE"


def test_legacy_repository_pack_skips_pin_barrier_and_configmap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack import outcome as outcome_module
    from dpone_airflow_pack import pack_tasks as pack_tasks_module
    from dpone_airflow_pack.launch_pin import launch_pin_required
    from dpone_airflow_pack.launch_pin_barrier import LAUNCH_PIN_BARRIER_INIT_NAME
    from dpone_airflow_pack.operators import PinnedXComSidecarKubernetesPodOperator
    from dpone_airflow_pack.pack_tasks import _build_tasks_from_loaded_pack

    calls: list[str] = []
    pack = {
        "kind": "gitops.airflow_pack",
        "schema_version": "3",
        "workload": {"workload_id": "orders"},
        "kpo_kwargs": {
            "task_id": "orders__runtime",
            "name": "orders-runtime",
            "namespace": "airflow",
            "image": "dpone:test",
            "cmds": ["/bin/sh", "-ec"],
            "arguments": ["true"],
            "kubernetes_conn_id": "kubernetes_default",
        },
        "steps": [],
        "outcome_gate": {"required_status": "passed"},
        "deployment_identity_pin": {"required": False},
        "xcom": {},
    }
    assert launch_pin_required(pack) is False

    class _BoomStore:
        def create_once(self, pin: object) -> object:
            calls.append("create_once")
            raise AssertionError(f"legacy pack must not open ConfigMap store: {pin!r}")

        def get(self, **_: object) -> None:
            calls.append("get")
            return None

    class _StubRuntime:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            self.pin_deployment_identity_for_separate_outcome_gate = bool(
                kwargs.get("pin_deployment_identity_for_separate_outcome_gate", False)
            )
            self.task_id = str(kwargs.get("task_id") or "orders__runtime")
            self.kubernetes_conn_id = kwargs.get("kubernetes_conn_id")
            self.downstream: list[object] = []

        def __rshift__(self, other: object) -> object:
            self.downstream.append(other)
            return other

        def build_pod_request_obj(self, context: object = None) -> dict[str, object]:
            del context
            # Mirror operator behaviour: pin flag off ⇒ no envelope/barrier injection.
            assert self.pin_deployment_identity_for_separate_outcome_gate is False
            return {
                "metadata": {"name": "orders-runtime", "namespace": "airflow", "annotations": {}},
                "spec": {"containers": [{"name": "base", "image": "dpone:test"}], "initContainers": []},
            }

        def get_or_create_pod(self, pod: object, context: object) -> object:
            del context
            assert self.pin_deployment_identity_for_separate_outcome_gate is False
            return pod

    class _StubPython:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            self.task_id = str(kwargs.get("task_id") or "outcome_gate")
            self.downstream: list[object] = []

        def __rshift__(self, other: object) -> object:
            self.downstream.append(other)
            return other

    monkeypatch.setattr(
        "dpone_airflow_pack.launch_pin.authoritative_launch_pin_store",
        lambda **_: _BoomStore(),
    )
    monkeypatch.setattr(pack_tasks_module, "_operator_class", lambda _pack: _StubRuntime)
    monkeypatch.setattr(outcome_module, "python_operator_class", lambda: _StubPython)
    tasks = _build_tasks_from_loaded_pack(
        pack,
        dag=object(),
        operator_overrides=None,
        node=None,
        task_group=None,
    )
    runtime = tasks["dpone_runtime"]
    assert isinstance(runtime, _StubRuntime)
    assert runtime.pin_deployment_identity_for_separate_outcome_gate is False
    assert "launch_pin_cleanup" not in tasks
    assert "outcome_gate" in tasks
    pod = runtime.build_pod_request_obj(
        {
            "ti": types.SimpleNamespace(
                dag_id="dag",
                run_id="run",
                task_id="orders__runtime",
                map_index=-1,
            )
        }
    )
    names = [item.get("name") for item in pod["spec"]["initContainers"]]  # type: ignore[index]
    assert LAUNCH_PIN_BARRIER_INIT_NAME not in names
    selected = runtime.get_or_create_pod(
        {
            "metadata": {
                "name": "orders-runtime",
                "namespace": "airflow",
                "uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            },
            "spec": {"containers": [{"name": "base", "image": "dpone:test"}]},
        },
        {"ti": _XComStore()},
    )
    assert selected is not None
    assert calls == []
    del PinnedXComSidecarKubernetesPodOperator


def test_early_gate_failure_publishes_result_xcom() -> None:
    from dpone_airflow_pack.launch_pin_cleanup import AIRFLOW_OUTCOME_GATE_RESULT_XCOM_KEY

    pushed: dict[str, object] = {}

    def _pull(*, task_ids: str, key: str = "return_value", **_: object) -> object:
        del task_ids, key
        return "not-a-json-object"

    def _push(*, key: str, value: object) -> None:
        pushed[key] = value

    with pytest.raises(RuntimeError):
        evaluate_pack_outcome(
            upstream_task_id="orders__runtime",
            ti=types.SimpleNamespace(dag_id="dag", run_id="run", xcom_pull=_pull, xcom_push=_push),
            launch_pin_required=True,
        )
    assert AIRFLOW_OUTCOME_GATE_RESULT_XCOM_KEY in pushed
    result = pushed[AIRFLOW_OUTCOME_GATE_RESULT_XCOM_KEY]
    assert isinstance(result, dict)
    assert result.get("passed") is False


def test_explicit_kubernetes_conn_id_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    from dpone_airflow_pack import kubernetes_runtime_client as client_module

    monkeypatch.setattr(client_module, "_core_v1_from_airflow_hook", lambda _conn: None)
    with pytest.raises(RuntimeError, match=PIN_UNAVAILABLE):
        client_module.build_core_v1_api(kubernetes_conn_id="kubernetes_default")


def test_barrier_requires_active_state_and_subject_coords() -> None:
    from dpone_airflow_pack.launch_pin_barrier import ensure_launch_pin_cas_barrier_on_pod

    pod = _pod(
        uid="",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
    )
    ensure_launch_pin_cas_barrier_on_pod(
        pod,
        dag_id="dag",
        run_id="run",
        task_id="orders__runtime",
        map_index=-1,
        try_number=1,
        store_namespace="airflow",
        envelope_sha256_digest=envelope_sha256(
            run_identity=_run_identity(),
            deployment_identity=_deployment_identity(),
            expected_runtime_evidence_sha256="sha256:" + "f" * 64,
        ),
    )
    env = {item["name"]: item for item in pod["spec"]["initContainers"][0]["env"]}  # type: ignore[index]
    assert env["PIN_DAG_ID"]["value"] == "dag"
    assert env["PIN_TASK_ID"]["value"] == "orders__runtime"
    assert env["PIN_HEAD_CM_NAME"]["value"].startswith("dpone-lph-")
    script = pod["spec"]["initContainers"][0]["command"][2]  # type: ignore[index]
    assert "BARRIER_HEAD_URL" in script
    assert 'state") or "") == "ACTIVE"' in script or '== "ACTIVE"' in script


def test_empty_head_concurrent_try_writers_single_winner() -> None:
    """Two writers on an empty subject head → exactly one ACTIVE dual winner."""

    import threading

    from dpone_airflow_pack.launch_pin_k8s_store import KubernetesConfigMapLaunchPinStore
    from dpone_airflow_pack.launch_pin_k8s_validate import LAUNCH_PIN_HEAD_LABEL_VALUE

    class _InterleavedApi:
        def __init__(self) -> None:
            self.objects: dict[str, dict[str, object]] = {}
            self.rv = 0
            self.lock = threading.Lock()
            self.head_creates = 0
            self.admit_gate = threading.Event()
            self.both_admitted = threading.Event()
            self._waiting = 0

        def create_namespaced_config_map(self, *, namespace: str, body: dict[str, object]) -> dict[str, object]:
            del namespace
            name = str(body["metadata"]["name"])  # type: ignore[index]
            labels = body["metadata"].get("labels") or {}  # type: ignore[union-attr]
            is_head = labels.get("dpone.dev/launch-pin") == LAUNCH_PIN_HEAD_LABEL_VALUE
            with self.lock:
                if name in self.objects:
                    exc = RuntimeError("already exists")
                    setattr(exc, "status", 409)
                    raise exc
                if is_head:
                    self.head_creates += 1
                    self._waiting += 1
                    if self._waiting == 1:
                        # First head creator waits until the second also attempts admit.
                        pass
            if is_head:
                self.admit_gate.wait(timeout=2)
            with self.lock:
                if name in self.objects:
                    exc = RuntimeError("already exists")
                    setattr(exc, "status", 409)
                    raise exc
                self.rv += 1
                stored = json.loads(json.dumps(body))
                stored["metadata"]["resourceVersion"] = str(self.rv)
                self.objects[name] = stored
                if is_head and self.head_creates >= 1:
                    self.both_admitted.set()
                return stored

        def read_namespaced_config_map(self, *, name: str, namespace: str) -> dict[str, object]:
            del namespace
            with self.lock:
                if name not in self.objects:
                    exc = RuntimeError("not found")
                    setattr(exc, "status", 404)
                    raise exc
                return json.loads(json.dumps(self.objects[name]))

        def replace_namespaced_config_map(
            self, *, name: str, namespace: str, body: dict[str, object]
        ) -> dict[str, object]:
            del namespace
            with self.lock:
                current = self.objects[name]
                if str(body["metadata"].get("resourceVersion")) != str(  # type: ignore[union-attr]
                    current["metadata"]["resourceVersion"]  # type: ignore[index]
                ):
                    exc = RuntimeError("conflict")
                    setattr(exc, "status", 409)
                    raise exc
                self.rv += 1
                stored = json.loads(json.dumps(body))
                stored["metadata"]["resourceVersion"] = str(self.rv)
                self.objects[name] = stored
                return stored

    api = _InterleavedApi()
    store = KubernetesConfigMapLaunchPinStore(namespace="airflow", api=api)
    pin1 = build_launch_pin(
        attempt={"dag_id": "dag", "run_id": "run", "task_id": "t", "map_index": -1, "try_number": 1},
        pod_namespace="airflow",
        pod_name="pod-a",
        pod_uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        run_identity=_run_identity(),
        deployment_identity=_deployment_identity(),
        expected_runtime_evidence_sha256=None,
        kubernetes_conn_id="kubernetes_default",
    )
    pin2 = build_launch_pin(
        attempt={"dag_id": "dag", "run_id": "run", "task_id": "t", "map_index": -1, "try_number": 2},
        pod_namespace="airflow",
        pod_name="pod-b",
        pod_uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        run_identity=_run_identity(),
        deployment_identity=_deployment_identity(),
        expected_runtime_evidence_sha256=None,
        kubernetes_conn_id="kubernetes_default",
    )
    results: list[dict[str, object] | BaseException] = []
    ready = threading.Barrier(2)

    def _run(pin: dict[str, object]) -> None:
        ready.wait()
        # Release both head-create attempts after both threads enter create_once.
        api.admit_gate.set()
        try:
            results.append(store.create_once(pin))
        except BaseException as exc:  # noqa: BLE001 - collect for assertion
            results.append(exc)

    threads = [threading.Thread(target=_run, args=(pin1,)), threading.Thread(target=_run, args=(pin2,))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
    winners = [item for item in results if isinstance(item, dict)]
    losers = [item for item in results if isinstance(item, BaseException)]
    assert len(winners) == 1, results
    assert len(losers) == 1, results
    winner = winners[0]
    assert winner["state"] == "ACTIVE"
    head = store.get(dag_id="dag", run_id="run", task_id="t", map_index=-1)
    assert head is not None
    assert head["pod_uid"] == winner["pod_uid"]
    assert head["state"] == "ACTIVE"
    # Exactly one barrier-releasable ACTIVE dual record.
    other_try = 2 if int(winner["try_number"]) == 1 else 1
    other = store.get(dag_id="dag", run_id="run", task_id="t", map_index=-1, try_number=other_try)
    assert other is None or str(other.get("state") or "") != "ACTIVE" or other["pod_uid"] != winner["pod_uid"]


def test_empty_launch_pin_ref_object_is_invalid() -> None:
    from dpone_airflow_pack.launch_pin_summary import launch_pin_ref_error, pull_locator_summary

    assert "non-empty" in launch_pin_ref_error({})
    summary, status, detail = pull_locator_summary(
        ti=types.SimpleNamespace(xcom_pull=lambda **_: None),
        upstream_task_id="orders__runtime",
        runtime_summary={"launch_pin_ref": {}},
        required=True,
    )
    assert summary is None
    assert status == "INVALID"
    assert "non-empty" in detail


def test_pre_active_per_try_failure_releases_own_head_candidate() -> None:
    """Head CANDIDATE + proved-absent per-try + abandon → head released so try2 admits."""

    from dpone_airflow_pack.launch_pin_codes import LAUNCH_PIN_STATE_CONSUMED, PIN_UNAVAILABLE
    from dpone_airflow_pack.launch_pin_pod import remember_launch_pin_pod
    from dpone_airflow_pack.launch_pin_store import InMemoryLaunchPinStore, set_launch_pin_store

    class _FailPerTryCreate(InMemoryLaunchPinStore):
        def __init__(self) -> None:
            super().__init__()
            self._fail_once = True

        def create_once(self, pin: object) -> object:
            assert isinstance(pin, dict)
            if not self._fail_once:
                return super().create_once(pin)
            self._fail_once = False
            subject = (str(pin["dag_id"]), str(pin["run_id"]), str(pin["task_id"]), int(pin["map_index"]))
            with self._lock:
                self._admit_head_candidate_locked(subject=subject, pin=pin)
            raise RuntimeError(f"{PIN_UNAVAILABLE}: simulated per-try create proved absent")

    store = _FailPerTryCreate()
    set_launch_pin_store(store)
    ti = _XComStore()
    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
    )
    remember_launch_pin_pod(pod)
    with pytest.raises(RuntimeError, match="CAS failed before base authority"):
        commit_launch_pin_for_selected_pod(context={"ti": ti}, pod=pod)
    head = store._subject_heads.get(("dag", "run", "orders__runtime", -1))
    assert head is not None
    assert head["state"] == LAUNCH_PIN_STATE_CONSUMED

    pod2 = _pod(
        uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        deployment_identity=_deployment_identity(),
        run_identity=_run_identity(),
    )
    remember_launch_pin_pod(pod2)
    ti2 = types.SimpleNamespace(
        dag_id="dag",
        run_id="run",
        task_id="orders__runtime",
        map_index=-1,
        try_number=2,
        xcom_push=ti.xcom_push,
        xcom_pull=ti.xcom_pull,
    )
    retained = commit_launch_pin_for_selected_pod(context={"ti": ti2}, pod=pod2)
    assert retained["state"] == "ACTIVE"
    assert retained["try_number"] == 2


def test_cleanup_returns_structured_pod_head_per_try_statuses() -> None:
    from dpone_airflow_pack.launch_pin_cleanup import (
        AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_XCOM_KEY,
        cleanup_launch_pin_pod,
    )

    launch_deployment = _deployment_identity()
    launch_run = _run_identity()
    runtime_ti = _XComStore()
    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=launch_deployment,
        run_identity=launch_run,
        evidence="sha256:" + "f" * 64,
    )
    retained = commit_launch_pin_for_selected_pod(context={"ti": runtime_ti}, pod=pod)
    summary = _xcom_summary(deployment_identity=launch_deployment, run_identity=launch_run, pin=retained)
    gate_values: dict[str, object] = {}

    def _pull(*, task_ids: str, key: str = "return_value", **_: object) -> object:
        if key == "return_value":
            return summary
        if task_ids.endswith("outcome_gate") or task_ids == "orders__outcome_gate":
            return gate_values.get(key)
        return runtime_ti.values.get(key)

    def _push(*, key: str, value: object) -> None:
        gate_values[key] = value

    gate_ti = types.SimpleNamespace(dag_id="dag", run_id="run", xcom_pull=_pull, xcom_push=_push)
    assert (
        evaluate_pack_outcome(
            upstream_task_id="orders__runtime",
            ti=gate_ti,
            launch_pin_required=True,
        )["passed"]
        is True
    )
    assert AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_XCOM_KEY in gate_values
    cleanup = cleanup_launch_pin_pod(
        upstream_task_id="orders__runtime",
        outcome_gate_task_id="orders__outcome_gate",
        ti=gate_ti,
    )
    assert cleanup["status"] == "deleted"
    assert cleanup["pod_delete"]["status"] == "deleted"
    assert cleanup["head_transition"]["status"] in {"consumed", "already_consumed", "absent"}
    assert cleanup["per_try_delete"]["status"] in {"deleted", "absent"}


def test_store_kpo_conn_none_mismatch_fail_closed() -> None:
    from dpone_airflow_pack.launch_pin_locator import require_same_cluster_launch_pin_store
    from dpone_airflow_pack.launch_pin_store import set_launch_pin_store

    set_launch_pin_store(None)
    with pytest.raises(RuntimeError, match="incompatible with KPO in-cluster"):
        require_same_cluster_launch_pin_store(
            kpo_kubernetes_conn_id=None,
            launch_pin_store={"kubernetes_conn_id": "remote-control", "namespace": "dpone"},
        )


def test_store_inherits_kpo_conn_when_absent() -> None:
    from dpone_airflow_pack.launch_pin_locator import require_same_cluster_launch_pin_store

    closed = require_same_cluster_launch_pin_store(
        kpo_kubernetes_conn_id="kubernetes_default",
        launch_pin_store={"namespace": "airflow"},
    )
    assert closed.kubernetes_conn_id == "kubernetes_default"
    assert closed.namespace == "airflow"


def test_delete_occurrence_skips_per_try_when_head_not_proven_closed() -> None:
    from dpone_airflow_pack.launch_pin_store import InMemoryLaunchPinStore, get_launch_pin_store

    store = get_launch_pin_store()
    assert isinstance(store, InMemoryLaunchPinStore)
    subject = ("dag", "run", "orders__runtime", -1)
    store._subject_heads[subject] = {  # type: ignore[attr-defined]
        "try_number": 1,
        "pod_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "pin_sha256": "sha256:" + "0" * 64,
        "state": "ACTIVE",
    }
    key = (*subject, 1)
    store._rows[key] = {  # type: ignore[attr-defined]
        "try_number": 1,
        "pod_uid": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        "pin_sha256": "sha256:" + "1" * 64,
        "pointer_resource_version": "99",
        "state": "ACTIVE",
    }
    result = store.delete_occurrence(
        dag_id="dag",
        run_id="run",
        task_id="orders__runtime",
        map_index=-1,
        pin_sha256="sha256:" + "1" * 64,
        pointer_resource_version="99",
        pod_uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        try_number=1,
    )
    assert result["head_transition"] == "skipped_mismatch"
    assert result["per_try_delete"]["status"] == "skipped"
    assert result["per_try_delete"]["reason"] == "head_not_proven_closed"
    assert key in store._rows  # type: ignore[attr-defined]


def test_cleanup_pod_already_absent_continues_head_and_per_try() -> None:
    from dpone_airflow_pack.launch_pin_cleanup import (
        cleanup_launch_pin_pod,
    )
    from dpone_airflow_pack.launch_pin_pod import get_launch_pin_pod_reader

    launch_deployment = _deployment_identity()
    launch_run = _run_identity()
    runtime_ti = _XComStore()
    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=launch_deployment,
        run_identity=launch_run,
        evidence="sha256:" + "f" * 64,
    )
    retained = commit_launch_pin_for_selected_pod(context={"ti": runtime_ti}, pod=pod)
    summary = _xcom_summary(deployment_identity=launch_deployment, run_identity=launch_run, pin=retained)
    gate_values: dict[str, object] = {}

    def _pull(*, task_ids: str, key: str = "return_value", **_: object) -> object:
        if key == "return_value":
            return summary
        if task_ids.endswith("outcome_gate") or task_ids == "orders__outcome_gate":
            return gate_values.get(key)
        return runtime_ti.values.get(key)

    def _push(*, key: str, value: object) -> None:
        gate_values[key] = value

    gate_ti = types.SimpleNamespace(dag_id="dag", run_id="run", xcom_pull=_pull, xcom_push=_push)
    assert (
        evaluate_pack_outcome(
            upstream_task_id="orders__runtime",
            ti=gate_ti,
            launch_pin_required=True,
        )["passed"]
        is True
    )
    reader = get_launch_pin_pod_reader()
    remove = getattr(reader, "_pods", None)
    if isinstance(remove, dict):
        remove.clear()
    cleanup = cleanup_launch_pin_pod(
        upstream_task_id="orders__runtime",
        outcome_gate_task_id="orders__outcome_gate",
        ti=gate_ti,
    )
    assert cleanup["pod_delete"]["status"] == "already_absent"
    assert cleanup["status"] == "deleted"
    assert cleanup["head_transition"]["status"] in {"consumed", "already_consumed", "absent"}
    assert cleanup["per_try_delete"]["status"] in {"deleted", "absent"}


def test_head_release_cleanup_failed_raises_recovery_required() -> None:
    from dpone_airflow_pack.launch_pin_codes import PIN_RECOVERY_REQUIRED
    from dpone_airflow_pack.launch_pin_commit import _release_head_after_pre_active_failure

    class _FailReleaseStore:
        def release_own_head_candidate(self, pin: object) -> str:
            del pin
            return "cleanup_failed"

    pin = {
        "dag_id": "dag",
        "run_id": "run",
        "task_id": "t",
        "map_index": -1,
        "try_number": 1,
        "pod_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "pin_sha256": "sha256:" + "a" * 64,
    }
    with pytest.raises(RuntimeError, match=PIN_RECOVERY_REQUIRED):
        _release_head_after_pre_active_failure(store=_FailReleaseStore(), pin=pin)


def test_head_cas_409_readback_consumed_other_writer_is_idempotent() -> None:
    """409 then readback CONSUMED (other writer finished) → already_consumed."""
    from dpone_airflow_pack.launch_pin_k8s_head import mark_head_consumed_or_delete
    from dpone_airflow_pack.launch_pin_k8s_validate import (
        LAUNCH_PIN_CONFIGMAP_LABEL,
        LAUNCH_PIN_HEAD_LABEL_VALUE,
        configmap_name_for_subject,
    )

    pin = {
        "dag_id": "dag",
        "run_id": "run",
        "task_id": "t",
        "map_index": -1,
        "try_number": 1,
        "pod_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "pin_sha256": "sha256:" + "a" * 64,
    }
    head_name = configmap_name_for_subject(dag_id="dag", run_id="run", task_id="t", map_index=-1, try_number=None)
    labels = {LAUNCH_PIN_CONFIGMAP_LABEL: LAUNCH_PIN_HEAD_LABEL_VALUE}
    reads = [
        {
            "metadata": {"name": head_name, "namespace": "airflow", "resourceVersion": "7", "labels": labels},
            "data": {
                "head.json": json.dumps(
                    {**{k: pin[k] for k in ("try_number", "pod_uid", "pin_sha256")}, "state": "ACTIVE"}
                )
            },
        },
        {
            "metadata": {"name": head_name, "namespace": "airflow", "resourceVersion": "8", "labels": labels},
            "data": {
                "head.json": json.dumps(
                    {**{k: pin[k] for k in ("try_number", "pod_uid", "pin_sha256")}, "state": "CONSUMED"}
                )
            },
        },
    ]

    def _read(name: str) -> tuple[dict[str, object], str]:
        body = reads.pop(0)
        return body, str(body["metadata"]["resourceVersion"])

    def _replace(**_: object) -> None:
        exc = RuntimeError("conflict")
        setattr(exc, "status", 409)
        raise exc

    status = mark_head_consumed_or_delete(
        api=object(),
        namespace="airflow",
        pin=pin,
        read_fn=_read,
        replace_fn=_replace,
        delete_fn=lambda **_: None,
        not_found_exc=RuntimeError,
    )
    assert status == "already_consumed"


def test_head_cas_409_readback_active_self_retries_to_consumed() -> None:
    """ACTIVE rv7 → replace 409 → ACTIVE rv8 → replace success → CONSUMED → consumed."""

    from dpone_airflow_pack.launch_pin_k8s_head import mark_head_consumed_or_delete
    from dpone_airflow_pack.launch_pin_k8s_validate import (
        LAUNCH_PIN_CONFIGMAP_LABEL,
        LAUNCH_PIN_HEAD_LABEL_VALUE,
        configmap_name_for_subject,
    )

    pin = {
        "dag_id": "dag",
        "run_id": "run",
        "task_id": "t",
        "map_index": -1,
        "try_number": 1,
        "pod_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "pin_sha256": "sha256:" + "a" * 64,
    }
    head_name = configmap_name_for_subject(dag_id="dag", run_id="run", task_id="t", map_index=-1, try_number=None)
    labels = {LAUNCH_PIN_CONFIGMAP_LABEL: LAUNCH_PIN_HEAD_LABEL_VALUE}
    head_state = {"rv": "7", "state": "ACTIVE"}
    replace_calls = 0

    def _head_payload() -> dict[str, object]:
        return {
            **{k: pin[k] for k in ("try_number", "pod_uid", "pin_sha256")},
            "state": head_state["state"],
            "dag_id": "dag",
            "run_id": "run",
            "task_id": "t",
            "map_index": -1,
            "store_namespace": "airflow",
        }

    def _read(name: str) -> tuple[dict[str, object], str]:
        assert name == head_name
        return (
            {
                "metadata": {
                    "name": head_name,
                    "namespace": "airflow",
                    "resourceVersion": head_state["rv"],
                    "labels": labels,
                },
                "data": {"head.json": json.dumps(_head_payload())},
            },
            head_state["rv"],
        )

    def _replace(**_: object) -> dict[str, object]:
        nonlocal replace_calls
        replace_calls += 1
        if replace_calls == 1:
            exc = RuntimeError("conflict")
            setattr(exc, "status", 409)
            raise exc
        head_state["rv"] = "9"
        head_state["state"] = "CONSUMED"
        return {
            "metadata": {
                "name": head_name,
                "namespace": "airflow",
                "resourceVersion": head_state["rv"],
                "labels": labels,
            },
            "data": {"head.json": json.dumps(_head_payload())},
        }

    def _read_sequence(name: str) -> tuple[dict[str, object], str]:
        body, rv = _read(name)
        if replace_calls == 1 and head_state["state"] == "ACTIVE" and head_state["rv"] == "7":
            # First 409 readback: still ACTIVE at rv8 (concurrent writer bumped RV).
            head_state["rv"] = "8"
            body = {
                "metadata": {
                    "name": head_name,
                    "namespace": "airflow",
                    "resourceVersion": "8",
                    "labels": labels,
                },
                "data": {"head.json": json.dumps(_head_payload())},
            }
            return body, "8"
        return body, rv

    status = mark_head_consumed_or_delete(
        api=object(),
        namespace="airflow",
        pin=pin,
        read_fn=_read_sequence,
        replace_fn=_replace,
        delete_fn=lambda **_: None,
        not_found_exc=RuntimeError,
    )
    assert status == "consumed"
    assert replace_calls == 2


def test_head_cas_409_active_second_replace_timeout_readback_consumed() -> None:
    """Second replace timeout after server-side success → final readback CONSUMED → already_consumed."""

    from dpone_airflow_pack.launch_pin_k8s_head import mark_head_consumed_or_delete
    from dpone_airflow_pack.launch_pin_k8s_validate import (
        LAUNCH_PIN_CONFIGMAP_LABEL,
        LAUNCH_PIN_HEAD_LABEL_VALUE,
        configmap_name_for_subject,
    )

    pin = {
        "dag_id": "dag",
        "run_id": "run",
        "task_id": "t",
        "map_index": -1,
        "try_number": 1,
        "pod_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "pin_sha256": "sha256:" + "a" * 64,
    }
    head_name = configmap_name_for_subject(dag_id="dag", run_id="run", task_id="t", map_index=-1, try_number=None)
    labels = {LAUNCH_PIN_CONFIGMAP_LABEL: LAUNCH_PIN_HEAD_LABEL_VALUE}
    head_state = {"rv": "7", "state": "ACTIVE", "phase": "initial"}
    replace_calls = 0

    def _head_payload() -> dict[str, object]:
        return {
            **{k: pin[k] for k in ("try_number", "pod_uid", "pin_sha256")},
            "state": head_state["state"],
            "dag_id": "dag",
            "run_id": "run",
            "task_id": "t",
            "map_index": -1,
            "store_namespace": "airflow",
        }

    def _read(name: str) -> tuple[dict[str, object], str]:
        assert name == head_name
        return (
            {
                "metadata": {
                    "name": head_name,
                    "namespace": "airflow",
                    "resourceVersion": head_state["rv"],
                    "labels": labels,
                },
                "data": {"head.json": json.dumps(_head_payload())},
            },
            head_state["rv"],
        )

    def _replace(**_: object) -> None:
        nonlocal replace_calls
        replace_calls += 1
        if replace_calls == 1:
            exc = RuntimeError("conflict")
            setattr(exc, "status", 409)
            raise exc
        # Simulate client timeout after server already applied CONSUMED.
        head_state["rv"] = "9"
        head_state["state"] = "CONSUMED"
        raise TimeoutError("replace timed out")

    def _read_sequence(name: str) -> tuple[dict[str, object], str]:
        if replace_calls == 1 and head_state["phase"] == "initial":
            head_state["phase"] = "post_409"
            head_state["rv"] = "8"
            body = {
                "metadata": {
                    "name": head_name,
                    "namespace": "airflow",
                    "resourceVersion": "8",
                    "labels": labels,
                },
                "data": {"head.json": json.dumps(_head_payload())},
            }
            return body, "8"
        return _read(name)

    status = mark_head_consumed_or_delete(
        api=object(),
        namespace="airflow",
        pin=pin,
        read_fn=_read_sequence,
        replace_fn=_replace,
        delete_fn=lambda **_: None,
        not_found_exc=RuntimeError,
    )
    assert status == "already_consumed"
    assert replace_calls == 2


def test_head_cas_409_readback_candidate_self_retries_to_consumed() -> None:
    from dpone_airflow_pack.launch_pin_k8s_head import release_own_head_candidate
    from dpone_airflow_pack.launch_pin_k8s_validate import (
        LAUNCH_PIN_CONFIGMAP_LABEL,
        LAUNCH_PIN_HEAD_LABEL_VALUE,
        configmap_name_for_subject,
    )

    pin = {
        "dag_id": "dag",
        "run_id": "run",
        "task_id": "t",
        "map_index": -1,
        "try_number": 1,
        "pod_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "pin_sha256": "sha256:" + "a" * 64,
    }
    head_name = configmap_name_for_subject(dag_id="dag", run_id="run", task_id="t", map_index=-1, try_number=None)
    labels = {LAUNCH_PIN_CONFIGMAP_LABEL: LAUNCH_PIN_HEAD_LABEL_VALUE}
    head_state = {"rv": "7", "state": "CANDIDATE"}
    replace_calls = 0

    def _head_payload() -> dict[str, object]:
        return {
            **{k: pin[k] for k in ("try_number", "pod_uid", "pin_sha256")},
            "state": head_state["state"],
            "dag_id": "dag",
            "run_id": "run",
            "task_id": "t",
            "map_index": -1,
            "store_namespace": "airflow",
        }

    def _read(name: str) -> tuple[dict[str, object], str]:
        assert name == head_name
        return (
            {
                "metadata": {
                    "name": head_name,
                    "namespace": "airflow",
                    "resourceVersion": head_state["rv"],
                    "labels": labels,
                },
                "data": {"head.json": json.dumps(_head_payload())},
            },
            head_state["rv"],
        )

    def _replace(**_: object) -> dict[str, object]:
        nonlocal replace_calls
        replace_calls += 1
        if replace_calls == 1:
            exc = RuntimeError("conflict")
            setattr(exc, "status", 409)
            raise exc
        head_state["rv"] = "8"
        head_state["state"] = "CONSUMED"
        return {
            "metadata": {
                "name": head_name,
                "namespace": "airflow",
                "resourceVersion": head_state["rv"],
                "labels": labels,
            },
            "data": {"head.json": json.dumps(_head_payload())},
        }

    status = release_own_head_candidate(
        api=object(),
        namespace="airflow",
        pin=pin,
        read_fn=_read,
        replace_fn=_replace,
        delete_fn=lambda **_: None,
        not_found_exc=RuntimeError,
    )
    assert status == "consumed"
    assert replace_calls == 2


def test_cleanup_skipped_conflict_is_not_false_success() -> None:
    from dpone_airflow_pack.launch_pin_cleanup import _overall_cleanup_status

    overall = _overall_cleanup_status(
        head_transition={"status": "skipped_conflict"},
        per_try_delete={"status": "skipped", "reason": "head_not_proven_closed"},
    )
    assert overall == "head_transition_failed"


@pytest.mark.parametrize(
    ("head_status", "per_try_status"),
    [
        ("skipped_conflict", "deleted"),
        ("skipped_mismatch", "deleted"),
        ("cleanup_failed", "deleted"),
        ("skipped", "deleted"),
        ("unknown", "deleted"),
        ("consumed", "skipped"),
        ("consumed", "delete_failed"),
        ("consumed", "unknown"),
        ("already_consumed", "skipped"),
    ],
)
def test_overall_cleanup_status_never_deleted_on_non_closed_phases(
    head_status: str,
    per_try_status: str,
) -> None:
    from dpone_airflow_pack.launch_pin_cleanup import _overall_cleanup_status

    overall = _overall_cleanup_status(
        head_transition={"status": head_status},
        per_try_delete={"status": per_try_status},
    )
    assert overall != "deleted"


def test_namespace_only_store_inherits_kpo_conn_in_from_pack() -> None:
    from dpone_airflow_pack.launch_pin_locator import launch_pin_store_locator_from_pack

    locator = launch_pin_store_locator_from_pack(
        {
            "launch_pin_store": {"namespace": "airflow"},
            "kpo_kwargs": {"kubernetes_conn_id": "kubernetes_default"},
        }
    )
    assert locator is not None
    assert locator.kubernetes_conn_id == "kubernetes_default"
    assert locator.namespace == "airflow"


def test_closed_locator_shared_between_runtime_and_outcome_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack import outcome as outcome_module
    from dpone_airflow_pack.launch_pin_wiring import closed_locator_mapping

    class _StubPython:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

    monkeypatch.setattr(outcome_module, "python_operator_class", lambda: _StubPython)
    pack = {
        "launch_pin_store": {"namespace": "airflow"},
        "kpo_kwargs": {
            "task_id": "orders__runtime",
            "kubernetes_conn_id": "kubernetes_default",
            "namespace": "airflow",
        },
        "outcome_gate": {"required_status": "passed"},
        "_dpone_deployment_identity": _deployment_identity(),
    }
    closed = closed_locator_mapping(pack, kpo_kwargs=pack["kpo_kwargs"])
    assert closed["kubernetes_conn_id"] == "kubernetes_default"
    assert closed["namespace"] == "airflow"
    assert str(closed["authority_digest"]).startswith("sha256:")
    from dpone_airflow_pack.outcome import build_pack_outcome_task

    outcome = build_pack_outcome_task(
        pack=pack,
        dag=object(),
        upstream_task_id="orders__runtime",
        launch_pin_store=closed,
    )
    assert outcome.kwargs["op_kwargs"]["launch_pin_store"] == closed


def test_closed_locator_frozen_when_scheduler_and_worker_env_namespaces_differ(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack.launch_pin_locator import close_launch_pin_store_locator

    materialized = {"kubernetes_conn_id": "kubernetes_default", "namespace": "airflow"}
    closed = close_launch_pin_store_locator(
        launch_pin_store=materialized,
        kpo_kubernetes_conn_id="kubernetes_default",
        kpo_namespace="airflow",
    )
    monkeypatch.setenv("DPONE_LAUNCH_PIN_STORE_NAMESPACE", "other-namespace")
    again = close_launch_pin_store_locator(
        launch_pin_store=closed,
        kpo_kubernetes_conn_id="kubernetes_default",
        kpo_namespace="airflow",
    )
    assert again == closed


def test_remote_kpo_without_concrete_namespace_fails_at_close() -> None:
    from dpone_airflow_pack.launch_pin_locator import close_launch_pin_store_locator

    with pytest.raises(RuntimeError, match="requires a concrete launch_pin_store.namespace"):
        close_launch_pin_store_locator(
            launch_pin_store={"kubernetes_conn_id": None, "namespace": None},
            kpo_kubernetes_conn_id="kubernetes_default",
            kpo_namespace=None,
        )


def test_store_conn_mismatch_with_kpo_conn_fails_at_close() -> None:
    from dpone_airflow_pack.launch_pin_locator import close_launch_pin_store_locator

    with pytest.raises(RuntimeError, match="must equal KPO kubernetes_conn_id"):
        close_launch_pin_store_locator(
            launch_pin_store={"kubernetes_conn_id": "remote-control", "namespace": "airflow"},
            kpo_kubernetes_conn_id="kubernetes_default",
            kpo_namespace="airflow",
        )


def test_cleanup_handle_rejects_top_level_store_conn_mismatch() -> None:
    from dpone_airflow_pack.launch_pin_cleanup_handle import build_cleanup_handle_from_pin, cleanup_handle_error

    pin = {
        "try_number": 1,
        "pod_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "pod_namespace": "airflow",
        "pod_name": "pod-a",
        "pin_sha256": "sha256:" + "0" * 64,
        "pointer_resource_version": "1",
        "dag_id": "dag",
        "run_id": "run",
        "task_id": "orders__runtime",
        "map_index": -1,
    }
    handle = build_cleanup_handle_from_pin(
        pin,
        store_kubernetes_conn_id="kubernetes_default",
        store_namespace="airflow",
    )
    handle["kubernetes_conn_id"] = "other-conn"
    assert cleanup_handle_error(handle) == (
        "cleanup handle.kubernetes_conn_id must equal cleanup handle.store.kubernetes_conn_id"
    )


def test_cleanup_handle_rejects_non_string_store_conn() -> None:
    from dpone_airflow_pack.launch_pin_cleanup_handle import build_cleanup_handle_from_pin, cleanup_handle_error

    pin = {
        "try_number": 1,
        "pod_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "pod_namespace": "airflow",
        "pod_name": "pod-a",
        "pin_sha256": "sha256:" + "0" * 64,
        "pointer_resource_version": "1",
        "dag_id": "dag",
        "run_id": "run",
        "task_id": "orders__runtime",
        "map_index": -1,
    }
    handle = build_cleanup_handle_from_pin(
        pin,
        store_kubernetes_conn_id="kubernetes_default",
        store_namespace="airflow",
    )
    handle["store"]["kubernetes_conn_id"] = 42  # type: ignore[assignment]
    assert cleanup_handle_error(handle) == "kubernetes_conn_id must be null or a non-empty string"


def test_cleanup_handle_rejects_malformed_top_level_conn() -> None:
    from dpone_airflow_pack.launch_pin_cleanup_handle import build_cleanup_handle_from_pin, cleanup_handle_error

    pin = {
        "try_number": 1,
        "pod_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "pod_namespace": "airflow",
        "pod_name": "pod-a",
        "pin_sha256": "sha256:" + "0" * 64,
        "pointer_resource_version": "1",
        "dag_id": "dag",
        "run_id": "run",
        "task_id": "orders__runtime",
        "map_index": -1,
    }
    handle = build_cleanup_handle_from_pin(
        pin,
        store_kubernetes_conn_id="kubernetes_default",
        store_namespace="airflow",
    )
    handle["kubernetes_conn_id"] = ["bad"]  # type: ignore[assignment]
    assert cleanup_handle_error(handle) == "kubernetes_conn_id must be null or a non-empty string"


def test_cleanup_handle_rejects_digest_for_none_while_fallback_non_null() -> None:
    from dpone_airflow_pack.launch_pin_cleanup_handle import cleanup_handle_error
    from dpone_airflow_pack.launch_pin_locator import store_authority_digest

    handle = {
        "schema": "dpone.airflow-launch-pin-cleanup-handle.v1",
        "try_number": 1,
        "pod_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "pod_namespace": "airflow",
        "pod_name": "pod-a",
        "pin_sha256": "sha256:" + "0" * 64,
        "pointer_resource_version": "1",
        "dag_id": "dag",
        "run_id": "run",
        "task_id": "orders__runtime",
        "map_index": -1,
        "kubernetes_conn_id": None,
        "store": {
            "kubernetes_conn_id": None,
            "namespace": "airflow",
            "authority_digest": store_authority_digest(kubernetes_conn_id="kubernetes_default", namespace="airflow"),
        },
    }
    assert cleanup_handle_error(handle) == (
        "cleanup handle.store.authority_digest does not match frozen store coordinates"
    )


def test_launch_pin_store_locator_from_mapping_rejects_malformed_conn() -> None:
    from dpone_airflow_pack.launch_pin_locator import launch_pin_store_locator_from_mapping

    with pytest.raises(RuntimeError, match=PIN_INVALID):
        launch_pin_store_locator_from_mapping({"kubernetes_conn_id": 42, "namespace": "airflow"})
    with pytest.raises(RuntimeError, match=PIN_INVALID):
        launch_pin_store_locator_from_mapping({"kubernetes_conn_id": "kubernetes_default", "namespace": []})


def test_outcome_gate_resolves_attempt_via_pinned_store_when_gate_tip_differs() -> None:
    """Runtime pack A store + gate rematerialized with tip B must not PIN_MISSING."""

    launch_deployment = _deployment_identity()
    launch_run = _run_identity()
    pod = _pod(
        uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        deployment_identity=launch_deployment,
        run_identity=launch_run,
    )
    retained = _store_pin_with_pod(pod=pod)
    summary = _xcom_summary(
        deployment_identity=launch_deployment,
        run_identity=launch_run,
        pin=retained,
    )
    from dpone_airflow_pack.launch_pin_locator import store_authority_digest

    tip_b_store = {
        "kubernetes_conn_id": "kubernetes_default",
        "namespace": "other-tip-namespace",
        "authority_digest": store_authority_digest(
            kubernetes_conn_id="kubernetes_default",
            namespace="other-tip-namespace",
        ),
    }
    payload = evaluate_pack_outcome(
        upstream_task_id="orders__runtime",
        ti=types.SimpleNamespace(
            dag_id="dag",
            run_id="run",
            xcom_pull=_xcom_pull_with_locator(summary=summary, pin=retained),
        ),
        expected_deployment_identity=launch_deployment,
        expected_run_identity=launch_run,
        launch_pin_required=True,
        launch_pin_store=tip_b_store,
    )
    assert payload["passed"] is True


def test_same_store_authority_new_deployment_identity_gate_passes() -> None:
    launch_deployment = _deployment_identity()
    launch_run = _run_identity()
    new_deployment = _deployment_identity(
        deployment_digest="c",
        activation_id="4f60628e-ef48-48b0-84c3-a9e27a82a7f2",
    )
    pod = _pod(
        uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        deployment_identity=launch_deployment,
        run_identity=launch_run,
    )
    retained = _store_pin_with_pod(pod=pod)
    summary = _xcom_summary(
        deployment_identity=launch_deployment,
        run_identity=launch_run,
        pin=retained,
    )
    from dpone_airflow_pack.launch_pin_locator import store_authority_digest

    same_store = {
        "kubernetes_conn_id": None,
        "namespace": "airflow",
        "authority_digest": store_authority_digest(kubernetes_conn_id=None, namespace="airflow"),
    }
    payload = evaluate_pack_outcome(
        upstream_task_id="orders__runtime",
        ti=types.SimpleNamespace(
            dag_id="dag",
            run_id="run",
            xcom_pull=_xcom_pull_with_locator(summary=summary, pin=retained),
        ),
        expected_deployment_identity=new_deployment,
        expected_run_identity=launch_run,
        launch_pin_required=True,
        launch_pin_store=same_store,
    )
    assert payload["passed"] is True


def test_build_pod_request_obj_always_closes_launch_pin_store(monkeypatch: pytest.MonkeyPatch) -> None:
    from dpone_airflow_pack.launch_pin_locator import store_authority_digest
    from dpone_airflow_pack.launch_pin_store import InMemoryLaunchPinStore, set_launch_pin_store
    from dpone_airflow_pack.operators import PinnedXComSidecarKubernetesPodOperator

    set_launch_pin_store(InMemoryLaunchPinStore())
    base_operator = PinnedXComSidecarKubernetesPodOperator.__mro__[1]
    monkeypatch.setattr(
        base_operator,
        "build_pod_request_obj",
        lambda _self, context=None: {
            "metadata": {"name": "orders-runtime-pod"},
            "spec": {"containers": [{"name": "base", "env": []}]},
        },
    )
    preclosed = {
        "kubernetes_conn_id": "kubernetes_default",
        "namespace": "airflow",
        "authority_digest": "sha256:deadbeef",
    }
    operator = PinnedXComSidecarKubernetesPodOperator(
        task_id="orders__runtime",
        name="orders-runtime",
        namespace="airflow",
        pin_deployment_identity_for_separate_outcome_gate=True,
        launch_pin_store=preclosed,
    )
    operator.kubernetes_conn_id = "kubernetes_default"
    operator.namespace = "airflow"
    operator.build_pod_request_obj(context={})
    assert operator.launch_pin_store is not None
    assert operator.launch_pin_store["authority_digest"] == store_authority_digest(
        kubernetes_conn_id="kubernetes_default",
        namespace="airflow",
    )
    set_launch_pin_store(None)


class _RoutingLaunchPinStoreRegistry:
    """Hermetic multi-store router keyed by frozen (kubernetes_conn_id, namespace)."""

    def __init__(self) -> None:
        self.stores: dict[tuple[str | None, str | None], InMemoryLaunchPinStore] = {}

    def store_for(self, *, kubernetes_conn_id: str | None, namespace: str | None) -> InMemoryLaunchPinStore:
        key = (kubernetes_conn_id, namespace)
        if key not in self.stores:
            self.stores[key] = InMemoryLaunchPinStore()
        return self.stores[key]


def _store_pin_in_routed_store(
    *,
    registry: _RoutingLaunchPinStoreRegistry,
    pod: dict[str, object],
    store_kubernetes_conn_id: str | None,
    store_namespace: str,
    attempt: dict[str, object] | None = None,
) -> tuple[dict[str, object], InMemoryLaunchPinStore]:
    from dpone_airflow_pack.launch_pin_envelope import launch_envelope_from_pod
    from dpone_airflow_pack.launch_pin_pod import remember_launch_pin_pod

    meta = pod["metadata"]
    assert isinstance(meta, dict)
    envelope = launch_envelope_from_pod(pod)
    pin = build_launch_pin(
        attempt=attempt
        or {
            "dag_id": "dag",
            "run_id": "run",
            "task_id": "orders__runtime",
            "map_index": -1,
            "try_number": 1,
        },
        pod_namespace=str(meta["namespace"]),
        pod_name=str(meta["name"]),
        pod_uid=str(meta["uid"]),
        kubernetes_conn_id=store_kubernetes_conn_id,
        run_identity=envelope["run_identity"],
        deployment_identity=envelope["deployment_identity"],
        expected_runtime_evidence_sha256=envelope["expected_runtime_evidence_sha256"],
    )
    remember_launch_pin_pod(pod)
    pin = _stamp_attempt_store_authority(
        pin,
        kubernetes_conn_id=store_kubernetes_conn_id,
        namespace=store_namespace,
    )
    store = registry.store_for(kubernetes_conn_id=store_kubernetes_conn_id, namespace=store_namespace)
    retained = store.create_once(pin)
    return dict(retained), store


def test_outcome_gate_cleanup_uses_attempt_pinned_store_not_gate_tip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Gate PASS + cleanup must target store A when gate rematerialized with tip B."""

    from dpone_airflow_pack import launch_pin as launch_pin_module
    from dpone_airflow_pack.launch_pin_cleanup import (
        AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_XCOM_KEY,
        AIRFLOW_OUTCOME_GATE_RESULT_XCOM_KEY,
        cleanup_launch_pin_pod,
    )
    from dpone_airflow_pack.launch_pin_codes import LAUNCH_PIN_STATE_ACTIVE, LAUNCH_PIN_STATE_CONSUMED
    from dpone_airflow_pack.launch_pin_locator import (
        store_authority_digest,
    )

    set_launch_pin_store(None)
    registry = _RoutingLaunchPinStoreRegistry()

    def _authoritative(
        *,
        kubernetes_conn_id: str | None = None,
        namespace: str | None = None,
        allow_remote: bool = True,
    ) -> InMemoryLaunchPinStore:
        del allow_remote
        return registry.store_for(kubernetes_conn_id=kubernetes_conn_id, namespace=namespace)

    monkeypatch.setattr(launch_pin_module, "authoritative_launch_pin_store", _authoritative)

    store_a_ns = "store-a-ns"
    store_b_ns = "other-tip-namespace"
    store_conn = "kubernetes_default"
    pod_ns = "airflow"
    launch_deployment = _deployment_identity()
    launch_run = _run_identity()
    pod = _pod(
        uid="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        deployment_identity=launch_deployment,
        run_identity=launch_run,
        name="orders-runtime-pod",
    )
    pod["metadata"]["namespace"] = pod_ns  # type: ignore[index]
    retained, store_a = _store_pin_in_routed_store(
        registry=registry,
        pod=pod,
        store_kubernetes_conn_id=store_conn,
        store_namespace=store_a_ns,
    )
    store_b = registry.store_for(kubernetes_conn_id=store_conn, namespace=store_b_ns)
    assert store_b is not store_a

    summary = _xcom_summary(
        deployment_identity=launch_deployment,
        run_identity=launch_run,
        pin=retained,
    )

    tip_b_store = {
        "kubernetes_conn_id": store_conn,
        "namespace": store_b_ns,
        "authority_digest": store_authority_digest(kubernetes_conn_id=store_conn, namespace=store_b_ns),
    }

    gate_ti = _XComStore()
    gate_ti.xcom_pull = _xcom_pull_with_locator(summary=summary, pin=retained)  # type: ignore[method-assign]

    payload = evaluate_pack_outcome(
        upstream_task_id="orders__runtime",
        ti=gate_ti,
        expected_deployment_identity=launch_deployment,
        expected_run_identity=launch_run,
        launch_pin_required=True,
        launch_pin_store=tip_b_store,
    )
    assert payload["passed"] is True

    handle = gate_ti.values.get(AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_XCOM_KEY)
    assert isinstance(handle, dict)
    assert handle["store"]["namespace"] == store_a_ns
    assert handle["store"]["kubernetes_conn_id"] == store_conn
    assert handle["store"]["authority_digest"] == store_authority_digest(
        kubernetes_conn_id=store_conn,
        namespace=store_a_ns,
    )

    head_before = store_a.get(
        dag_id="dag",
        run_id="run",
        task_id="orders__runtime",
        map_index=-1,
    )
    assert head_before is not None
    assert head_before.get("state") == LAUNCH_PIN_STATE_ACTIVE

    cleanup_ti = _XComStore(task_id="orders__outcome_gate")
    cleanup_ti.values[AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_XCOM_KEY] = handle
    cleanup_ti.values[AIRFLOW_OUTCOME_GATE_RESULT_XCOM_KEY] = {"passed": True, "payload": payload}

    result = cleanup_launch_pin_pod(
        upstream_task_id="orders__runtime",
        outcome_gate_task_id="orders__outcome_gate",
        ti=cleanup_ti,
    )
    assert result["status"] == "deleted"

    head_after_a = store_a.get(
        dag_id="dag",
        run_id="run",
        task_id="orders__runtime",
        map_index=-1,
        try_number=1,
    )
    assert head_after_a is None
    subject_head_a = store_a._subject_heads.get(("dag", "run", "orders__runtime", -1))  # noqa: SLF001
    assert subject_head_a is not None
    assert subject_head_a.get("state") == LAUNCH_PIN_STATE_CONSUMED

    assert store_b._rows == {}  # noqa: SLF001
    assert store_b._subject_heads == {}  # noqa: SLF001


def test_outcome_gate_cleanup_store_a_when_pod_namespace_differs_from_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Remote KPO pod namespace must not become cleanup store namespace fallback."""

    from dpone_airflow_pack import launch_pin as launch_pin_module
    from dpone_airflow_pack.launch_pin_cleanup import AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_XCOM_KEY
    from dpone_airflow_pack.launch_pin_locator import (
        store_authority_digest,
    )

    set_launch_pin_store(None)
    registry = _RoutingLaunchPinStoreRegistry()
    monkeypatch.setattr(
        launch_pin_module,
        "authoritative_launch_pin_store",
        lambda *, kubernetes_conn_id=None, namespace=None, allow_remote=True: registry.store_for(
            kubernetes_conn_id=kubernetes_conn_id,
            namespace=namespace,
        ),
    )

    store_conn = "kubernetes_default"
    store_a_ns = "dpone-launch-pin-a"
    pod_ns = "workload-runtime-ns"
    launch_deployment = _deployment_identity()
    launch_run = _run_identity()
    pod = _pod(
        uid="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        deployment_identity=launch_deployment,
        run_identity=launch_run,
        name="orders-runtime-pod",
    )
    pod["metadata"]["namespace"] = pod_ns  # type: ignore[index]
    retained, _store_a = _store_pin_in_routed_store(
        registry=registry,
        pod=pod,
        store_kubernetes_conn_id=store_conn,
        store_namespace=store_a_ns,
    )
    tip_b_store = {
        "kubernetes_conn_id": store_conn,
        "namespace": "other-tip-namespace",
        "authority_digest": store_authority_digest(kubernetes_conn_id=store_conn, namespace="other-tip-namespace"),
    }
    summary = _xcom_summary(deployment_identity=launch_deployment, run_identity=launch_run, pin=retained)

    gate_ti = _XComStore()
    gate_ti.xcom_pull = _xcom_pull_with_locator(summary=summary, pin=retained)  # type: ignore[method-assign]
    evaluate_pack_outcome(
        upstream_task_id="orders__runtime",
        ti=gate_ti,
        expected_deployment_identity=launch_deployment,
        expected_run_identity=launch_run,
        launch_pin_required=True,
        launch_pin_store=tip_b_store,
    )
    handle = gate_ti.values[AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_XCOM_KEY]
    assert isinstance(handle, dict)
    assert handle["pod_namespace"] == pod_ns
    assert handle["store"]["namespace"] == store_a_ns
    assert handle["store"]["namespace"] != pod_ns
