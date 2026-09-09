from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

import dpone.adapters.kubernetes_metadata as metadata_module
from dpone.adapters.kubernetes_airflow_runtime_pod_retention import (
    KubernetesAirflowRuntimePodRetentionAdapter,
)
from dpone.cli import main as cli_main
from dpone.contracts.airflow_runtime_pod_retention_template import (
    RUNTIME_POD_RETENTION_TEMPLATE_SHA_ANNOTATION,
    runtime_pod_retention_template_sha256,
    verify_runtime_pod_retention_template_identity,
)
from dpone.gitops.airflow_runtime_pod_retention_manifests import (
    AirflowRuntimePodRetentionRenderRequest,
    render_runtime_pod_retention,
)
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.ports.kubernetes_metadata import KubernetesMetadataApiError

_IMAGE = "registry.example/dpone@sha256:" + "a" * 64


def test_plan_render_is_deterministic_least_privilege_and_shell_free() -> None:
    request = AirflowRuntimePodRetentionRenderRequest(
        namespace="airflow-example",
        image=_IMAGE,
    )

    first = render_runtime_pod_retention(request)
    second = render_runtime_pod_retention(request)

    assert first == second
    assert (
        GitOpsSchemaValidator().validate(
            first,
            expected_kind="dpone.airflow-runtime-pod-retention-render.v1",
        )
        == ()
    )
    assert first["mode"] == "plan"
    assert len(first["manifests"]) == 4
    resources = {item["kind"]: item for item in first["manifests"]}
    assert resources["Role"]["rules"] == [{"apiGroups": [""], "resources": ["pods"], "verbs": ["list"]}]
    cron = resources["CronJob"]
    assert cron["spec"]["concurrencyPolicy"] == "Forbid"
    assert cron["spec"]["suspend"] is False
    assert cron["spec"]["jobTemplate"]["spec"]["activeDeadlineSeconds"] == 300
    labels = cron["metadata"]["labels"]
    assert cron["spec"]["jobTemplate"]["metadata"]["labels"] == labels
    assert cron["spec"]["jobTemplate"]["spec"]["template"]["metadata"]["labels"] == labels
    annotations = cron["spec"]["jobTemplate"]["metadata"]["annotations"]
    assert annotations["dpone.dev/runtime-pod-retention-mode"] == "plan"
    assert annotations["dpone.dev/runtime-pod-retention-template-sha256"].startswith("sha256:")
    assert cron["spec"]["jobTemplate"]["spec"]["template"]["metadata"]["annotations"] == annotations
    container = cron["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]
    assert container["image"] == _IMAGE
    assert container["command"][:3] == ["dpone", "airflow", "runtime-pod-retention-plan"]
    assert "sh" not in container["command"]
    assert container["securityContext"]["readOnlyRootFilesystem"] is True


def test_template_identity_covers_all_execution_fields() -> None:
    report = render_runtime_pod_retention(
        AirflowRuntimePodRetentionRenderRequest(namespace="airflow-example", image=_IMAGE)
    )
    cron = next(item for item in report["manifests"] if item["kind"] == "CronJob")
    template = cron["spec"]["jobTemplate"]
    expected = template["metadata"]["annotations"]["dpone.dev/runtime-pod-retention-template-sha256"]
    assert runtime_pod_retention_template_sha256(template) == expected
    assert verify_runtime_pod_retention_template_identity(template) == expected

    mutations = (
        lambda value: value["metadata"]["labels"].update({"unexpected": "label"}),
        lambda value: value["spec"].update({"ttlSecondsAfterFinished": 1}),
        lambda value: value["spec"]["template"]["spec"].update({"serviceAccountName": "other"}),
        lambda value: value["spec"]["template"]["spec"]["containers"][0].update({"args": ["unexpected"]}),
        lambda value: value["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"].update(
            {"memory": "1Gi"}
        ),
    )
    for mutate in mutations:
        candidate = deepcopy(template)
        mutate(candidate)
        assert runtime_pod_retention_template_sha256(candidate) != expected


def test_template_identity_requires_exact_digest_mirrors() -> None:
    report = render_runtime_pod_retention(
        AirflowRuntimePodRetentionRenderRequest(namespace="airflow-example", image=_IMAGE)
    )
    cron = next(item for item in report["manifests"] if item["kind"] == "CronJob")
    template = cron["spec"]["jobTemplate"]
    digest_paths = (
        ("metadata", "annotations", RUNTIME_POD_RETENTION_TEMPLATE_SHA_ANNOTATION),
        (
            "spec",
            "template",
            "metadata",
            "annotations",
            RUNTIME_POD_RETENTION_TEMPLATE_SHA_ANNOTATION,
        ),
    )

    for path in digest_paths:
        candidate = deepcopy(template)
        parent, key = _parent_at(candidate, path)
        parent[key] = "sha256:" + "f" * 64
        with pytest.raises(ValueError, match="digest value"):
            verify_runtime_pod_retention_template_identity(candidate)

        candidate = deepcopy(template)
        parent, key = _parent_at(candidate, path)
        parent.pop(key)
        with pytest.raises(ValueError, match="digest mirrors"):
            verify_runtime_pod_retention_template_identity(candidate)

    candidate = deepcopy(template)
    candidate["spec"]["template"]["spec"]["annotations"] = {
        RUNTIME_POD_RETENTION_TEMPLATE_SHA_ANNOTATION: runtime_pod_retention_template_sha256(candidate)
    }
    with pytest.raises(ValueError, match="digest mirrors"):
        verify_runtime_pod_retention_template_identity(candidate)


def test_template_identity_changes_for_every_non_self_leaf_and_structure() -> None:
    report = render_runtime_pod_retention(
        AirflowRuntimePodRetentionRenderRequest(namespace="airflow-example", image=_IMAGE)
    )
    cron = next(item for item in report["manifests"] if item["kind"] == "CronJob")
    template = cron["spec"]["jobTemplate"]
    expected = runtime_pod_retention_template_sha256(template)
    leaf_paths, mapping_paths, sequence_paths = _template_paths(template)

    assert len(leaf_paths) >= 36
    for path in leaf_paths:
        candidate = deepcopy(template)
        parent, key = _parent_at(candidate, path)
        parent[key] = _mutated_scalar(parent[key])
        assert runtime_pod_retention_template_sha256(candidate) != expected, path

        candidate = deepcopy(template)
        parent, key = _parent_at(candidate, path)
        if isinstance(parent, dict):
            parent.pop(key)
        else:
            parent.pop(key)
        assert runtime_pod_retention_template_sha256(candidate) != expected, path

    for path in mapping_paths:
        candidate = deepcopy(template)
        mapping = _value_at(candidate, path)
        mapping["dpone-test-extra"] = "mutation"
        assert runtime_pod_retention_template_sha256(candidate) != expected, path

    for path in sequence_paths:
        candidate = deepcopy(template)
        sequence = _value_at(candidate, path)
        sequence.append("mutation")
        assert runtime_pod_retention_template_sha256(candidate) != expected, path


def _template_paths(
    value: object,
    path: tuple[object, ...] = (),
) -> tuple[list[tuple[object, ...]], list[tuple[object, ...]], list[tuple[object, ...]]]:
    leaf_paths: list[tuple[object, ...]] = []
    mapping_paths: list[tuple[object, ...]] = []
    sequence_paths: list[tuple[object, ...]] = []
    if isinstance(value, dict):
        mapping_paths.append(path)
        for key, item in value.items():
            if path and path[-1] == "annotations" and key == "dpone.dev/runtime-pod-retention-template-sha256":
                continue
            leaves, mappings, sequences = _template_paths(item, (*path, key))
            leaf_paths.extend(leaves)
            mapping_paths.extend(mappings)
            sequence_paths.extend(sequences)
    elif isinstance(value, list):
        sequence_paths.append(path)
        for index, item in enumerate(value):
            leaves, mappings, sequences = _template_paths(item, (*path, index))
            leaf_paths.extend(leaves)
            mapping_paths.extend(mappings)
            sequence_paths.extend(sequences)
    else:
        leaf_paths.append(path)
    return leaf_paths, mapping_paths, sequence_paths


def _value_at(value: Any, path: tuple[object, ...]) -> Any:
    current = value
    for key in path:
        current = current[key]
    return current


def _parent_at(value: Any, path: tuple[object, ...]) -> tuple[Any, object]:
    return _value_at(value, path[:-1]), path[-1]


def _mutated_scalar(value: object) -> object:
    if isinstance(value, bool):
        return not value
    if isinstance(value, int):
        return value + 1
    if isinstance(value, str):
        return value + "-mutation"
    return "mutation"


def test_apply_render_requires_explicit_authority_and_can_include_alerts() -> None:
    actor = "serviceaccount://airflow-example/dpone-runtime-pod-retention"
    with pytest.raises(Exception, match="confirm-delete"):
        render_runtime_pod_retention(
            AirflowRuntimePodRetentionRenderRequest(
                namespace="airflow-example",
                image=_IMAGE,
                mode="apply",
                actor=actor,
                allowed_actors=(actor,),
            )
        )

    report = render_runtime_pod_retention(
        AirflowRuntimePodRetentionRenderRequest(
            namespace="airflow-example",
            image=_IMAGE,
            mode="apply",
            actor=actor,
            allowed_actors=(actor,),
            confirm_delete=True,
            alerts="prometheus",
            stale_after_seconds=7200,
        )
    )

    resources = {item["kind"]: item for item in report["manifests"]}
    assert set(resources) == {"ServiceAccount", "Role", "RoleBinding", "CronJob", "PrometheusRule"}
    command = resources["CronJob"]["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]["command"]
    assert "runtime-pod-retention-apply" in command
    assert "--confirm-delete" in command
    assert resources["Role"]["rules"] == [{"apiGroups": [""], "resources": ["pods"], "verbs": ["list", "delete"]}]
    annotations = resources["CronJob"]["spec"]["jobTemplate"]["metadata"]["annotations"]
    assert annotations["dpone.dev/runtime-pod-retention-mode"] == "apply"
    rules = resources["PrometheusRule"]["spec"]["groups"][0]["rules"]
    assert {rule["alert"] for rule in rules} == {
        "DponeRuntimePodRetentionJobFailed",
        "DponeRuntimePodRetentionStale",
    }
    assert rules[0]["expr"] == (
        'kube_job_status_failed{namespace="airflow-example",job_name=~"dpone-runtime-pod-retention-.*"} > 0'
    )
    assert rules[1]["expr"] == (
        '(time() - kube_cronjob_status_last_successful_time{namespace="airflow-example",'
        'cronjob="dpone-runtime-pod-retention"} > 7200) or '
        '((time() - kube_cronjob_created{namespace="airflow-example",cronjob="dpone-runtime-pod-retention"} '
        "> 7200) unless on(namespace, cronjob) "
        'kube_cronjob_status_last_successful_time{namespace="airflow-example",'
        'cronjob="dpone-runtime-pod-retention"})'
    )


@pytest.mark.parametrize("image", ["dpone:latest", "dpone@sha256:abc", "dpone @sha256:" + "a" * 64])
def test_render_rejects_mutable_or_invalid_images(image: str) -> None:
    with pytest.raises(ValueError, match="immutable"):
        render_runtime_pod_retention(AirflowRuntimePodRetentionRenderRequest(namespace="airflow", image=image))


def test_plan_render_validates_apply_batch_limit_even_without_delete_authority() -> None:
    with pytest.raises(Exception, match="max_delete_count"):
        render_runtime_pod_retention(
            AirflowRuntimePodRetentionRenderRequest(
                namespace="airflow-example",
                image=_IMAGE,
                max_delete_count=0,
            )
        )


def test_plan_render_rejects_apply_only_options_instead_of_ignoring_them() -> None:
    with pytest.raises(ValueError, match="apply-only"):
        render_runtime_pod_retention(
            AirflowRuntimePodRetentionRenderRequest(
                namespace="airflow-example",
                image=_IMAGE,
                confirm_delete=True,
            )
        )


def test_prometheus_alerts_require_an_explicit_schedule_appropriate_threshold() -> None:
    with pytest.raises(ValueError, match="stale_after_seconds"):
        render_runtime_pod_retention(
            AirflowRuntimePodRetentionRenderRequest(
                namespace="airflow-example",
                image=_IMAGE,
                schedule="@daily",
                alerts="prometheus",
            )
        )

    report = render_runtime_pod_retention(
        AirflowRuntimePodRetentionRenderRequest(
            namespace="airflow-example",
            image=_IMAGE,
            schedule="@daily",
            alerts="prometheus",
            stale_after_seconds=172_800,
        )
    )
    rule = next(item for item in report["manifests"] if item["kind"] == "PrometheusRule")
    assert "> 172800" in rule["spec"]["groups"][0]["rules"][1]["expr"]


def test_render_schema_requires_threshold_exactly_when_prometheus_alerts_are_enabled() -> None:
    report = render_runtime_pod_retention(
        AirflowRuntimePodRetentionRenderRequest(
            namespace="airflow-example",
            image=_IMAGE,
            alerts="prometheus",
            stale_after_seconds=7200,
        )
    )
    validator = GitOpsSchemaValidator()

    missing = deepcopy(report)
    missing.pop("stale_after_seconds")
    assert {
        issue.path
        for issue in validator.validate(
            missing,
            expected_kind="dpone.airflow-runtime-pod-retention-render.v1",
        )
    } == {"stale_after_seconds"}

    forbidden = render_runtime_pod_retention(
        AirflowRuntimePodRetentionRenderRequest(namespace="airflow-example", image=_IMAGE)
    )
    forbidden["stale_after_seconds"] = 7200
    assert validator.validate(
        forbidden,
        expected_kind="dpone.airflow-runtime-pod-retention-render.v1",
    )


@pytest.mark.parametrize(
    "schedule",
    (
        "0 3 * * *",
        "? * * * *",
        "0 0 ? * MON",
        "0 0 * ? *",
        "?/15 1-5 1,15 JAN,MAR MON-FRI",
        "@daily",
    ),
)
def test_render_accepts_kubernetes_cron_contract(schedule: str) -> None:
    report = render_runtime_pod_retention(
        AirflowRuntimePodRetentionRenderRequest(namespace="airflow-example", image=_IMAGE, schedule=schedule)
    )

    assert report["schedule"] == schedule


@pytest.mark.parametrize("schedule", ("not-a-cron", "0 0 * * 7", "0 0 * * MON-SUN", "*/0 * * * *"))
def test_render_rejects_invalid_kubernetes_cron_contract(schedule: str) -> None:
    with pytest.raises(ValueError, match="schedule"):
        render_runtime_pod_retention(
            AirflowRuntimePodRetentionRenderRequest(namespace="airflow-example", image=_IMAGE, schedule=schedule)
        )


def test_cli_render_does_not_construct_app_context_and_emits_yaml(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fail_context(**_: object) -> object:
        raise AssertionError("AppContext must not be constructed")

    monkeypatch.setattr("dpone.cli.main.AppContext.from_env", fail_context)
    with pytest.raises(SystemExit) as exit_info:
        cli_main.main(
            [
                "airflow",
                "runtime-pod-retention-render",
                "--namespace",
                "airflow-example",
                "--image",
                _IMAGE,
            ]
        )

    captured = capsys.readouterr()
    assert exit_info.value.code == 0
    assert captured.err == ""
    assert "kind: CronJob" in captured.out
    assert "runtime-pod-retention-plan" in captured.out


def test_cli_render_invalid_input_recommends_corrected_render_command(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exit_info:
        cli_main.main(
            [
                "airflow",
                "runtime-pod-retention-render",
                "--namespace",
                "airflow-example",
                "--image",
                _IMAGE,
                "--schedule",
                "* * * * * *",
                "--format",
                "json",
            ]
        )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_info.value.code == 2
    assert captured.err == ""
    assert payload["errors"][0]["fixes"] == [
        {
            "id": "review_runtime_pod_retention_render_help",
            "safety": "manual",
            "command": "dpone airflow runtime-pod-retention-render --help",
        }
    ]


def test_cli_render_invalid_yaml_input_preserves_machine_readable_yaml(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exit_info:
        cli_main.main(
            [
                "airflow",
                "runtime-pod-retention-render",
                "--namespace",
                "airflow-example",
                "--image",
                _IMAGE,
                "--schedule",
                "* * * * * *",
                "--format",
                "yaml",
            ]
        )

    captured = capsys.readouterr()
    payload = yaml.safe_load(captured.out)
    assert exit_info.value.code == 2
    assert captured.err == ""
    assert payload["errors"][0]["schema"] == "dpone.error.v1"
    assert payload["errors"][0]["code"] == "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_INPUT_INVALID"


@pytest.mark.parametrize(
    ("command", "contract_fragment"),
    (
        ("runtime-pod-retention-plan", "read-only and never deletes Pods"),
        ("runtime-pod-retention-apply", "Mutation is bounded and UID/resourceVersion-conditional"),
        ("runtime-pod-retention-render", "performs no Kubernetes API calls and writes no files"),
    ),
)
def test_cli_help_exposes_io_side_effect_and_exit_contract(
    command: str,
    contract_fragment: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exit_info:
        cli_main.main(["airflow", command, "--help"])

    captured = capsys.readouterr()
    assert exit_info.value.code == 0
    assert captured.err == ""
    assert "I/O, side effects, and exit contract:" in captured.out
    assert "Exit " in captured.out or "exit " in captured.out
    assert contract_fragment in captured.out


def test_apply_cli_requires_explicit_auth_before_runtime_composition(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fail_context(**_: object) -> object:
        raise AssertionError("AppContext must not be constructed")

    monkeypatch.setattr("dpone.cli.main.AppContext.from_env", fail_context)
    with pytest.raises(SystemExit) as exit_info:
        cli_main.main(
            [
                "airflow",
                "runtime-pod-retention-apply",
                "--namespace",
                "airflow-example",
                "--actor",
                "user://reviewer",
                "--allowed-actor",
                "user://reviewer",
                "--confirm-delete",
            ]
        )

    captured = capsys.readouterr()
    assert exit_info.value.code == 2
    assert "--kube-auth" in captured.err
    assert captured.out == ""


def test_metadata_client_restarts_one_expired_phase_from_the_beginning() -> None:
    class Expired(RuntimeError):
        status = 410

    class Transport:
        def __init__(self) -> None:
            self.calls = 0

        def list_metadata_page(self, **_: object) -> dict[str, object]:
            self.calls += 1
            if self.calls == 1:
                raise Expired("expired token with secret body")
            return {"kind": "PartialObjectMetadataList", "metadata": {"continue": ""}, "items": []}

        def delete_pod(self, **_: object) -> None:
            return None

    transport = Transport()
    adapter = KubernetesAirflowRuntimePodRetentionAdapter(
        transport=transport,
        credential_mode="kubeconfig",
        credential_context="reviewed-dev",
        label_selector="dpone.dev/managed-by=airflow-provider",
    )

    assert adapter.list_terminal_pod_metadata(namespace="airflow-example", page_size=500) == ()
    assert transport.calls == 3


def test_kubernetes_transport_deletes_pod_with_observed_preconditions(monkeypatch: pytest.MonkeyPatch) -> None:
    class Preconditions:
        def __init__(self, *, uid: str, resource_version: str) -> None:
            self.uid = uid
            self.resource_version = resource_version

    class DeleteOptions:
        def __init__(self, *, preconditions: Preconditions) -> None:
            self.preconditions = preconditions

    client_module = ModuleType("kubernetes.client")
    client_module.V1DeleteOptions = DeleteOptions
    client_module.V1Preconditions = Preconditions
    kubernetes_module = ModuleType("kubernetes")
    kubernetes_module.client = client_module
    monkeypatch.setitem(__import__("sys").modules, "kubernetes", kubernetes_module)
    monkeypatch.setitem(__import__("sys").modules, "kubernetes.client", client_module)

    class CoreApi:
        body: DeleteOptions | None = None

        def delete_namespaced_pod(self, *, name: str, namespace: str, body: object, **kwargs: object) -> object:
            assert name == "runtime-pod"
            assert namespace == "airflow-example"
            assert isinstance(body, DeleteOptions)
            assert kwargs["_request_timeout"] == (5, 30)
            self.body = body
            return object()

    core = CoreApi()
    transport = metadata_module.KubernetesApiMetadataTransport(api_client=object(), core_api=core)  # type: ignore[arg-type]
    transport.delete_pod(namespace="airflow-example", name="runtime-pod", uid="uid-1", resource_version="rv-2")

    assert core.body is not None
    assert core.body.preconditions.uid == "uid-1"
    assert core.body.preconditions.resource_version == "rv-2"


def test_failure_reports_never_echo_vendor_error_details() -> None:
    error = KubernetesMetadataApiError(operation="list", status=403, reason="access_denied")
    assert "token" not in str(error)
    assert json.dumps(error.__dict__) == '{"operation": "list", "status": 403, "reason": "access_denied"}'


def test_generated_schema_files_exist() -> None:
    root = Path(__file__).parents[1] / "docs" / "schemas" / "gitops"
    for name in (
        "airflow-runtime-pod-retention-plan",
        "airflow-runtime-pod-retention-apply",
        "airflow-runtime-pod-retention-event",
        "airflow-runtime-pod-retention-render",
    ):
        assert (root / f"{name}.schema.json").is_file()
