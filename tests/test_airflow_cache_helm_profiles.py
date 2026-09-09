from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]

DEPLOYMENT_LEAVES = (
    "airflow-cache-kubernetes-prerequisites.md",
    "airflow-cache-kubernetes-runtime-wrappers.md",
    "airflow-cache-kubernetes-wrapper-delivery.md",
    "airflow-cache-kubernetes-chart-operations.md",
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _deployment_runbook() -> str:
    return "\n".join((ROOT / "docs" / name).read_text(encoding="utf-8") for name in DEPLOYMENT_LEAVES)


def _bash_block_after(content: str, marker: str) -> str:
    tail = content.split(marker, 1)[1]
    return tail.split("```bash", 1)[1].split("```", 1)[0].strip()


def _run_bash(block: str, *, cwd: Path, prelude: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "-c", f"{prelude}\n{block}"],
        cwd=cwd,
        env={**os.environ, "CALLS_FILE": str(cwd / "calls.txt")},
        check=False,
        text=True,
        capture_output=True,
    )


@pytest.mark.parametrize(
    ("profile_name", "component_name"),
    [
        ("airflow-cache-values-2.10.yaml", "scheduler"),
        ("airflow-cache-values-3.2.yaml", "dagProcessor"),
    ],
)
def test_cache_profile_keeps_long_running_processes_out_of_init_containers(
    profile_name: str,
    component_name: str,
) -> None:
    profile = yaml.safe_load((ROOT / "docs" / "examples" / profile_name).read_text(encoding="utf-8"))
    component = profile[component_name]
    init_containers = component["extraInitContainers"]
    sidecars = component["extraContainers"]

    assert [container["name"] for container in init_containers] == ["dpone-cache-init"]
    assert {container["name"] for container in sidecars} == {
        "dpone-cache-status-projector",
        "dpone-cache-watch",
    }
    assert all("while :" not in " ".join(container.get("args", ())) for container in init_containers)


@pytest.mark.parametrize(
    ("profile_name", "component_name"),
    [
        ("airflow-cache-values-2.10.yaml", "scheduler"),
        ("airflow-cache-values-3.2.yaml", "dagProcessor"),
    ],
)
def test_cache_profile_separates_parser_cache_and_loader_ack(
    profile_name: str,
    component_name: str,
) -> None:
    profile = yaml.safe_load((ROOT / "docs" / "examples" / profile_name).read_text(encoding="utf-8"))
    mounts = {mount["mountPath"]: mount for mount in profile[component_name]["extraVolumeMounts"]}

    assert mounts["/opt/airflow/.dpone-cache"]["readOnly"] is True
    assert mounts["/opt/airflow/.dpone-cache"]["name"] == "dpone-airflow-cache"
    assert "/opt/airflow/.dpone-ack" not in mounts
    sidecars = {item["name"]: item for item in profile[component_name]["extraContainers"]}
    for name in ("dpone-cache-status-projector", "dpone-cache-watch"):
        ack_mounts = [mount for mount in sidecars[name]["volumeMounts"] if mount["name"] == "dpone-airflow-loader-ack"]
        assert ack_mounts == [
            {
                "name": "dpone-airflow-loader-ack",
                "mountPath": "/opt/airflow/.dpone-ack",
                "readOnly": True,
            }
        ]


@pytest.mark.parametrize(
    ("profile_name", "component_name"),
    [
        ("airflow-cache-values-2.10.yaml", "scheduler"),
        ("airflow-cache-values-3.2.yaml", "dagProcessor"),
    ],
)
def test_authority_configmap_uses_projected_directory_not_subpath(
    profile_name: str,
    component_name: str,
) -> None:
    profile = yaml.safe_load((ROOT / "docs" / "examples" / profile_name).read_text(encoding="utf-8"))
    component = profile[component_name]

    for container in (*component["extraInitContainers"], *component["extraContainers"]):
        authority_mounts = [
            mount for mount in container.get("volumeMounts", ()) if mount["name"] == "dpone-desired-state-authority"
        ]
        if not authority_mounts:
            continue
        assert authority_mounts == [
            {
                "name": "dpone-desired-state-authority",
                "mountPath": "/etc/dpone/airflow-authority",
                "readOnly": True,
            }
        ]
        authority_env = next(
            item["value"] for item in container["env"] if item["name"] == "DPONE_AIRFLOW_DESIRED_STATE_AUTHORITY_FILE"
        )
        assert authority_env == "/etc/dpone/airflow-authority/airflow-desired-state-authority.json"


def test_helm_ack_runbook_covers_repository_deploy_observe_and_rollback() -> None:
    runbook = _deployment_runbook()

    for expected in (
        'test "$(sha256sum "${DPONE_AIRFLOW_CHART_TGZ}"',
        'helm upgrade --install "${AIRFLOW_RELEASE}" "${DPONE_AIRFLOW_CHART_TGZ}"',
        '--post-renderer "${POST_RENDERER}"',
        'helm get manifest "${AIRFLOW_RELEASE}"',
        '"${POST_RENDERER}" --verify-only',
        'helm rollback "${AIRFLOW_RELEASE}" "${PREVIOUS_REVISION}"',
        'rollout restart "${PARSE_AUTHORITY_WORKLOAD}"',
        'rollout status "${PARSE_AUTHORITY_WORKLOAD}"',
    ):
        assert expected in runbook
    assert ': "${KUBE_CONTEXT:?set the reviewed kubeconfig context}"' in runbook
    for line in runbook.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("kubectl "):
            assert '--context "${KUBE_CONTEXT}"' in stripped
        if stripped.startswith(
            ("helm upgrade ", "helm status ", "helm get manifest ", "helm history ", "helm rollback ")
        ):
            assert '--kube-context "${KUBE_CONTEXT}"' in runbook[runbook.index(line) : runbook.index(line) + 300]


def test_desired_state_authority_lifecycle_requires_namespace_review_and_preserves_previous_bytes() -> None:
    guide = (ROOT / "docs" / "airflow-desired-state.md").read_text(encoding="utf-8")

    assert ': "${AIRFLOW_NAMESPACE:?set the namespace used by the reviewed Airflow Helm release}"' in guide
    assert "AIRFLOW_NAMESPACE:=airflow-example" not in guide
    assert 'get namespace "${AIRFLOW_NAMESPACE}"' in guide
    assert "DPONE_AIRFLOW_AUTHORITY_APPLY_ACK" in guide
    assert 'diff -f "${rendered}"' in guide
    assert "apply --server-side --field-manager=dpone-airflow-authority" in guide
    assert "--force-conflicts" in guide
    assert "previous.json" in guide
    assert 'cmp -- "${DPONE_AIRFLOW_AUTHORITY_JSON}" "${observed}"' in guide
    assert "does not project later ConfigMap updates through a" in guide
    assert "`subPath` mount" in guide


def test_candidate_airflow_pack_journey_is_version_aware_and_uses_owned_temporary_directory() -> None:
    readme = (ROOT / "packages" / "dpone-airflow-pack" / "README.md").read_text(encoding="utf-8")
    candidate = readme.split("For a candidate checkout", 1)[1].split("`dist-airflow` is", 1)[0]
    block = _bash_block_after(readme, "For a candidate checkout")

    assert ': "${AIRFLOW_VERSION:?set the certified Airflow version}"' in candidate
    assert "2.10.5)" in candidate
    assert "3.2.0)" in candidate
    assert "airflow-cache-values-2.10.yaml" in candidate
    assert "airflow-cache-values-3.2.yaml" in candidate
    assert 'DIST_AIRFLOW="$(mktemp -d dpone-airflow-candidate.XXXXXX)"' in candidate
    assert "rm -rf dist-airflow" not in candidate
    assert 'cp "docs/examples/${PROFILE}" airflow-cache-values.yaml' not in candidate
    assert 'HELM_EVIDENCE="$(mktemp -d "${TMPDIR:-/tmp}/dpone-airflow-helm-evidence.XXXXXX")"' in candidate
    assert "airflow-helm-ack-mounts-${DPONE_CANDIDATE_SHA}" in candidate
    assert 'test "$(cat "${HELM_EVIDENCE}/SOURCE_COMMIT")" = "${DPONE_REVIEWED_CANDIDATE_SHA}"' in candidate
    assert 'sha256sum -c "${HELM_EVIDENCE}/CHART_SHA256SUMS"' in candidate
    assert "raw.githubusercontent.com" not in candidate
    assert 'values="${DIST_AIRFLOW}/${PROFILE}"' in candidate
    assert "checksum_covers" in candidate
    assert "airflow-cache-values-2.10.yaml airflow-cache-values-3.2.yaml" in candidate
    assert "DPONE_REVIEWED_CANDIDATE_SHA" in candidate
    assert 'DPONE_CANDIDATE_SHA="$(git rev-parse HEAD)"' not in candidate
    assert 'fetch --quiet --depth 1 origin "${DPONE_REVIEWED_CANDIDATE_SHA}"' in candidate
    assert 'checkout --quiet --detach "${DPONE_REVIEWED_CANDIDATE_SHA}"' in candidate
    assert 'core_wheels=("${DIST_AIRFLOW}"/dpone-*.whl)' in candidate
    assert '"${core_wheels[0]}[kubernetes]"' in candidate
    assert '"${provider_wheels[0]}"' in candidate
    assert block.index("sha256sum --check SHA256SUMS") < block.index("python3 -m pip install")
    assert block.index("checksum_covers") < block.index("python3 -m pip install")
    syntax = subprocess.run(["bash", "-n"], input=block, text=True, capture_output=True, check=False)
    assert syntax.returncode == 0, syntax.stderr


def test_cache_wrapper_configmap_has_reviewed_lifecycle_and_explicit_namespace() -> None:
    runbook = _deployment_runbook()
    approval = (ROOT / "docs" / "airflow-cache-retention-approval.md").read_text(encoding="utf-8")

    assert "DPONE_CACHE_SCRIPTS_APPLY_ACK" in runbook
    assert "apply --dry-run=server" in runbook
    assert "--field-manager=dpone-airflow-cache-scripts" in runbook
    assert "length == 1 and" in runbook
    assert '.[0].metadata.name == "dpone-airflow-cache-scripts"' in runbook
    assert '["cache-init-fail-open.sh", "cache-watch.sh"]' in runbook
    assert "previous.absent" in runbook
    assert "DPONE_CACHE_SCRIPTS_ROLLBACK_ACK" in runbook
    assert "restore_previous_configmap" in runbook
    assert '"${DPONE_CACHE_SCRIPTS_EVIDENCE_DIR}/observed.json"' in runbook
    assert ".metadata.uid == $activated[0].metadata.uid" in runbook
    assert ".metadata.resourceVersion == $activated[0].metadata.resourceVersion" in runbook
    assert "replace --dry-run=server" in runbook
    assert "delete_namespaced_config_map" in runbook
    assert "V1Preconditions(uid=uid, resource_version=resource_version)" in runbook
    assert "dpone[kubernetes]" in runbook
    assert 'cmp -- "${candidate_data}" "${observed_data}"' in runbook
    assert 'rollout restart "${DPONE_CACHE_PARSER_WORKLOAD}"' in runbook
    assert 'rollout status "${DPONE_CACHE_PARSER_WORKLOAD}"' in runbook
    assert "expected-script-sha256.txt" in runbook
    assert "DPONE_CACHE_RETENTION_APPROVED_PLAN_SHA256" in runbook
    assert "reason=approval_missing" in runbook
    assert "reason=approval_mismatch" in runbook
    assert "DPONE_APPROVAL_WITHDRAWAL_REQUEST_TIMEOUT_SECONDS" in approval
    assert '--request-timeout="${DPONE_APPROVAL_WITHDRAWAL_REQUEST_TIMEOUT_SECONDS}s"' in approval
    assert "sleep_seconds=${DPONE_APPROVAL_WITHDRAWAL_POLL_SECONDS}" in approval
    assert runbook.count('approved_sha256="${DPONE_CACHE_RETENTION_APPROVED_PLAN_SHA256:-}"') == 2
    assert '--expected-plan-sha256 "${approved_sha256}"' in runbook
    assert '--expected-plan-sha256 "${plan_sha256}"' not in runbook
    assert "--namespace airflow" not in runbook
    assert "-n airflow" not in runbook


def test_cache_wrapper_configmap_rejects_multi_resource_manifest_before_server_apply(tmp_path: Path) -> None:
    runbook = _deployment_runbook()
    block = _bash_block_after(runbook, "reproducible lifecycle for first install, update and rollback:")
    manifest = tmp_path / "scripts.yaml"
    manifest.write_text("kind: ConfigMap\n", encoding="utf-8")
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude=f"""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
DPONE_CACHE_SCRIPTS_MANIFEST={manifest}
DPONE_CACHE_SCRIPTS_APPLY_ACK=review-and-apply-cache-scripts
DPONE_CACHE_PARSER_WORKLOAD=deployment/airflow-dag-processor
DPONE_CACHE_WATCH_CONTAINER=dpone-cache-watch
kubectl() {{
  printf '%s\\n' "$*" >>"${{CALLS_FILE}}"
  case "$*" in
    *" --dry-run=client "*)
      printf '{{"apiVersion":"v1","kind":"ConfigMap","metadata":{{"name":"dpone-airflow-cache-scripts"}},"data":{{"cache-init-fail-open.sh":"x","cache-watch.sh":"y"}}}}\\n'
      printf '{{"apiVersion":"v1","kind":"Secret","metadata":{{"name":"unexpected"}}}}\\n'
      ;;
    *) return 91 ;;
  esac
}}
""",
    )

    assert result.returncode != 0
    calls = (tmp_path / "calls.txt").read_text(encoding="utf-8").splitlines()
    assert len(calls) == 1
    assert "--dry-run=client" in calls[0]
    assert "--dry-run=server" not in calls[0]


def test_configmap_update_rollback_rejects_replacement_before_mutation(tmp_path: Path) -> None:
    runbook = _deployment_runbook()
    block = _bash_block_after(runbook, "When the chart or post-renderer deployment itself must be rolled back")
    evidence = tmp_path / "activation-evidence"
    evidence.mkdir()
    (evidence / "previous.json").write_text(
        '{"metadata":{"uid":"original","resourceVersion":"10"},"data":{"cache-init-fail-open.sh":"old","cache-watch.sh":"old"}}\n',
        encoding="utf-8",
    )
    (evidence / "previous-data.json").write_text(
        '{"cache-init-fail-open.sh":"old","cache-watch.sh":"old"}\n', encoding="utf-8"
    )
    (evidence / "candidate-data.json").write_text(
        '{"cache-init-fail-open.sh":"new","cache-watch.sh":"new"}\n', encoding="utf-8"
    )
    (evidence / "observed.json").write_text(
        '{"metadata":{"uid":"activated","resourceVersion":"20"},"data":{"cache-init-fail-open.sh":"new","cache-watch.sh":"new"}}\n',
        encoding="utf-8",
    )
    subprocess.run(
        ["bash", "-c", "sha256sum previous.json previous-data.json candidate-data.json observed.json >SHA256SUMS"],
        cwd=evidence,
        check=True,
    )
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude=f"""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
AIRFLOW_RELEASE=airflow
PREVIOUS_REVISION=41
DPONE_CACHE_SCRIPTS_EVIDENCE_DIR={evidence}
DPONE_CACHE_SCRIPTS_ROLLBACK_ACK=restore-cache-scripts-and-helm
DPONE_CACHE_PARSER_WORKLOAD=deployment/airflow-dag-processor
DPONE_CACHE_WATCH_CONTAINER=dpone-cache-watch
dpone-airflow-pack-helm-post-renderer() {{ return 0; }}
kubectl() {{
  printf '%s\\n' "$*" >>"${{CALLS_FILE}}"
  case "$*" in
    *" get configmap dpone-airflow-cache-scripts -o json")
      printf '%s\\n' '{{"metadata":{{"uid":"replacement","resourceVersion":"30"}},"data":{{"cache-init-fail-open.sh":"old","cache-watch.sh":"old"}}}}'
      ;;
    *) return 0 ;;
  esac
}}
helm() {{ printf 'helm %s\\n' "$*" >>"${{CALLS_FILE}}"; return 9; }}
""",
    )

    assert result.returncode != 0
    calls = (tmp_path / "calls.txt").read_text(encoding="utf-8").splitlines()
    assert not any(" apply --server-side " in f" {call} " or " replace -f " in f" {call} " for call in calls)


def test_cache_deployment_preflight_stops_on_first_kubectl_failure(tmp_path: Path) -> None:
    runbook = _deployment_runbook()
    block = _bash_block_after(runbook, "Before deployment, reject")
    values = tmp_path / "reviewed-values.yaml"
    values.write_text(
        f"images:\n  airflow:\n    repository: registry.internal/airflow\n    digest: sha256:{'a' * 64}\n",
        encoding="utf-8",
    )
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude=f"""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
PLATFORM_VALUES={values}
DPONE_CACHE_STATUS_PROJECTOR_ENABLED=false
kubectl() {{
  printf '%s\\n' "$*" >>"${{CALLS_FILE}}"
  return 9
}}
""",
    )

    assert result.returncode == 9
    assert len((tmp_path / "calls.txt").read_text(encoding="utf-8").splitlines()) == 1


@pytest.mark.parametrize("profile_name", ["airflow-cache-values-2.10.yaml", "airflow-cache-values-3.2.yaml"])
def test_cache_deployment_preflight_rejects_every_shipped_placeholder_profile(
    tmp_path: Path,
    profile_name: str,
) -> None:
    runbook = _deployment_runbook()
    block = _bash_block_after(runbook, "Before deployment, reject")
    profile = ROOT / "docs" / "examples" / profile_name
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude=f"""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
PLATFORM_VALUES={profile}
DPONE_CACHE_STATUS_PROJECTOR_ENABLED=true
kubectl() {{ printf '%s\\n' "$*" >>"${{CALLS_FILE}}"; }}
""",
    )

    assert result.returncode == 1
    assert "still contain placeholders" in result.stderr
    assert not (tmp_path / "calls.txt").exists()


def test_cache_deployment_preflight_rejects_mutable_airflow_tag_without_digest(tmp_path: Path) -> None:
    runbook = _deployment_runbook()
    block = _bash_block_after(runbook, "Before deployment, reject")
    values = tmp_path / "mutable-values.yaml"
    values.write_text(
        "images:\n  airflow:\n    repository: registry.internal/airflow\n    tag: mutable-latest\n",
        encoding="utf-8",
    )
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude=f"""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
PLATFORM_VALUES={values}
DPONE_CACHE_STATUS_PROJECTOR_ENABLED=false
kubectl() {{ printf '%s\\n' "$*" >>"${{CALLS_FILE}}"; }}
""",
    )

    assert result.returncode != 0
    assert "images.airflow.digest" in result.stderr
    assert not (tmp_path / "calls.txt").exists()


def test_cache_deployment_preflight_skips_only_explicitly_disabled_projector(tmp_path: Path) -> None:
    runbook = _deployment_runbook()
    block = _bash_block_after(runbook, "Before deployment, reject")
    values = tmp_path / "reviewed-values.yaml"
    values.write_text(
        f"images:\n  airflow:\n    repository: registry.internal/airflow\n    digest: sha256:{'a' * 64}\n",
        encoding="utf-8",
    )
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude=f"""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
PLATFORM_VALUES={values}
DPONE_CACHE_STATUS_PROJECTOR_ENABLED=false
kubectl() {{ printf '%s\\n' "$*" >>"${{CALLS_FILE}}"; }}
""",
    )

    assert result.returncode == 0, result.stderr
    calls = (tmp_path / "calls.txt").read_text(encoding="utf-8").splitlines()
    assert len(calls) == 3
    assert not any("dpone-airflow-status-projector" in call for call in calls)


def test_cache_helm_deploy_failure_cannot_report_success(tmp_path: Path) -> None:
    runbook = _deployment_runbook()
    block = _bash_block_after(runbook, "Deploy the same reviewed inputs")
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude="""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
PLATFORM_VALUES=reviewed-values.yaml
AIRFLOW_VERSION=3.2.0
AIRFLOW_RELEASE=airflow
DPONE_CACHE_STATUS_PROJECTOR_ENABLED=false
DPONE_AIRFLOW_CHART_TGZ="$PWD/airflow-1.22.0.tgz"
printf 'reviewed chart bytes\n' >"${DPONE_AIRFLOW_CHART_TGZ}"
DPONE_AIRFLOW_CHART_SHA256="$(sha256sum "${DPONE_AIRFLOW_CHART_TGZ}" | awk '{print $1}')"
DPONE_HELM_DEPLOY_EVIDENCE_DIR="$PWD/deployment-evidence"
dpone-airflow-pack-helm-post-renderer() { return 0; }
helm() {
  printf '%s\\n' "$*" >>"${CALLS_FILE}"
  case "$1" in
    template)
      cat <<'YAML'
kind: Deployment
metadata:
  name: airflow-dag-processor
spec:
  template:
    spec:
      initContainers:
        - name: dpone-cache-init
          image: registry.internal/dpone@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
      containers:
        - name: dpone-cache-watch
          image: registry.internal/dpone@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
YAML
      ;;
    upgrade) return 9 ;;
  esac
  return 0
}
""",
    )

    assert result.returncode == 9
    calls = (tmp_path / "calls.txt").read_text(encoding="utf-8").splitlines()
    assert len(calls) == 2
    assert calls[0].startswith(f"template airflow {tmp_path}/airflow-1.22.0.tgz")
    assert calls[1].startswith(f"upgrade --install airflow {tmp_path}/airflow-1.22.0.tgz")
    assert "--kube-context reviewed-dev" in calls[1]


def test_cache_helm_deploy_uses_exact_chart_and_seals_evidence(tmp_path: Path) -> None:
    runbook = _deployment_runbook()
    block = _bash_block_after(runbook, "Deploy the same reviewed inputs")
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude="""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
PLATFORM_VALUES=reviewed-values.yaml
AIRFLOW_VERSION=3.2.0
AIRFLOW_RELEASE=airflow
DPONE_CACHE_STATUS_PROJECTOR_ENABLED=false
DPONE_AIRFLOW_CHART_TGZ="$PWD/airflow-1.22.0.tgz"
printf 'reviewed chart bytes\n' >"${DPONE_AIRFLOW_CHART_TGZ}"
DPONE_AIRFLOW_CHART_SHA256="$(sha256sum "${DPONE_AIRFLOW_CHART_TGZ}" | awk '{print $1}')"
DPONE_HELM_DEPLOY_EVIDENCE_DIR="$PWD/deployment-evidence"
dpone-airflow-pack-helm-post-renderer() { cat; }
rendered_manifest() {
  cat <<'YAML'
kind: Deployment
metadata:
  name: airflow-dag-processor
spec:
  template:
    spec:
      initContainers:
        - name: dpone-cache-init
          image: registry.internal/dpone@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
      containers:
        - name: dpone-cache-watch
          image: registry.internal/dpone@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
YAML
}
helm() {
  printf '%s\n' "$*" >>"${CALLS_FILE}"
  case "$1 $2" in
    "template airflow") rendered_manifest ;;
    "get manifest") rendered_manifest ;;
    "status airflow") printf '%s\n' '{"version":7}' ;;
  esac
}
kubectl() {
  printf '%s\n' "$*" >>"${CALLS_FILE}"
  printf '%s\n' '{"metadata":{"uid":"namespace-uid"}}'
}
""",
    )

    assert result.returncode == 0, result.stderr
    evidence = json.loads((tmp_path / "deployment-evidence" / "deployment.json").read_text(encoding="utf-8"))
    assert evidence["kube_context"] == "reviewed-dev"
    assert evidence["namespace"] == "airflow-example"
    assert evidence["namespace_uid"] == "namespace-uid"
    assert evidence["release_revision"] == 7
    assert evidence["airflow_version"] == "3.2.0"
    assert evidence["chart_version"] == "1.22.0"
    assert evidence["chart_sha256"] == "sha256:" + _sha256(tmp_path / "airflow-1.22.0.tgz")
    assert evidence["rendered_manifest_sha256"] == evidence["installed_manifest_sha256"]
    assert (tmp_path / "deployment-evidence" / "SHA256SUMS").is_file()


def test_cache_helm_deploy_rejects_uncertified_airflow_version_before_helm(tmp_path: Path) -> None:
    runbook = _deployment_runbook()
    block = _bash_block_after(runbook, "Deploy the same reviewed inputs")
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude="""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
PLATFORM_VALUES=reviewed-values.yaml
AIRFLOW_VERSION=3.3.0
AIRFLOW_RELEASE=airflow
DPONE_CACHE_STATUS_PROJECTOR_ENABLED=false
DPONE_AIRFLOW_CHART_TGZ="$PWD/airflow-unknown.tgz"
DPONE_AIRFLOW_CHART_SHA256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
DPONE_HELM_DEPLOY_EVIDENCE_DIR="$PWD/deployment-evidence"
dpone-airflow-pack-helm-post-renderer() { return 0; }
helm() { printf '%s\n' "$*" >>"${CALLS_FILE}"; }
""",
    )

    assert result.returncode == 2
    assert "AIRFLOW_VERSION is not certified" in result.stderr
    assert not (tmp_path / "calls.txt").exists()


def test_cache_helm_deploy_requires_context_in_its_own_block(tmp_path: Path) -> None:
    runbook = _deployment_runbook()
    block = _bash_block_after(runbook, "Deploy the same reviewed inputs")
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude="""
unset KUBE_CONTEXT
AIRFLOW_NAMESPACE=airflow-example
PLATFORM_VALUES=reviewed-values.yaml
AIRFLOW_VERSION=3.2.0
dpone-airflow-pack-helm-post-renderer() { return 0; }
helm() { printf '%s\\n' "$*" >>"${CALLS_FILE}"; }
""",
    )

    assert result.returncode != 0
    assert not (tmp_path / "calls.txt").exists()


def test_cache_helm_rollback_failure_stops_before_manifest_check(tmp_path: Path) -> None:
    runbook = _deployment_runbook()
    block = _bash_block_after(runbook, "When the chart or post-renderer deployment itself must be rolled back")
    evidence = tmp_path / "activation-evidence"
    evidence.mkdir()
    (evidence / "previous.absent").write_text("first_install=true\n", encoding="utf-8")
    (evidence / "candidate-data.json").write_text("{}\n", encoding="utf-8")
    subprocess.run(
        ["bash", "-c", "sha256sum previous.absent candidate-data.json >SHA256SUMS"],
        cwd=evidence,
        check=True,
    )
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude=f"""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
AIRFLOW_RELEASE=airflow
PREVIOUS_REVISION=41
DPONE_CACHE_SCRIPTS_EVIDENCE_DIR={evidence}
DPONE_CACHE_SCRIPTS_ROLLBACK_ACK=restore-cache-scripts-and-helm
DPONE_CACHE_PARSER_WORKLOAD=deployment/airflow-dag-processor
DPONE_CACHE_WATCH_CONTAINER=dpone-cache-watch
dpone-airflow-pack-helm-post-renderer() {{ return 0; }}
helm() {{
  printf '%s\\n' "$*" >>"${{CALLS_FILE}}"
  case "$1" in rollback) return 9 ;; esac
  return 0
}}
""",
    )

    assert result.returncode == 9
    calls = (tmp_path / "calls.txt").read_text(encoding="utf-8").splitlines()
    assert len(calls) == 2
    assert calls[0].startswith("history airflow --kube-context reviewed-dev")
    assert calls[1].startswith("rollback airflow 41 --kube-context reviewed-dev")


def test_published_pack_readme_uses_self_contained_links_and_values() -> None:
    readme = (ROOT / "packages" / "dpone-airflow-pack" / "README.md").read_text(encoding="utf-8")
    normalized_readme = " ".join(readme.split())

    assert "](../../docs/" not in readme
    assert "raw.githubusercontent.com/PaulKov/dpone/v0.73.32" not in readme
    assert "DPONE_VERSION=X.Y.Z" in readme
    assert "python3 -m pip download --no-deps --only-binary=:all:" in readme
    assert 'gh attestation verify "${release_wheel}"' in readme
    assert '--source-digest "${DPONE_RELEASE_SOURCE_COMMIT}"' in readme
    assert 'python3 -m pip install "${release_wheel}"' in readme
    assert "${DPONE_RELEASE_SOURCE_COMMIT}/docs/examples/${PROFILE}" in readme
    assert "install the exact candidate wheel produced from the reviewed commit" in normalized_readme
    assert "Helm `3.19.0` or newer" in readme
    assert "render-only policy example" in readme
    assert "https://paulkov.github.io/dpone/airflow-cache-kubernetes-deployment/" in readme


def test_provider_docs_recommend_combined_loader_for_production() -> None:
    provider_docs = (ROOT / "docs" / "airflow-provider-api.md").read_text(encoding="utf-8")

    assert "`load_and_acknowledge_dpone_dags()` remains the recommended production" in provider_docs
    assert "recommended zero-boilerplate path" not in provider_docs
    assert "recommended `load_dpone_dags()` loader" not in provider_docs
