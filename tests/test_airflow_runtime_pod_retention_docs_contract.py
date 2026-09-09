from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from dpone.contracts.deployment_cache_retention_state import canonical_digest, retention_operation_id
from tests.support.runtime_pod_withdrawal import publish_withdrawal_ack, write_withdrawal_ack_verifier

ROOT = Path(__file__).parents[1]
DOCS = ROOT / "docs"

DECOMPOSED_DOCS = {
    "airflow-runtime-pod-retention.md": (
        "airflow-runtime-pod-retention-architecture.md",
        "airflow-runtime-pod-retention-manual.md",
        "airflow-runtime-pod-retention-python-api.md",
        "airflow-runtime-pod-retention-kubernetes.md",
        "airflow-runtime-pod-retention-upgrade.md",
        "airflow-runtime-pod-retention-operations.md",
        "airflow-runtime-pod-retention-withdrawal.md",
    ),
    "airflow-cache-kubernetes-deployment.md": (
        "airflow-cache-kubernetes-prerequisites.md",
        "airflow-cache-kubernetes-runtime-wrappers.md",
        "airflow-cache-kubernetes-wrapper-delivery.md",
        "airflow-cache-kubernetes-chart-operations.md",
    ),
    "airflow-cache-sync.md": (
        "airflow-cache-sync-materialization.md",
        "airflow-cache-sync-strict-v2.md",
        "airflow-cache-sync-promotion.md",
        "airflow-cache-sync-recovery.md",
        "airflow-cache-retention-state-downgrade.md",
        "airflow-cache-retention.md",
        "airflow-cache-retention-approval.md",
        "airflow-cache-sync-python-api.md",
    ),
}

RUNTIME_LEAVES = DECOMPOSED_DOCS["airflow-runtime-pod-retention.md"]
RUNTIME_DOCS = (*RUNTIME_LEAVES, "airflow-runtime-pod-retention.md")
CACHE_SYNC_LEAVES = DECOMPOSED_DOCS["airflow-cache-sync.md"]


def _read_docs(*names: str) -> str:
    return "\n".join((DOCS / name).read_text(encoding="utf-8") for name in names)


def test_airflow_operations_docs_are_stable_hubs_with_task_focused_leaves() -> None:
    for overview_name, leaf_names in DECOMPOSED_DOCS.items():
        overview = (DOCS / overview_name).read_text(encoding="utf-8")
        assert len(overview.splitlines()) < 250

        for leaf_name in leaf_names:
            assert f"({leaf_name})" in overview
            leaf = (DOCS / leaf_name).read_text(encoding="utf-8")
            opening = "\n".join(leaf.splitlines()[:12])
            assert "**Purpose.**" in opening
            assert "**Audience.**" in opening
            assert f"({overview_name})" in opening
            assert "**Next likely task:**" in opening


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


def _json_block_after(content: str, marker: str) -> dict[str, object]:
    tail = content.split(marker, 1)[1]
    payload = tail.split("```json", 1)[1].split("```", 1)[0]
    return json.loads(payload)


def _canonical_json_bytes(payload: dict[str, object]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()


def _retention_publication(expected_schema: str, payload: dict[str, object]) -> dict[str, object]:
    digest = "sha256:" + hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()
    return {
        "schema": "dpone.airflow-cache-status-publication.v1",
        "attempted_at": datetime.now(UTC).isoformat(),
        "passed": True,
        "status": "published",
        "source": "cache-status-source.json",
        "target": "cache-status-target.json",
        "failure_marker": "cache-status.failed",
        "expected_schema": expected_schema,
        "source_sha256": digest,
        "target_sha256": digest,
    }


def _write_retention_review_evidence(path: Path, plan: dict[str, object]) -> None:
    path.mkdir(mode=0o700)
    publication = _retention_publication("dpone.deployment-cache-retention-plan.v1", plan)
    (path / "plan.json").write_bytes(_canonical_json_bytes(plan))
    (path / "publication.json").write_bytes(_canonical_json_bytes(publication))
    lines = "".join(
        f"{hashlib.sha256((path / name).read_bytes()).hexdigest()}  {name}\n"
        for name in ("plan.json", "publication.json")
    )
    (path / "SHA256SUMS").write_text(lines, encoding="utf-8")


def test_retention_state_downgrade_is_executable_and_fail_closed() -> None:
    guide = (DOCS / "airflow-cache-retention-state-downgrade.md").read_text(encoding="utf-8")

    for expected in (
        "source_runtime_image",
        "source_cache_image",
        "source-runtime-versions-",
        "source-cache-status-",
        "namespace_uid",
        "old-pod-uids.txt",
        "sha256sum -c SHA256SUMS",
        "RESTORE_SHA256SUMS",
        "chmod 0400",
        "kube exec",
        "emptyDir",
        "Persistent cache downgrade is not certified",
        "persistent cache downgrade is UNVERIFIED",
        "Never restore",
        "over a mounted live volume",
    ):
        assert expected in guide
    assert "hand-edit" in guide.lower()
    assert "no v2 state was copied" in guide

    public_contract = "\n".join(
        (DOCS / path).read_text(encoding="utf-8")
        for path in (
            "airflow-cache-sync.md",
            "compatibility.md",
            "adr/0043-airflow-retention-occurrence-review-identity.md",
        )
    ).lower()
    assert "snapshot-bound persistent" not in public_contract
    assert "persistent-volume snapshot restoration" not in public_contract
    assert "persistent-volume recovery path" not in public_contract
    assert "persistent-volume downgrade" in public_contract
    assert "unverified" in public_contract


def test_executable_cache_wrapper_runbooks_require_python3() -> None:
    executable_docs = "\n".join(
        (DOCS / path).read_text(encoding="utf-8")
        for path in (
            "airflow-cache-kubernetes-runtime-wrappers.md",
            "airflow-cache-kubernetes-wrapper-delivery.md",
            "airflow-cache-kubernetes-chart-operations.md",
            "airflow-runtime-pod-retention-manual.md",
        )
    )

    assert re.search(r"(?<![A-Za-z0-9_])python(?=\s|$)", executable_docs) is None
    assert "python3" in executable_docs


def test_public_airflow_install_examples_use_explicit_python3() -> None:
    executable_docs = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (
            ROOT / "README.md",
            DOCS / "getting-started/first-airflow-dag.md",
            ROOT / "packages/dpone-airflow-pack/README.md",
            ROOT / "packages/apache-airflow-providers-dpone/README.md",
        )
    )

    assert re.search(r"(?m)^python(?:\s|$)", executable_docs) is None
    assert "python3 -m pip" in executable_docs


def test_persistent_cache_downgrade_guard_stops_as_unverified(tmp_path: Path) -> None:
    guide = (DOCS / "airflow-cache-retention-state-downgrade.md").read_text(encoding="utf-8")
    block = _bash_block_after(guide, "Persistent cache downgrade is not certified")
    evidence = tmp_path / "evidence"
    evidence.mkdir()
    (evidence / "occurrence.json").write_text('{"storage_kind":"persistent"}\n', encoding="utf-8")
    subprocess.run(
        "sha256sum occurrence.json >SHA256SUMS",
        cwd=evidence,
        shell=True,
        check=True,
    )

    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude=f'DOWNGRADE_EVIDENCE_DIR="{evidence}"\n',
    )

    assert result.returncode == 4
    assert "persistent cache downgrade is UNVERIFIED" in result.stderr


@pytest.mark.parametrize("pod_image_matches", (True, False))
def test_downgrade_occurrence_derives_live_image_and_cache_identity(
    tmp_path: Path,
    pod_image_matches: bool,
) -> None:
    guide = (DOCS / "airflow-cache-retention-state-downgrade.md").read_text(encoding="utf-8")
    block = _bash_block_after(guide, "Capture exact namespace")
    runtime_digest = "a" * 64
    cache_digest = "b" * 64
    observed_runtime_digest = runtime_digest if pod_image_matches else "c" * 64
    workload = {
        "metadata": {"uid": "workload-uid"},
        "spec": {
            "selector": {"matchLabels": {"app": "parser"}},
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "cache-watch",
                            "image": f"repo/cache@sha256:{cache_digest}",
                            "volumeMounts": [{"name": "cache", "mountPath": "/cache"}],
                        },
                        {"name": "airflow", "image": f"repo/runtime@sha256:{runtime_digest}"},
                    ],
                    "volumes": [{"name": "cache", "emptyDir": {}}],
                }
            },
        },
    }
    pods = {
        "items": [
            {
                "metadata": {"name": "parser-1", "uid": "pod-uid"},
                "spec": {"containers": [{"name": "airflow"}, {"name": "cache-watch"}]},
                "status": {
                    "conditions": [{"type": "Ready", "status": "True"}],
                    "containerStatuses": [
                        {
                            "name": "airflow",
                            "imageID": f"containerd://repo/runtime@sha256:{observed_runtime_digest}",
                        },
                        {
                            "name": "cache-watch",
                            "imageID": f"containerd://repo/cache@sha256:{cache_digest}",
                        },
                    ],
                },
            }
        ]
    }
    release_id = "sha256:" + "d" * 64
    deployment_id = "sha256:" + "e" * 64
    evidence = tmp_path / "evidence"
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude=f"""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
PARSE_AUTHORITY_WORKLOAD=deployment/parser
CACHE_STATUS_CONTAINER=cache-watch
AIRFLOW_RUNTIME_CONTAINER=airflow
CACHE_ROOT=/cache
DOWNGRADE_EVIDENCE_DIR="{evidence}"
CACHE_STORAGE_KIND=emptyDir
kubectl() {{
  case "$*" in
    *" get namespace airflow-example -o json") printf '%s\\n' '{{"metadata":{{"uid":"namespace-uid"}}}}' ;;
    *" get deployment/parser -o json") printf '%s\\n' '{json.dumps(workload)}' ;;
    *" get pods -l app=parser -o json") printf '%s\\n' '{json.dumps(pods)}' ;;
    *" exec pod/parser-1 -c airflow -i -- python3 -")
      printf '%s\\n' '{{"airflow":"3.2.0","dpone_distributions":{{"dpone":"0.73.32"}}}}'
      ;;
    *" exec pod/parser-1 -c cache-watch -- dpone-airflow-pack-cache-status"*)
      printf '%s\\n' '{{"release_id":"{release_id}","current_deployment_id":"{deployment_id}","blockers":[]}}'
      ;;
  esac
}}
""",
    )

    if not pod_image_matches:
        assert result.returncode != 0
        assert not (evidence / "occurrence.json").exists()
        return
    assert result.returncode == 0, result.stderr
    occurrence = json.loads((evidence / "occurrence.json").read_text(encoding="utf-8"))
    assert occurrence["runtime_image"] == f"repo/runtime@sha256:{runtime_digest}"
    assert occurrence["cache_image"] == f"repo/cache@sha256:{cache_digest}"
    assert occurrence["release_id"] == release_id
    assert occurrence["deployment_id"] == deployment_id
    assert (evidence / "SHA256SUMS").is_file()


def test_manual_retention_commands_preserve_nonzero_evidence() -> None:
    content = _read_docs(*RUNTIME_DOCS)
    manual = content.split("## Expected machine-readable evidence", 1)[0]

    assert manual.count("|| rc=$?") == 2
    assert 'if [ ! -s "${tmp}" ]; then' in manual
    assert 'if [ ! -s "${report_tmp}" ]; then' in manual
    assert manual.count('exit "${rc}"') >= 5
    assert manual.count("dpone gitops schema validate") == 3
    assert "dpone.airflow-runtime-pod-retention-plan.v1" in manual
    assert "dpone.airflow-runtime-pod-retention-apply.v1" in manual
    assert "dpone.airflow-runtime-pod-retention-event.v1" in manual
    assert 'destination="${evidence_dir}/attention.json"' in manual
    assert "report_name=failed.json" in manual
    assert '--format json >"${report_tmp}" 2>"${events}"' in manual
    assert "set -euo pipefail\n\ndpone airflow runtime-pod-retention-plan" not in manual
    assert "--kube-auth kubeconfig" in manual
    assert '--kube-context "${KUBE_CONTEXT}"' in manual
    assert "DPONE_RETENTION_ACTOR:?set the reviewed operator identity" in manual
    assert "auth can-i delete pods \\" in manual


def test_documented_retention_evidence_examples_match_public_schemas() -> None:
    content = _read_docs(*RUNTIME_DOCS)
    cases = (
        (
            "<!-- runtime-pod-retention-plan-example -->",
            "airflow-runtime-pod-retention-plan.schema.json",
        ),
        (
            "<!-- runtime-pod-retention-apply-example -->",
            "airflow-runtime-pod-retention-apply.schema.json",
        ),
    )

    for marker, schema_name in cases:
        payload = _json_block_after(content, marker)
        schema_path = DOCS / "schemas" / "gitops" / schema_name
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        Draft202012Validator(schema, format_checker=FormatChecker()).validate(payload)


def test_python_api_examples_keep_plan_read_only_and_apply_dependencies_explicit() -> None:
    content = _read_docs(*RUNTIME_DOCS)
    section = content.split("## Python API", 1)[1].split("## Render the Kubernetes control", 1)[0]

    assert "classifier=" not in section
    assert "label_selector=RUNTIME_POD_LABEL_SELECTOR" in section
    assert "AirflowRuntimePodRetentionPlanRequest" in section
    assert ".plan(" in section
    assert "json.dumps(report" in section
    assert "AirflowRuntimePodRetentionApplyRequest" in section
    assert ".apply(" in section
    assert "deletion=adapter" in section
    assert "credentials=adapter" in section
    assert "JsonLinesAirflowRuntimePodRetentionEvidencePublisher(sys.stderr)" in section
    assert "confirm_delete=True" in section
    assert 'kube_auth_mode="kubeconfig"' in section


def test_render_examples_publish_only_validated_output_atomically() -> None:
    content = _read_docs(*RUNTIME_DOCS)

    assert content.count('tmp="$(mktemp runtime-pod-retention-') >= 2
    assert content.count('evidence_dir="$(mktemp -d runtime-pod-retention-') == 3
    assert content.count("trap 'rm -f \"${tmp}\"' EXIT") == 2
    assert 'mv -f "${tmp}" runtime-pod-retention.yaml' in content
    assert 'mv -f "${tmp}" runtime-pod-retention-apply.yaml' in content


def test_documented_prometheus_render_examples_execute_with_explicit_threshold(tmp_path: Path) -> None:
    content = (DOCS / "airflow-runtime-pod-retention-kubernetes.md").read_text(encoding="utf-8")
    blocks = tuple(
        block.replace("registry.example/dpone@sha256:<64-hex-digest>", "registry.example/dpone@sha256:" + "a" * 64)
        for block in re.findall(r"```bash\n(.*?)\n```", content, flags=re.DOTALL)[:2]
    )

    assert len(blocks) == 2
    for block in blocks:
        assert "--stale-after-seconds 7200" in block
        result = _run_bash(
            block,
            cwd=tmp_path,
            prelude='export AIRFLOW_NAMESPACE=airflow-example; dpone() { uv run --project "'
            + str(ROOT)
            + '" dpone "$@"; }',
        )
        assert result.returncode == 0, result.stderr

    assert (tmp_path / "runtime-pod-retention.yaml").exists()
    assert (tmp_path / "runtime-pod-retention-apply.yaml").exists()


def test_api_only_diagnostics_materialize_the_file_they_later_verify() -> None:
    content = (DOCS / "airflow-cache-diagnostics-without-kubectl.md").read_text(encoding="utf-8")

    assert content.count('mv -f "${tmp}" cache-status-from-variable.json') == 2
    predicate = 'type == "object" and .kind == "dpone.airflow_pack_cache_status" and .schema_version == "1"'
    assert content.count(f"jq -e '{predicate}'") == 2
    assert ': "${AIRFLOW_API_TOKEN:?set a masked Airflow 3 bearer token}"' in content
    assert ': "${AIRFLOW_API_PASSWORD:?set the masked Airflow 2 API password}"' in content
    dag_section = content.split("## Compare Airflow-visible DAGs", 1)[1].split("## Classify the result", 1)[0]
    assert "/api/v2/dags?" in dag_section
    assert "/api/v1/dags?" in dag_section
    assert dag_section.count("airflow_api_get()") == 2
    assert ': "${AIRFLOW_API_TOKEN:?set a masked Airflow 3 bearer token}"' in dag_section
    assert ': "${AIRFLOW_API_PASSWORD:?set the masked Airflow 2 API password}"' in dag_section
    assert dag_section.count("DPONE_EXPECTED_DAG_IDS_FILE") == 8
    assert dag_section.count('sort -u "${DPONE_EXPECTED_DAG_IDS_FILE}"') == 2
    assert dag_section.count("comm -3 expected-dag-ids.sorted") == 2
    assert dag_section.count("if [ -s dag-set.diff ]; then") == 2
    assert ".total_entries // 0" not in dag_section
    assert dag_section.count("total_entries changed during pagination") == 2
    assert dag_section.count('== "dpone"') == 2
    assert "airflow-desired-deployment.json" in dag_section
    assert "activated deployment has no expected DAG IDs" in dag_section
    assert "--config -" not in content
    assert content.count("--header @-") == 4
    assert content.count("Authorization: Bearer %s") == 2
    assert content.count("Authorization: Basic %s") == 2
    assert content.count("printf -v auth_header") == 4
    assert content.count('<<<"${auth_header}"') == 4
    assert not re.search(r"printf 'Authorization: (?:Bearer|Basic).*?\\\n\s*\| curl", content)
    assert content.count("| base64 | tr -d") == 2
    assert content.count("curl --disable --proto '=https' --tlsv1.2 --fail") == 4

    from dpone_airflow_pack.cache_status import frozen_missing_cache_status

    status = frozen_missing_cache_status(Path("/bounded/cache"), workload_ids=("orders",))
    validated = subprocess.run(
        ["jq", "-e", predicate],
        input=json.dumps(status),
        check=False,
        text=True,
        capture_output=True,
    )
    assert validated.returncode == 0, validated.stderr
    assert content.count("AIRFLOW_BASE_URL must use https://") == 4


def test_scheduled_job_evidence_is_split_and_schema_validated() -> None:
    content = _read_docs(*RUNTIME_DOCS)
    section = content.split("### Validate one retained scheduled Job", 1)[1].split("## Classification reference", 1)[0]

    assert "ownerReferences" in section
    assert "runtime-pod-retention-template-sha256" in section
    assert 'expected_report_kind="dpone.airflow-runtime-pod-retention-${current_mode}.v1"' in section
    assert "exact_execution_template(job)" in section
    assert "RETENTION_ROLLOUT_NOT_BEFORE" in section
    assert "reviewed_manifest_sha256" in section
    assert "rollout-occurrence.json" in section
    assert '--limit="${JOB_INVENTORY_LIMIT}"' in section
    assert "JOB_INVENTORY_MAX_BYTES" in section
    assert "capture_kube_json_bounded" in section
    assert '(.metadata.continue // "") == ""' in section
    assert "--all-containers" in section
    assert '"dpone.airflow-runtime-pod-retention-plan.v1"' in section
    assert "dpone.airflow-runtime-pod-retention-apply.v1" in section
    assert "dpone.airflow-runtime-pod-retention-event.v1" in section
    assert "dpone gitops schema validate --payload" in section
    assert '[ -s "${evidence_dir}/events.jsonl" ]' in section
    assert "json.JSONDecoder()" in section
    assert "and ([.[].sequence] == [range(1; length + 1)])" in section
    assert '.status == "ok" and .evidence_status == "complete"' in section
    assert 'last.event == "operation_completed"' in section
    assert "sha256sum -c SHA256SUMS" in section


def test_scheduled_job_parser_accepts_multiline_report_and_jsonl_events(tmp_path: Path) -> None:
    content = _read_docs(*RUNTIME_DOCS)
    section = content.split("### Validate one retained scheduled Job", 1)[1].split("## Classification reference", 1)[0]
    parser = section.split('python3 - "${evidence_dir}/combined.log"', 1)[1]
    script = parser.split("<<'PY'", 1)[1].split("\nPY", 1)[0]
    combined = tmp_path / "combined.log"
    reports = tmp_path / "reports.jsonl"
    events = tmp_path / "events.jsonl"
    combined.write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-runtime-pod-retention-event.v1",
                "operation_id": "sha256:" + "a" * 64,
                "sequence": 1,
            },
            separators=(",", ":"),
        )
        + "\n"
        + json.dumps(
            {
                "schema": "dpone.airflow-runtime-pod-retention-apply.v1",
                "status": "ok",
                "operation_id": "sha256:" + "a" * 64,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        ["python3", "-", str(combined), str(reports), str(events)],
        input=script,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(reports.read_text(encoding="utf-8"))["status"] == "ok"
    assert json.loads(events.read_text(encoding="utf-8"))["sequence"] == 1


def test_scheduled_job_selector_rejects_old_plan_job_after_apply_rollout(tmp_path: Path) -> None:
    content = _read_docs(*RUNTIME_DOCS)
    section = content.split("### Validate one retained scheduled Job", 1)[1].split("## Classification reference", 1)[0]
    selector = section.split('python3 - "${evidence_dir}/cronjob.json"', 1)[1]
    script = selector.split("<<'PY'", 1)[1].split("\nPY", 1)[0]

    def fingerprint(mode: str) -> tuple[str, list[str]]:
        command = ["dpone", "airflow", f"runtime-pod-retention-{mode}"]
        canonical = json.dumps(
            {"command": command, "image": "registry/dpone@sha256:" + "a" * 64},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        import hashlib

        return "sha256:" + hashlib.sha256(canonical).hexdigest(), command

    def job(mode: str, created_at: str, *, extra_args: bool = False) -> dict[str, object]:
        sha, command = fingerprint(mode)
        annotations = {
            "dpone.dev/runtime-pod-retention-mode": mode,
            "dpone.dev/runtime-pod-retention-template-sha256": sha,
        }
        container: dict[str, object] = {
            "image": "registry/dpone@sha256:" + "a" * 64,
            "command": command,
        }
        if extra_args:
            container["args"] = ["unexpected"]
        return {
            "metadata": {
                "name": f"retention-{mode}",
                "uid": f"job-{mode}-{created_at}",
                "creationTimestamp": created_at,
                "annotations": annotations,
                "ownerReferences": [{"kind": "CronJob", "uid": "cron-uid", "controller": True}],
            },
            "spec": {
                "template": {
                    "metadata": {"annotations": annotations},
                    "spec": {"containers": [container]},
                }
            },
            "status": {"conditions": [{"type": "Complete", "status": "True"}]},
        }

    apply_sha, apply_command = fingerprint("apply")
    annotations = {
        "dpone.dev/runtime-pod-retention-mode": "apply",
        "dpone.dev/runtime-pod-retention-template-sha256": apply_sha,
    }
    cronjob = {
        "metadata": {"uid": "cron-uid", "resourceVersion": "42", "generation": 7},
        "spec": {
            "jobTemplate": {
                "metadata": {"annotations": annotations},
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [
                                {
                                    "image": "registry/dpone@sha256:" + "a" * 64,
                                    "command": apply_command,
                                }
                            ]
                        }
                    }
                },
            }
        },
    }
    cronjob_path = tmp_path / "cronjob.json"
    jobs_path = tmp_path / "jobs.json"
    selected_path = tmp_path / "selected.json"
    cronjob_path.write_text(json.dumps(cronjob), encoding="utf-8")
    jobs_path.write_text(
        json.dumps(
            {
                "items": [
                    job("plan", "2026-08-04T10:00:00Z"),
                    job("apply", "2026-08-04T11:00:00Z", extra_args=True),
                    job("apply", "2026-08-04T09:00:00Z"),
                    job("apply", "2026-08-04T08:00:00Z"),
                ]
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            "python3",
            "-",
            str(cronjob_path),
            str(jobs_path),
            str(selected_path),
            "2026-08-04T08:30:00Z",
        ],
        input=script,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    selected = json.loads(selected_path.read_text(encoding="utf-8"))
    assert selected["metadata"]["name"] == "retention-apply"
    assert selected["metadata"]["creationTimestamp"] == "2026-08-04T09:00:00Z"


def test_runtime_pod_retention_runbook_maps_every_stable_error_family_to_recovery() -> None:
    content = _read_docs(*RUNTIME_DOCS)
    table = content.split("## Error recovery table", 1)[1].split("## Alerts and acceptance evidence", 1)[0]

    for suffix in (
        "ACTOR_MISSING",
        "CREDENTIAL_SOURCE_MISSING",
        "ACCESS_DENIED",
        "INVENTORY_EXPIRED",
        "DELETE_CAPABILITY_MISSING",
        "EVIDENCE_CAPABILITY_MISSING",
        "INTERRUPTED",
        "INPUT_INVALID",
    ):
        assert suffix in table
    assert "never bypass the check" in table
    assert "do not reuse candidates" in table
    assert "Replan before retrying" in table


def test_authenticated_airflow_api_examples_reject_cleartext_before_curl(tmp_path: Path) -> None:
    content = (DOCS / "airflow-cache-diagnostics-without-kubectl.md").read_text(encoding="utf-8")
    block = _bash_block_after(content, "For Airflow 3 API v2")
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude="""
AIRFLOW_BASE_URL=http://airflow.example
AIRFLOW_API_TOKEN=secret
curl() { printf '%s\n' "$*" >>"${CALLS_FILE}"; }
jq() { cat; }
""",
    )

    assert result.returncode == 2
    assert "must use https://" in result.stderr
    assert not (tmp_path / "calls.txt").exists()


def test_api_diagnostic_failure_preserves_previous_valid_snapshot(tmp_path: Path) -> None:
    content = (DOCS / "airflow-cache-diagnostics-without-kubectl.md").read_text(encoding="utf-8")
    block = _bash_block_after(content, "For Airflow 3 API v2")
    reviewed = tmp_path / "cache-status-from-variable.json"
    reviewed.write_text('{"schema":"last-known-good"}\n', encoding="utf-8")
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude="""
AIRFLOW_BASE_URL=https://airflow.example
AIRFLOW_API_TOKEN=secret
curl() { return 9; }
""",
    )

    assert result.returncode == 9
    assert reviewed.read_text(encoding="utf-8") == '{"schema":"last-known-good"}\n'


def test_dag_set_diagnostics_fail_when_pagination_metadata_is_missing(tmp_path: Path) -> None:
    content = (DOCS / "airflow-cache-diagnostics-without-kubectl.md").read_text(encoding="utf-8")
    block = _bash_block_after(content, "Airflow 3 API v2:")
    expected = tmp_path / "expected.txt"
    expected.write_text("DAG__platform__dpone_runtime__smoke\n", encoding="utf-8")
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude=f"""
AIRFLOW_BASE_URL=https://airflow.example
AIRFLOW_API_TOKEN=secret
DPONE_EXPECTED_DAG_IDS_FILE={expected}
curl() {{ printf '{{"dags":[]}}\n'; }}
""",
    )

    assert result.returncode != 0
    assert not (tmp_path / "observed-dag-ids.sorted").exists()


def test_dag_set_diagnostics_reject_same_count_churn_between_complete_scans(tmp_path: Path) -> None:
    content = (DOCS / "airflow-cache-diagnostics-without-kubectl.md").read_text(encoding="utf-8")
    block = _bash_block_after(content, "Airflow 3 API v2:")
    expected = tmp_path / "expected.txt"
    counter = tmp_path / "counter.txt"
    expected.write_text("DAG__platform__expected__refresh\n", encoding="utf-8")
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude=f"""
AIRFLOW_BASE_URL=https://airflow.example
AIRFLOW_API_TOKEN=secret
DPONE_EXPECTED_DAG_IDS_FILE={expected}
curl() {{
  count="$(cat {counter} 2>/dev/null || printf 0)"
  count=$((count + 1))
  printf '%s\n' "${{count}}" >{counter}
  if [ "${{count}}" -eq 1 ]; then
    printf '{{"total_entries":1,"dags":[{{"dag_id":"DAG__platform__expected__refresh","tags":[{{"name":"dpone"}}]}}]}}\n'
  else
    printf '{{"total_entries":1,"dags":[{{"dag_id":"DAG__platform__replacement__refresh","tags":[{{"name":"dpone"}}]}}]}}\n'
  fi
}}
""",
    )

    assert result.returncode != 0
    assert "changed between complete scans" in result.stderr
    assert not (tmp_path / "observed-dag-ids.sorted").exists()


def test_changed_remote_downloads_disable_ambient_config_and_require_https() -> None:
    package = (ROOT / "packages" / "dpone-airflow-pack" / "README.md").read_text(encoding="utf-8")
    workflow = (ROOT / ".github" / "workflows" / "airflow-pack-compat.yml").read_text(encoding="utf-8")

    assert "curl --disable --proto '=https' --tlsv1.2 --fail" in package
    assert "curl --disable --proto '=https' --tlsv1.2 --fail" in workflow
    assert "2.10.5) PROFILE=airflow-cache-values-2.10.yaml; CHART_VERSION=1.19.0" in package
    assert "3.2.0) PROFILE=airflow-cache-values-3.2.yaml; CHART_VERSION=1.22.0" in package


def test_runtime_wheel_smoke_covers_pod_retention_and_recovery_v2() -> None:
    workflow = (ROOT / ".github" / "workflows" / "airflow-pack-compat.yml").read_text(encoding="utf-8")

    assert "runtime-pod-retention-plan --help" in workflow
    assert "runtime-pod-retention-apply --help" in workflow
    assert "tests/test_deployment_cache_retention_state.py" in workflow
    assert "tests/test_airflow_cache_retention_occurrence_hardening.py" in workflow


def test_cache_retention_runbook_requests_machine_readable_plan_evidence() -> None:
    content = _read_docs(*CACHE_SYNC_LEAVES)

    snippet = content.split("Retention apply is also", 1)[1].split("Example generated action", 1)[0]
    assert "cache-retention-plan \\" in snippet
    assert "destination=cache-retention-plan.json" in snippet
    assert 'destination="${evidence_dir}/failed.json"' in snippet
    assert '--format json >"${tmp}" || rc=$?' in snippet
    assert "dpone gitops schema validate" in snippet
    assert "--kind dpone.deployment-cache-retention-plan.v1" in snippet
    assert "printf 'retention plan produced no JSON" in snippet


def test_apply_mode_cronjob_has_an_explicit_activation_step() -> None:
    content = _read_docs(*RUNTIME_DOCS)

    assert 'kubectl --context "${KUBE_CONTEXT}" apply --dry-run=server' in content
    assert 'kubectl --context "${KUBE_CONTEXT}" apply \\' in content
    lifecycle = content.split("## Render the Kubernetes control", 1)[1]
    assert "set -euo pipefail" in lifecycle
    assert lifecycle.count('|| { rc=$?; exit "${rc}"; }') >= 7
    assert "job_state=timeout" in lifecycle
    assert "*Failed=True*) job_state=failed" in lifecycle
    assert 'if [ "${job_state}" != complete ]; then' in lifecycle
    assert "plan ServiceAccount must report delete=no" in content
    for line in lifecycle.splitlines():
        if line.lstrip().startswith("kubectl "):
            assert '--context "${KUBE_CONTEXT}"' in line


def test_destructive_examples_require_an_explicit_namespace() -> None:
    content = _read_docs(*RUNTIME_DOCS)

    assert "--namespace airflow-example" not in content
    assert "system:serviceaccount:airflow-example" not in content
    assert 'namespace = "airflow-example"' not in content
    assert content.count(': "${AIRFLOW_NAMESPACE:?') >= 8
    assert '--namespace "${AIRFLOW_NAMESPACE}"' in content


def test_stock_apply_is_explicitly_non_production_without_durable_ack() -> None:
    content = _read_docs(*RUNTIME_DOCS)
    acceptance = content.split("## Alerts and acceptance evidence", 1)[1].split(
        "## Rollback and incident response",
        1,
    )[0]

    assert "rendered apply CronJob is a non-production certification profile" in content
    assert "durable_acknowledged" in acceptance
    assert "process_ordered" in acceptance
    assert "must remain disabled in production" in acceptance


def test_architecture_does_not_claim_unimplemented_runtime_pod_projector() -> None:
    content = (DOCS / "architecture.md").read_text(encoding="utf-8")
    section = content.split("Kubernetes runtime Pod cleanup", 1)[1].split(
        "Remote delivery is a separate clean-architecture path",
        1,
    )[0]

    assert "not projected into the scheduler pack cache" in section
    assert "Plan --> Projector" not in section
    assert section.index('OperationStarted["operation_started"]') < section.index('Intent["delete intent"]')
    assert section.index('Intent["delete intent"]') < section.index('DeleteAccepted["delete accepted"]')
    assert section.index('DeleteAccepted["delete accepted"]') < section.index('Outcome["outcome"]')
    assert section.index('Outcome["outcome"]') < section.index('Completion["operation_completed"]')
    assert "process_ordered" in section
    assert "durable_acknowledged" in section
    assert "publisher failure" in section


def test_provider_readme_deploys_and_certifies_the_shipped_retention_control() -> None:
    readme = (ROOT / "packages" / "apache-airflow-providers-dpone" / "README.md").read_text(encoding="utf-8")
    normalized = " ".join(readme.split())

    assert "ships the runtime Pod retention control" in normalized
    assert "deploy and certify" in normalized
    assert "does not ship a Kubernetes pod janitor" not in normalized


def test_apply_runbook_stops_after_failed_server_validation(tmp_path: Path) -> None:
    content = _read_docs(*RUNTIME_DOCS)
    block = _bash_block_after(content, "Then validate and activate the already reviewed apply manifest:")
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude="""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
kubectl() {
  printf '%s\\n' "$*" >>"${CALLS_FILE}"
  case "$*" in
    *"apply --dry-run=server"*) return 9 ;;
  esac
  return 0
}
""",
    )

    calls = (tmp_path / "calls.txt").read_text(encoding="utf-8").splitlines()
    assert result.returncode == 9
    assert len(calls) == 1
    assert calls[0].startswith("--context reviewed-dev apply --dry-run=server")


def test_plan_runbook_captures_logs_before_propagating_failed_job(tmp_path: Path) -> None:
    content = _read_docs(*RUNTIME_DOCS)
    block = _bash_block_after(content, "Validate the rendered API objects server-side and deploy plan mode first:")
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude="""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
kubectl() {
  printf '%s\\n' "$*" >>"${CALLS_FILE}"
  case "$*" in
    *" get job/"*) printf 'Failed=True\\n' ;;
    *" logs job/"*) printf '{"status":"needs_attention"}\\n' ;;
  esac
  return 0
}
sleep() { return 0; }
""",
    )

    calls = (tmp_path / "calls.txt").read_text(encoding="utf-8").splitlines()
    assert result.returncode == 1
    assert any(" logs job/" in call for call in calls)
    assert all(call.startswith("--context reviewed-dev ") for call in calls)
    logs = list(tmp_path.glob("dpone-runtime-pod-retention-manual-*.log"))
    assert len(logs) == 1
    assert json.loads(logs[0].read_text(encoding="utf-8"))["status"] == "needs_attention"


def test_cache_retention_runbook_publishes_only_schema_valid_output(tmp_path: Path) -> None:
    content = _read_docs(*CACHE_SYNC_LEAVES)
    block = _bash_block_after(content, "the reviewed protection scope:")
    reviewed = tmp_path / "cache-retention-plan.json"
    reviewed.write_text('{"reviewed":true}\n', encoding="utf-8")
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude="""
DPONE_SCHEDULER_CACHE_ROOT=/tmp/cache
dpone() {
  if [ "$1" = airflow ]; then
    printf '{"schema":"dpone.deployment-cache-retention-plan.v1","passed":false}\\n'
    return 4
  fi
  return 0
}
""",
    )

    assert result.returncode == 4
    assert reviewed.read_text(encoding="utf-8") == '{"reviewed":true}\n'
    failed_paths = list(tmp_path.glob("cache-retention-plan.*/failed.json"))
    assert len(failed_paths) == 1
    failed = json.loads(failed_paths[0].read_text(encoding="utf-8"))
    assert failed["schema"] == "dpone.deployment-cache-retention-plan.v1"
    assert (failed_paths[0].parent / "SHA256SUMS").is_file()


def test_cache_retention_runbook_does_not_publish_empty_failure_evidence(tmp_path: Path) -> None:
    content = _read_docs(*CACHE_SYNC_LEAVES)
    block = _bash_block_after(content, "the reviewed protection scope:")
    reviewed = tmp_path / "cache-retention-plan.json"
    reviewed.write_text('{"reviewed":true}\n', encoding="utf-8")
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude="""
DPONE_SCHEDULER_CACHE_ROOT=/tmp/cache
dpone() { return 3; }
""",
    )

    assert result.returncode == 3
    assert reviewed.read_text(encoding="utf-8") == '{"reviewed":true}\n'
    assert not list(tmp_path.glob("cache-retention-plan.*/failed.json"))
    assert "produced no JSON" in result.stderr


def test_runtime_pod_plan_does_not_replace_evidence_with_generic_error(tmp_path: Path) -> None:
    content = _read_docs(*RUNTIME_DOCS)
    block = _bash_block_after(content, "Plan is read-only:")
    reviewed = tmp_path / "runtime-pod-retention-plan.json"
    reviewed.write_text('{"schema":"dpone.airflow-runtime-pod-retention-plan.v1"}\n', encoding="utf-8")
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude="""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
dpone() {
  if [ "$1" = airflow ]; then
    printf '{"passed":false,"errors":[{"schema":"dpone.error.v1"}]}\\n'
    return 2
  fi
  return 2
}
""",
    )

    assert result.returncode == 2
    assert reviewed.read_text(encoding="utf-8") == ('{"schema":"dpone.airflow-runtime-pod-retention-plan.v1"}\n')
    evidence_dirs = [path for path in tmp_path.glob("runtime-pod-retention-plan.*") if path.is_dir()]
    assert len(evidence_dirs) == 1
    temporary = evidence_dirs[0] / "output.json"
    assert json.loads(temporary.read_text(encoding="utf-8"))["errors"][0]["schema"] == "dpone.error.v1"
    assert "not valid plan evidence" in result.stderr


def test_runtime_pod_apply_keeps_nonzero_schema_valid_evidence_separate(tmp_path: Path) -> None:
    content = _read_docs(*RUNTIME_DOCS)
    block = _bash_block_after(content, "Then apply with an exact identity.")
    reviewed = tmp_path / "runtime-pod-retention-apply.json"
    reviewed.write_text('{"schema":"dpone.airflow-runtime-pod-retention-apply.v1"}\n', encoding="utf-8")
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude="""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
DPONE_RETENTION_ACTOR=user://reviewer
kubectl() { return 0; }
dpone() {
  if [ "$1" = airflow ]; then
    printf '{"schema":"dpone.airflow-runtime-pod-retention-apply.v1","status":"failed"}\\n'
    return 5
  fi
  return 0
}
""",
    )

    assert result.returncode == 5
    assert reviewed.read_text(encoding="utf-8") == ('{"schema":"dpone.airflow-runtime-pod-retention-apply.v1"}\n')
    evidence_dirs = [path for path in tmp_path.glob("runtime-pod-retention-apply.*") if path.is_dir()]
    assert len(evidence_dirs) == 1
    failed = json.loads((evidence_dirs[0] / "output.json").read_text(encoding="utf-8"))
    assert failed["schema"] == "dpone.airflow-runtime-pod-retention-apply.v1"
    assert (evidence_dirs[0] / "events.jsonl").is_file()
    assert not (evidence_dirs[0] / "SHA256SUMS").exists()
    assert "unsealed directory" in result.stderr


def test_runtime_pod_apply_seals_coherent_nonzero_evidence(tmp_path: Path) -> None:
    content = _read_docs(*RUNTIME_DOCS)
    block = _bash_block_after(content, "Then apply with an exact identity.")
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude="""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
DPONE_RETENTION_ACTOR=user://reviewer
kubectl() { return 0; }
dpone() {
  if [ "$1" = airflow ]; then
    printf '{"schema":"dpone.airflow-runtime-pod-retention-apply.v1","status":"failed","operation_id":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}\n'
    printf '{"schema":"dpone.airflow-runtime-pod-retention-event.v1","operation_id":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","sequence":1,"event":"operation_started","outcome":"started"}\n' >&2
    printf '{"schema":"dpone.airflow-runtime-pod-retention-event.v1","operation_id":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","sequence":2,"event":"operation_completed","outcome":"failed"}\n' >&2
    return 5
  fi
  return 0
}
""",
    )

    assert result.returncode == 5
    failed_paths = list(tmp_path.glob("runtime-pod-retention-apply.*/failed.json"))
    assert len(failed_paths) == 1
    assert (failed_paths[0].parent / "SHA256SUMS").is_file()


def test_runtime_pod_apply_preserves_jsonl_events_with_success_report(tmp_path: Path) -> None:
    content = _read_docs(*RUNTIME_DOCS)
    block = _bash_block_after(content, "Then apply with an exact identity.")
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude="""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
DPONE_RETENTION_ACTOR=user://reviewer
kubectl() { return 0; }
dpone() {
  if [ "$1" = airflow ]; then
    printf '{"schema":"dpone.airflow-runtime-pod-retention-apply.v1","status":"ok","operation_id":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}\n'
    printf '{"schema":"dpone.airflow-runtime-pod-retention-event.v1","operation_id":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","sequence":1,"event":"operation_started","outcome":"started"}\n' >&2
    printf '{"schema":"dpone.airflow-runtime-pod-retention-event.v1","operation_id":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","sequence":2,"event":"operation_completed","outcome":"ok"}\n' >&2
  fi
  return 0
}
""",
    )

    assert result.returncode == 0
    canonical = json.loads((tmp_path / "runtime-pod-retention-apply.json").read_text(encoding="utf-8"))
    assert canonical["status"] == "ok"
    bundles = list(tmp_path.glob("runtime-pod-retention-apply.*/report.json"))
    assert len(bundles) == 1
    events = (bundles[0].parent / "events.jsonl").read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["event"] for line in events] == ["operation_started", "operation_completed"]
    assert (bundles[0].parent / "SHA256SUMS").is_file()


def test_runtime_pod_apply_rejects_unrelated_event_chain(tmp_path: Path) -> None:
    content = _read_docs(*RUNTIME_DOCS)
    block = _bash_block_after(content, "Then apply with an exact identity.")
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude="""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
DPONE_RETENTION_ACTOR=user://reviewer
kubectl() { return 0; }
dpone() {
  if [ "$1" = airflow ]; then
    printf '{"schema":"dpone.airflow-runtime-pod-retention-apply.v1","status":"ok","operation_id":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}\n'
    printf '{"schema":"dpone.airflow-runtime-pod-retention-event.v1","operation_id":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","sequence":1,"event":"operation_started","outcome":"started"}\n' >&2
    printf '{"schema":"dpone.airflow-runtime-pod-retention-event.v1","operation_id":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","sequence":2,"event":"operation_started","outcome":"started"}\n' >&2
  fi
  return 0
}
""",
    )

    assert result.returncode != 0
    assert not (tmp_path / "runtime-pod-retention-apply.json").exists()
    evidence_dirs = [path for path in tmp_path.glob("runtime-pod-retention-apply.*") if path.is_dir()]
    assert len(evidence_dirs) == 1
    assert not (evidence_dirs[0] / "SHA256SUMS").exists()
    assert "not complete valid evidence" in result.stderr


def test_runtime_pod_nonzero_evidence_is_immutable_across_runs(tmp_path: Path) -> None:
    content = _read_docs(*RUNTIME_DOCS)
    block = _bash_block_after(content, "Plan is read-only:")
    prelude = """
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
dpone() {
  if [ "$1" = airflow ]; then
    printf '{"schema":"dpone.airflow-runtime-pod-retention-plan.v1","status":"needs_attention"}\\n'
    return 1
  fi
  return 0
}
"""

    first = _run_bash(block, cwd=tmp_path, prelude=prelude)
    second = _run_bash(block, cwd=tmp_path, prelude=prelude)

    assert first.returncode == second.returncode == 1
    evidence = list(tmp_path.glob("runtime-pod-retention-plan.*/attention.json"))
    assert len(evidence) == 2
    assert all(json.loads(path.read_text(encoding="utf-8"))["status"] == "needs_attention" for path in evidence)
    assert all((path.parent / "SHA256SUMS").is_file() for path in evidence)
    assert all(path.stat().st_mode & 0o777 == 0o400 for path in evidence)


def test_cache_retention_nonzero_evidence_is_sealed_per_run(tmp_path: Path) -> None:
    content = _read_docs(*CACHE_SYNC_LEAVES)
    block = _bash_block_after(content, "the reviewed protection scope:")
    prelude = """
DPONE_SCHEDULER_CACHE_ROOT=/tmp/cache
dpone() {
  if [ "$1" = airflow ]; then
    printf '{"schema":"dpone.deployment-cache-retention-plan.v1","passed":false}\n'
    return 4
  fi
  return 0
}
"""

    first = _run_bash(block, cwd=tmp_path, prelude=prelude)
    second = _run_bash(block, cwd=tmp_path, prelude=prelude)

    assert first.returncode == second.returncode == 4
    failed_paths = list(tmp_path.glob("cache-retention-plan.*/failed.json"))
    assert len(failed_paths) == 2
    assert all((path.parent / "SHA256SUMS").is_file() for path in failed_paths)
    assert all(path.stat().st_mode & 0o777 == 0o400 for path in failed_paths)


def test_withdrawal_preserves_forensics_before_exact_owned_job_delete(tmp_path: Path) -> None:
    content = _read_docs(*RUNTIME_DOCS)
    assert "batch.kubernetes.io/cronjob-name" not in content
    assert "kube delete jobs" not in content
    block = _bash_block_after(content, "For full withdrawal, keep the reviewed context")
    evidence_root = tmp_path / "evidence"
    evidence_root.mkdir()
    verifier = write_withdrawal_ack_verifier(tmp_path)
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude=f"""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
DPONE_RUNTIME_POD_WITHDRAW_EVIDENCE_DIR={evidence_root}/operation
DPONE_RUNTIME_POD_WITHDRAW_REMOVAL_COMMIT={"a" * 40}
DPONE_RUNTIME_POD_WITHDRAW_ACK=preserve-forensics-and-withdraw
DPONE_RUNTIME_POD_RETENTION_ALERTS=off
{verifier.shell_exports()}
controller_deleted=0
access_deleted=0
python3() {{
  case "$*" in
    *" controller-jobs "*) printf 'python3 %s\\n' "$*" >>"${{CALLS_FILE}}"; controller_deleted=1 ;;
    *" quiescence "*) printf 'python3 %s\\n' "$*" >>"${{CALLS_FILE}}"; return 0 ;;
    *" access "*) printf 'python3 %s\\n' "$*" >>"${{CALLS_FILE}}"; access_deleted=1 ;;
    *) command python3 "$@" ;;
  esac
}}
kubectl() {{
  printf '%s\\n' "$*" >>"${{CALLS_FILE}}"
  case "$*" in
    *" get cronjob "*" -o json"*)
      if grep -q ' patch cronjob ' "${{CALLS_FILE}}"; then
        printf '{{"metadata":{{"uid":"cron-uid","resourceVersion":"cron-rv-2"}},"spec":{{"suspend":true}}}}\\n'
      else
        printf '{{"metadata":{{"uid":"cron-uid","resourceVersion":"cron-rv"}},"spec":{{"suspend":false}}}}\\n'
      fi
      ;;
    *" patch cronjob "*) return 0 ;;
    *" get namespace airflow-example -o json"*)
      printf '{{"metadata":{{"uid":"namespace-uid","resourceVersion":"namespace-rv"}}}}\\n'
      ;;
    *" get jobs "*" -o json"*)
      if [ "${{controller_deleted}}" -eq 1 ]; then
        printf '{{"metadata":{{"resourceVersion":"jobs-rv-2"}},"items":[{{"metadata":{{"name":"unrelated","uid":"other-uid"}}}}]}}\\n'
      else
        printf '{{"metadata":{{"resourceVersion":"jobs-rv-1"}},"items":[{{"metadata":{{"name":"retention-1","uid":"job-uid","resourceVersion":"job-rv","ownerReferences":[{{"kind":"CronJob","uid":"cron-uid","controller":true}}]}},"status":{{"active":0,"conditions":[{{"type":"Complete","status":"True"}}]}}}},{{"metadata":{{"name":"unrelated","uid":"other-uid","labels":{{"app.kubernetes.io/name":"dpone-runtime-pod-retention"}}}}}}]}}\\n'
      fi
      ;;
    *" get pods "*" -o json"*)
      if [ "${{controller_deleted}}" -eq 1 ]; then
        printf '{{"metadata":{{"resourceVersion":"pods-rv-2"}},"items":[]}}\\n'
      else
        printf '{{"metadata":{{"resourceVersion":"pods-rv-1"}},"items":[{{"metadata":{{"name":"retention-pod","uid":"pod-uid","resourceVersion":"pod-rv","ownerReferences":[{{"kind":"Job","uid":"job-uid","controller":true}}]}}}}]}}\n'
      fi
      ;;
    *" get pod/retention-pod -o json"*)
      printf '{{"metadata":{{"name":"retention-pod","uid":"pod-uid","resourceVersion":"pod-rv"}},"spec":{{"initContainers":[{{"name":"init-fetch"}}],"containers":[{{"name":"base"}}],"ephemeralContainers":[{{"name":"debugger"}}]}}}}\n'
      ;;
    *" logs pod/retention-pod "*) printf 'captured pod log\n' ;;
    *" get events "*) printf '{{"items":[]}}\\n' ;;
    *" get serviceaccount "*" -o json"*)
      printf '{{"metadata":{{"uid":"sa-uid","resourceVersion":"sa-rv"}}}}\\n'
      ;;
    *" get rolebinding "*" -o json"*)
      printf '{{"metadata":{{"uid":"rb-uid","resourceVersion":"rb-rv"}}}}\\n'
      ;;
    *" get role "*" -o json"*)
      printf '{{"metadata":{{"uid":"role-uid","resourceVersion":"role-rv"}}}}\\n'
      ;;
    *"--ignore-not-found -o name"*) return 0 ;;
    *) printf 'kind: Evidence\\n' ;;
  esac
}}
""",
    )

    assert result.returncode == 0
    calls = (tmp_path / "calls.txt").read_text(encoding="utf-8").splitlines()
    capture = next(index for index, call in enumerate(calls) if " get cronjob " in f" {call} " and "-o json" in call)
    suspend = next(index for index, call in enumerate(calls) if " patch cronjob " in f" {call} ")
    controller_delete = next(index for index, call in enumerate(calls) if " controller-jobs " in f" {call} ")
    access_delete = next(index for index, call in enumerate(calls) if " access " in f" {call} ")
    assert capture < suspend < controller_delete < access_delete
    assert '"op":"test"' in calls[suspend]
    evidence_dirs = [evidence_root / "operation"]
    exact_delete = (evidence_dirs[0] / "pre" / "exact-delete.py").read_text(encoding="utf-8")
    assert "V1DeleteOptions" in exact_delete
    assert "V1Preconditions(uid=uid, resource_version=resource_version)" in exact_delete
    assert "if exc.status == 404" in exact_delete
    assert "_request_timeout=request_timeout(deadline)" in exact_delete
    captures = [
        json.loads(line)
        for line in (evidence_dirs[0] / "pre" / "pod-log-captures.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert len(captures) == 6
    assert {item["container_kind"] for item in captures} == {"init", "container", "ephemeral"}
    assert {item["log_kind"] for item in captures} == {"current", "previous"}
    assert all((evidence_dirs[0] / "pre" / item["log_file"]).is_file() for item in captures)
    assert (evidence_dirs[0] / "pre" / "SHA256SUMS").is_file()
    suspended = json.loads((evidence_dirs[0] / "pre" / "cronjob-suspended.json").read_text(encoding="utf-8"))
    assert suspended["metadata"]["resourceVersion"] == "cron-rv-2"
    post_attempts = list((evidence_dirs[0] / "post").glob("attempt.*"))
    assert len(post_attempts) == 1
    assert (post_attempts[0] / "SHA256SUMS").is_file()
    assert (post_attempts[0] / "verification.txt").read_text(encoding="utf-8") == (
        "withdrawal_verified=true\nlate_owned_jobs=0\nlate_owned_pods=0\n"
    )


def test_withdrawal_blocks_late_owned_jobs_before_access_deletion() -> None:
    content = _read_docs(*RUNTIME_DOCS)
    block = _bash_block_after(content, "For full withdrawal, keep the reviewed context")

    late_job_check = block.index("reason=late_owned_jobs")
    access_delete = block.index('exact-delete.py" access')
    assert late_job_check < access_delete
    assert "withdrawal_verified=false" in block
    assert "exit 7" in block
    assert "late_owned_jobs_preserved" not in block
    assert block.index("reason=late_owned_pods") < access_delete


def test_withdrawal_requires_permissions_terminal_jobs_and_quiescence_before_delete() -> None:
    content = _read_docs(*RUNTIME_DOCS)
    block = _bash_block_after(content, "For full withdrawal, keep the reviewed context")

    permission_preflight = block.index("preflight_permissions")
    first_patch = block.index("kube patch cronjob")
    terminal_gate = block.index("owned retention Jobs must be terminal and inactive")
    quiescence_gate = block.index("owned Job inventory changed during forensic capture")
    seal = block.index("sha256sum -- * > SHA256SUMS")
    controller_delete = block.index('exact-delete.py" controller-jobs')

    assert permission_preflight < first_patch
    assert terminal_gate < quiescence_gate < seal < controller_delete
    assert "owned Pod inventory changed during forensic capture" in block
    assert "auth can-i" in block


def test_withdrawal_kubernetes_reads_and_logs_are_bounded() -> None:
    content = _read_docs(*RUNTIME_DOCS)
    block = _bash_block_after(content, "For full withdrawal, keep the reviewed context")

    assert "DPONE_RUNTIME_POD_WITHDRAW_REQUEST_TIMEOUT_SECONDS" in block
    assert "DPONE_RUNTIME_POD_WITHDRAW_MAX_JSON_BYTES" in block
    assert "DPONE_RUNTIME_POD_WITHDRAW_LOG_TAIL_LINES" in block
    assert "DPONE_RUNTIME_POD_WITHDRAW_LOG_LIMIT_BYTES" in block
    assert "DPONE_RUNTIME_POD_WITHDRAW_INVENTORY_LIMIT" in block
    assert "kube() {" in block
    assert '--request-timeout="${DPONE_RUNTIME_POD_WITHDRAW_REQUEST_TIMEOUT_SECONDS}s"' in block
    assert '--selector "${withdraw_selector}"' in block
    assert '--tail="${DPONE_RUNTIME_POD_WITHDRAW_LOG_TAIL_LINES}"' in block
    assert '--limit-bytes="${DPONE_RUNTIME_POD_WITHDRAW_LOG_LIMIT_BYTES}"' in block
    assert "capture_json_bounded" in block
    assert "capture_relevant_events" in block
    assert '(.metadata.continue // "") == ""' in block
    assert block.count('kubectl --context "${KUBE_CONTEXT}"') == 1


def test_full_withdrawal_can_resume_from_sealed_pre_delete_evidence() -> None:
    content = _read_docs(*RUNTIME_DOCS)
    block = _bash_block_after(content, "For full withdrawal, keep the reviewed context")

    assert "DPONE_RUNTIME_POD_WITHDRAW_EVIDENCE_DIR" in block
    assert 'if [ -f "${evidence_dir}/pre/SHA256SUMS" ]; then' in block
    assert "sha256sum -c SHA256SUMS" in block
    assert "sealed pre-delete evidence is required when the CronJob is absent" in block
    assert 'post_attempt_dir="$(mktemp -d "${evidence_dir}/post/attempt.XXXXXX")"' in block


def test_full_withdrawal_resumes_when_controller_is_already_absent(tmp_path: Path) -> None:
    content = _read_docs(*RUNTIME_DOCS)
    block = _bash_block_after(content, "For full withdrawal, keep the reviewed context")
    operation = tmp_path / "withdrawal"
    pre = operation / "pre"
    pre.mkdir(parents=True)
    removal_commit = "a" * 40
    verifier = write_withdrawal_ack_verifier(tmp_path)
    fixtures = {
        "cronjob.json": '{"metadata":{"uid":"cron-uid","resourceVersion":"cron-rv"}}\n',
        "cronjob-suspended.json": (
            '{"metadata":{"uid":"cron-uid","resourceVersion":"cron-rv-2"},"spec":{"suspend":true}}\n'
        ),
        "namespace.json": '{"metadata":{"uid":"namespace-uid","resourceVersion":"namespace-rv"}}\n',
        "operation.json": json.dumps(
            {
                "schema": "dpone.airflow-runtime-pod-retention-withdrawal-operation.v1",
                "removal_commit": removal_commit,
                "kube_context": "reviewed-dev",
                "namespace": "airflow-example",
                "namespace_uid": "namespace-uid",
                "alert_topology": "off",
                "cronjob_uid": "cron-uid",
                "acknowledgement_issuer": verifier.issuer,
                "ack_verifier_sha256": verifier.sha256,
            }
        )
        + "\n",
        "owned-jobs.json": "[]\n",
        "job-names.txt": "",
        "serviceaccount.json": '{"metadata":{"uid":"sa-uid","resourceVersion":"sa-rv"}}\n',
        "role.json": '{"metadata":{"uid":"role-uid","resourceVersion":"role-rv"}}\n',
        "rolebinding.json": '{"metadata":{"uid":"rb-uid","resourceVersion":"rb-rv"}}\n',
        "exact-delete.py": "# sealed deletion helper\n",
    }
    for name, payload in fixtures.items():
        (pre / name).write_text(payload, encoding="utf-8")
    subprocess.run("sha256sum -- * > FORENSIC-SHA256SUMS", cwd=pre, shell=True, check=True)
    publish_withdrawal_ack(
        verifier,
        evidence_directory=pre,
        manifest=pre / "FORENSIC-SHA256SUMS",
        operation=pre / "operation.json",
        output=pre / "durable-ack.json",
    )
    subprocess.run("sha256sum -- * > SHA256SUMS", cwd=pre, shell=True, check=True)

    wrong_context = _run_bash(
        block,
        cwd=tmp_path,
        prelude=f"""
KUBE_CONTEXT=other-cluster
AIRFLOW_NAMESPACE=airflow-example
DPONE_RUNTIME_POD_WITHDRAW_EVIDENCE_DIR={operation}
DPONE_RUNTIME_POD_WITHDRAW_REMOVAL_COMMIT={removal_commit}
DPONE_RUNTIME_POD_WITHDRAW_ACK=preserve-forensics-and-withdraw
DPONE_RUNTIME_POD_RETENTION_ALERTS=off
{verifier.shell_exports()}
kubectl() {{ printf '%s\n' "$*" >>"${{CALLS_FILE}}"; }}
""",
    )
    assert wrong_context.returncode == 9
    assert "sealed withdrawal identity does not match this retry" in wrong_context.stderr
    assert not (tmp_path / "calls.txt").exists()

    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude=f"""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
DPONE_RUNTIME_POD_WITHDRAW_EVIDENCE_DIR={operation}
DPONE_RUNTIME_POD_WITHDRAW_REMOVAL_COMMIT={removal_commit}
DPONE_RUNTIME_POD_WITHDRAW_ACK=preserve-forensics-and-withdraw
DPONE_RUNTIME_POD_RETENTION_ALERTS=off
{verifier.shell_exports()}
python3() {{
  case "$*" in *"exact-delete.py"*) return 0 ;; *) command python3 "$@" ;; esac
}}
    kubectl() {{
  printf '%s\\n' "$*" >>"${{CALLS_FILE}}"
      case "$*" in
        *" get cronjob "*) return 91 ;;
        *" get namespace airflow-example -o json"*)
          printf '{{"metadata":{{"uid":"namespace-uid","resourceVersion":"namespace-rv"}}}}\n'
          ;;
    *" get jobs "*) printf '{{"metadata":{{"resourceVersion":"jobs-rv"}},"items":[]}}\\n' ;;
    *" get pods "*) printf '{{"metadata":{{"resourceVersion":"pods-rv"}},"items":[]}}\\n' ;;
    *"--ignore-not-found -o name"*) return 0 ;;
  esac
}}
""",
    )

    assert result.returncode == 0, result.stderr
    assert not any(" get cronjob " in f" {line} " for line in (tmp_path / "calls.txt").read_text().splitlines())
    assert (operation / "withdrawal.json").is_file()
    assert (operation / "SHA256SUMS").is_file()
    withdrawal = json.loads((operation / "withdrawal.json").read_text(encoding="utf-8"))
    assert withdrawal["kube_context"] == "reviewed-dev"
    assert withdrawal["namespace"] == "airflow-example"
    assert withdrawal["namespace_uid"] == "namespace-uid"
    assert withdrawal["alert_topology"] == "off"
    assert withdrawal["operation_sha256"].startswith("sha256:")


def test_full_withdrawal_removes_the_optional_alert_resource_exactly() -> None:
    content = _read_docs(*RUNTIME_DOCS)
    block = _bash_block_after(content, "For full withdrawal, keep the reviewed context")

    assert "DPONE_RUNTIME_POD_RETENTION_ALERTS" in block
    assert "get prometheusrule" in block
    assert "dpone-runtime-pod-retention -o json" in block
    assert 'load("prometheusrule.json")' in block
    assert 'delete_custom_exact("dpone-runtime-pod-retention"' in block
    assert "V1Preconditions(uid=uid, resource_version=resource_version)" in block
    assert "require_absent prometheusrule.monitoring.coreos.com" in block


def test_withdrawal_evidence_failure_prevents_delete(tmp_path: Path) -> None:
    content = _read_docs(*RUNTIME_DOCS)
    block = _bash_block_after(content, "For full withdrawal, keep the reviewed context")
    evidence_root = tmp_path / "evidence"
    evidence_root.mkdir()
    verifier = write_withdrawal_ack_verifier(tmp_path)
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude=f"""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
DPONE_RUNTIME_POD_WITHDRAW_EVIDENCE_DIR={evidence_root}/operation
DPONE_RUNTIME_POD_WITHDRAW_REMOVAL_COMMIT={"a" * 40}
DPONE_RUNTIME_POD_WITHDRAW_ACK=preserve-forensics-and-withdraw
DPONE_RUNTIME_POD_RETENTION_ALERTS=off
{verifier.shell_exports()}
kubectl() {{
  printf '%s\\n' "$*" >>"${{CALLS_FILE}}"
  case "$*" in
        *" get cronjob "*" -o json"*)
          if grep -q ' patch cronjob ' "${{CALLS_FILE}}"; then
            printf '{{"metadata":{{"uid":"cron-uid","resourceVersion":"cron-rv-2"}},"spec":{{"suspend":true}}}}\\n'
          else
            printf '{{"metadata":{{"uid":"cron-uid","resourceVersion":"cron-rv"}},"spec":{{"suspend":false}}}}\\n'
          fi
          ;;
        *" get namespace airflow-example -o json"*)
          printf '{{"metadata":{{"uid":"namespace-uid"}}}}\\n'
          ;;
    *" get serviceaccount "*) printf '{{"metadata":{{"uid":"sa-uid","resourceVersion":"sa-rv"}}}}\\n' ;;
    *" get rolebinding "*) printf '{{"metadata":{{"uid":"rb-uid","resourceVersion":"rb-rv"}}}}\\n' ;;
    *" get role "*) printf '{{"metadata":{{"uid":"role-uid","resourceVersion":"role-rv"}}}}\\n' ;;
    *" get jobs "*" -o json"*) printf '{{"items":[]}}\\n' ;;
    *" get pods "*" -o json"*) printf '{{"items":[]}}\\n' ;;
    *" get events "*) return 9 ;;
    *) printf 'kind: Evidence\\n' ;;
  esac
}}
""",
    )

    assert result.returncode == 9
    calls = (tmp_path / "calls.txt").read_text(encoding="utf-8").splitlines()
    assert not any("exact-delete.py" in call for call in calls)


def test_withdrawal_api_error_is_not_treated_as_absence(tmp_path: Path) -> None:
    content = _read_docs(*RUNTIME_DOCS)
    block = _bash_block_after(content, "For full withdrawal, keep the reviewed context")
    function_body = block.split("  require_absent() {", 1)[1].split(
        "  require_absent cronjob",
        1,
    )[0]
    result = _run_bash(
        "require_absent() {" + function_body + "\nrequire_absent cronjob retained /tmp/absence-check",
        cwd=tmp_path,
        prelude="""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
kubectl() { printf 'forbidden\n' >&2; return 77; }
kube() { kubectl "$@"; }
""",
    )

    assert result.returncode == 6
    assert "could not verify absence" in result.stderr


def test_desired_state_authority_read_failure_is_not_first_install(tmp_path: Path) -> None:
    content = (DOCS / "airflow-desired-state.md").read_text(encoding="utf-8")
    block = _bash_block_after(content, "The first installation and every intentional authority")
    authority = tmp_path / "authority.json"
    authority.write_text("{}\n", encoding="utf-8")
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude=f"""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
DPONE_AIRFLOW_AUTHORITY_JSON={authority}
DPONE_AIRFLOW_AUTHORITY_APPLY_ACK=review-and-apply-authority
dpone() {{ return 0; }}
kubectl() {{
  printf '%s\n' "$*" >>"${{CALLS_FILE}}"
  case "$*" in
    *" get namespace "*) return 0 ;;
    *" get configmap dpone-airflow-desired-state-authority --ignore-not-found "*) return 3 ;;
    *) return 0 ;;
  esac
}}
""",
    )

    assert result.returncode == 3
    calls = (tmp_path / "calls.txt").read_text(encoding="utf-8").splitlines()
    assert not any(" apply " in f" {call} " for call in calls)
    assert not list(tmp_path.glob("airflow-desired-state-authority.*/previous.absent"))


def test_candidate_install_requires_external_reviewed_sha() -> None:
    content = (ROOT / "packages" / "dpone-airflow-pack" / "README.md").read_text(encoding="utf-8")

    assert "DPONE_REVIEWED_CANDIDATE_SHA:?set the exact externally reviewed" in content
    assert 'DPONE_CANDIDATE_SHA="$(git rev-parse HEAD)"' not in content
    assert 'test "$(cat "${DIST_AIRFLOW}/SOURCE_COMMIT")" = "${DPONE_REVIEWED_CANDIDATE_SHA}"' in content


def test_candidate_install_verifies_the_immutable_helm_chart_artifact() -> None:
    content = (ROOT / "packages" / "dpone-airflow-pack" / "README.md").read_text(encoding="utf-8")

    assert "airflow-helm-ack-mounts-${DPONE_CANDIDATE_SHA}" in content
    assert 'test "$(cat "${HELM_EVIDENCE}/SOURCE_COMMIT")" = "${DPONE_REVIEWED_CANDIDATE_SHA}"' in content
    assert "CHART_SHA256SUMS" in content
    assert 'helm pull apache-airflow/airflow --version "${CHART_VERSION}"' in content
    assert 'sha256sum -c "${HELM_EVIDENCE}/CHART_SHA256SUMS"' in content
    assert 'install -m 0444 "${CHART_DIR}/airflow-${CHART_VERSION}.tgz"' in content
    assert 'helm template airflow "airflow-${CHART_VERSION}.tgz"' in content


def test_approval_withdrawal_waits_for_emitted_plan_only_cycle_and_times_out(tmp_path: Path) -> None:
    content = (DOCS / "airflow-cache-retention-approval.md").read_text(encoding="utf-8")
    block = _bash_block_after(content, "After removing the approval in GitOps")
    common = """
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
AIRFLOW_RELEASE=airflow
DPONE_CACHE_PARSER_WORKLOAD=deployment/airflow-dag-processor
DPONE_CACHE_WATCH_CONTAINER=dpone-cache-watch
APPROVAL_REMOVED_AT=2026-08-04T00:00:00Z
APPROVAL_REMOVAL_COMMIT=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
DPONE_APPROVAL_WITHDRAWAL_EVIDENCE_DIR="$PWD/evidence"
DPONE_APPROVAL_WITHDRAWAL_MAX_WAIT_SECONDS=10
DPONE_APPROVAL_WITHDRAWAL_POLL_SECONDS=1
sleep() { SECONDS=$((SECONDS + $1)); }
kubectl() {
  printf '%s\\n' "$*" >>"${CALLS_FILE}"
  case "$*" in
    *" get namespace airflow-example -o json")
      printf '%s\\n' '{"metadata":{"uid":"namespace-uid"}}'
      ;;
    *" get deployment/airflow-dag-processor -o json")
      printf '%s\\n' '{"kind":"Deployment","metadata":{"generation":2,"uid":"workload-uid"},"spec":{"replicas":1,"selector":{"matchLabels":{"app":"parser"}},"template":{"spec":{"containers":[{"name":"dpone-cache-watch","env":[{"name":"DPONE_PACK_SYNC_INTERVAL_SECONDS","value":"2"},{"name":"DPONE_PACK_SYNC_TIMEOUT_SECONDS","value":"1"},{"name":"DPONE_PACK_RETENTION_INTERVAL_CYCLES","value":"3"}],"envFrom":[{"secretRef":{"name":"dpone-airflow-artifacts-reader"}}]}]}}},"status":{"observedGeneration":2,"updatedReplicas":1,"readyReplicas":1,"availableReplicas":1}}'
      ;;
    *" get secret/dpone-airflow-artifacts-reader --ignore-not-found -o json")
      printf '%s\\n' '{"data":{"AIRFLOW_CONN_S3":"redacted"}}'
      ;;
    *" get pods -l app=parser -o json")
      printf '%s\\n' '{"items":[{"metadata":{"name":"parser-0","uid":"pod-uid","resourceVersion":"pod-rv","creationTimestamp":"2026-08-04T00:00:01Z"},"spec":{"containers":[{"name":"dpone-cache-watch","env":[],"envFrom":[{"secretRef":{"name":"dpone-airflow-artifacts-reader"}}]}]},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}'
      ;;
    *" logs pod/parser-0 "*)
      attempts_file="${CALLS_FILE}.attempts"
      attempts=0
      [ ! -f "${attempts_file}" ] || attempts="$(cat "${attempts_file}")"
      attempts=$((attempts + 1))
      printf '%s\\n' "${attempts}" >"${attempts_file}"
      [ "${EMIT_APPROVAL_MISSING:-0}" -eq 0 ] || [ "${attempts}" -lt 2 ] || \
        printf '%s\\n' 'dpone cache retention plan-only reason=approval_missing plan_sha256=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
      ;;
  esac
}
helm() { printf '%s\\n' '{"version":7}'; }
"""

    success_dir = tmp_path / "success"
    success_dir.mkdir()
    success = _run_bash(block, cwd=success_dir, prelude="EMIT_APPROVAL_MISSING=1\n" + common)
    assert success.returncode == 0, success.stderr
    assert int((success_dir / "calls.txt.attempts").read_text(encoding="utf-8")) >= 2
    result_path = success_dir / "evidence" / "approval-withdrawal.json"
    sealed_result = json.loads(result_path.read_text(encoding="utf-8"))
    assert sealed_result["removal_commit"] == "a" * 40
    assert sealed_result["kube_context"] == "reviewed-dev"
    assert sealed_result["namespace"] == "airflow-example"
    assert sealed_result["namespace_uid"] == "namespace-uid"
    assert sealed_result["workload_uid"] == "workload-uid"
    assert sealed_result["helm_release"] == "airflow"
    assert sealed_result["helm_revision"] == 7
    assert sealed_result["watcher_container"] == "dpone-cache-watch"
    assert sealed_result["pod_evidence"][0]["uid"] == "pod-uid"
    assert sealed_result["log_evidence"][0]["sha256"].startswith("sha256:")
    assert (success_dir / "evidence" / "SHA256SUMS").is_file()
    repeated = _run_bash(block, cwd=success_dir, prelude="EMIT_APPROVAL_MISSING=1\n" + common)
    assert repeated.returncode == 0, repeated.stderr
    calls_before_wrong_context = (success_dir / "calls.txt").read_text(encoding="utf-8").splitlines()
    wrong_context = _run_bash(
        block,
        cwd=success_dir,
        prelude="EMIT_APPROVAL_MISSING=1\n" + common.replace("KUBE_CONTEXT=reviewed-dev", "KUBE_CONTEXT=other"),
    )
    assert wrong_context.returncode == 5
    assert "sealed approval-withdrawal identity does not match this live occurrence" in wrong_context.stderr
    calls_after_wrong_context = (success_dir / "calls.txt").read_text(encoding="utf-8").splitlines()
    assert len(calls_after_wrong_context) == len(calls_before_wrong_context) + 2
    assert not any(" logs " in f" {call} " for call in calls_after_wrong_context[len(calls_before_wrong_context) :])

    timeout_dir = tmp_path / "timeout"
    timeout_dir.mkdir()
    timeout = _run_bash(block, cwd=timeout_dir, prelude="EMIT_APPROVAL_MISSING=0\n" + common)
    assert timeout.returncode != 0
    assert "approval withdrawal timed out" in timeout.stderr
    assert "configured_cycle_seconds=9" in timeout.stderr


def test_approval_withdrawal_seals_removal_commit_and_runtime_evidence() -> None:
    content = (DOCS / "airflow-cache-retention-approval.md").read_text(encoding="utf-8")
    block = _bash_block_after(content, "After removing the approval in GitOps")

    assert "DPONE_APPROVAL_WITHDRAWAL_EVIDENCE_DIR" in block
    assert "APPROVAL_REMOVAL_COMMIT" in block
    assert "approval-withdrawal.json" in block
    assert "SHA256SUMS" in block
    assert 'chmod 0400 "${withdrawal_evidence_dir}"/*' in block
    assert '--tail="${DPONE_APPROVAL_WITHDRAWAL_LOG_TAIL_LINES}"' in block
    assert '--limit-bytes="${DPONE_APPROVAL_WITHDRAWAL_LOG_LIMIT_BYTES}"' in block
    assert "removal_commit" in block
    assert "kube_context" in block
    assert "namespace" in block
    assert "namespace_uid" in block
    assert "workload_uid" in block
    assert "helm_release" in block
    assert "helm_revision" in block
    assert "watcher_container" in block
    assert "workload_generation" in block
    assert "pod_evidence" in block
    assert "log_evidence" in block
    assert block.count('kubectl --context "${KUBE_CONTEXT}"') == 1


@pytest.mark.parametrize(
    ("observed_environment", "reviewed_environment", "expected_returncode"),
    (("dev", "dev", 0), ("prod", "dev", 4)),
)
def test_post_apply_verifier_binds_v3_noop_to_reviewed_environment(
    tmp_path: Path,
    observed_environment: str,
    reviewed_environment: str,
    expected_returncode: int,
) -> None:
    content = (DOCS / "airflow-cache-retention-approval.md").read_text(encoding="utf-8")
    block = _bash_block_after(content, "After the one-time approval is deployed")
    digest = "sha256:" + "a" * 64
    plan_body: dict[str, object] = {
        "schema": "dpone.deployment-cache-retention-plan.v1",
        "environment": reviewed_environment,
        "current_deployment_id": None,
        "protected_deployment_ids": [],
        "items": [],
        "delete_candidates": [],
        "recovery_revision": digest,
    }
    plan = {**plan_body, "plan_sha256": canonical_digest(plan_body)}
    review_evidence = tmp_path / "review-evidence"
    _write_retention_review_evidence(review_evidence, plan)
    apply: dict[str, object] = {
        "schema": "dpone.deployment-cache-retention-apply.v3",
        "environment": observed_environment,
        "promoted_by": "ci://retention",
        "current_deployment_id": None,
        "items": [],
        "deleted_deployment_ids": [],
        "skipped_deployment_ids": [],
        "reviewed_plan_sha256": plan["plan_sha256"],
    }
    snapshot = tmp_path / "cache-status.json"
    snapshot.write_text(
        json.dumps(
            {
                "warnings": [],
                "operational_status": {
                    "retention_apply_publication_failure": None,
                    "retention_apply": apply,
                    "retention_apply_publication": _retention_publication(
                        "dpone.deployment-cache-retention-apply.v3", apply
                    ),
                },
            }
        ),
        encoding="utf-8",
    )

    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude=(
            f'CACHE_STATUS_SNAPSHOT="{snapshot}"\n'
            f'DPONE_ENVIRONMENT="{reviewed_environment}"\n'
            f'DPONE_CACHE_RETENTION_APPROVED_PLAN_SHA256="{plan["plan_sha256"]}"\n'
            "DPONE_CACHE_RETENTION_REVIEW_ID=00000000-0000-4000-8000-000000000001\n"
            f'DPONE_CACHE_RETENTION_REVIEW_EVIDENCE_DIR="{review_evidence}"\n'
        ),
    )

    assert result.returncode == expected_returncode, result.stderr
    if expected_returncode == 0:
        assert "verified plan-bound schema-valid v3 no-op" in result.stdout
    else:
        assert "retention apply environment mismatch" in result.stderr


def test_post_apply_verifier_accepts_environment_bound_destructive_receipt(tmp_path: Path) -> None:
    content = (DOCS / "airflow-cache-retention-approval.md").read_text(encoding="utf-8")
    block = _bash_block_after(content, "After the one-time approval is deployed")
    digest = "sha256:" + "a" * 64
    recovery = "sha256:" + "b" * 64
    plan_body: dict[str, object] = {
        "schema": "dpone.deployment-cache-retention-plan.v1",
        "environment": "dev",
        "current_deployment_id": None,
        "protected_deployment_ids": [],
        "items": [
            {
                "deployment_id": digest,
                "action": "delete",
                "reason": "unreferenced",
                "path": "/cache/generations/stale",
            }
        ],
        "delete_candidates": [digest],
        "recovery_revision": recovery,
    }
    plan = {**plan_body, "plan_sha256": canonical_digest(plan_body)}
    review_evidence = tmp_path / "review-evidence"
    _write_retention_review_evidence(review_evidence, plan)
    review_id = "00000000-0000-4000-8000-000000000001"
    operation_id = retention_operation_id(
        environment="dev",
        reviewed_plan_sha256=str(plan["plan_sha256"]),
        review_id=review_id,
    )
    apply: dict[str, object] = {
        "schema": "dpone.deployment-cache-retention-apply.v3",
        "environment": "dev",
        "promoted_by": "ci://retention",
        "current_deployment_id": None,
        "items": [
            {
                "deployment_id": digest,
                "action": "deleted",
                "reason": "unreferenced",
                "path": "/cache/generations/stale",
            }
        ],
        "deleted_deployment_ids": [digest],
        "skipped_deployment_ids": [],
        "reviewed_plan_sha256": plan["plan_sha256"],
        "activation_history_revision": recovery,
        "operation_id": operation_id,
        "review_id": review_id,
        "receipt_revision": digest,
        "transaction_status": "committed",
    }
    snapshot = tmp_path / "cache-status.json"
    snapshot.write_text(
        json.dumps(
            {
                "warnings": [],
                "operational_status": {
                    "retention_apply_publication_failure": None,
                    "retention_apply": apply,
                    "retention_apply_publication": _retention_publication(
                        "dpone.deployment-cache-retention-apply.v3", apply
                    ),
                },
            }
        ),
        encoding="utf-8",
    )

    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude=(
            f'CACHE_STATUS_SNAPSHOT="{snapshot}"\n'
            "DPONE_ENVIRONMENT=dev\n"
            f'DPONE_CACHE_RETENTION_APPROVED_PLAN_SHA256="{plan["plan_sha256"]}"\n'
            f'DPONE_CACHE_RETENTION_REVIEW_ID="{review_id}"\n'
            f'DPONE_CACHE_RETENTION_REVIEW_EVIDENCE_DIR="{review_evidence}"\n'
        ),
    )

    assert result.returncode == 0, result.stderr
    assert "verified plan-bound destructive v3 apply evidence" in result.stdout


@pytest.mark.parametrize(
    ("cache_volume", "runtime_image"),
    (
        ({"persistentVolumeClaim": {"claimName": "live-v2"}}, "repo/old@sha256:" + "a" * 64),
        ({"emptyDir": {}}, "repo/wrong@sha256:" + "c" * 64),
    ),
)
def test_empty_dir_downgrade_rejects_pvc_or_wrong_runtime_image(
    tmp_path: Path,
    cache_volume: dict[str, object],
    runtime_image: str,
) -> None:
    content = (DOCS / "airflow-cache-retention-state-downgrade.md").read_text(encoding="utf-8")
    block = _bash_block_after(content, "Promote the exact older image")
    evidence = tmp_path / "evidence"
    evidence.mkdir()
    (evidence / "occurrence.json").write_text(
        json.dumps(
            {
                "kube_context": "reviewed-dev",
                "namespace": "airflow-example",
                "namespace_uid": "namespace-uid",
                "workload": "deployment/parser",
                "workload_uid": "workload-uid",
                "storage_kind": "emptyDir",
            }
        ),
        encoding="utf-8",
    )
    (evidence / "namespace.json").write_text('{"metadata":{"uid":"namespace-uid"}}', encoding="utf-8")
    (evidence / "old-pod-uids.txt").write_text("old-pod-uid\n", encoding="utf-8")
    subprocess.run(
        "sha256sum occurrence.json namespace.json old-pod-uids.txt >SHA256SUMS",
        cwd=evidence,
        shell=True,
        check=True,
    )
    wrong_workload = json.dumps(
        {
            "metadata": {"uid": "workload-uid"},
            "spec": {
                "selector": {"matchLabels": {"app": "parser"}},
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "name": "cache-watch",
                                "image": "repo/old@sha256:" + "a" * 64,
                                "volumeMounts": [{"name": "cache", "mountPath": "/cache"}],
                            },
                            {"name": "airflow", "image": runtime_image},
                        ],
                        "volumes": [{"name": "cache", **cache_volume}],
                    }
                },
            },
        }
    )
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude=f"""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
PARSE_AUTHORITY_WORKLOAD=deployment/parser
CACHE_STATUS_CONTAINER=cache-watch
AIRFLOW_RUNTIME_CONTAINER=airflow
CACHE_ROOT=/cache
DOWNGRADE_EVIDENCE_DIR="{evidence}"
EXPECTED_DEPLOYMENT_ID=sha256:{"b" * 64}
EXPECTED_RUNTIME_IMAGE=repo/old@sha256:{"a" * 64}
EXPECTED_CACHE_STATUS_IMAGE=repo/old@sha256:{"a" * 64}
EXPECTED_DPONE_VERSION=0.73.32
EXPECTED_AIRFLOW_VERSION=3.2.0
kubectl() {{
  case "$*" in
    *" rollout status deployment/parser "*) return 0 ;;
    *" get deployment/parser -o json") printf '%s\\n' '{wrong_workload}' ;;
  esac
}}
""",
    )

    assert result.returncode != 0
    assert not (evidence / "RESTORE_SHA256SUMS").exists()


def test_approval_withdrawal_rejects_active_workload_approval(tmp_path: Path) -> None:
    content = (DOCS / "airflow-cache-retention-approval.md").read_text(encoding="utf-8")
    block = _bash_block_after(content, "After removing the approval in GitOps")
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude="""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
AIRFLOW_RELEASE=airflow
DPONE_CACHE_PARSER_WORKLOAD=deployment/airflow-dag-processor
DPONE_CACHE_WATCH_CONTAINER=dpone-cache-watch
APPROVAL_REMOVED_AT=2026-08-04T00:00:00Z
APPROVAL_REMOVAL_COMMIT=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
DPONE_APPROVAL_WITHDRAWAL_EVIDENCE_DIR="$PWD/evidence"
DPONE_APPROVAL_WITHDRAWAL_MAX_WAIT_SECONDS=10
DPONE_APPROVAL_WITHDRAWAL_POLL_SECONDS=1
kubectl() {
  printf '%s\n' "$*" >>"${CALLS_FILE}"
  case "$*" in
    *" get namespace airflow-example -o json")
      printf '%s\n' '{"metadata":{"uid":"namespace-uid"}}'
      ;;
    *" get deployment/airflow-dag-processor -o json")
      printf '%s\n' '{"kind":"Deployment","metadata":{"generation":2,"uid":"workload-uid"},"spec":{"replicas":1,"selector":{"matchLabels":{"app":"parser"}},"template":{"spec":{"containers":[{"name":"dpone-cache-watch","env":[{"name":"DPONE_CACHE_RETENTION_APPROVED_PLAN_SHA256","value":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]}]}}},"status":{"observedGeneration":2,"updatedReplicas":1,"readyReplicas":1,"availableReplicas":1}}'
      ;;
  esac
}
helm() { printf '%s\n' '{"version":7}'; }
""",
    )

    assert result.returncode == 4
    assert "still has an approval value" in result.stderr
    assert not any(" logs " in f" {call} " for call in (tmp_path / "calls.txt").read_text().splitlines())


def test_approval_withdrawal_rejects_approval_in_env_from_secret(tmp_path: Path) -> None:
    content = (DOCS / "airflow-cache-retention-approval.md").read_text(encoding="utf-8")
    block = _bash_block_after(content, "After removing the approval in GitOps")
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude="""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
AIRFLOW_RELEASE=airflow
DPONE_CACHE_PARSER_WORKLOAD=deployment/airflow-dag-processor
DPONE_CACHE_WATCH_CONTAINER=dpone-cache-watch
APPROVAL_REMOVED_AT=2026-08-04T00:00:00Z
APPROVAL_REMOVAL_COMMIT=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
DPONE_APPROVAL_WITHDRAWAL_EVIDENCE_DIR="$PWD/evidence"
DPONE_APPROVAL_WITHDRAWAL_MAX_WAIT_SECONDS=10
DPONE_APPROVAL_WITHDRAWAL_POLL_SECONDS=1
kubectl() {
  printf '%s\n' "$*" >>"${CALLS_FILE}"
  case "$*" in
    *" get namespace airflow-example -o json")
      printf '%s\n' '{"metadata":{"uid":"namespace-uid"}}'
      ;;
    *" get deployment/airflow-dag-processor -o json")
      printf '%s\n' '{"kind":"Deployment","metadata":{"generation":2,"uid":"workload-uid"},"spec":{"replicas":1,"selector":{"matchLabels":{"app":"parser"}},"template":{"spec":{"containers":[{"name":"dpone-cache-watch","env":[],"envFrom":[{"secretRef":{"name":"retention-approval"}}]}]}}},"status":{"observedGeneration":2,"updatedReplicas":1,"readyReplicas":1,"availableReplicas":1}}'
      ;;
    *" get secret/retention-approval --ignore-not-found -o json")
      printf '%s\n' '{"data":{"DPONE_CACHE_RETENTION_REVIEW_ID":"redacted"}}'
      ;;
  esac
}
helm() { printf '%s\n' '{"version":7}'; }
""",
    )

    assert result.returncode == 4
    assert "envFrom source can still inject a retention approval" in result.stderr
    assert not any(" logs " in f" {call} " for call in (tmp_path / "calls.txt").read_text().splitlines())


def test_approval_withdrawal_does_not_treat_optional_env_from_lookup_failure_as_absence(tmp_path: Path) -> None:
    content = (DOCS / "airflow-cache-retention-approval.md").read_text(encoding="utf-8")
    block = _bash_block_after(content, "After removing the approval in GitOps")
    result = _run_bash(
        block,
        cwd=tmp_path,
        prelude="""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
AIRFLOW_RELEASE=airflow
DPONE_CACHE_PARSER_WORKLOAD=deployment/airflow-dag-processor
DPONE_CACHE_WATCH_CONTAINER=dpone-cache-watch
APPROVAL_REMOVED_AT=2026-08-04T00:00:00Z
APPROVAL_REMOVAL_COMMIT=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
DPONE_APPROVAL_WITHDRAWAL_EVIDENCE_DIR="$PWD/evidence"
kubectl() {
  case "$*" in
    *" get namespace airflow-example -o json")
      printf '%s\n' '{"metadata":{"uid":"namespace-uid"}}'
      ;;
    *" get deployment/airflow-dag-processor -o json")
      printf '%s\n' '{"kind":"Deployment","metadata":{"generation":2,"uid":"workload-uid"},"spec":{"replicas":1,"selector":{"matchLabels":{"app":"parser"}},"template":{"spec":{"containers":[{"name":"dpone-cache-watch","env":[],"envFrom":[{"secretRef":{"name":"optional-approval","optional":true}}]}]}}},"status":{"observedGeneration":2,"updatedReplicas":1,"readyReplicas":1,"availableReplicas":1}}'
      ;;
    *" get secret/optional-approval --ignore-not-found -o json")
      printf 'Error from server (Forbidden): secrets is forbidden\n' >&2
      return 1
      ;;
  esac
}
helm() { printf '%s\n' '{"version":7}'; }
""",
    )

    assert result.returncode == 4
    assert "envFrom source lookup failed: secret/optional-approval" in result.stderr
    assert not (tmp_path / "evidence" / "approval-withdrawal.json").exists()
