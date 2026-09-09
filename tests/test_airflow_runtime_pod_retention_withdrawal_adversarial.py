from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from tests.support.runtime_pod_withdrawal import (
    WithdrawalAckVerifier,
    publish_withdrawal_ack,
    write_withdrawal_ack_verifier,
)

ROOT = Path(__file__).parents[1]
RUNBOOK = ROOT / "docs" / "airflow-runtime-pod-retention-withdrawal.md"


def _withdrawal_block() -> str:
    content = RUNBOOK.read_text(encoding="utf-8")
    tail = content.split("For full withdrawal, keep the reviewed context", 1)[1]
    return tail.split("```bash", 1)[1].split("```", 1)[0].strip()


def _seal(directory: Path, manifest_name: str) -> None:
    import hashlib

    entries = []
    for path in sorted(directory.iterdir()):
        if path.name == manifest_name:
            continue
        entries.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n")
    (directory / manifest_name).write_text("".join(entries), encoding="utf-8")


def _retry_evidence(
    tmp_path: Path,
    *,
    receipt_overrides: dict[str, object] | None = None,
) -> tuple[Path, WithdrawalAckVerifier]:
    operation_root = tmp_path / "withdrawal"
    pre = operation_root / "pre"
    (operation_root / "post").mkdir(parents=True)
    pre.mkdir()
    verifier = write_withdrawal_ack_verifier(tmp_path, receipt_overrides=receipt_overrides)
    operation = {
        "schema": "dpone.airflow-runtime-pod-retention-withdrawal-operation.v1",
        "removal_commit": "a" * 40,
        "kube_context": "reviewed-dev",
        "namespace": "airflow-example",
        "namespace_uid": "namespace-uid",
        "alert_topology": "prometheus",
        "cronjob_uid": "cron-uid",
        "acknowledgement_issuer": verifier.issuer,
        "ack_verifier_sha256": verifier.sha256,
    }
    fixtures = {
        "cronjob.json": {"metadata": {"uid": "cron-uid", "resourceVersion": "cron-rv"}},
        "cronjob-suspended.json": {
            "metadata": {"uid": "cron-uid", "resourceVersion": "cron-rv-2"},
            "spec": {"suspend": True},
        },
        "namespace.json": {"metadata": {"uid": "namespace-uid"}},
        "operation.json": operation,
        "owned-jobs.json": [{"name": "retention-1", "uid": "job-uid", "resource_version": "job-rv"}],
        "serviceaccount.json": {"metadata": {"uid": "sa-uid", "resourceVersion": "sa-rv"}},
        "role.json": {"metadata": {"uid": "role-uid", "resourceVersion": "role-rv"}},
        "rolebinding.json": {"metadata": {"uid": "rb-uid", "resourceVersion": "rb-rv"}},
        "prometheusrule.json": {"metadata": {"uid": "monitoring-uid", "resourceVersion": "monitoring-rv"}},
    }
    for name, payload in fixtures.items():
        (pre / name).write_text(json.dumps(payload) + "\n", encoding="utf-8")
    (pre / "job-names.txt").write_text("retention-1\n", encoding="utf-8")
    (pre / "exact-delete.py").write_text("# sealed helper\n", encoding="utf-8")
    _seal(pre, "FORENSIC-SHA256SUMS")

    publish_withdrawal_ack(
        verifier,
        evidence_directory=pre,
        manifest=pre / "FORENSIC-SHA256SUMS",
        operation=pre / "operation.json",
        output=pre / "durable-ack.json",
    )
    _seal(pre, "SHA256SUMS")
    return operation_root, verifier


def _run(
    block: str,
    *,
    cwd: Path,
    operation_root: Path,
    verifier: WithdrawalAckVerifier,
) -> subprocess.CompletedProcess[str]:
    prelude = f"""
KUBE_CONTEXT=reviewed-dev
AIRFLOW_NAMESPACE=airflow-example
DPONE_RUNTIME_POD_WITHDRAW_EVIDENCE_DIR={operation_root}
DPONE_RUNTIME_POD_WITHDRAW_REMOVAL_COMMIT={"a" * 40}
DPONE_RUNTIME_POD_WITHDRAW_ACK=preserve-forensics-and-withdraw
DPONE_RUNTIME_POD_RETENTION_ALERTS=prometheus
{verifier.shell_exports()}
python3() {{
  case "$*" in
    *"exact-delete.py"*) printf '%s\\n' "$*" >>"${{MUTATIONS_FILE}}"; return 0 ;;
    *) command python3 "$@" ;;
  esac
}}
kubectl() {{
  printf '%s\\n' "$*" >>"${{CALLS_FILE}}"
  case "$*" in
    *" get namespace airflow-example -o json"*)
      printf '%s\\n' '{{"metadata":{{"uid":"namespace-uid"}}}}'
      ;;
    *" get jobs "*" -o json"*)
      printf '%s\\n' '{{"metadata":{{"resourceVersion":"jobs-rv"}},"items":[]}}'
      ;;
    *" get pods "*" -o json"*)
      count_file="${{CALLS_FILE}}.pod-count"
      count=0
      [ ! -f "${{count_file}}" ] || count="$(command cat "${{count_file}}")"
      count=$((count + 1))
      printf '%s\\n' "${{count}}" >"${{count_file}}"
      if [ "${{count}}" -eq 1 ]; then
        printf '%s\\n' '{{"metadata":{{"resourceVersion":"pods-rv-1"}},"items":[]}}'
      else
        printf '%s\\n' '{{"metadata":{{"resourceVersion":"pods-rv-2"}},"items":[{{"metadata":{{"name":"late-pod","uid":"late-pod-uid","ownerReferences":[{{"kind":"Job","uid":"job-uid","controller":true}}]}}}}]}}'
      fi
      ;;
    *"--ignore-not-found -o name"*) return 0 ;;
  esac
}}
"""
    return subprocess.run(
        ["bash", "-c", f"{prelude}\n{block}"],
        cwd=cwd,
        env={
            **os.environ,
            "CALLS_FILE": str(cwd / "calls.txt"),
            "MUTATIONS_FILE": str(cwd / "mutations.txt"),
        },
        check=False,
        text=True,
        capture_output=True,
    )


@pytest.mark.parametrize(
    "receipt_overrides",
    (
        {"evidence_classification": "internal"},
        {"encryption": "at_rest_only"},
        {"evidence_manifest_sha256": "sha256:" + "0" * 64},
    ),
)
def test_unqualified_external_acknowledgement_blocks_every_mutation(
    tmp_path: Path,
    receipt_overrides: dict[str, object],
) -> None:
    operation_root, verifier = _retry_evidence(tmp_path, receipt_overrides=receipt_overrides)

    result = _run(_withdrawal_block(), cwd=tmp_path, operation_root=operation_root, verifier=verifier)

    assert result.returncode != 0
    assert "durable acknowledgement" in result.stderr
    assert not (tmp_path / "mutations.txt").exists()


def test_pre_mutation_ack_job_revalidation_and_monitoring_order_are_explicit() -> None:
    block = _withdrawal_block()

    forensic_seal = block.index("sha256sum -- * > FORENSIC-SHA256SUMS")
    durable_ack = block.index('verify_durable_ack "${ack_tmp}"')
    suspend = block.index("kube patch cronjob")
    controller_delete = block.index('exact-delete.py" controller-jobs')
    second_job_snapshot = block.index("jobs-quiescence.json")
    second_job_gate = block.index("became active or non-terminal during forensic capture")
    final_jobs = block.index("jobs-final-before-access.json")
    final_pods = block.index("pods-final-before-access.json")
    access_delete = block.index('exact-delete.py" access')
    monitoring_delete = block.index('exact-delete.py" monitoring')
    success = block.index("withdrawal_verified=true")

    assert second_job_snapshot < second_job_gate < forensic_seal
    assert forensic_seal < durable_ack < suspend < controller_delete
    assert controller_delete < final_jobs < final_pods < access_delete < monitoring_delete < success
    second_snapshot_gate = block[second_job_snapshot:forensic_seal]
    assert ".status.active" in second_snapshot_gate
    assert '(.type == "Complete" or .type == "Failed")' in second_snapshot_gate


def test_late_pod_on_final_recapture_preserves_access_and_monitoring(tmp_path: Path) -> None:
    operation_root, verifier = _retry_evidence(tmp_path)

    result = _run(_withdrawal_block(), cwd=tmp_path, operation_root=operation_root, verifier=verifier)

    assert result.returncode != 0
    verification_files = list((operation_root / "post").glob("attempt.*/verification.txt"))
    assert len(verification_files) == 1
    assert "reason=late_owned_pods" in verification_files[0].read_text(encoding="utf-8")
    mutations = (tmp_path / "mutations.txt").read_text(encoding="utf-8").splitlines()
    assert any("controller-jobs" in mutation for mutation in mutations)
    assert not any(" access " in f" {mutation} " for mutation in mutations)
    assert not any(" monitoring " in f" {mutation} " for mutation in mutations)
