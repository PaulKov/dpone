"""Opt-in live K3d launch-pin CAS/RBAC certification."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
RECEIPT = ROOT / "test_artifacts" / "launch-pin-k3d-cert" / "receipt.json"


@pytest.mark.integration_live
def test_launch_pin_k3d_cert_receipt_passes() -> None:
    if os.environ.get("LAUNCH_PIN_K3D_CERT") != "1":
        pytest.skip("Set LAUNCH_PIN_K3D_CERT=1 to run live k3d certification")
    script = ROOT / "tools" / "launch_pin_k3d_cert.sh"
    subprocess.run([str(script)], check=True, cwd=ROOT)
    payload = json.loads(RECEIPT.read_text(encoding="utf-8"))
    assert payload["status"] == "PASS", payload
    names = {item["name"] for item in payload["scenarios"]}
    assert {
        "worker_configmap_cas",
        "worker_pod_delete",
        "runtime_configmap_get_only",
        "multi_worker_cas_single_winner",
        "runtime_reads_active_barrier",
    } <= names
