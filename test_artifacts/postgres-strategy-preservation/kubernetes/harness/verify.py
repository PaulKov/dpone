"""Join original service files, admitted Pods, Airflow XComs and database rows.

This verifier consumes one explicitly named controller attempt. Earlier failed
attempts remain untouched. It verifies only this synthetic PostgreSQL full-refresh
journey; hooks, manifest resources and other source/sink routes are outside scope.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

from cluster import NAMESPACE, ROOT, credentials, save

CASES = {
    "refresh-first": [[1, "alpha"], [2, "beta"]],
    "refresh-changed": [[3, "changed"]],
    "refresh-replay": [[3, "changed"]],
    "refresh-rejected": [[3, "changed"]],
    "refresh-retry": [[5, "retry"]],
}


def read(path: Path) -> dict | list:
    return json.loads(path.read_text())


def sha(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def one_xcom(items: list, key: str, task_id: str | None = None) -> dict:
    selected = [
        item["value"] for item in items if item["key"] == key and (task_id is None or item["task_id"] == task_id)
    ]
    assert len(selected) == 1, key
    return selected[0]


def verify_case(results: Path, case: str, expected: list, baseline: dict, images: dict, index: dict) -> dict:
    observed = read(results / (case + ".json"))
    assert observed["status"] == "PASS" and observed["source_commit"] == images["runtime"]["source_commit"]
    rejected = case == "refresh-rejected"
    assert observed["dag_state"] == ("failed" if rejected else "success")
    catalog = read(results / case / "catalog.json")
    assert catalog["rows"] == catalog["metadata"]["view"] == expected
    for key in ("oid", "constraints", "columns", "indexes"):
        assert catalog["metadata"][key] == baseline["metadata"][key], key
    xcoms = read(results / case / "xcoms.json")
    pin = one_xcom(xcoms, "dpone_runtime_launch_pin")
    for identity in (pin["run_identity"], pin["deployment_identity"]):
        assert identity["release_id"] == index["release_id"]
        assert identity["deployment_id"] == index["deployment_id"]
    assert pin["run_identity"]["runtime_image_digest"] == images["runtime"]["node_ref"].split("@", 1)[1]
    assert pin["run_identity"]["airflow_bundle"]["version"] == images["runtime"]["source_commit"]
    persisted = one_xcom(xcoms, "return_value", pin["task_id"])
    gate = one_xcom(xcoms, "dpone_outcome_gate_result")
    assert gate["passed"] is (not rejected)
    assert pin["run_id"] == observed["run_id"] and pin["dag_id"] == observed["dag_id"]
    uid = pin["pod_uid"]
    events = [read(path) for path in (ROOT / "pod-events" / uid).glob("*.json")]
    assert events and any(event["type"] == "DELETED" for event in events)
    pods = [event["object"] for event in events]
    assert all(
        candidate["metadata"]["uid"] == uid
        and candidate["metadata"]["namespace"] == NAMESPACE
        and candidate["metadata"]["name"] == pin["pod_name"]
        for candidate in pods
    )
    pod = max(pods, key=lambda value: int(value["metadata"]["resourceVersion"]))
    assert pod["metadata"]["namespace"] == NAMESPACE and pod["metadata"]["name"] == pin["pod_name"]
    envelope = json.loads(pod["metadata"]["annotations"]["dpone.airflow/runtime-launch-envelope.v1"])
    assert envelope["run_identity"] == pin["run_identity"]
    assert envelope["deployment_identity"] == pin["deployment_identity"]
    expected_images = {"base": images["runtime"]["node_ref"], "airflow-xcom-sidecar": images["xcom"]["node_ref"]}
    for container in [*pod["spec"]["initContainers"], *pod["spec"]["containers"]]:
        expected_image = expected_images.get(container["name"], images["runtime"]["node_ref"])
        assert container["image"] == expected_image
        statuses = [
            status
            for candidate in pods
            for status in [
                *candidate["status"].get("initContainerStatuses", []),
                *candidate["status"].get("containerStatuses", []),
            ]
            if status["name"] == container["name"]
        ]
        assert any(status.get("state", {}).get("terminated", {}).get("exitCode") == 0 for status in statuses)
        digests = (
            images["runtime"]["allowed_image_digests"]
            if container["name"] != "airflow-xcom-sidecar"
            else [expected_image.split("@", 1)[1]]
        )
        assert any(any(status.get("imageID", "").endswith(digest) for digest in digests) for status in statuses)
    files = ROOT / "service-files" / uid
    raw_xcom = read(files / "return.json")
    evidence = read(files / "runtime-evidence.json")
    metadata = read(files / "metadata.json")
    assert raw_xcom["status"] == ("failed" if rejected else "passed")
    assert raw_xcom["runtime_evidence_sha256"] == sha(files / "runtime-evidence.json")
    assert gate["payload"]["runtime_evidence_sha256"] == raw_xcom["runtime_evidence_sha256"]
    assert gate["payload"]["status"] == raw_xcom["status"]
    assert evidence["passed"] is (not rejected)
    assert raw_xcom["interval"]["dag_run_id"] == observed["run_id"]
    assert raw_xcom["run_identity"] == pin["run_identity"]
    assert raw_xcom["deployment_identity"] == pin["deployment_identity"]
    assert persisted == {**raw_xcom, "launch_pin_ref": pin["launch_pin_ref"]}
    assert pin["launch_pin_ref"]["pod_uid"] == uid
    if rejected:
        assert evidence["result"]["status"] == "error"
        errors = evidence["result"]["errors"]
        assert any(error.startswith("CheckViolation:") and '"orders_label_check"' in error for error in errors)
        assert any(
            blocker["code"] == "runtime_execution_failed"
            and blocker["message"].startswith("CheckViolation:")
            and '"orders_label_check"' in blocker["message"]
            for blocker in raw_xcom["blockers"]
        )
        stderr = (files / "runtime-stderr.log").read_text()
        assert 0 <= stderr.find("PG_TARGET_TRUNCATED") < stderr.find('violates check constraint "orders_label_check"')
    else:
        assert evidence["result"]["final_rows"] == len(expected)
    for name in ("return.json", "runtime-evidence.json", "runtime-stderr.log"):
        assert metadata[name]["uid"] == 65532 and metadata[name]["bytes"] == (files / name).stat().st_size
        exported = read(ROOT / "service-files/export-receipt.json")["files"][uid + "/" + name]
        assert "sha256:" + exported["sha256"] == sha(files / name)
        assert exported["bytes"] == metadata[name]["bytes"]
    return {
        "case": case,
        "status": "PASS",
        "pod_uid": uid,
        "run_id": observed["run_id"],
        "release_id": observed["release_id"],
        "deployment_id": observed["deployment_id"],
        "runtime_evidence_sha256": sha(files / "runtime-evidence.json"),
        "runtime_xcom_sha256": sha(files / "return.json"),
        "target_oid": catalog["metadata"]["oid"],
        "rows": expected,
    }


def verify(controller_uid: str) -> dict:
    credentials()
    images = read(ROOT / "registry-images.json")
    results = ROOT / "controller" / controller_uid / "results"
    baseline = read(results / "before/catalog.json")
    assert "DPONE_LIVE_CONTROLLER_DONE exit=0" in (ROOT / "pod-logs" / controller_uid / "controller.log").read_text()
    index = read(results / "orders/deployment-index.json")
    assert index["runtime_artifact_delivery"]["mode"] == "init_fetch"
    cases = [verify_case(results, case, expected, baseline, images, index) for case, expected in CASES.items()]
    assert {case["release_id"] for case in cases} == {index["release_id"]}
    assert {case["deployment_id"] for case in cases} == {index["deployment_id"]}
    assert len({case["pod_uid"] for case in cases}) == len(CASES)
    provenance = read(ROOT / "image/source-provenance.json")
    assert provenance["source_commit"] == images["runtime"]["source_commit"]
    installed = read(ROOT / "controller" / controller_uid / "installed-source-verification.json")
    assert installed["exit_code"] == 0 and installed["controller_uid"] == controller_uid
    assert "Verified exact-source installed Python files: 3776" in installed["stdout"]
    assert read(ROOT / "controller" / controller_uid / "source-provenance.json") == provenance
    controllers = [read(path)["object"] for path in (ROOT / "pod-events" / controller_uid).glob("*.json")]
    assert controllers and all(
        pod["metadata"]["uid"] == controller_uid
        and pod["metadata"]["namespace"] == NAMESPACE
        and pod["spec"]["containers"][0]["image"] == images["runtime"]["node_ref"]
        for pod in controllers
    )
    assert any(
        any(status.get("imageID", "").endswith(digest) for digest in images["runtime"]["allowed_image_digests"])
        for pod in controllers
        for status in pod["status"].get("containerStatuses", [])
        if status["name"] == "controller"
    )
    assert sha(ROOT / "image/runtime-manifest.json") == images["runtime"]["node_ref"].split("@", 1)[1]
    for wheel, claim in provenance["wheels"].items():
        assert sha(ROOT / "image/wheels" / wheel) == "sha256:" + claim["sha256"]
    coverage = read(ROOT / "pod-watch-coverage.json")
    assert coverage["status"] == "COMPLETE"
    return {
        "status": "PASS",
        "source_commit": images["runtime"]["source_commit"],
        "controller_uid": controller_uid,
        "namespace": NAMESPACE,
        "scope": "Five actual strict Airflow DAG runs with default PostgreSQL full_refresh; synthetic tables only",
        "runtime_image": images["runtime"],
        "cases": cases,
        "outside_scope": ["hooks", "manifest resources", "other routes", "package publication"],
        "evidence_files": {
            str(path.relative_to(ROOT)): sha(path)
            for directory in (ROOT / "controller" / controller_uid, ROOT / "service-files", ROOT / "pod-events")
            for path in sorted(directory.rglob("*"))
            if path.is_file()
        },
    }


if __name__ == "__main__":
    report = verify(sys.argv[1])
    save("verification.json", report)
    summary = {key: value for key, value in report.items() if key != "evidence_files"}
    summary["raw_evidence_manifest"] = "kubernetes/verification.json"
    summary["raw_evidence_manifest_sha256"] = sha(ROOT / "verification.json")
    save("../verification-kubernetes.json", summary)
    print(f"PASS: {len(report['cases'])} strict DAG cases, source {report['source_commit']}")
