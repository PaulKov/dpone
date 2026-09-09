from __future__ import annotations

import json
import subprocess
import sys

import pytest
import yaml
from dpone_airflow_pack.helm_ack_evidence import build_helm_ack_evidence
from dpone_airflow_pack.helm_ack_mounts import harden_loader_ack_mounts


def test_helm_evidence_is_deterministic_and_excludes_rendered_secret_values() -> None:
    first = _documents(secret_value="generated-first-secret")
    second = _documents(secret_value="generated-second-secret")

    first_evidence = _evidence(first)
    second_evidence = _evidence(second)
    encoded = json.dumps(first_evidence, sort_keys=True)

    assert first_evidence == second_evidence
    assert str(first_evidence["rendered_structure_sha256"]).startswith("sha256:")
    assert str(first_evidence["subject_sha256"]).startswith("sha256:")
    assert first_evidence["rendered_secret_document_count"] == 1
    assert first_evidence["ack_workloads"] == [
        {
            "kind": "Deployment",
            "name": "airflow-scheduler",
            "parser_container": "scheduler",
            "writer_mount_path": "/opt/airflow/.dpone-ack",
            "writer_read_only": False,
            "reader_containers": ["dpone-cache-watch"],
            "reader_read_only": True,
        }
    ]
    assert "generated-first-secret" not in encoded
    assert "generated-second-secret" not in encoded
    assert '"data"' not in encoded
    assert '"stringData"' not in encoded
    assert '"annotations"' not in encoded


def test_helm_evidence_cli_emits_json_not_rendered_manifests() -> None:
    rendered = yaml.safe_dump_all(_documents(secret_value="never-persist-me"), explicit_start=True).encode()

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "dpone_airflow_pack.cli_helm_evidence",
            "--source-commit",
            "a" * 40,
            "--helm-version",
            "v3.19.0",
            "--chart-name",
            "apache-airflow/airflow",
            "--chart-version",
            "1.22.0",
            "--chart-sha256",
            "b" * 64,
            "--pack-wheel-sha256",
            "c" * 64,
            "--provider-wheel-sha256",
            "d" * 64,
            "--values-sha256",
            "e" * 64,
            "--renderer-sha256",
            "f" * 64,
        ],
        input=rendered,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr.decode()
    payload = json.loads(result.stdout)
    assert payload["schema"] == "dpone.internal.airflow-helm-ack-evidence.v1"
    assert payload["chart"]["sha256"] == "sha256:" + "b" * 64
    assert "never-persist-me" not in result.stdout.decode()


def _evidence(documents: list[object]) -> dict[str, object]:
    return build_helm_ack_evidence(
        documents,
        source_commit="a" * 40,
        helm_version="v3.19.0",
        chart_name="apache-airflow/airflow",
        chart_version="1.22.0",
        chart_sha256="b" * 64,
        pack_wheel_sha256="c" * 64,
        provider_wheel_sha256="d" * 64,
        values_sha256="e" * 64,
        renderer_sha256="f" * 64,
    )


def test_helm_evidence_rejects_abbreviated_or_non_hex_source_commit() -> None:
    for source_commit in ("abc123", "z" * 40):
        with pytest.raises(ValueError, match="full lowercase"):
            build_helm_ack_evidence(
                _documents(secret_value="redacted"),
                source_commit=source_commit,
                helm_version="v3.19.0",
                chart_name="apache-airflow/airflow",
                chart_version="1.22.0",
                chart_sha256="b" * 64,
                pack_wheel_sha256="c" * 64,
                provider_wheel_sha256="d" * 64,
                values_sha256="e" * 64,
                renderer_sha256="f" * 64,
            )


def _documents(*, secret_value: str) -> list[object]:
    unsafe_mount = {
        "name": "dpone-airflow-loader-ack",
        "mountPath": "/opt/airflow/.dpone-ack",
        "readOnly": False,
    }
    manifest: dict[str, object] = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "airflow-scheduler", "annotations": {"secret-derived": secret_value}},
        "spec": {
            "template": {
                "spec": {
                    "volumes": [{"name": "dpone-airflow-loader-ack", "emptyDir": {}}],
                    "containers": [
                        {"name": "scheduler", "volumeMounts": [dict(unsafe_mount)]},
                        {"name": "dpone-cache-watch", "volumeMounts": [dict(unsafe_mount)]},
                    ],
                }
            }
        },
    }
    secret = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "generated"},
        "data": {"fernet-key": secret_value},
    }
    return [*harden_loader_ack_mounts([manifest]), secret]
