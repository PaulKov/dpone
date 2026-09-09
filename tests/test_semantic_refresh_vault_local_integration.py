from __future__ import annotations

import json
import os
import socket
import subprocess
import time
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from dpone.adapters.semantic_refresh_vault_authority import (
    VaultSemanticRefreshAuthorityIndexReader,
    semantic_refresh_vault_authority_index,
)
from dpone.adapters.semantic_refresh_vault_seal_policy import (
    VaultSemanticRefreshSealPolicyAuthority,
    semantic_refresh_seal_policy_locator,
)
from dpone.adapters.vault_kv_v2 import HvacKubernetesKvV2Reader
from dpone.ports.semantic_refresh_seal_policy import (
    SemanticRefreshSealPolicyAuthority,
    SemanticRefreshSealPolicySubject,
)
from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver


def _enabled() -> bool:
    return os.getenv("DPONE_RUN_SEMANTIC_REFRESH_VAULT_LIVE") == "1"


@pytest.mark.integration_live
def test_local_vault_kubernetes_auth_and_versioned_resolution(tmp_path: Path) -> None:
    if not _enabled():
        pytest.skip("set DPONE_RUN_SEMANTIC_REFRESH_VAULT_LIVE=1 for the local k3d/Vault profile")
    context = os.getenv("DPONE_IT_KUBECONFIG_CONTEXT", "k3d-dpone-semref-v2")
    namespace = os.getenv("DPONE_IT_VAULT_NAMESPACE", "dpone-semref-v2")
    token = subprocess.run(
        [
            "kubectl",
            "--context",
            context,
            "-n",
            namespace,
            "create",
            "token",
            "semantic-refresh-runner",
            "--duration=10m",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    token_file = tmp_path / "service-account-token"
    token_file.write_text(token, encoding="utf-8")
    token_file.chmod(0o600)

    with _vault_port_forward(context=context, namespace=namespace) as address:
        reader = HvacKubernetesKvV2Reader(
            address=address,
            role="semantic-refresh",
            jwt_file=str(token_file),
            verify=True,
        )
        resolver = BindingCredentialResolver(
            binding_set={
                "environment": "local-integration",
                "bindings": {"semantic_runtime": {"connection_ref": "semantic_runtime"}},
            },
            connection_registry={
                "connections": {
                    "semantic_runtime": {
                        "type": "mssql",
                        "connection": {"host": "mssql.local", "database": "semantic_refresh"},
                        "credentials": {
                            "resolver": "vault_kv",
                            "support": "production",
                            "mount": "dpone-kv",
                            "path": "semantic-refresh/runtime",
                            "kv_version": 2,
                            "version_policy": "latest",
                            "resolution_scope": "workload_start",
                            "payload_format": "fields",
                            "fields": {"username": "username", "password": "password"},
                        },
                    }
                }
            },
            vault_kv_reader=reader,
        )

        resolved = resolver.resolve("semantic_runtime")

    assert resolved.safe_metadata["resolver"] == "vault_kv"
    assert resolved.safe_metadata["resolved_version"] == 1
    assert resolved.descriptor.properties == {"host": "mssql.local", "database": "semantic_refresh"}
    assert resolved.credentials.username == "local-runtime"
    assert resolved.credentials.password == "local-value"
    assert "local-value" not in repr(resolved.safe_metadata)


@pytest.mark.integration_live
def test_local_vault_pins_exact_seal_policy_authority(tmp_path: Path) -> None:
    if not _enabled():
        pytest.skip("set DPONE_RUN_SEMANTIC_REFRESH_VAULT_LIVE=1 for the local k3d/Vault profile")
    context = os.getenv("DPONE_IT_KUBECONFIG_CONTEXT", "k3d-dpone-semref-v2")
    namespace = os.getenv("DPONE_IT_VAULT_NAMESPACE", "dpone-semref-v2")
    policy = _seal_policy()
    index = semantic_refresh_vault_authority_index(
        deployment_subject_sha256=_digest("a"),
        route_certification_receipt_sha256=_digest("b"),
        runtime_assurance_receipt_sha256s=(_digest("c"),),
        seal_policy_authority_sha256s=(policy.seal_policy_authority_sha256,),
    )
    policy_path = f"semantic-refresh/seal-policies/{semantic_refresh_seal_policy_locator(policy.subject)}"
    index_path = "semantic-refresh/local-seal-authority-index"
    _put_vault_document(context, namespace, policy_path, policy.to_dict())
    _put_vault_document(context, namespace, index_path, index)
    token_file = _service_account_token(context, namespace, tmp_path)

    with _vault_port_forward(context=context, namespace=namespace) as address:
        reader = HvacKubernetesKvV2Reader(
            address=address,
            role="semantic-refresh",
            jwt_file=str(token_file),
            verify=True,
        )
        index_version = int(reader.get_secret(mount_point="dpone-kv", path=index_path)["_metadata"]["version"])
        policy_version = int(reader.get_secret(mount_point="dpone-kv", path=policy_path)["_metadata"]["version"])
        pinned_index = VaultSemanticRefreshAuthorityIndexReader(
            client=reader,
            mount_point="dpone-kv",
            path=index_path,
            expected_version=index_version,
        )
        resolved = VaultSemanticRefreshSealPolicyAuthority(
            client=reader,
            index=pinned_index,
            mount_point="dpone-kv",
            path_prefix="semantic-refresh/seal-policies",
            expected_version=policy_version,
        ).load(policy.subject)

    assert resolved == policy


def _seal_policy() -> SemanticRefreshSealPolicyAuthority:
    subject = SemanticRefreshSealPolicySubject(
        release_id=_digest("1"),
        deployment_id=_digest("2"),
        model_unique_id="model.local.events",
        event_time_source_type="datetime2(6)",
        effective_key_mapping_sha256=_digest("3"),
        ordered_writable_schema_sha256=_digest("4"),
        route_certification_receipt_sha256=_digest("5"),
        writer_exclusivity_assurance_receipt_sha256=_digest("6"),
        ddl_freeze_assurance_receipt_sha256=_digest("7"),
        artifact_authority_sha256=_digest("8"),
        serializer_sha256=_digest("9"),
        parquet_schema_mapping_sha256=_digest("a"),
        utc_semantics_assurance_receipt_sha256=_digest("b"),
    )
    return SemanticRefreshSealPolicyAuthority.build(
        subject=subject,
        clickhouse_input_mapping_sha256=_digest("c"),
        codec_mapping_certification_sha256=_digest("d"),
        seal_policy_sha256=_digest("e"),
        issuer_authority="vault://local/semantic-refresh/seal-policy",
        issuer_attestation_sha256=_digest("f"),
        issuer_signature_sha256=_digest("0"),
    )


def _digest(character: str) -> str:
    return "sha256:" + character * 64


def _service_account_token(context: str, namespace: str, tmp_path: Path) -> Path:
    value = subprocess.run(
        [
            "kubectl",
            "--context",
            context,
            "-n",
            namespace,
            "create",
            "token",
            "semantic-refresh-runner",
            "--duration=10m",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    path = tmp_path / "service-account-token"
    path.write_text(value, encoding="utf-8")
    path.chmod(0o600)
    return path


def _put_vault_document(
    context: str,
    namespace: str,
    path: str,
    document: dict[str, object],
) -> None:
    pod = subprocess.run(
        [
            "kubectl",
            "--context",
            context,
            "-n",
            namespace,
            "get",
            "pods",
            "-l",
            "app=vault",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    args = json.loads(
        subprocess.run(
            [
                "kubectl",
                "--context",
                context,
                "-n",
                namespace,
                "get",
                "deploy",
                "vault",
                "-o",
                "json",
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )["spec"]["template"]["spec"]["containers"][0]["args"]
    root_token = next(
        value.removeprefix("-dev-root-token-id=") for value in args if value.startswith("-dev-root-token-id=")
    )
    completed = subprocess.run(
        [
            "kubectl",
            "--context",
            context,
            "-n",
            namespace,
            "exec",
            "-i",
            pod,
            "--",
            "env",
            f"VAULT_TOKEN={root_token}",
            "sh",
            "-c",
            "tmp=$(mktemp); trap 'rm -f $tmp' EXIT; cat > $tmp; vault kv put -mount=dpone-kv \"$1\" @$tmp >/dev/null",
            "sh",
            path,
        ],
        input=json.dumps(document, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError("local Vault authority fixture could not be persisted")


@contextmanager
def _vault_port_forward(*, context: str, namespace: str) -> Iterator[str]:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = int(listener.getsockname()[1])
    process = subprocess.Popen(
        [
            "kubectl",
            "--context",
            context,
            "-n",
            namespace,
            "port-forward",
            "service/vault",
            f"{port}:8200",
            "--address=127.0.0.1",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    address = f"http://127.0.0.1:{port}"
    try:
        _wait_for_health(address, process)
        yield address
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def _wait_for_health(address: str, process: subprocess.Popen[bytes]) -> None:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("Vault port-forward terminated before becoming ready")
        try:
            with urllib.request.urlopen(f"{address}/v1/sys/health?standbyok=true", timeout=1) as response:
                if response.status == 200:
                    return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError("Vault local integration endpoint did not become ready")
