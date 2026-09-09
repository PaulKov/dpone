"""Executable durable-ack verifier used by runtime Pod withdrawal tests."""

from __future__ import annotations

import hashlib
import json
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class WithdrawalAckVerifier:
    path: Path
    sha256: str
    issuer: str = "urn:dpone:test:forensic-vault"

    def shell_exports(self) -> str:
        return "\n".join(
            (
                f"DPONE_RUNTIME_POD_WITHDRAW_ACK_COMMAND={shlex.quote(str(self.path))}",
                f"DPONE_RUNTIME_POD_WITHDRAW_ACK_COMMAND_SHA256={shlex.quote(self.sha256)}",
                f"DPONE_RUNTIME_POD_WITHDRAW_ACK_ISSUER={shlex.quote(self.issuer)}",
            )
        )


def write_withdrawal_ack_verifier(
    directory: Path,
    *,
    receipt_overrides: dict[str, object] | None = None,
) -> WithdrawalAckVerifier:
    path = directory / "withdrawal-ack-verifier"
    overrides = repr(json.dumps(receipt_overrides or {}, sort_keys=True))
    path.write_text(_VERIFIER_SOURCE.replace("__OVERRIDES_JSON__", overrides), encoding="utf-8")
    path.chmod(0o700)
    digest = "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
    return WithdrawalAckVerifier(path=path, sha256=digest)


def publish_withdrawal_ack(
    verifier: WithdrawalAckVerifier,
    *,
    evidence_directory: Path,
    manifest: Path,
    operation: Path,
    output: Path,
) -> None:
    subprocess.run(
        [
            str(verifier.path),
            "publish-and-ack",
            "--evidence-directory",
            str(evidence_directory),
            "--manifest",
            str(manifest),
            "--operation",
            str(operation),
            "--issuer",
            verifier.issuer,
            "--classification",
            "restricted",
            "--encryption",
            "at_rest_and_in_transit",
            "--output",
            str(output),
        ],
        check=True,
    )


_VERIFIER_SOURCE = r"""#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import sys

OVERRIDES = json.loads(__OVERRIDES_JSON__)
SELF_SHA256 = "sha256:" + hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()


def digest(path: str) -> str:
    return "sha256:" + hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def write_json(path: str, payload: dict[str, object]) -> None:
    pathlib.Path(path).write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


command = sys.argv[1]
parser = argparse.ArgumentParser()
if command == "verify-storage":
    parser.add_argument("--directory", required=True)
    parser.add_argument("--classification", required=True)
    parser.add_argument("--required-encryption", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(sys.argv[2:])
    write_json(
        args.output,
        {
            "schema": "dpone.airflow-runtime-pod-retention-storage-verification.v1",
            "passed": True,
            "status": "verified",
            "directory": args.directory,
            "evidence_classification": args.classification,
            "encryption_at_rest": args.required_encryption == "at_rest",
            "verifier_sha256": SELF_SHA256,
        },
    )
elif command == "publish-and-ack":
    parser.add_argument("--evidence-directory", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--operation", required=True)
    parser.add_argument("--issuer", required=True)
    parser.add_argument("--classification", required=True)
    parser.add_argument("--encryption", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(sys.argv[2:])
    payload = {
        "schema": "dpone.airflow-runtime-pod-retention-withdrawal-durable-ack.v1",
        "status": "acknowledged",
        "durability": "external_durable_acknowledged",
        "evidence_classification": args.classification,
        "encryption": args.encryption,
        "verification_method": "credentialed_remote_readback",
        "issuer": args.issuer,
        "verifier_sha256": SELF_SHA256,
        "operation_sha256": digest(args.operation),
        "evidence_manifest_sha256": digest(args.manifest),
        "sink_uri": "s3://forensic-vault/runtime-pod-withdrawal",
        "acknowledgement_id": "vault-write-0001",
        "acknowledged_at": "2026-08-04T08:00:00Z",
    }
    payload.update(OVERRIDES)
    write_json(args.output, payload)
elif command == "verify-receipt":
    parser.add_argument("--receipt", required=True)
    parser.add_argument("--expected-operation-sha256", required=True)
    parser.add_argument("--expected-manifest-sha256", required=True)
    parser.add_argument("--expected-issuer", required=True)
    parser.add_argument("--classification", required=True)
    parser.add_argument("--encryption", required=True)
    args = parser.parse_args(sys.argv[2:])
    payload = json.loads(pathlib.Path(args.receipt).read_text(encoding="utf-8"))
    expected = {
        "operation_sha256": args.expected_operation_sha256,
        "evidence_manifest_sha256": args.expected_manifest_sha256,
        "issuer": args.expected_issuer,
        "evidence_classification": args.classification,
        "encryption": args.encryption,
        "verifier_sha256": SELF_SHA256,
    }
    if any(payload.get(key) != value for key, value in expected.items()):
        print("external durable acknowledgement mismatch", file=sys.stderr)
        raise SystemExit(10)
else:
    raise SystemExit(f"unsupported verifier command: {command}")
"""


__all__ = ["WithdrawalAckVerifier", "publish_withdrawal_ack", "write_withdrawal_ack_verifier"]
