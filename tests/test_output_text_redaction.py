from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from dpone.commands.airflow_connection_secret_gc_rendering import (
    connection_gc_apply_text,
    connection_gc_plan_text,
)
from dpone.commands.airflow_self_service_rendering import (
    self_service_check_text,
    self_service_connections_text,
    self_service_live_check_text,
)
from dpone.commands.output_text import (
    write_line as canonical_write_line,
)
from dpone.commands.output_text import (
    write_text as canonical_write_text,
)
from dpone.gitops.airflow_connection_bridge_plan_rendering import (
    render_gitops_airflow_connection_bridge_plan_markdown,
)
from dpone.output_text import write_line, write_text
from dpone.security_redaction import REDACTION_TOKEN, public_path_label, redact_absolute_paths, redact_text


def test_legacy_text_output_import_reexports_canonical_command_port() -> None:
    assert write_line is canonical_write_line
    assert write_text is canonical_write_text


def test_console_output_redacts_assignments_flags_and_authorization(capsys) -> None:
    write_text('password="zx-qzxv" token: xvq --client-secret qzx-qzxvqz Authorization: Bearer vqzxvq-xvqzx\n')

    output = capsys.readouterr().out

    assert "zx-qzxv" not in output
    assert "xvq" not in output
    assert "qzx-qzxvqz" not in output
    assert "vqzxvq-xvqzx" not in output
    assert output.count(REDACTION_TOKEN) == 4


def test_console_output_redacts_uri_credentials_private_keys_and_secret_refs(capsys) -> None:
    write_line(
        "uri=postgresql://dpone:qzxvqzx-qzxvqzxv@db.internal/dwh "
        "secret_ref: zx-qzxvqz\n"
        "-----BEGIN PRIVATE KEY-----\nvqzxvqz-vqzxvqzx\n-----END PRIVATE KEY-----"
    )

    output = capsys.readouterr().out

    assert "qzxvqzx-qzxvqzxv" not in output
    assert "zx-qzxvqz" not in output
    assert "vqzxvqz-vqzxvqzx" not in output
    assert output.count(REDACTION_TOKEN) == 3


def test_console_output_preserves_safe_logical_references(capsys) -> None:
    write_text("resolver: vault_kv\nconnection_ref: pg_prod\nnamespace: airflow-example\n")

    assert capsys.readouterr().out == ("resolver: vault_kv\nconnection_ref: pg_prod\nnamespace: airflow-example\n")


def test_shared_redactor_preserves_route_executor_extra_secret_contract() -> None:
    text = redact_text("command result opaque-value", extra_secrets=("opaque-value",))

    assert text == f"command result {REDACTION_TOKEN}"


@pytest.mark.parametrize("output_format", ["text", "md", "json"])
def test_run_failure_never_exposes_traceback_paths_or_secrets(
    output_format: str,
    capsys: pytest.CaptureFixture[str],
    tmp_path,
) -> None:
    from dpone.commands.run_cmd import _write_run_failure

    private_path = tmp_path / "private" / "runtime.py"
    secret = "runtime-password-value"
    try:
        raise RuntimeError(f"password={secret} failed at {private_path}")
    except RuntimeError as exc:
        _write_run_failure(
            SimpleNamespace(
                format=output_format,
                path=tmp_path / "pipelines/orders/pipeline.yaml",
                selector=None,
                run_id="orders-run",
                retry_attempts=0,
                retry_backoff_seconds=0.0,
            ),
            exc,
        )

    output = capsys.readouterr().out
    if output_format == "json":
        json.loads(output)
    assert "Traceback (most recent call last)" not in output
    assert private_path.as_posix() not in output
    assert tmp_path.as_posix() not in output
    assert secret not in output
    assert REDACTION_TOKEN in output


def test_public_path_helpers_preserve_remote_uris_and_hide_physical_paths() -> None:
    assert public_path_label("cache://releases/sha256-a/pack.json") == ("cache://releases/sha256-a/pack.json")
    assert public_path_label("dpone://catalog/releases/pack.json?version=7#private") == (
        "dpone://catalog/releases/pack.json"
    )
    assert public_path_label("https://alice:secret@example.test/releases/pack.json?download=1#private") == (
        "https://example.test/releases/pack.json"
    )
    assert (
        public_path_label("https://user%40tenant:p%40ss@[2001:db8::1]/object?X-Amz-Signature=signed#private")
        == "https://[2001:db8::1]/object"
    )
    assert public_path_label("file:///Users/alice/private/pack.json") == "artifact"
    assert public_path_label("/Users/alice/private/pack.json") == "pack.json"
    assert public_path_label("C:\\work\\private\\pack.json") == "pack.json"
    assert redact_absolute_paths("See https://docs.example.test/path and /Users/alice/private/file") == (
        "See https://docs.example.test/path and $ABSOLUTE_PATH"
    )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (
            "failed at /Users/alice/private.py and C:\\work\\private.py",
            "failed at $ABSOLUTE_PATH and $ABSOLUTE_PATH",
        ),
        (
            "failed at C:\\work\\private.py or /Users/alice/private.py",
            "failed at $ABSOLUTE_PATH or $ABSOLUTE_PATH",
        ),
    ],
)
def test_redact_absolute_paths_preserves_conjunctions_between_multiple_paths(
    value: str,
    expected: str,
) -> None:
    assert redact_absolute_paths(value) == expected


def test_redact_text_hides_uri_userinfo_secret_queries_and_fragments() -> None:
    text = redact_text(
        "postgresql://qzxvq-xvqz@[2001:db8::1]/dwh?sslmode=require "
        "postgresql://:qzxvq-xvqzxvqz@db.example.test/dwh "
        "https://qzxv%40vqzxvq:v%40vqzxvq@example.test/private "
        "https://s3.example.test/object?X-Amz-Credential=xvq-xv&X-Amz-Signature=xvq-xvqzxvqzx&region=eu "
        "https://blob.example.test/object?sv=1&sig=vqzxv-zxvqzxvqz&sp=r "
        "https://storage.example.test/object?GoogleAccessId=vqz-vq&X-Goog-Signature=vqz-vqzxvqzxv "
        "https://api.example.test/items?password=zx-qzxv&api_key=zxv-zxvqzx&token=qzxvqz"
        "&access_token=zxvqzx-qzxvqz&key=xvq-xvqzxv&view=compact "
        "https://docs.example.test/runbook#xvqzxvq-xvqzxvqz"
    )

    for secret in (
        "qzxvq-xvqz",
        "qzxvq-xvqzxvqz",
        "qzxv%40vqzxvq",
        "v%40vqzxvq",
        "xvq-xv",
        "xvq-xvqzxvqzx",
        "vqzxv-zxvqzxvqz",
        "vqz-vq",
        "vqz-vqzxvqzxv",
        "zx-qzxv",
        "zxv-zxvqzx",
        "qzxvqz",
        "zxvqzx-qzxvqz",
        "xvq-xvqzxv",
        "xvqzxvq-xvqzxvqz",
    ):
        assert secret not in text
    assert "postgresql://[REDACTED]@[2001:db8::1]/dwh?sslmode=require" in text
    assert "region=eu" in text
    assert "sv=1" in text
    assert "sp=r" in text
    assert "view=compact" in text
    assert "https://docs.example.test/runbook#[REDACTED]" in text


def test_human_renderers_do_not_expose_secret_topology() -> None:
    secret_name = "dpone-runtime-secret"
    secret_ref = "sha256:" + "a" * 64
    bridge = SimpleNamespace(
        passed=True,
        artifact_dir=".dpone/gitops/airflow",
        output_path="connection-bridge-plan.json",
        mode="external_secret",
        runtime_mode="kubernetes",
        secret_name=secret_name,
        required_connection_ids=(),
        artifacts=(),
        warnings=(),
        blockers=(),
    )
    gc_payload = {
        "status": "ok",
        "namespace": "airflow-example",
        "inventory": {"managed_secrets": 1, "active_references": 1, "quarantined": 0},
        "delete_candidates": [secret_ref],
        "deleted_secret_refs": [secret_ref],
        "skipped_secret_refs": [],
        "failed_secret_refs": [],
        "items": [{"action": "keep", "secret_ref": secret_ref, "reason": "active", "age_seconds": 30}],
    }

    rendered = "".join(
        (
            render_gitops_airflow_connection_bridge_plan_markdown(bridge),
            connection_gc_plan_text(gc_payload),
            connection_gc_apply_text(gc_payload),
        )
    )

    assert secret_name not in rendered
    assert secret_ref not in rendered


def test_check_renderers_report_access_mode_without_reading_secret_payload() -> None:
    marker = "must-not-reach-console"
    common = {"passed": True, "network": False, "secrets": marker, "source_queries": False}
    rendered = "".join(
        (
            self_service_check_text(common | {"mode": "static"}, target="pipelines/orders"),
            self_service_connections_text(
                common | {"mode": "connections", "handshake": "configuration_only"},
                target="pipelines/orders",
            ),
            self_service_live_check_text(
                common
                | {
                    "passed": False,
                    "live_preflight": "runner_not_configured",
                    "planned_network": True,
                    "planned_secrets": marker,
                    "planned_source_queries": "bounded_probes",
                    "live_preflight_report": {"probes": []},
                    "errors": [],
                },
                target="pipelines/orders",
            ),
        )
    )

    assert marker not in rendered
    assert rendered.count("- secrets: no") == 1
    assert "- secrets: planned" in rendered
    assert "dpone check: OK" in rendered
    assert "- next: dpone airflow preview orders" in rendered
