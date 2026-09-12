from __future__ import annotations


def test_redact_mapping_removes_secret_keys_and_sanitizes_sensitive_messages() -> None:
    from dpone.services.safe_sample_execution_common import redact_mapping

    payload = {
        "backend": "mssql",
        "resolver": "vault_kv",
        "note": "Vault resolver selected",
        "password": "zxvq-xvq-xvqz",
        "api_key": "zxv-zxv-zxvq-xvq-xvqz",
        "aws_access_key_id": "qzxvqz-vqz-vqzx-qzx-qzxv",
        "message": "driver failed with api_token=zxvq-xvq-xvqz and password = xvqz-vqzx-qzx-qzxv",
        "nested": {
            "lease_id": "lease-zxvq-xvq-xvqz",
            "connection_string": "Server=tcp://host;Password=zxvq-xvq-xvqz",
            "message": "vault response included secret=vqzx-qzxv-zxv-zxvq",
        },
        "events": [
            {"stage": "read", "message": "jwt=qzxv-zxvq-xvq-xvqz"},
            {"stage": "connect", "message": "driver emitted api_key=xvq-xvq-xvqz-vqzx-qzx-qzxv"},
            "aws_access_key_id=zxvqzx-qzx-qzxv-zxvq-xvq-xvqz",
            "token=vqzxvq-xvqz-vqzx-qzx-qzxv",
            "plain status",
        ],
    }

    redacted = redact_mapping(payload)

    serialized = repr(redacted)
    assert redacted["backend"] == "mssql"
    assert redacted["resolver"] == "vault_kv"
    assert redacted["note"] == "Vault resolver selected"
    assert redacted["message"] == "details redacted; check runtime adapter diagnostics and safe sample configuration."
    assert redacted["nested"] == {
        "message": "details redacted; check runtime adapter diagnostics and safe sample configuration."
    }
    assert redacted["events"] == [
        {
            "stage": "read",
            "message": "details redacted; check runtime adapter diagnostics and safe sample configuration.",
        },
        {
            "stage": "connect",
            "message": "details redacted; check runtime adapter diagnostics and safe sample configuration.",
        },
        "details redacted; check runtime adapter diagnostics and safe sample configuration.",
        "details redacted; check runtime adapter diagnostics and safe sample configuration.",
        "plain status",
    ]
    assert "zxvq-xvq-xvqz" not in serialized
    assert "xvqz-vqzx-qzx-qzxv" not in serialized
    assert "vqzx-qzxv-zxv-zxvq" not in serialized
    assert "qzxv-zxvq-xvq-xvqz" not in serialized
    assert "zxv-zxv-zxvq-xvq-xvqz" not in serialized
    assert "qzxvqz-vqz-vqzx-qzx-qzxv" not in serialized
    assert "xvq-xvq-xvqz-vqzx-qzx-qzxv" not in serialized
    assert "zxvqzx-qzx-qzxv-zxvq-xvq-xvqz" not in serialized
    assert "vqzxvq-xvqz-vqzx-qzx-qzxv" not in serialized


def test_contains_sensitive_assignment_preserves_safe_resolver_text() -> None:
    from dpone.services.safe_sample_redaction import contains_sensitive_assignment

    assert not contains_sensitive_assignment("Vault resolver selected")
    assert not contains_sensitive_assignment("resolver=vault_kv")
    assert contains_sensitive_assignment("driver failed with token=must-not-leak")
    assert contains_sensitive_assignment("driver failed with api_token = must-not-leak")
    assert contains_sensitive_assignment("driver failed with vault_token=must-not-leak")
    assert contains_sensitive_assignment("driver failed with password: must-not-leak")
    assert contains_sensitive_assignment("driver failed with api_key=must-not-leak")
    assert contains_sensitive_assignment("driver failed with aws_access_key_id=must-not-leak")
    assert contains_sensitive_assignment('driver failed with {"connection_string": "must-not-leak"}')


def test_safe_sample_redaction_removes_absolute_workstation_paths() -> None:
    from dpone.services.safe_sample_redaction import redact_safe_sample_text

    message = "failed to read /Users/alice/projects/dpone/pipeline.yaml and C:\\work\\dpone\\pipeline.yaml"

    redacted = redact_safe_sample_text(message)

    assert "/Users/alice" not in redacted
    assert "C:\\work" not in redacted
    assert redacted.count("$ABSOLUTE_PATH") == 2


def test_safe_sample_redaction_preserves_https_urls() -> None:
    from dpone.services.safe_sample_redaction import redact_safe_sample_text

    assert redact_safe_sample_text("See https://docs.example.test/path/to/error") == (
        "See https://docs.example.test/path/to/error"
    )
