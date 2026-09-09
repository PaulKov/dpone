from __future__ import annotations


def test_redact_mapping_removes_secret_keys_and_sanitizes_sensitive_messages() -> None:
    from dpone.services.safe_sample_execution_common import redact_mapping

    payload = {
        "backend": "mssql",
        "resolver": "vault_kv",
        "note": "Vault resolver selected",
        "password": "must-not-leak",
        "api_key": "api-key-must-not-leak",
        "aws_access_key_id": "access-key-must-not-leak",
        "message": "driver failed with api_token=must-not-leak and password = also-must-not-leak",
        "nested": {
            "lease_id": "lease-must-not-leak",
            "connection_string": "Server=tcp://host;Password=must-not-leak",
            "message": "vault response included secret=deep-must-not-leak",
        },
        "events": [
            {"stage": "read", "message": "jwt=list-must-not-leak"},
            {"stage": "connect", "message": "driver emitted api_key=api-key-list-must-not-leak"},
            "aws_access_key_id=access-key-list-must-not-leak",
            "token=string-list-must-not-leak",
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
    assert "must-not-leak" not in serialized
    assert "also-must-not-leak" not in serialized
    assert "deep-must-not-leak" not in serialized
    assert "list-must-not-leak" not in serialized
    assert "api-key-must-not-leak" not in serialized
    assert "access-key-must-not-leak" not in serialized
    assert "api-key-list-must-not-leak" not in serialized
    assert "access-key-list-must-not-leak" not in serialized
    assert "string-list-must-not-leak" not in serialized


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
