from __future__ import annotations

from dpone.contracts.runtime_connection import (
    ResolvedBindingConnection,
    ResolvedConnectionDescriptor,
)
from dpone.runtime.api_registry import build_api_runtime_source_from_connection
from dpone.runtime.credentials.config import CredentialsConfig


def test_resolved_mindbox_factory_adapts_options_to_typed_connector_config() -> None:
    source = build_api_runtime_source_from_connection(
        source_cfg={
            "type": "api",
            "api_type": "mindbox",
            "options": {
                "max_retries": 5,
                "rate_limit_delay": 0.25,
                "timeout": 17,
            },
        },
        resolved_connection=ResolvedBindingConnection(
            credentials=CredentialsConfig(api_key="secret"),
            safe_metadata={},
            descriptor=ResolvedConnectionDescriptor(
                connection_type="mindbox",
                properties={
                    "endpoint": "https://api.example.invalid",
                    "endpoint_id": "example",
                },
            ),
        ),
    )

    assert source.connector.timeout == 17
    assert source.connector.retry_config.max_retries == 5
    assert source.connector.rate_limit_config.requests_per_second == 4.0


def test_resolved_credential_free_api_factory_needs_no_fake_connection() -> None:
    source = build_api_runtime_source_from_connection(
        source_cfg={
            "type": "api",
            "api_type": "cbr",
            "options": {"timeout": 7},
        },
        resolved_connection=None,
    )

    assert source.connector.timeout == 7
