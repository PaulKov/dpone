"""Text rendering keeps structured identity while exposing usable docs links."""

from __future__ import annotations

from dpone.commands.docs_url_rendering import public_docs_url
from dpone.readiness.error_contract import error_docs_url


def test_error_contract_stays_repository_relative_but_cli_link_is_hosted() -> None:
    structured = error_docs_url("DPONE_AIRFLOW_DISABLED")

    assert structured == "docs/errors/DPONE_AIRFLOW_DISABLED.md"
    assert public_docs_url(structured) == "https://paulkov.github.io/dpone/errors/DPONE_AIRFLOW_DISABLED/"


def test_unknown_or_external_docs_url_is_not_rewritten() -> None:
    assert public_docs_url("docs/studio.md#troubleshooting") == "docs/studio.md#troubleshooting"
    assert public_docs_url("https://example.com/help") == "https://example.com/help"
