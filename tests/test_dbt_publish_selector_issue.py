"""Selector failures retain their public diagnostic and recovery contract."""

import pytest

from dpone.commands.dbt_publish_cli_support import model_selector_issue


@pytest.mark.parametrize("ambiguous", [False, True])
def test_selector_issue_preserves_public_diagnostic(ambiguous: bool) -> None:
    issue = model_selector_issue("sales.orders", ambiguous=ambiguous)

    assert issue.code == ("DPONE_DBT_MODEL_AMBIGUOUS" if ambiguous else "DPONE_DBT_MODEL_NOT_FOUND")
    assert issue.message == (
        "dbt model selector is ambiguous: sales.orders"
        if ambiguous
        else "dbt model is not publish-enabled or does not exist: sales.orders"
    )
    assert issue.path == "manifest.json"
    assert issue.remediation == (
        "Retry with the exact dbt unique_id shown by `dpone dbt check`."
        if ambiguous
        else "Run `dpone dbt check` to list publish-enabled models, then retry with the exact dbt unique_id."
    )
