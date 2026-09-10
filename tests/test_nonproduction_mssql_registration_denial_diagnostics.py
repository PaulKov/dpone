"""Unclassified SQL errors retain safe context and never become permission proof."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from tests.integration.composition import nonproduction_mssql_registration_live_support as support
from tests.integration.composition import test_nonproduction_mssql_registration_live as live
from tests.integration.composition.mssql_gate_live_provisioning import SqlFailure
from tests.integration.composition.nonproduction_mssql_trust_live_support import require_sql_rejection


@pytest.mark.parametrize("index,label", enumerate(support.DENIAL_OPERATIONS, 1))
def test_unclassified_driver_error_retains_operation_and_fails(index, label):
    observed = []
    case = SimpleNamespace(record=lambda name, payload: observed.append((name, support.observation_document(payload))))
    with pytest.raises(AssertionError, match="unexpected_sql_rejection"):
        live.observe_denial(case, index, label, Mock(side_effect=SqlFailure(None)), {229, 4834})
    assert observed == [
        (
            f"denial_{index}",
            json.dumps(
                {"operation": label, "operation_index": index, "unclassified_sql_error": True},
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
    ]


@pytest.mark.parametrize("code", [102, 4861, 4902, None])
def test_bulk_denial_helper_rejects_syntax_missing_file_and_unclassified_errors(code):
    with pytest.raises(AssertionError, match="unexpected_sql_rejection"):
        require_sql_rejection(Mock(side_effect=SqlFailure(code)), {229, 4834})


def test_bulk_denial_helper_rejects_unexpected_success():
    with pytest.raises(pytest.fail.Exception):
        require_sql_rejection(lambda: None, {229, 4834})


def test_unclassified_diagnostic_accepts_only_boolean():
    with pytest.raises(ValueError, match="unsafe_registration_observation"):
        support.observation_document({"unclassified_sql_error": "private-driver-text"})
