"""Invocation identity is usable before execution-pack assembly or SDK import."""

import subprocess
import sys
from dataclasses import FrozenInstanceError, replace

import pytest

from dpone.contracts.dbt_contract_validation import DbtPublishingError


@pytest.mark.parametrize(
    "module", ["dpone.contracts.dbt_invocation", "dpone.contracts.dbt_selection", "dpone.adapters.dbt_parse_target"]
)
def test_target_import_does_not_acquire_execution_or_sdk_contracts(module):
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            f"import {module}; "
            "from dpone.contracts.dbt_invocation import DbtInvocationTarget; "
            "import sys; "
            "assert DbtInvocationTarget('analytics', 'base').to_dict() "
            "== {'database': 'analytics', 'schema': 'base'}; "
            "assert not ({'dpone.contracts.dbt_execution_pack', "
            "'dpone.contracts.dbt_toolchain', 'dbt', 'airflow'} & sys.modules.keys())",
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == result.stderr == ""


def test_execution_pack_import_keeps_same_target_class_and_detached_mapping():
    from dpone.contracts.dbt_execution_pack import DbtInvocationTarget as PackTarget
    from dpone.contracts.dbt_invocation import DbtInvocationTarget

    assert PackTarget is DbtInvocationTarget
    raw = {"database": "analytics", "schema": "base"}
    target = DbtInvocationTarget.from_mapping(raw)
    raw["schema"] = "changed"
    emitted = target.to_dict()
    assert emitted == {"database": "analytics", "schema": "base"}
    emitted["database"] = "changed"
    assert target == PackTarget("analytics", "base")
    with pytest.raises(FrozenInstanceError):
        target.schema = "changed"


@pytest.mark.parametrize("field", ["database", "schema"])
@pytest.mark.parametrize("value", [None, False, 42, "", " leading", "trailing ", "a b", "a\x00b", "-flag", "x" * 257])
def test_target_preserves_strict_tokens_bounds_and_pack_error(field, value):
    from dpone.contracts.dbt_invocation import DbtInvocationTarget

    raw = {"database": "analytics", "schema": "base", field: value}
    with pytest.raises(DbtPublishingError) as caught:
        DbtInvocationTarget.from_mapping(raw)
    assert caught.value.code == "DPONE_DBT_PACK_INVALID"
    with pytest.raises(DbtPublishingError) as replaced:
        replace(DbtInvocationTarget("analytics", "base"), **{field: value})
    assert replaced.value.code == caught.value.code
    assert str(replaced.value) == str(caught.value)


@pytest.mark.parametrize("raw", [None, [], {}, {"database": "db"}, {"database": "db", "schema": "s", "extra": 1}])
def test_target_rejects_missing_or_extra_fields(raw):
    from dpone.contracts.dbt_invocation import DbtInvocationTarget

    with pytest.raises(DbtPublishingError) as caught:
        DbtInvocationTarget.from_mapping(raw)
    assert caught.value.code == "DPONE_DBT_PACK_INVALID"


def test_target_preserves_unicode_case_and_maximum_length():
    from dpone.contracts.dbt_invocation import DbtInvocationTarget

    raw = {"database": "Данные_É", "schema": "x" * 256}
    assert DbtInvocationTarget.from_mapping(raw).to_dict() == raw
