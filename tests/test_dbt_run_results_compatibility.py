"""Released runtime result identities survive delegation to the pure parser."""

import base64
import inspect
import pickle
from dataclasses import FrozenInstanceError, fields
from typing import get_type_hints

import pytest

from dpone.contracts import dbt_run_results as canonical
from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.runtime import dbt_run_results as runtime


def test_released_runtime_pickle_and_record_identity():
    # Protocol-4 bytes produced by the unchanged v0.80.0 runtime DTO classes.
    original = base64.b64decode(
        "gASVnwAAAAAAAACMHWRwb25lLnJ1bnRpbWUuZGJ0X3J1bl9yZXN1bHRzlIwTUGFyc2VkRGJ0"
        "UnVuUmVzdWx0c5STlCmBlF2UKIwCdjaUjAYxLjEyLjOUjAZsZWdhY3mUaACMFFBhcnNlZERidE5v"
        "ZGVPdXRjb21llJOUKYGUXZQojAxtb2RlbC5kZW1vLniUjAdzdWNjZXNzlEc/+AAAAAAAAGVihZRlYi4="
    )
    result = pickle.loads(original)
    assert type(result) is runtime.ParsedDbtRunResults
    assert type(result.nodes[0]) is runtime.ParsedDbtNodeOutcome
    assert result == runtime.ParsedDbtRunResults(
        "v6", "1.12.3", "legacy", (runtime.ParsedDbtNodeOutcome("model.demo.x", "success", 1.5),)
    )
    assert pickle.dumps(result, protocol=4) == original
    assert not hasattr(result, "__dict__")
    with pytest.raises(FrozenInstanceError):
        result.invocation_id = "changed"
    assert [field.name for field in fields(result)] == ["schema_version", "dbt_version", "invocation_id", "nodes"]


def test_released_signatures_and_runtime_type_hints():
    assert str(inspect.signature(runtime.ParsedDbtNodeOutcome)) == (
        "(unique_id: 'str', status: 'str', execution_time: 'float') -> None"
    )
    assert str(inspect.signature(runtime.ParsedDbtRunResults)) == (
        "(schema_version: 'str', dbt_version: 'str', invocation_id: 'str', "
        "nodes: 'tuple[ParsedDbtNodeOutcome, ...]') -> None"
    )
    assert get_type_hints(runtime.ParsedDbtRunResults)["nodes"] == tuple[runtime.ParsedDbtNodeOutcome, ...]
    assert get_type_hints(runtime.parse_dbt_run_results)["return"] is runtime.ParsedDbtRunResults
    assert (
        get_type_hints(runtime.read_dbt_run_results)["return"]
        == (tuple[runtime.ParsedDbtRunResults | None, DbtPublishingError | None])
    )
    assert get_type_hints(runtime.dbt_node_outcomes)["parsed"] == runtime.ParsedDbtRunResults | None
    assert runtime.ParsedDbtRunResults is not canonical.ParsedDbtRunResults


@pytest.mark.parametrize("status", ["success", "pass", "no-op", "warn", "error", "fail", "skipped"])
@pytest.mark.parametrize("policy", ["fail", "allow", "invalid"])
def test_canonical_and_compatibility_policy_agree(status, policy):
    old = runtime.ParsedDbtRunResults("v6", "1.12.3", "id", (runtime.ParsedDbtNodeOutcome("m", status, 0.0),))
    pure = canonical.ParsedDbtRunResults("v6", "1.12.3", "id", (canonical.ParsedDbtNodeOutcome("m", status, 0.0),))
    assert old.warning_count == pure.warning_count
    if policy == "invalid":
        for value in (old, pure):
            with pytest.raises(DbtPublishingError) as caught:
                value.passes(policy)
            assert caught.value.code == "DPONE_DBT_RUN_RESULTS_INVALID"
    else:
        assert old.passes(policy) == pure.passes(policy)


def test_runtime_parser_adapts_canonical_nodes_without_reinterpreting_payload():
    payload = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v6.json",
            "dbt_version": "1.12.3",
            "generated_at": "now",
            "invocation_id": "id",
            "env": {},
        },
        "elapsed_time": 1,
        "results": [
            {
                "unique_id": "model.demo.x",
                "status": "success",
                "execution_time": 1.5,
                "timing": [],
                "adapter_response": {},
            }
        ],
    }
    options = dict(
        expected_run_result_unique_ids=("model.demo.x",), expected_dbt_version="1.12.3", expected_schema_version="v6"
    )
    parsed = runtime.parse_dbt_run_results(payload, **options)
    assert type(parsed) is runtime.ParsedDbtRunResults
    assert type(parsed.nodes[0]) is runtime.ParsedDbtNodeOutcome
    assert parsed.nodes[0].execution_time == canonical.parse_dbt_run_results(payload, **options).nodes[0].execution_time
    payload["results"][0]["unique_id"] = "foreign"
    for parser in (runtime.parse_dbt_run_results, canonical.parse_dbt_run_results):
        with pytest.raises(DbtPublishingError) as caught:
            parser(payload, **options)
        assert caught.value.code == "DPONE_DBT_SELECTION_DRIFT"
