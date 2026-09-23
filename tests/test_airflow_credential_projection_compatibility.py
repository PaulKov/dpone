"""Freeze old wire schemas while requiring explicit closed v6 credential delivery."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest
from dpone_airflow_pack.init_fetch_contract import InitFetchProviderError, init_fetch_context_from_payload

from dpone.gitops.schema_contracts import get_gitops_schema_contract
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from tests.test_airflow_credential_projection_delivery import delivery_case


@pytest.mark.parametrize("family", ["deployment-set", "airflow-deployment-index", "airflow-runtime-init-fetch-plan"])
@pytest.mark.parametrize("version", [2, 3, 4, 5])
def test_preexisting_wire_schema_is_unchanged(family, version):
    name = f"{family}-v{version}"
    path = Path(__file__).parents[1] / "docs" / "schemas" / "gitops" / f"{name}.schema.json"
    assert get_gitops_schema_contract(f"dpone.{family}.v{version}").schema == json.loads(path.read_text())


def test_v6_index_and_plan_validate_without_development_authority():
    pack, _kwargs, context, _body, _inputs, index = delivery_case()
    validator = GitOpsSchemaValidator()
    assert validator.validate(index, expected_kind=index["schema"]) == ()
    encoded = context.encode_plan(
        workload_id=pack["workload"]["workload_id"],
        execution_kind="runtime",
        execution_scope="workload",
        hook_execution="externalized",
    )
    plan = json.loads(encoded.payload)
    assert validator.validate(plan, expected_kind=plan["schema"]) == ()


@pytest.mark.parametrize("version", [2, 3, 4, 5])
def test_projection_cannot_be_smuggled_into_older_index(version):
    *_rest, index = delivery_case()
    index["schema"] = f"dpone.airflow-deployment-index.v{version}"
    with pytest.raises(InitFetchProviderError):
        init_fetch_context_from_payload(index)


def test_v6_projection_is_required_and_unknown_fields_fail():
    *_rest, index = delivery_case()
    missing = deepcopy(index)
    missing.pop("credential_projection")
    with pytest.raises(InitFetchProviderError):
        init_fetch_context_from_payload(missing)
    unknown = deepcopy(index)
    unknown["credential_projection"]["inline_value"] = "forbidden-value"
    assert GitOpsSchemaValidator().validate(unknown, expected_kind=unknown["schema"])
