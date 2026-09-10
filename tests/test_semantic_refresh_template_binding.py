"""Characterize immutable template proof bindings across the provider boundary."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from copy import deepcopy
from dataclasses import replace
from typing import Any, get_type_hints

import pytest
from dpone_airflow_pack.pack_identity import PackIdentityError, compute_pack_fingerprint
from dpone_airflow_pack.semantic_refresh_topology import SemanticRefreshTopologyError

from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_publishing import DbtExecutionPack
from dpone.contracts.semantic_refresh_core import semantic_refresh_sha256
from dpone.readiness import dbt_semantic_refresh_airflow_validation as validation
from dpone.readiness.dbt_semantic_refresh_airflow_pack import semantic_refresh_activated_pack
from tests.test_dbt_execution_pack_versions import _payload as _v2_execution
from tests.test_dbt_semantic_refresh_plan_compiler import (
    DIGESTS,
    MODEL_ID,
    _activation_receipt,
    _execution_pack,
    _plan_bundle,
    _pre_release,
    _run_bundle,
    _template_pack,
)


def _bytes(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")).encode()


@pytest.mark.parametrize(
    ("stage", "error", "message"),
    [
        (0, ValueError, "semantic-refresh template fields are not closed"),
        (1, PackIdentityError, "pack_fingerprint does not match the derived workload pack identity"),
        (2, ValueError, "semantic-refresh release input is not a non-executable V2 template"),
        (3, SemanticRefreshTopologyError, "semantic-refresh topology fields are not closed"),
        (4, DbtPublishingError, "dbt execution pack schema is invalid"),
        (5, DbtPublishingError, "dbt execution pack differs from release wire version"),
        (6, ValueError, "pre_release_bundle_sha256 must be a canonical sha256 digest"),
        (7, ValueError, "package_artifacts_sha256 must be a canonical sha256 digest"),
    ],
)
def test_template_preserves_first_error_with_competing_faults(stage: int, error: type[Exception], message: str) -> None:
    pack: dict[str, Any] = deepcopy(_template_pack())
    semantic = pack["semantic_refresh"]
    semantic["package_artifacts_sha256"] = "bad"
    if stage < 7:
        semantic["pre_release_bundle_sha256"] = "bad"
    if stage < 6:
        pack["dbt_execution_pack"] = _v2_execution() if stage == 5 else {}
    if stage < 4:
        semantic["topology"] = {}
    if stage < 3:
        pack["executable"] = True
    if stage == 0:
        pack["extra"] = True
    if stage >= 2:
        pack["pack_fingerprint"] = compute_pack_fingerprint(pack)
    before = _bytes(pack)

    with pytest.raises(error, match=message) as caught:
        validation.protected_template(pack)

    assert type(caught.value) is error
    assert _bytes(pack) == before


def test_template_and_activation_keep_exact_producer_bytes() -> None:
    template = _template_pack()
    before = _bytes(template)
    topology, execution, pre_release, package = validation.protected_template(template)
    assert execution.to_dict() == template["dbt_execution_pack"]
    assert topology.workflow_name == "daily_events"
    assert topology.model_unique_ids == (MODEL_ID,)
    assert pre_release == _pre_release().pre_release_bundle_sha256
    assert package == _pre_release().lifecycle_policy.package_artifacts_digest
    assert len(before) == 4188
    assert hashlib.sha256(before).hexdigest() == "598b1133cd32f545b450fc27f733de0b010188d564064d2f62e38bd829b2ea3e"

    plan = _plan_bundle()
    activated = semantic_refresh_activated_pack(
        template_pack=template,
        plan_bundle=plan,
        run_execution=_run_bundle(plan),
        authority_receipt=_activation_receipt(plan),
    )
    raw = _bytes(activated.to_dict())
    assert len(raw) == 8219
    assert hashlib.sha256(raw).hexdigest() == "a729ce54abe76a83790b4b55ac5c7dabdefc53e73719c34b8a759bfb56d8749c"
    assert _bytes(template) == before


@pytest.mark.parametrize("invalid", [None, {}, object()])
def test_proof_admission_rejects_untyped_values_before_coordinate_access(invalid: Any) -> None:
    with pytest.raises(TypeError, match="typed pre-release proof bundle"):
        validation.SemanticRefreshTemplateProofAuthority.from_pre_release(invalid)
    with pytest.raises(TypeError, match="typed proof coordinates"):
        validation.validate_pre_release_template(_execution_pack(), {}, invalid)
    with pytest.raises(TypeError, match="typed plan bundle"):
        validation.validate_plan_template(invalid, None, None, "bad", "bad")
    with pytest.raises(TypeError, match="typed plan and run bundles"):
        validation.validate_plan_run_template(invalid, invalid, None, None, "bad", "bad")
    with pytest.raises(TypeError, match="typed plan and run bundles"):
        validation.validate_plan_run_template(_plan_bundle(), invalid, None, None, "bad", "bad")
    with pytest.raises(TypeError, match="protected persistence receipt"):
        validation.validate_activation_inputs(invalid, invalid, invalid, None, None, "bad", "bad")
    with pytest.raises(TypeError, match="typed plan and run bundles"):
        validation.validate_activation_inputs(
            invalid, invalid, _activation_receipt(_plan_bundle()), None, None, "bad", "bad"
        )


def test_native_wire_rejection_precedes_untyped_pre_release_proof() -> None:
    with pytest.raises(DbtPublishingError, match="differs from release wire version"):
        validation.validate_pre_release_template(DbtExecutionPack.from_mapping(_v2_execution()), {}, None)


def test_pre_release_digest_check_precedes_model_closure() -> None:
    proof = _pre_release()
    invalid_closure = type(proof.mutation_closure).build(
        status=proof.mutation_closure.status,
        selectors=("fqn:foreign",),
        selected_node_ids=("model.foreign",),
        selected_mutating_node_ids=("model.foreign",),
        selected_read_only_node_ids=(),
        unclassified_mutating_node_ids=(),
    )
    forged = replace(proof, mutation_closure=invalid_closure, pre_release_bundle_sha256=DIGESTS[14])
    with pytest.raises(ValueError, match="pre-release proof bundle digest differs"):
        validation.SemanticRefreshTemplateProofAuthority.from_pre_release(forged)
    payload = forged.to_dict()
    payload.pop("pre_release_bundle_sha256")
    forged = replace(forged, pre_release_bundle_sha256=semantic_refresh_sha256(payload))
    with pytest.raises(ValueError, match="pre-release mutation closure differs from its models"):
        validation.SemanticRefreshTemplateProofAuthority.from_pre_release(forged)


@pytest.mark.parametrize(
    "field", ["workflow_name", "manifest_sha256", "profile_sha256", "toolchain_sha256", "model_unique_ids"]
)
def test_pre_release_requires_every_exact_proof_coordinate(field: str) -> None:
    authority = validation.SemanticRefreshTemplateProofAuthority.from_pre_release(_pre_release())
    changed = {
        field: ("model.foreign",)
        if field == "model_unique_ids"
        else "foreign"
        if field == "workflow_name"
        else DIGESTS[14]
    }
    authority = replace(authority, **changed)
    with pytest.raises(ValueError, match="exact pre-release proof closure"):
        validation.validate_pre_release_template(
            _execution_pack(), _template_pack()["semantic_refresh"]["topology"], authority
        )


@pytest.mark.parametrize("models", [(MODEL_ID,), [], [MODEL_ID, MODEL_ID]])
def test_pre_release_topology_keeps_exact_array_and_model_closure(models: object) -> None:
    topology = _template_pack()["semantic_refresh"]["topology"]
    topology["model_unique_ids"] = models
    authority = validation.SemanticRefreshTemplateProofAuthority.from_pre_release(_pre_release())
    with pytest.raises(ValueError, match="exact pre-release proof closure"):
        validation.validate_pre_release_template(_execution_pack(), topology, authority)


@pytest.mark.parametrize("field", ["publish_model_unique_ids", "selected_graph_unique_ids"])
def test_pre_release_does_not_reduce_ordered_selection_to_a_set(field: str) -> None:
    execution = _execution_pack()
    # Deliberately tamper with an already typed contract: equality must not trust
    # its type or collapse duplicate selected IDs into the admitted model set.
    object.__setattr__(execution.selection_lock, field, (MODEL_ID, MODEL_ID))
    authority = validation.SemanticRefreshTemplateProofAuthority.from_pre_release(_pre_release())
    with pytest.raises(ValueError, match="exact pre-release proof closure"):
        validation.validate_pre_release_template(execution, _template_pack()["semantic_refresh"]["topology"], authority)


@pytest.mark.parametrize("coordinate", ["pre_release", "package", "models", "workflow", "execution_models"])
def test_deployment_requires_every_template_coordinate(coordinate: str) -> None:
    topology, execution, pre_release, package = validation.protected_template(_template_pack())
    if coordinate == "pre_release":
        pre_release = DIGESTS[14]
    elif coordinate == "package":
        package = DIGESTS[14]
    elif coordinate == "models":
        topology = replace(topology, model_unique_ids=("model.foreign",))
    elif coordinate == "workflow":
        topology = replace(topology, workflow_name="foreign")
    else:
        object.__setattr__(execution.selection_lock, "publish_model_unique_ids", ("model.foreign",))
    with pytest.raises(ValueError, match="template/plan closure"):
        validation.validate_plan_template(_plan_bundle(), topology, execution, pre_release, package)


@pytest.mark.parametrize("coordinate", ["plan", "workflow_plan", "selected_ids"])
def test_run_proof_requires_exact_plan_and_ordered_selected_closure(coordinate: str) -> None:
    plan = _plan_bundle()
    run = _run_bundle(plan)
    if coordinate == "plan":
        run = replace(run, plan_bundle_sha256=DIGESTS[14])
    elif coordinate == "workflow_plan":
        object.__setattr__(run.workflow_execution_binding, "workflow_plan_sha256", DIGESTS[14])
    else:
        object.__setattr__(run.workflow_execution_binding, "selected_mutating_node_ids", (MODEL_ID, MODEL_ID))
    topology, execution, pre_release, package = validation.protected_template(_template_pack())
    with pytest.raises(ValueError, match="template/plan/run closure"):
        validation.validate_plan_run_template(plan, run, topology, execution, pre_release, package)
    # Template disagreement retains priority over competing run disagreement.
    with pytest.raises(ValueError, match="template/plan closure"):
        validation.validate_plan_run_template(plan, run, topology, execution, "bad", package)


@pytest.mark.parametrize(
    "field",
    [
        "plan_bundle_sha256",
        "release_id",
        "deployment_id",
        "baseline_receipts",
        "runtime_assurance_receipts",
        "route_certification_receipt_sha256",
    ],
)
def test_activation_rejects_each_receipt_coordinate(field: str) -> None:
    plan = _plan_bundle()
    receipt = _activation_receipt(plan)
    values = receipt.to_dict()
    values.pop("schema")
    values.pop("activation_authority_receipt_sha256")
    values["baseline_receipts"] = receipt.baseline_receipts
    values["runtime_assurance_receipts"] = receipt.runtime_assurance_receipts
    if field == "baseline_receipts":
        values[field] = ((MODEL_ID, DIGESTS[14]),)
    elif field == "runtime_assurance_receipts":
        values[field] = tuple((model, kind, DIGESTS[14]) for model, kind, _ in receipt.runtime_assurance_receipts)
    else:
        values[field] = DIGESTS[14]
    mismatched = type(receipt).build(**values)

    with pytest.raises(ValueError, match="template/plan/run closure"):
        validation.validate_activation_inputs(
            plan, _run_bundle(plan), mismatched, *validation.protected_template(_template_pack())
        )


@pytest.mark.parametrize("value", [None, b"sha256:" + b"a" * 64, "sha256:" + "A" * 64, "a" * 64, "sha256:" + "g" * 64])
def test_template_digest_requires_canonical_lowercase_text(value: object) -> None:
    with pytest.raises(ValueError, match="manifest_sha256 must be a canonical sha256 digest"):
        validation.digest(value, "manifest_sha256")


def test_canonical_proof_owner_is_framework_independent_and_reexported() -> None:
    from dpone.contracts.dbt_semantic_refresh_template_binding import SemanticRefreshTemplateProofAuthority
    from dpone.readiness.dbt_semantic_refresh_airflow_pack import (
        SemanticRefreshTemplateProofAuthority as producer_authority,
    )

    assert SemanticRefreshTemplateProofAuthority is validation.SemanticRefreshTemplateProofAuthority
    assert SemanticRefreshTemplateProofAuthority is producer_authority
    assert SemanticRefreshTemplateProofAuthority.__module__ == "dpone.contracts.dbt_semantic_refresh_template_binding"
    assert get_type_hints(SemanticRefreshTemplateProofAuthority)["model_unique_ids"] == tuple[str, ...]


def test_pure_policy_accepts_primitive_coordinates_and_retains_runtime_type_guards() -> None:
    from dpone.contracts import dbt_semantic_refresh_template_binding as binding

    coordinates = binding.SemanticRefreshTemplateCoordinates("daily_events", (MODEL_ID,))
    plan = _plan_bundle()
    proof = _pre_release()
    args = (
        coordinates,
        _execution_pack(),
        proof.pre_release_bundle_sha256,
        proof.lifecycle_policy.package_artifacts_digest,
    )
    binding.validate_activation_inputs(plan, _run_bundle(plan), _activation_receipt(plan), *args)
    assert binding.expected_assurance_receipts(plan) == _activation_receipt(plan).runtime_assurance_receipts
    with pytest.raises(TypeError, match="typed plan bundle"):
        binding.validate_plan_template(plan.to_dict(), *args)
    with pytest.raises(TypeError, match="typed plan and run bundles"):
        binding.validate_plan_run_template(plan, _run_bundle(plan).to_dict(), *args)
    with pytest.raises(TypeError, match="protected persistence receipt"):
        binding.validate_activation_inputs(plan, _run_bundle(plan), _activation_receipt(plan).to_dict(), *args)
    assert validation.expected_assurance_receipts is binding.expected_assurance_receipts
    assert validation.digest is binding.digest
    assert validation.validate_pre_release_template is binding.validate_pre_release_template


def test_compatibility_adapters_preserve_raw_signature_annotations() -> None:
    from dpone.contracts import dbt_semantic_refresh_template_binding as binding

    expected = {
        "plan": "SemanticRefreshPlanBundle",
        "run": "SemanticRefreshRunExecutionBundle",
        "receipt": "SemanticRefreshActivationAuthorityReceipt",
        "topology": "SemanticRefreshTopologyTemplate",
        "execution_pack": "DbtExecutionPack",
        "pre_release_sha256": "str",
        "package_sha256": "str",
        "return": "None",
    }
    assert validation.validate_activation_inputs.__annotations__ == expected
    for method, omitted in (
        (validation.validate_plan_run_template, {"receipt"}),
        (validation.validate_plan_template, {"run", "receipt"}),
    ):
        assert method.__annotations__ == {key: value for key, value in expected.items() if key not in omitted}
    namespace = {**vars(binding), **vars(validation)}
    assert get_type_hints(validation.validate_activation_inputs, globalns=namespace)["plan"] is type(_plan_bundle())


def test_contract_import_does_not_load_provider_or_readiness() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; from dpone.contracts import dbt_semantic_refresh_template_binding; print([name for name in sys.modules if name.startswith(('dpone_airflow_pack', 'airflow.', 'dpone.readiness'))])",
        ],
        text=True,
        capture_output=True,
        check=True,
    )
    assert result.stdout.strip() == "[]"
