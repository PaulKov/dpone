"""Launch evidence precedes actual helper startup; descriptions are not ACKs."""

from copy import deepcopy
from dataclasses import replace
from hashlib import sha256
from uuid import UUID

import pytest

from dpone.contracts import mssql_sqlclient_observe_departure_evidence as evidence
from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind as Kind
from dpone.contracts.mssql_sqlclient_observe_departure import SqlClientObserveDepartureRequest
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from tests.mssql_sqlclient_departure_v2_fixtures import alias, corrupt, leaves
from tests.test_mssql_sqlclient_observe_departure_contract import request


def test_plan_only_precedes_startup_and_preserves_legacy_bytes(monkeypatch):
    r = request()
    plan, admission = r.plan, deepcopy(r.plan.observer_admission)
    legacy = evidence.encode_observe_departure_evidence(
        plan, kind=Kind.LAUNCH_INTENT, request=r, observer_admission=admission
    )
    legacy_receipt = evidence.observe_departure_evidence_receipt(
        legacy, kind=Kind.LAUNCH_INTENT, request=r, observer_admission=admission
    )
    del r

    def no_startup(*args, **kwargs):
        pytest.fail("prelaunch encoding attempted to construct a request/startup")

    monkeypatch.setattr(SqlClientObserveDepartureRequest, "__post_init__", no_startup)
    body = evidence.encode_observe_departure_launch_intent(plan, observer_admission=admission)
    assert body == legacy
    # Frozen pre-change producer vector; independent of both current entrypoints.
    assert len(body) == 4248
    assert sha256(body).hexdigest() == "4cc9288be9389af95b760e58ee288b2c3e420b6991246acb90c791a60f19a736"
    assert evidence.decode_observe_departure_launch_intent(body, plan=plan, observer_admission=admission) == plan
    receipt = evidence.observe_departure_launch_intent_receipt(body, plan=plan, observer_admission=admission)
    assert receipt == legacy_receipt
    assert receipt.payload_sha256 == sha256(body).hexdigest()
    assert receipt.byte_count == len(body)


@pytest.mark.parametrize("target", ["plan", "admission"])
def test_nested_corruption_rejects_before_hash(target, monkeypatch):
    r = request()
    payload = evidence.encode_observe_departure_launch_intent(r.plan, observer_admission=r.plan.observer_admission)
    original = r.plan if target == "plan" else r.plan.observer_admission

    def no_hash(*args, **kwargs):
        pytest.fail("malformed original reached evidence hash")

    monkeypatch.setattr(evidence, "sha256", no_hash)
    for path, value in leaves(original):
        bad = corrupt(deepcopy(original), path, alias(value))
        plan = bad if target == "plan" else r.plan
        admission = bad if target == "admission" else r.plan.observer_admission
        with pytest.raises(ValueError):
            evidence.encode_observe_departure_launch_intent(plan, observer_admission=admission)
        with pytest.raises(ValueError):
            evidence.observe_departure_launch_intent_receipt(payload, plan=plan, observer_admission=admission)


@pytest.mark.parametrize("change", ["helper", "artifact", "admission"])
def test_independent_subject_and_verifier_binding(change):
    r = request()
    raw = evidence.encode_observe_departure_launch_intent(r.plan, observer_admission=r.plan.observer_admission)
    plan, admission = r.plan, r.plan.observer_admission
    if change == "helper":
        plan = replace(plan, helper_id=UUID(int=973))
    elif change == "artifact":
        plan = replace(plan, preparation_artifact_sha256="f" * 64)
    else:
        admission = replace(admission, login=replace(admission.login, name="different", original_name="different"))
    with pytest.raises(ValueError):
        evidence.observe_departure_launch_intent_receipt(raw, plan=plan, observer_admission=admission)


@pytest.mark.parametrize("change", ["space", "duplicate", "extra", "schema", "oversize", "nested_oversize"])
def test_noncanonical_unknown_and_bounded_payloads(change):
    r = request()
    raw = evidence.encode_observe_departure_launch_intent(r.plan, observer_admission=r.plan.observer_admission)
    if change == "space":
        raw += b" "
    elif change == "duplicate":
        raw = b'{"schema":"duplicate",' + raw[1:]
    elif change == "oversize":
        raw = b" " * (131072 + 1)
    else:
        data = strict_json_object(raw)
        if change == "extra":
            data["startup"] = {}
        elif change == "schema":
            data["schema"] = "dpone.sqlclient.departure-launch-intent-evidence.v2"
        else:
            data["plan"]["package_root"] = "/" + "x" * 65536
        raw = canonical_json_bytes(data)
    with pytest.raises(ValueError):
        evidence.decode_observe_departure_launch_intent(raw, plan=r.plan, observer_admission=r.plan.observer_admission)


@pytest.mark.parametrize("entry", ["encode", "decode", "receipt"])
def test_full_request_launch_wrapper_still_validates_startup(entry):
    r = request()
    raw = evidence.encode_observe_departure_launch_intent(r.plan, observer_admission=r.plan.observer_admission)
    object.__setattr__(r.startup.process, "pid", True)
    kw = dict(kind=Kind.LAUNCH_INTENT, request=r, observer_admission=r.plan.observer_admission)
    with pytest.raises(ValueError):
        if entry == "encode":
            evidence.encode_observe_departure_evidence(r.plan, **kw)
        elif entry == "decode":
            evidence.decode_observe_departure_evidence(raw, **kw)
        else:
            evidence.observe_departure_evidence_receipt(raw, **kw)
