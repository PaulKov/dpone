"""Original retained OBSERVE → authenticated evidence → one terminal Prepared ACK.

Real baseline admission is deliberately closed. Controlled tests replace only
that trusted admission seam; no production fixture/Boolean/hash escape exists.
"""

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import asdict, fields, is_dataclass
from datetime import datetime
from enum import Enum
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import UUID

from dpone.adapters.mssql_permission_preparation_capabilities import (
    AdmittedSqlClientInstallation,
    DescriptorPinnedCreateOnlyEvidenceWriter,
    PreparationEvidenceActor,
    parse_observer_incarnation_rows,
    query_hashes,
    require_input_descriptor,
)
from dpone.app.mssql_sqlclient_create_evidence_composition import (
    acquire_authenticated_create,
    recheck_authenticated_create,
)
from dpone.app.mssql_sqlclient_grant_inventory_composition import SqlClientGrantInventoryCollector
from dpone.app.mssql_sqlclient_observe_composition import SqlClientRetainedObserve
from dpone.app.mssql_sqlclient_stage_locator_composition import read_sqlclient_stage_locator
from dpone.contracts.mssql_permission_preparation_capabilities import (
    ERROR,
    PreparationReceipt,
    attempt_identity_digest,
    parse_baseline,
    preparation_bytes,
)
from dpone.contracts.mssql_tds_api import (
    OPCODE_LIMITS,
    AdmittedPreparationBaseline,
    NativeBulkTransportPolicy,
    PreparationBaselineAuthority,
    SqlClientDatabasePrincipal,
    SqlClientGrantMember,
    SqlClientInputDescriptor,
    SqlClientStageLookup,
    WindowOutcomeUnknown,
    _original,
    authenticated_create_bytes,
    canonical_json_bytes,
    encode_authenticated_inventory,
    encode_input_descriptor,
    encode_preparation_rows,
    encode_stage_identity,
    observation_body,
    stage_identity_from_create,
    stage_object_identity,
    strict_json_object,
    validate_profile,
)
from dpone.services.mssql_tds_attempt import TdsAttempt
from dpone.services.mssql_tds_original_continuation import PreparationTransition


def _admit_baseline(value: AdmittedPreparationBaseline, authority: PreparationBaselineAuthority) -> dict[str, Any]:
    """Revalidate the deployment-qualified exact bytes before any effect."""
    if type(value) is not AdmittedPreparationBaseline or type(authority) is not PreparationBaselineAuthority:
        raise ValueError(ERROR)
    value.assert_authority(authority)
    return parse_baseline(value.baseline_bytes)


def _plain(value: Any) -> Any:
    """Technical originals only; caller validates records before this projection."""
    if isinstance(value, Enum):
        return value.value
    if type(value) is bytes:
        return value.hex()
    if type(value) is UUID:
        return str(value)
    if type(value) is datetime:
        return value.isoformat(timespec="microseconds")
    if is_dataclass(value):
        return {field.name: _plain(getattr(value, field.name)) for field in fields(value)}
    if type(value) in (list, tuple):
        return [_plain(item) for item in value]
    if type(value) is dict:
        return {key: _plain(item) for key, item in value.items()}
    return value


def _inventory(handle: SqlClientRetainedObserve, reader_factory: Any) -> Any:
    handle.assert_current()
    assert handle.catalog is not None and handle.continuation is not None
    request = handle.request
    principal = request.writer_principal
    collector = SqlClientGrantInventoryCollector.from_catalog(
        handle.catalog,
        management_admission=request.management_admission,
        writer_admission=request.writer_admission,
        writer_principal=SqlClientDatabasePrincipal(principal.principal_id, principal.name, principal.sid),
        admitted_factory=handle.factory,
        pool=handle.pool,
        limits=request.limits,
        deadline=handle.operation_deadline,
        continuation=handle.continuation,
    )
    result = collector.collect_authenticated(reader_factory=reader_factory)
    handle._observations.append(result)
    handle.assert_current()
    return result


def _selected_create(handle: SqlClientRetainedObserve, reader_factory: Any) -> tuple[Any, Any]:
    handle.assert_current()
    assert handle.authority is not None
    selected = handle.request.selected_stage
    lookup = SqlClientStageLookup(
        handle.factory.domain_id,
        handle.request.management_admission.server,
        handle.authority.database,
        selected.owner_binding,
        selected.object_nonce,
    )
    locator = read_sqlclient_stage_locator(
        admitted_factory=handle.factory, lookup=lookup, pool=handle.pool, deadline=handle.operation_deadline
    )
    handle.assert_current()
    member = SqlClientGrantMember(selected, locator)
    proof = acquire_authenticated_create(
        admitted_factory=handle.factory,
        member=member,
        reader_factory=reader_factory,
        pool=handle.pool,
        deadline=handle.operation_deadline,
        continuation=handle.continuation,
    )
    if (
        stage_identity_from_create(proof.evidence) != selected
        or proof.original.state.identity.parent != handle.request.parent
    ):
        raise ValueError(ERROR)
    handle.assert_current()
    return member, proof


def _profile(handle: SqlClientRetainedObserve, management: Any) -> dict[str, Any]:
    assert handle.catalog is not None
    result = {}
    for opcode in OPCODE_LIMITS:
        handle.assert_current()
        result[opcode] = handle.catalog.preparation(opcode)
        handle.assert_current()
    restored = parse_observer_incarnation_rows(
        [result["PREP_EFFECTIVE"][0]["restored_management"]], admission=handle.request.management_admission
    )
    if restored != management:
        raise ValueError(ERROR)
    return result


class SqlClientPreparationUnknown(WindowOutcomeUnknown):
    """Original retained operation; no forward or retry capability is exposed."""

    def __init__(self, retained: PreparationTransition) -> None:
        self.retained = retained
        super().__init__("mssql_native.sqlclient_preparation_unknown")

    def close(self, *, deadline: float) -> None:
        _cleanup(self.retained, deadline)


def _cleanup(state: PreparationTransition, deadline: float) -> None:
    failed = False
    if state._handle_close_attempted and not state._handle_closed:
        failed = True
    elif not state._handle_closed:
        state._handle_close_attempted = True
        try:
            state.handle.close(deadline=deadline)
            state._handle_closed = True
        except BaseException:
            failed = True
    if state.evidence is not None:
        if state._evidence_close_attempted and not state._evidence_closed:
            failed = True
        elif not state._evidence_closed:
            state._evidence_close_attempted = True
            try:
                state.evidence.close(deadline=deadline)
                state._evidence_closed = True
            except BaseException:
                failed = True
    if failed:
        state.failed = True
        raise SqlClientPreparationUnknown(state)


def prepare_sqlclient_attempt(
    handle: SqlClientRetainedObserve,
    *,
    parent_input: SqlClientInputDescriptor,
    input_fd: int,
    baseline: AdmittedPreparationBaseline,
    baseline_authority: PreparationBaselineAuthority,
    policy: Any,
    build: Any,
    reader_factory: Any,
    evidence_root: Path,
    expected_typed_digest: str,
) -> PreparationReceipt:
    """Consume the exact opened owner; require actual ACKs before returning Prepared."""
    baseline_document = _admit_baseline(baseline, baseline_authority)
    baseline_identity = baseline.evidence_identity()
    if (
        type(handle) is not SqlClientRetainedObserve
        or type(handle.attempt) is not TdsAttempt
        or handle.attempt._observe_origin is not handle
        or handle._used
        or handle._faulted
        or handle._closed
        or handle.attempt._preparation is not None
    ):
        raise ValueError(ERROR)
    handle.attempt._assert_composition_origin(handle.factory)
    assert handle.catalog is not None
    departure = handle.attempt._create_departure_outcome
    if departure is None or departure is not handle.attempt._create_departure_registered:
        raise ValueError(ERROR)
    _original(departure)
    if _plain(departure) != _plain(handle.attempt._create_departure_snapshot):
        raise ValueError(ERROR)
    if type(policy) is not NativeBulkTransportPolicy or type(build) is not AdmittedSqlClientInstallation:
        raise ValueError(ERROR)
    _original(policy)
    _original(build)
    if (
        policy.backend != "mssql_sqlclient"
        or sha256(canonical_json_bytes(policy.to_dict())).hexdigest() != handle.request.parent.policy_sha256
    ):
        raise ValueError(ERROR)
    handle._used = True
    state = PreparationTransition(handle.attempt, handle)
    handle.attempt._preparation = state
    state.policy, state.build, state.input = policy, build, parent_input
    from dpone.contracts.mssql_tds_api import SqlClientStageContentExpectation

    content_expectation = SqlClientStageContentExpectation(
        parent_input.expected.rows,
        parent_input.expected.file_sha256,
        expected_typed_digest,
    )
    state.capture_writer_inputs(
        policy,
        parent_input,
        build,
        policy_snapshot=canonical_json_bytes(policy.to_dict()),
        input_snapshot=encode_input_descriptor(parent_input),
        build_sha256=build.build_sha256,
        operation_deadline_ns=handle.request.operation_deadline_ns,
        installation_type=AdmittedSqlClientInstallation,
        content_expectation=content_expectation,
    )
    subject = attempt_identity_digest(handle.request.parent)

    @contextmanager
    def writer():
        yield DescriptorPinnedCreateOnlyEvidenceWriter(evidence_root)

    try:
        admitted_build = build.assert_admitted(deadline_ns=handle.request.operation_deadline_ns)
        input_stat = require_input_descriptor(input_fd, parent_input, parent=True)
        if parent_input.expected.file_sha256 != handle.request.parent.file_sha256:
            raise ValueError(ERROR)
        state.evidence = handle.pool.open(
            lambda end, clock: PreparationEvidenceActor(writer, subject, end, clock), deadline=state.deadline
        )
        with state.sequence():
            member, selected_create = _selected_create(handle, reader_factory)
            opening = state.opening = _inventory(handle, reader_factory)
            state.capture_management()
            stage_before = handle.catalog.observe_selected()
            observation_body(stage_before, handle.request)
            profile_open = _profile(handle, opening.inventory.management_before)
            validate_profile(baseline_document, handle.request, opening.inventory, profile_open)
            closing = state.closing = _inventory(handle, reader_factory)
            profile_close = _profile(handle, closing.inventory.management_before)
            stage_after = handle.catalog.observe_selected()
            observation_body(stage_after, handle.request)
            validate_profile(baseline_document, handle.request, closing.inventory, profile_close)
            if (
                encode_authenticated_inventory(opening) != encode_authenticated_inventory(closing)
                or profile_open != profile_close
                or stage_before != stage_after
            ):
                raise ValueError(ERROR)
            recheck_authenticated_create(
                admitted_factory=handle.factory,
                member=member,
                proof=selected_create,
                pool=handle.pool,
                deadline=state.deadline,
            )
            require_input_descriptor(input_fd, parent_input, parent=True)
            _original(departure)
            if departure is not handle.attempt._create_departure_outcome or _plain(departure) != _plain(
                handle.attempt._create_departure_snapshot
            ):
                raise ValueError(ERROR)
            state.assert_current()
            if build.assert_admitted(deadline_ns=handle.request.operation_deadline_ns) != admitted_build:
                raise ValueError(ERROR)
            baseline.assert_authority(baseline_authority)
            if baseline.evidence_identity() != baseline_identity:
                raise ValueError(ERROR)
            _original(policy)
            if sha256(canonical_json_bytes(policy.to_dict())).hexdigest() != handle.request.parent.policy_sha256:
                raise ValueError(ERROR)
            body = dict(
                schema="dpone.sqlclient.preparation.v1",
                attempt=_plain(handle.request.parent),
                ownership=_plain(state.parent.state.ownership),
                parent_before=_plain(state.parent),
                directory_before=_plain(state.directory),
                observe_identity=_plain(handle.identity),
                observe_registration=_plain(handle.startup_receipt),
                selected_create=strict_json_object(authenticated_create_bytes(selected_create)),
                selected_departure=_plain(departure),
                parent_input=strict_json_object(encode_input_descriptor(parent_input)),
                input_stat=asdict(input_stat),
                policy=_plain(policy),
                build={"admission": admitted_build, "queries": query_hashes()},
                baseline=deepcopy(baseline_document),
                baseline_qualification=baseline.evidence_identity(),
                inventory_open=strict_json_object(encode_authenticated_inventory(opening)),
                inventory_close=strict_json_object(encode_authenticated_inventory(closing)),
                profile_open={key: encode_preparation_rows(key, rows) for key, rows in profile_open.items()},
                profile_close={key: encode_preparation_rows(key, rows) for key, rows in profile_close.items()},
                stage_before=strict_json_object(encode_stage_identity(stage_before.before)),
                stage_after=strict_json_object(encode_stage_identity(stage_after.after)),
                empty=stage_after.empty,
                operation_deadline_ns=handle.request.operation_deadline_ns,
            )
            payload = preparation_bytes(body)
            state.begin_preparation(payload)
            receipt = state.evidence.write(payload, deadline=state.deadline)
            state.capture_preparation(payload, receipt)
            if receipt != PreparationReceipt.for_payload(subject, payload):
                raise ValueError(ERROR)
            state.assert_current()
            require_input_descriptor(input_fd, parent_input, parent=True)
            state.advance(stage_object_identity(stage_after.after), receipt)
        _cleanup(state, handle._capture_cleanup(None))
        state.finish_cleanup()
        return receipt
    except BaseException:
        state.failed = handle.attempt._poisoned = True
        try:
            _cleanup(state, handle._capture_cleanup(None))
        except BaseException:
            pass
        raise SqlClientPreparationUnknown(state) from None
