"""Closed discovery metadata preserves original types and bounded identities."""

from dataclasses import replace
from uuid import UUID

import pytest

from dpone.contracts.bounded_window import WindowRecord
from dpone.contracts.mssql_sqlclient_observation import SqlClientServerAuthority
from dpone.contracts.mssql_sqlclient_stage_locator import (
    SqlClientStageLocator,
    SqlClientStageLookup,
    decode_stage_locator,
    encode_stage_locator,
    encode_state_domain,
    stage_locator_key,
    validate_state_domain_record,
)
from dpone.contracts.mssql_tds_coordinator import TdsCoordinatorIdentity
from dpone.contracts.mssql_tds_coordinator_authority import TdsDatabaseObservation
from dpone.contracts.mssql_tds_create import TdsCreateColumn, TdsCreateRequest, TdsCreateType, create_command_digest
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand, TdsDirectoryLimits
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity, TdsAttemptOwnership
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

DOMAIN = UUID(int=1)
SERVER = SqlClientServerAuthority("server", "machine", "instance", "physical")
DATABASE = TdsDatabaseObservation("database", 7, UUID(int=2))
PARENT = TdsAttemptIdentity("target", "run", 0, 0, *(["a" * 64] * 4), "database", "stage", "table", "b" * 64)
OWNER = TdsAttemptOwnership("owner", 1, str(UUID(int=3)))
LIMITS = TdsDirectoryLimits(10, 2, 16000, 4000)
REQUEST = TdsCreateRequest(PARENT, UUID(int=4), (TdsCreateColumn("id", TdsCreateType.BIGINT, False),))
OPERATION = TdsCoordinatorIdentity(
    PARENT, 0, UUID(int=5), TdsCoordinatorCommand.CREATE, create_command_digest(REQUEST), 1, "c" * 64
)
LOCATOR = SqlClientStageLocator(DOMAIN, SERVER, DATABASE, REQUEST.object_nonce, OPERATION, OWNER, LIMITS)
LOOKUP = SqlClientStageLookup(DOMAIN, SERVER, DATABASE, PARENT.owner_binding, REQUEST.object_nonce)


def test_round_trip_and_stable_lookup():
    assert decode_stage_locator(encode_stage_locator(LOCATOR)) == LOCATOR
    assert stage_locator_key(LOOKUP) == stage_locator_key(
        replace(LOOKUP, database=replace(DATABASE, name="renamed", database_id=8))
    )
    assert stage_locator_key(LOOKUP) != stage_locator_key(replace(LOOKUP, object_nonce=UUID(int=9)))


def test_domain_round_trip():
    record = WindowRecord(3, encode_state_domain(DOMAIN).decode())
    assert validate_state_domain_record(record) == DOMAIN


@pytest.mark.parametrize(
    "record",
    [
        None,
        WindowRecord(True, "{}"),
        WindowRecord(0, "{}"),
        WindowRecord(2**63, "{}"),
        WindowRecord(1, b"{}"),
        WindowRecord(1, " " * 257),
    ],
)
def test_domain_invalid_records(record):
    with pytest.raises(ValueError):
        validate_state_domain_record(record)


@pytest.mark.parametrize("change", ["extra", "duplicate", "whitespace", "uuid_alias", "zero", "bool", "overflow"])
def test_locator_rejects_noncanonical_or_malformed_wire(change):
    body = strict_json_object(encode_stage_locator(LOCATOR))
    if change == "extra":
        body["permission"] = True
    elif change == "uuid_alias":
        body["object_nonce"] = "{" + body["object_nonce"] + "}"
    elif change == "zero":
        body["state_domain_id"] = str(UUID(int=0))
    elif change == "bool":
        body["create_operation"]["original_fence"] = True
    payload = canonical_json_bytes(body)
    if change == "duplicate":
        payload = payload[:-1] + b',"schema":"dpone.sqlclient.stage-locator.v1"}'
    elif change == "whitespace":
        payload += b" "
    elif change == "overflow":
        payload += b" " * 16384
    with pytest.raises(ValueError):
        decode_stage_locator(payload)


@pytest.mark.parametrize(
    "path,value", [("original_fence", True), ("command", "create"), ("slot_index", 0.0), ("parent", None)]
)
def test_revalidate_mutated_nested_original_before_encoding(path, value):
    operation = replace(OPERATION)
    object.__setattr__(operation, path, value)
    with pytest.raises(ValueError):
        replace(LOCATOR, create_operation=operation)


def test_maximum_mixed_original_record_fits_fixed_cap():
    # Text maxima include 4-byte code points for unrestricted fields, 3-byte
    # SQL identifiers (128 UTF-16 code units), quotes and reverse-solidus escapes.
    wide = "\U0001f642" * 256
    sql_name = "\uffff" * 128
    parent = replace(
        PARENT,
        target_key=wide,
        run_id=wide,
        ordinal=2**63 - 1,
        attempt=2,
        database=sql_name,
        schema=sql_name,
        table=sql_name,
    )
    operation = replace(OPERATION, parent=parent, slot_index=2**63 - 1, original_fence=2**63 - 1)
    locator = replace(
        LOCATOR,
        server=SqlClientServerAuthority(*([sql_name] * 4)),
        database=replace(DATABASE, name=sql_name, database_id=2**31 - 1),
        create_operation=operation,
        execution_owner=replace(OWNER, owner=wide, fence=2**63 - 1),
        directory_limits=TdsDirectoryLimits(2**63 - 1, 2**63 - 2, 2**63 - 1, 2**63 - 2),
    )
    payload = encode_stage_locator(locator)
    assert len(payload) <= 16384
    assert decode_stage_locator(payload) == locator


class ScalarAlias(str):
    def __deepcopy__(self, memo):
        return str(self)


@pytest.mark.parametrize(
    "part,field,value",
    [
        ("server", "server_name", ScalarAlias("server")),
        ("directory_limits", "max_entries", True),
        ("execution_owner", "owner", ScalarAlias("owner")),
        ("database", "name", ScalarAlias("database")),
    ],
)
def test_scalar_original_cannot_normalize_through_dataclass_copy(part, field, value):
    nested = replace(getattr(LOCATOR, part))
    object.__setattr__(nested, field, value)
    with pytest.raises(ValueError):
        replace(LOCATOR, **{part: nested})


@pytest.mark.parametrize(
    "payload",
    [
        b"{}",
        b'{"schema":"dpone.sqlclient.state-domain.v1","domain_id":"00000000-0000-0000-0000-000000000001","domain_id":"00000000-0000-0000-0000-000000000001"}',
        b'{"schema":"dpone.sqlclient.state-domain.v1","domain_id":"00000000-0000-0000-0000-000000000000"}',
        encode_state_domain(DOMAIN) + b" ",
        b"\xff",
    ],
)
def test_invalid_domain_wire(payload):
    with pytest.raises(ValueError):
        validate_state_domain_record(WindowRecord(1, payload.decode("utf-8", errors="surrogateescape")))


@pytest.mark.parametrize(
    "field",
    [
        "state_domain_id",
        "object_nonce",
        "server",
        "database",
        "create_operation",
        "execution_owner",
        "directory_limits",
    ],
)
def test_rejects_top_level_original_record_aliases(field):
    with pytest.raises(ValueError):
        replace(LOCATOR, **{field: None})


class IntegerAlias(int):
    pass


@pytest.mark.parametrize("integer", [True, False, 1.0, -1, 0, 2**128, "1", IntegerAlias(1)])
def test_domain_rejects_mutated_uuid_integer_before_encoding(integer):
    identity = UUID(int=4)
    object.__setattr__(identity, "int", integer)
    with pytest.raises(ValueError):
        encode_state_domain(identity)


@pytest.mark.parametrize("leaf", ["state_domain_id", "object_nonce", "database_guid", "operation_id"])
@pytest.mark.parametrize("integer", [True, 1.0, -1, 0, 2**128, IntegerAlias(1)])
def test_locator_rejects_every_mutated_uuid_leaf_before_encoding_or_hash(leaf, integer):
    locator = decode_stage_locator(encode_stage_locator(LOCATOR))
    identity = (
        getattr(locator.database, leaf)
        if leaf == "database_guid"
        else getattr(locator.create_operation, leaf)
        if leaf == "operation_id"
        else getattr(locator, leaf)
    )
    object.__setattr__(identity, "int", integer)
    with pytest.raises(ValueError):
        encode_stage_locator(locator)
    with pytest.raises(ValueError):
        stage_locator_key(locator.lookup())


@pytest.mark.parametrize("integer", [True, 1.0, -1, 0, 2**128, IntegerAlias(1)])
def test_request_nonce_rejects_mutated_uuid_even_when_value_alias_matches(integer):
    from dpone.contracts.mssql_sqlclient_stage_locator import validate_locator_request

    request = replace(REQUEST, object_nonce=UUID(int=1))
    locator = replace(
        LOCATOR,
        object_nonce=UUID(int=1),
        create_operation=replace(OPERATION, command_sha256=create_command_digest(request)),
    )
    object.__setattr__(request.object_nonce, "int", integer)
    with pytest.raises(ValueError):
        validate_locator_request(locator, request)
