"""Synthetic v2 records; fixtures establish no SQL acquisition or exclusion proof."""

from dataclasses import replace
from datetime import datetime
from uuid import UUID

from dpone.contracts.mssql_sqlclient_create_departure_v2 import (
    SqlClientCreateDepartureV2,
    SqlClientDepartureSampleV2,
    SqlClientObserverRequestV2,
)
from dpone.contracts.mssql_sqlclient_create_departure_v2 import (
    SqlClientDepartureSampleKind as Kind,
)
from dpone.contracts.mssql_sqlclient_observation import SqlClientPrincipalResolution, SqlClientSessionAuthority
from dpone.contracts.mssql_sqlclient_observer_incarnation import (
    SqlClientDepartureVisibilityV2,
    SqlClientObserverIncarnation,
    observer_incarnation_digest,
)
from tests.test_mssql_sqlclient_create_departure_codec import maximum as legacy_maximum
from tests.test_mssql_sqlclient_create_departure_codec import sample as legacy_sample

KINDS = (Kind.CONNECTIONS, Kind.SESSIONS, Kind.REQUESTS, Kind.TRANSACTIONS, Kind.CONNECTIONS, Kind.SESSIONS)


def sample(*, reused=True, character=None):
    old = legacy_sample() if character is None else legacy_maximum(character)
    if character is not None:
        old = replace(
            old,
            original=replace(
                old.original,
                connect_time=datetime(9998, 12, 31, 23, 59, 59, 999999),
                login_time=datetime(9998, 12, 31, 23, 59, 59, 999999),
            ),
            admission=replace(old.admission, transport=replace(old.admission.transport, encrypt_option="FALSE")),
        )
    a = old.admission
    own = SqlClientObserverIncarnation(
        connection_id=UUID(int=2),
        session_id=old.original.session_id if reused else old.original.session_id - 1,
        connect_time=datetime(9999, 12, 31, 23, 59, 59, 999998),
        login_time=datetime(9999, 12, 31, 23, 59, 59, 999998),
        parent_connection_id=None,
        mars_child_count=0,
        transaction_count=0,
        xact_state=0,
        authority=SqlClientSessionAuthority(
            a.server,
            a.database,
            a.login,
            a.transport,
            SqlClientPrincipalResolution(
                "mapped_user", old.principal.principal_id, old.principal.name, old.principal.sid
            ),
        ),
        visibility=SqlClientDepartureVisibilityV2(
            server_major_version=17,
            engine_edition=4,
            view_server_state=None,
            view_server_performance_state=1,
            database_id=old.database.database_id,
        ),
    )
    digest = observer_incarnation_digest(own)
    samples = tuple(
        SqlClientDepartureSampleV2(
            kind=kind,
            raw_count=int(reused and kind is not Kind.TRANSACTIONS),
            own_count=int(reused and kind is not Kind.TRANSACTIONS),
            original_uuid_count=0 if kind in (Kind.CONNECTIONS, Kind.REQUESTS) else None,
            request=SqlClientObserverRequestV2(
                connection_id=own.connection_id,
                session_id=own.session_id,
                request_id=2**31 - 1,
                start_time=datetime.max,
            )
            if reused and kind is Kind.REQUESTS
            else None,
            before_sha256=digest,
            after_sha256=digest,
        )
        for kind in KINDS
    )
    return SqlClientCreateDepartureV2(
        original=old.original,
        database=old.database,
        admission=a,
        principal=old.principal,
        observer=own,
        samples=samples,
    )


def leaves(value, prefix=()):
    """Enumerate every declared leaf to prevent gaps in original-scalar coverage."""
    from dataclasses import fields, is_dataclass

    if is_dataclass(value):
        for field in fields(value):
            yield from leaves(getattr(value, field.name), (*prefix, field.name))
    elif type(value) is tuple:
        for index, child in enumerate(value):
            yield from leaves(child, (*prefix, index))
    else:
        yield prefix, value


def corrupt(value, path, replacement):
    """Adversarial frozen DTO mutation; never a normal producer operation."""
    import copy

    changed = copy.deepcopy(value)
    target = changed
    for field in path[:-1]:
        target = target[field] if type(field) is int else getattr(target, field)
    object.__setattr__(target, path[-1], replacement)
    return changed


def alias(value, *, enum=False):
    """Equal-valued aliases that encoding must not normalize into valid originals."""
    from enum import IntEnum, StrEnum

    class Text(str):
        def __deepcopy__(self, memo):
            return str(self)

    class Stamp(datetime):
        pass

    class Guid(UUID):
        pass

    class Binary(bytes):
        pass

    if type(value) is str:
        return Text(value)
    if type(value) is bool:
        return int(value)
    if type(value) is int:
        return IntEnum("Alias", {"VALUE": value})(value) if enum else float(value)
    if type(value) is datetime:
        return Stamp.fromisoformat(value.isoformat())
    if type(value) is UUID:
        return Guid(str(value))
    if type(value) is bytes:
        return Binary(value)
    if isinstance(value, StrEnum):
        return value.value
    return False


OWN_GOLDEN = (
    b'{"authority":{"database":{"database_guid":"01234567-89ab-cdef-0123-456789abcdef","database_id":7,"da'
    b'tabase_name":"target","owner_sid":"bb"},"login":{"authenticating_database_id":0,"is_sysadmin":false,'
    b'"name":"writer","original_name":"writer","original_sid":"aa","principal_id":300,"sid":"aa"},"princip'
    b'al_resolution":{"kind":"mapped_user","name":"writer_user","principal_id":5,"sid":"aa"},"profile":"sq'
    b'l_login_initial_context_v1","schema_version":1,"server":{"instance_name":"MSSQLSERVER","machine_name'
    b'":"machine","physical_machine_name":"physical","server_name":"server"},"transport":{"auth_scheme":"S'
    b'QL","encrypt_option":"TRUE","net_transport":"TCP","protocol_type":"TSQL"}},"connect_time":"9999-12-3'
    b'1T23:59:59.999998","connection_id":"00000000-0000-0000-0000-000000000002","login_time":"9999-12-31T2'
    b'3:59:59.999998","mars_child_count":0,"parent_connection_id":null,"schema":"dpone.sqlclient.observer-'
    b'incarnation.v2","session_id":72,"transaction_count":0,"visibility":{"database_id":7,"engine_edition"'
    b':4,"server_major_version":17,"view_server_performance_state":1,"view_server_state":null},"xact_state'
    b'":0}'
)
OWN_GOLDEN_SHA256 = "1f081297f51dddd74fc858f5fdf44d401f796e2bf5920c7a7c68a2b53fb00caf"


DEPARTURE_GOLDEN = (
    b'{"admission":{"database":{"database_guid":"01234567-89ab-cdef-0123-456789abcdef","database_id":7,"da'
    b'tabase_name":"target","owner_sid":"bb"},"login":{"authenticating_database_id":0,"is_sysadmin":false,'
    b'"name":"writer","original_name":"writer","original_sid":"aa","principal_id":300,"sid":"aa"},"server"'
    b':{"instance_name":"MSSQLSERVER","machine_name":"machine","physical_machine_name":"physical","server_'
    b'name":"server"},"transport":{"auth_scheme":"SQL","encrypt_option":"TRUE","net_transport":"TCP","prot'
    b'ocol_type":"TSQL"}},"database":{"database_guid":"01234567-89ab-cdef-0123-456789abcdef","database_id"'
    b':7,"name":"target"},"observer":{"authority":{"database":{"database_guid":"01234567-89ab-cdef-0123-45'
    b'6789abcdef","database_id":7,"database_name":"target","owner_sid":"bb"},"login":{"authenticating_data'
    b'base_id":0,"is_sysadmin":false,"name":"writer","original_name":"writer","original_sid":"aa","princip'
    b'al_id":300,"sid":"aa"},"principal_resolution":{"kind":"mapped_user","name":"writer_user","principal_'
    b'id":5,"sid":"aa"},"profile":"sql_login_initial_context_v1","schema_version":1,"server":{"instance_na'
    b'me":"MSSQLSERVER","machine_name":"machine","physical_machine_name":"physical","server_name":"server"'
    b'},"transport":{"auth_scheme":"SQL","encrypt_option":"TRUE","net_transport":"TCP","protocol_type":"TS'
    b'QL"}},"connect_time":"9999-12-31T23:59:59.999998","connection_id":"00000000-0000-0000-0000-000000000'
    b'002","login_time":"9999-12-31T23:59:59.999998","mars_child_count":0,"parent_connection_id":null,"sch'
    b'ema":"dpone.sqlclient.observer-incarnation.v2","session_id":72,"transaction_count":0,"visibility":{"'
    b'database_id":7,"engine_edition":4,"server_major_version":17,"view_server_performance_state":1,"view_'
    b'server_state":null},"xact_state":0},"original":{"authority_sha256":"e1d5d7665d319ec23ed73d2073189d70'
    b'06e16bff95e73e8d05b6041b7d243cf1","connect_time":"2026-09-15T12:00:00.000000","connection_id":"01234'
    b'567-89ab-cdef-0123-456789abcdef","login_time":"2026-09-15T12:00:01.000000","nonce":"0001020304050607'
    b'08090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f","schema":"dpone.tds.remote-session.v1","session_id'
    b'":72},"principal":{"name":"writer_user","principal_id":5,"sid":"aa"},"samples":[{"after_sha256":"c14'
    b'ee8146824df827fb6a629a37296d564e12533d4f3982571b367ff08be4fc4","before_sha256":"c14ee8146824df827fb6'
    b'a629a37296d564e12533d4f3982571b367ff08be4fc4","kind":"connections","original_uuid_count":0,"own_coun'
    b't":1,"raw_count":1,"request":null},{"after_sha256":"c14ee8146824df827fb6a629a37296d564e12533d4f39825'
    b'71b367ff08be4fc4","before_sha256":"c14ee8146824df827fb6a629a37296d564e12533d4f3982571b367ff08be4fc4"'
    b',"kind":"sessions","original_uuid_count":null,"own_count":1,"raw_count":1,"request":null},{"after_sh'
    b'a256":"c14ee8146824df827fb6a629a37296d564e12533d4f3982571b367ff08be4fc4","before_sha256":"c14ee81468'
    b'24df827fb6a629a37296d564e12533d4f3982571b367ff08be4fc4","kind":"requests","original_uuid_count":0,"o'
    b'wn_count":1,"raw_count":1,"request":{"connection_id":"00000000-0000-0000-0000-000000000002","request'
    b'_id":2147483647,"session_id":72,"start_time":"9999-12-31T23:59:59.999999"}},{"after_sha256":"c14ee81'
    b'46824df827fb6a629a37296d564e12533d4f3982571b367ff08be4fc4","before_sha256":"c14ee8146824df827fb6a629'
    b'a37296d564e12533d4f3982571b367ff08be4fc4","kind":"transactions","original_uuid_count":null,"own_coun'
    b't":0,"raw_count":0,"request":null},{"after_sha256":"c14ee8146824df827fb6a629a37296d564e12533d4f39825'
    b'71b367ff08be4fc4","before_sha256":"c14ee8146824df827fb6a629a37296d564e12533d4f3982571b367ff08be4fc4"'
    b',"kind":"connections","original_uuid_count":0,"own_count":1,"raw_count":1,"request":null},{"after_sh'
    b'a256":"c14ee8146824df827fb6a629a37296d564e12533d4f3982571b367ff08be4fc4","before_sha256":"c14ee81468'
    b'24df827fb6a629a37296d564e12533d4f3982571b367ff08be4fc4","kind":"sessions","original_uuid_count":null'
    b',"own_count":1,"raw_count":1,"request":null}],"schema":"dpone.sqlclient.create-departure.v2"}'
)
DEPARTURE_GOLDEN_SHA256 = "3e0a01bf5f4dc71518eaf19e15f7a9f0f8cd4d9894d7db19310047bfa41a4090"
OWN_SEMANTIC_SHA256 = "c14ee8146824df827fb6a629a37296d564e12533d4f3982571b367ff08be4fc4"
