"""Fixed preparation reads on the original exclusive cursor and deadline.

No result is exposed from an impersonated context. Any fetch/drain/revert
ambiguity poisons the shared owner; only the enclosing process may contain it.
"""

from typing import Any

from dpone.adapters.mssql_sqlclient_observation_cursor import ObservationCursor
from dpone.adapters.mssql_sqlclient_preparation_sql import (
    EFFECTIVE_SQL,
    ENV_SQL,
    IDENTITY_SQL,
    OWNERSHIP_SQL,
    SERVER_COUNT_SQL,
    SERVER_PERMISSIONS_SQL,
    STAGE_SECURITY_SQL,
)
from dpone.contracts.mssql_sqlclient_observe_rows import ERROR, validate_rows
from dpone.contracts.mssql_tds_api import SqlClientObserveRequest


def _server_permission_row(row: tuple[Any, ...]) -> tuple[Any, ...]:
    """Detach SQL Server's fixed-width nchar(4) permission code."""
    if type(row) is not tuple or len(row) != 9 or type(row[4]) is not str:
        raise ValueError(ERROR)
    code = row[4].rstrip(" ")
    if not code or len(code) > 4:
        raise ValueError(ERROR)
    return (*row[:4], code, *row[5:])


class SqlClientPreparationCatalog:
    """A finite algorithm, borrowing the existing cursor lease without owning it."""

    def __init__(self, session: ObservationCursor, request: SqlClientObserveRequest) -> None:
        self.session, self.request = session, request

    def _rows(self, lease: object, statement: str, args: tuple[Any, ...], maximum: int) -> list[Any]:
        return self.session.rows(lease, statement, args, max_rows=maximum, nextset_required=True, detach_rows=True)

    def read_within(self, lease: object, opcode: str) -> list[Any]:
        request = self.request
        login = request.writer_admission.login
        user = request.writer_principal.principal_id
        try:
            self.session.current(lease)
            if opcode == "PREP_ENV":
                rows = self._rows(lease, ENV_SQL, (login.principal_id, user), 1)
            elif opcode == "PREP_OWNERSHIP_COUNTS":
                subjects = {"login": login.principal_id, "user": user, "sid": bytes.fromhex(login.sid)}
                rows = []
                for statement, subject in OWNERSHIP_SQL:
                    current = self._rows(lease, statement, (subjects[subject],), 1)
                    if len(current) != 1:
                        raise ValueError(ERROR)
                    rows.extend(current)
            elif opcode == "PREP_SERVER_PERMISSIONS":
                before = self._rows(lease, SERVER_COUNT_SQL, (login.principal_id,), 1)
                if (
                    len(before) != 1
                    or len(before[0]) != 1
                    or type(before[0][0]) is not int
                    or not 0 <= before[0][0] <= 4096
                ):
                    raise ValueError(ERROR)
                rows = self._rows(lease, SERVER_PERMISSIONS_SQL, (login.principal_id,), 4096)
                rows = [_server_permission_row(row) for row in rows]
                after = self._rows(lease, SERVER_COUNT_SQL, (login.principal_id,), 1)
                if (
                    after != before
                    or len(rows) != before[0][0]
                    or any(type(v) is not int for row in after for v in row)
                ):
                    raise ValueError(ERROR)
            elif opcode == "PREP_STAGE_SECURITY":
                selected = request.selected_stage.object_id
                rows = self._rows(lease, STAGE_SECURITY_SQL, (selected, selected), 1)
            elif opcode == "PREP_PERMISSION_IDENTITIES":
                resolved = {}
                for name, count, query, subject, repetitions, maximum in IDENTITY_SQL:
                    args = (login.principal_id if subject == "login" else user,) * repetitions
                    before = self._rows(lease, count, args, 1)
                    if (
                        len(before) != 1
                        or len(before[0]) != 1
                        or type(before[0][0]) is not int
                        or not 0 <= before[0][0] <= maximum
                    ):
                        raise ValueError(ERROR)
                    records = self._rows(lease, query, args, max(1, before[0][0]))
                    after = self._rows(lease, count, args, 1)
                    if (
                        after != before
                        or len(records) != before[0][0]
                        or any(type(item) is not int for row in after for item in row)
                    ):
                        raise ValueError(ERROR)
                    resolved[name] = records
                rows = [resolved]
            elif opcode == "PREP_EFFECTIVE":
                rows = [self._effective(lease)]
            else:
                raise ValueError(ERROR)
            result = validate_rows(opcode, rows)
            self.session.current(lease)
            return result
        except BaseException:
            self.session.fail(lease)
            raise

    def _effective(self, lease: object) -> dict[str, Any]:
        cursor = self.session.cursor
        self.session.current(lease)
        # Exactly one execute: SQL owns first REVERT attempt, never the client.
        cursor.execute(EFFECTIVE_SQL, self.request.writer_admission.login.name)
        self.session.current(lease)
        output: dict[str, Any] = {}
        segments = (
            ("subject", 1),
            ("login_token", 8),
            ("user_token", 8),
            ("server_permissions", 128),
            ("database_permissions", 128),
            ("restored_management", 1),
        )
        for index, (name, maximum) in enumerate(segments):
            rows = []
            for _ in range(maximum + 1):
                self.session.current(lease)
                row = cursor.fetchone()
                self.session.current(lease)
                if row is None:
                    break
                rows.append(tuple(row))
            else:
                raise ValueError(ERROR)
            if name in ("subject", "restored_management"):
                if len(rows) != 1:
                    raise ValueError(ERROR)
                output[name] = rows[0]
            else:
                output[name] = rows
            self.session.current(lease)
            extra = cursor.nextset()
            self.session.current(lease)
            if index == len(segments) - 1:
                if extra is not None and extra is not False:
                    raise ValueError(ERROR)
            elif extra is not True:
                raise ValueError(ERROR)
        return output
