"""Closed preparation profile comparisons; observations never define the baseline."""

from typing import Any

from dpone.contracts.mssql_sqlclient_observe_rows import ERROR, OPCODE_LIMITS, validate_rows
from dpone.contracts.strict_json import canonical_json_bytes


def validate_profile(baseline: dict[str, Any], request: Any, inventory: Any, profile: dict[str, Any]) -> None:
    """Accept only independently pinned exact baseline and authenticated stage grants.

    No observed row is adopted into baseline policy. Non-object database facts
    retain their full raw representation; unsupported baseline projections reject.
    """

    if type(profile) is not dict or set(profile) != set(OPCODE_LIMITS):
        raise ValueError(ERROR)
    values = {key: validate_rows(key, rows) for key, rows in profile.items()}
    env = values["PREP_ENV"][0]
    build, database = baseline["server_build"], baseline["database_profile"]
    login, principal = request.writer_admission.login, request.writer_principal
    if (
        env[:3] != (build["product_version"], build["edition"], build["engine_edition"])
        or env[3] not in (16, 17)
        or env[4:6] != (database["compatibility_level"], database["collation"])
        or env[6:8] != (request.selected_stage.database_id, request.selected_stage.database_name)
        or env[8:11] != (0, 0, 0)
        or any(database[key] != 0 for key in ("containment", "trustworthy", "database_chaining"))
        or env[11:16] != (login.principal_id, login.name, bytes.fromhex(login.sid), "SQL_LOGIN", 0)
        or env[16:21] != (principal.principal_id, principal.name, bytes.fromhex(principal.sid), "SQL_USER", "INSTANCE")
        or env[21] == bytes.fromhex(login.sid)
        or principal.principal_id <= 4
        or any(count != 0 for _, count in values["PREP_OWNERSHIP_COUNTS"])
        or values["PREP_STAGE_SECURITY"] != [(request.selected_stage.object_id, 0)]
    ):
        raise ValueError(ERROR)
    effective_rows = values["PREP_EFFECTIVE"][0]
    if effective_rows["subject"] != (
        login.name,
        bytes.fromhex(login.sid),
        principal.principal_id,
        principal.name,
        env[6],
        env[7],
        0,
        0,
        request.management_admission.login.name,
    ):
        raise ValueError(ERROR)
    for key in ("server_permissions", "database_permissions"):
        if canonical_json_bytes(effective_rows[key]) != canonical_json_bytes(baseline["effective_" + key]):
            raise ValueError(ERROR)
    principals, securables = _resolved(values, inventory, request)
    for name in ("login", "user"):
        tokens = effective_rows[name + "_token"]
        rules = baseline["token_rules"][name]
        if type(rules) is not list or len(tokens) != len(rules) or not tokens:
            raise ValueError(ERROR)
        expected = []
        for rule in rules:
            if type(rule) is not dict or set(rule) != {"subject", "type", "usage"}:
                raise ValueError(ERROR)
            if rule["subject"] not in ("writer", "public"):
                raise ValueError(ERROR)
            row = principals[name][rule["subject"]]
            expected.append((row[0], row[2], row[1], rule["type"], rule["usage"]))
        # SQL Server orders token rows by dynamic principal_id. A newly created
        # writer normally sorts after public, while the qualified semantic rules
        # are writer-first. Bind the exact unique rows without treating that
        # environment-specific numeric ordering as policy.
        if len(set(tokens)) != len(tokens) or len(set(expected)) != len(expected) or set(tokens) != set(expected):
            raise ValueError(ERROR)
    server = []
    for row in values["PREP_SERVER_PERMISSIONS"]:
        class_id, major, grantee, grantor, kind, permission, state, securable, name = row
        if (
            state != "G"
            or (class_id == 100 and (major != 0 or securable != "SERVER" or name is not None))
            or (class_id == 105 and (major <= 0 or securable != "ENDPOINT" or not name))
            or class_id not in (100, 105)
        ):
            raise ValueError(ERROR)
        endpoint = values["PREP_PERMISSION_IDENTITIES"][0]["server_endpoints"]
        resolved_endpoint = next((item for item in endpoint if item[0] == major), None) if class_id == 105 else None
        if class_id == 105 and (
            resolved_endpoint is None or resolved_endpoint[1] != name or resolved_endpoint[-1] != 1
        ):
            raise ValueError(ERROR)
        server.append(
            _permission(
                class_id,
                (securable, None, name, None, None)
                if resolved_endpoint is None
                else (securable, *resolved_endpoint[1:]),
                grantee,
                grantor,
                kind,
                permission,
                state,
                principals["login"],
                request,
            )
        )
    _equal(server, baseline["stock_server_permissions"])
    raw: list[dict] = []
    bundles: dict[int, list[str]] = {}
    for row in inventory.permissions:
        resolved = securables[(row.class_id, row.major_id, row.minor_id)]
        if row.class_id == 1 and resolved[-1] == 0:
            if (
                row.major_id <= 0
                or row.grantee_principal_id != inventory.writer.principal_id
                or row.minor_id != 0
                or row.state != "G"
                or row.major_id == request.selected_stage.object_id
            ):
                raise ValueError(ERROR)
            bundles.setdefault(row.major_id, []).append(row.permission_name)
        else:
            raw.append(
                _permission(
                    row.class_id,
                    resolved[3:],
                    row.grantee_principal_id,
                    row.grantor_principal_id,
                    row.type,
                    row.permission_name,
                    row.state,
                    principals["user"],
                    request,
                )
            )
    _equal(raw, baseline["stock_database_permissions"])
    if set(bundles) != {member.stage.object_id for member in inventory.members} or any(
        sorted(names) != ["INSERT", "SELECT", "VIEW DEFINITION"] for names in bundles.values()
    ):
        raise ValueError(ERROR)


def _equal(actual: Any, expected: Any) -> None:
    if canonical_json_bytes(actual) != canonical_json_bytes(expected):
        raise ValueError(ERROR)


def _resolved(values: dict[str, Any], inventory: Any, request: Any) -> tuple[dict, dict]:
    """Total exact lookup sets, independently bound identities, no missing joins."""
    resolver = values["PREP_PERMISSION_IDENTITIES"][0]
    login = request.writer_admission.login
    endpoints = resolver["server_endpoints"]
    expected_endpoints = {row[1] for row in values["PREP_SERVER_PERMISSIONS"] if row[0] == 105}
    if {row[0] for row in endpoints} != expected_endpoints:
        raise ValueError(ERROR)
    output = {}
    for name, key, writer_id, permissions in (
        ("login", "server_principals", login.principal_id, values["PREP_SERVER_PERMISSIONS"]),
        ("user", "database_principals", inventory.writer.principal_id, inventory.permissions),
    ):
        rows = {row[0]: row for row in resolver[key]}
        public = [
            row
            for row in rows.values()
            if row[1] == "public" and row[3] == ("SERVER_ROLE" if name == "login" else "DATABASE_ROLE")
        ]
        if len(public) != 1 or writer_id not in rows:
            raise ValueError(ERROR)
        public_row = public[0]
        expected_ids = {writer_id, public_row[0]}
        if name == "login":
            expected_ids.update(value for row in permissions for value in row[2:4])
            expected_writer = (writer_id, login.name, bytes.fromhex(login.sid), "SQL_LOGIN", 0)
            if public_row[4] != 0:
                raise ValueError(ERROR)
        else:
            expected_ids.add(1)
            expected_ids.update(
                value for row in permissions for value in (row.grantee_principal_id, row.grantor_principal_id)
            )
            writer = inventory.writer
            expected_writer = (
                writer_id,
                writer.name,
                bytes.fromhex(writer.sid),
                writer.type_desc,
                writer.authentication_type_desc,
            )
            if public_row != (
                inventory.public.principal_id,
                "public",
                bytes.fromhex(inventory.public.sid),
                "DATABASE_ROLE",
                "NONE",
            ):
                raise ValueError(ERROR)
        if set(rows) != expected_ids or rows[writer_id] != expected_writer or public_row[0] == writer_id:
            raise ValueError(ERROR)
        output[name] = {"writer": rows[writer_id], "public": public_row, "all": rows}
    keys = {(row.class_id, row.major_id, row.minor_id) for row in inventory.permissions}
    securables = {row[:3]: row for row in resolver["database_securables"]}
    if set(securables) != keys:
        raise ValueError(ERROR)
    for row in securables.values():
        class_id, major, minor, kind, schema, name, column, system = row
        if class_id == 0:
            valid = row == (0, 0, 0, "DATABASE", None, None, None, None)
        elif class_id == 1:
            valid = (
                major != 0
                and minor >= 0
                and kind == "OBJECT_OR_COLUMN"
                and bool(schema)
                and bool(name)
                and ((minor == 0 and column is None) or (minor > 0 and bool(column)))
                and system in (0, 1)
            )
        elif class_id in (3, 4):
            valid = (
                major >= 0
                and minor == 0
                and kind == {3: "SCHEMA", 4: "DATABASE_PRINCIPAL"}[class_id]
                and schema is None
                and bool(name)
                and column is None
                and system is None
            )
        else:
            valid = False
        if not valid:
            raise ValueError(ERROR)
    return output, securables


def _permission(
    class_id: int,
    securable: tuple,
    grantee: int,
    grantor: int,
    kind: str,
    permission: str,
    state: str,
    principals: dict,
    request: Any,
) -> dict:
    """Semantic grantor identity keeps complete observed binding, never just an ID."""
    subject = "writer" if grantee == principals["writer"][0] else "public"
    if grantee != principals[subject][0] or grantor not in principals["all"] or state != "G":
        raise ValueError(ERROR)
    _, name, sid, principal_type, detail = principals["all"][grantor]
    sid_binding, fixed_sid = "fixed", sid.hex()
    if principals["all"][grantor] == principals["writer"]:
        sid_binding, fixed_sid = "writer", None
    elif (
        grantor == 1
        and name == "dbo"
        and principal_type == "SQL_USER"
        and detail == "INSTANCE"
        and sid.hex() == request.management_admission.database.owner_sid
    ):
        sid_binding, fixed_sid = "database_owner", None
    grantor_identity = dict(name=name, type=principal_type, detail=detail, sid_binding=sid_binding, sid=fixed_sid)
    return dict(
        class_id=class_id,
        securable=securable,
        grantee=subject,
        grantor=grantor_identity,
        type=kind,
        permission=permission,
        state=state,
    )
