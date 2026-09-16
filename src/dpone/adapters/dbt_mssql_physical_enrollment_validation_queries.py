"""Finite native JSON and closed enrollment-shape SQL validation fragments."""


def _native_checks(native_sql: str, *, managed_vars: bool = False) -> str:
    """Expand the canonical bounded JSON loop; vars preserve actual command text."""
    import re

    from dpone.adapters.dbt_mssql_physical_catalog_v2_queries import _reject_difference
    from dpone.contracts.native_delivery_json import (
        MAX_NATIVE_JSON_DEPTH,
        MAX_NATIVE_JSON_STRING_BYTES,
        MAX_NATIVE_JSON_TOKENS,
    )

    result = native_sql.replace(
        "{{NATIVE_CANONICAL_CHECK}}",
        ""
        if managed_vars
        else "IF "
        + _reject_difference("@document", "@canonical_document")
        + "\n  THROW 51601, 'DPONE_ENROLLMENT_BYTES_MISMATCH', 1;",
    )
    for key, value in [
        ("DEPTH", MAX_NATIVE_JSON_DEPTH),
        ("STRING_BYTES", MAX_NATIVE_JSON_STRING_BYTES),
        ("TOKENS", MAX_NATIVE_JSON_TOKENS),
    ]:
        result = result.replace("{{NATIVE_MAX_" + key + "}}", str(value))
    if managed_vars:
        result = re.sub(
            r"@([A-Za-z_][A-Za-z0-9_]*)",
            lambda match: "@managed_vars" if match[1] == "enrollment" else "@vars_" + match[1],
            result,
        )
    return result


def enrollment_document_checks(*, native_sql: str, component_sql: str) -> str:
    """Produce bounded native/closed envelope checks, before any identity mutation."""
    from dpone.adapters.dbt_mssql_physical_namespace_queries import _shape

    shapes = {
        "$": dict(
            schema=1,
            subject=5,
            registration=5,
            catalog_binding_sha256=1,
            plan_set=5,
            reservation=5,
            executor=5,
            command=5,
        ),
        "$.subject": dict(schema=1, scope=1, authority=5, generation_id=1),
        "$.registration": dict(id=1, sha256=1),
        "$.plan_set": dict(reference=5, payload=5),
        "$.command": dict(reference=5, payload=5),
        "$.executor": dict(
            schema=1, generation_id=1, guard_epoch=2, invocation_id=1, reservation=5, profile=5, command=5
        ),
    }
    shapes.update(
        {
            "$.plan_set.payload": dict(
                schema=1,
                generation_id=1,
                runtime_registration_id=1,
                workspace_attempt=5,
                guard=5,
                profile=5,
                model_database=5,
                models=4,
            ),
            "$.plan_set.payload.workspace_attempt": dict(
                activation_id=1, attempt_id=1, workflow_id=1, write_subjects=4, request_sha256=1
            ),
            "$.plan_set.payload.guard": dict(guard_id=1, fencing_epoch=2),
            "$.plan_set.payload.model_database": dict(database_name=1, database_id=2, create_token=1, database_guid=1),
            "$.command.payload": dict(
                schema=1,
                executor_invocation_id=1,
                phase=1,
                project_root=5,
                output_root=5,
                profile_root=5,
                commands=4,
                total_termination_budget_seconds=2,
            ),
        }
    )
    for path in (
        "$.plan_set.payload.profile",
        "$.command.payload.project_root",
        "$.command.payload.output_root",
        "$.command.payload.profile_root",
        "$.plan_set.reference",
        "$.command.reference",
        "$.reservation",
        "$.executor.reservation",
        "$.executor.profile",
        "$.executor.command",
    ):
        shapes[path] = dict(locator=1, sha256=1)
    parts = [_native_checks(native_sql)]
    for path, fields in shapes.items():
        document = "@enrollment" if path == "$" else f"JSON_QUERY(@enrollment,'{path}')"
        parts.append(_shape(document, fields))
    return "\n".join(parts) + "\n" + _component_shapes(native_sql, component_sql)


def _component_shapes(native_sql: str, component_sql: str) -> str:
    """Validate every finite model/spec/column and ordered command entry shape."""
    from dpone.adapters.dbt_mssql_physical_namespace_queries import _shape

    plan_fields = dict(
        schema=1,
        generation_id=1,
        spec=5,
        predecessor=5,
        candidate_name=1,
        helper_name=1,
        backup_name=0,
        columnstore_index_name=0,
        model_plan_sha256=1,
    )
    shapes = [_shape("@model_document", plan_fields)]
    columnstore_shape = _shape("@model_document", {**plan_fields, "columnstore_index_name": 1})
    model = (
        "IF JSON_VALUE(@model_document,'$.spec.layout')=N'columnstore' BEGIN "
        + columnstore_shape
        + " END ELSE BEGIN "
        + shapes[0]
        + " END;"
    )
    for path, fields in {
        "$.spec": dict(
            schema=1,
            model_unique_id=1,
            source_graph_sha256=1,
            relation=5,
            columns=4,
            layout=1,
            physical_policy=1,
            filegroup=5,
            resource_bounds=5,
            model_spec_sha256=1,
        ),
        "$.predecessor": dict(kind=1),
        "$.spec.relation": dict(database=1, schema=1, table=1),
        "$.spec.filegroup": dict(data_space_id=2, name=1),
        "$.spec.resource_bounds": dict(locator=1, sha256=1),
    }.items():
        model += "\n" + _shape(f"JSON_QUERY(@model_document,'{path}')", fields)
    column = _shape("@column_document", dict(name=1, dtype=1, nullable=3, collation=0))
    collated = _shape("@column_document", dict(name=1, dtype=1, nullable=3, collation=1))
    command = _shape(
        "@command_document",
        dict(position=2, verb=1, argv_template=4, command_timeout_seconds=2, termination_allowance_seconds=2),
    )
    return _expand(
        component_sql,
        {
            "MODEL_COMPONENT_SHAPES": model,
            "COLUMN_SHAPE": column,
            "COLLATED_COLUMN_SHAPE": collated,
            "COMMAND_SHAPE": command,
            "MANAGED_VARS_NATIVE": _native_checks(native_sql, managed_vars=True),
            "MANAGED_INVOCATION_SHAPE": _shape(
                "JSON_QUERY(@managed_vars,'$.__dpone_managed')",
                dict(generation_id=1, invocation_id=1, plan_set=5, runtime_registration_id=1),
            ),
            "MANAGED_REFERENCE_SHAPE": _shape(
                "JSON_QUERY(@managed_vars,'$.__dpone_managed.plan_set')", dict(locator=1, sha256=1)
            ),
        },
    )


def _expand(template: str, replacements: dict[str, str]) -> str:
    """Resolve only finite reviewed fragments, rejecting every leftover marker."""
    for _ in range(4):
        for name, value in replacements.items():
            template = template.replace("{{" + name + "}}", value)
        if "{{" not in template:
            return template
    raise ValueError("enrollment package contains unsupported or recursive substitutions")
