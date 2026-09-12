"""Adversarial mutations for the source-schema authority implementation."""

from __future__ import annotations

import importlib
import subprocess
import sys
import textwrap
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from tests.test_postgres_mssql_r1_source_schema_runtime import (
    _load_config,
    _policy,
    _source_authority,
    issue_authority,
)
from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import bind_behavior_scenario

_CHECKOUT_SRC = (Path(__file__).resolve().parents[1] / "src").resolve(strict=True)


def _run_isolated_type_authority_import(
    *,
    import_src: Path = _CHECKOUT_SRC,
) -> subprocess.CompletedProcess[str]:
    script = textwrap.dedent(
        """
        import sys
        from pathlib import Path

        import_src = Path(sys.argv[1]).resolve(strict=True)
        expected_checkout_src = Path(sys.argv[2]).resolve(strict=True)
        sys.path.insert(0, str(import_src))

        import dpone.contracts.postgres_mssql_type_authority as type_authority

        observed_origin = Path(type_authority.__file__).resolve(strict=True)
        expected_origin = (
            expected_checkout_src
            / "dpone"
            / "contracts"
            / "postgres_mssql_type_authority.py"
        )
        assert observed_origin == expected_origin, (
            "external import origin cannot satisfy checkout probe: "
            f"{observed_origin} != {expected_origin}"
        )
        assert "dpone.contracts.postgres_mssql_source_schema_models" not in sys.modules
        assert "dpone.contracts.postgres_mssql_source_schema_authority" not in sys.modules
        print(observed_origin)
        """
    )
    return subprocess.run(
        [sys.executable, "-I", "-c", script, str(import_src), str(_CHECKOUT_SRC)],
        check=False,
        capture_output=True,
        text=True,
    )


def _module(name: str):
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError:
        pytest.fail(f"approved implementation missing: {name}", pytrace=False)


def _raises(call):
    try:
        call()
    except Exception as exc:
        assert type(exc) is not AssertionError
        return exc
    raise AssertionError("typed rejection required")


def _exact_bundle(modules: dict[str, Any]) -> dict[str, bool]:
    from dpone.runtime.sources.postgres_source_authority import PostgresSourceAuthorityVerifier

    verifier = PostgresSourceAuthorityVerifier(_source_authority())
    snapshot = modules["snapshot"].PostgresVerifiedRelationSnapshotIssuerV1()
    issuer = modules["issuer"].PostgresMssqlRelationSchemaAuthorityIssuerV1(type_policy_authority=_policy())
    factory = modules["boundary"].build_r1_postgres_fetched_schema
    projection = modules["projection"].PostgresMssqlSourceSchemaProjectionAdapterV1(fetched_schema_factory=factory)
    runtime_type = modules["runtime"].PostgresMssqlSourceSchemaRuntimeV1
    valid = runtime_type(
        verifier=verifier, snapshot_scope_issuer=snapshot, schema_authority_issuer=issuer, projection_adapter=projection
    )
    wrongs = {
        "verifier_exact_type": dict(verifier=object()),
        "snapshot_issuer_exact_type": dict(snapshot_scope_issuer=object()),
        "schema_issuer_exact_type": dict(schema_authority_issuer=object()),
        "projection_adapter_exact_type": dict(projection_adapter=object()),
    }
    results = {
        key: _raises(lambda values=values: replace(valid, **values)) is not None for key, values in wrongs.items()
    }
    model_error = _raises(lambda: modules["models"].require_type_policy(object()))
    results["type_policy_exact_type"] = getattr(model_error, "reason", "") == "type_policy_exact_type_required"

    def other_factory(**kwargs):
        return factory(**kwargs)

    bad_projection = modules["projection"].PostgresMssqlSourceSchemaProjectionAdapterV1(
        fetched_schema_factory=other_factory
    )
    results["fetched_schema_constructor_identity"] = (
        _raises(lambda: replace(valid, projection_adapter=bad_projection)) is not None
    )
    return results


def _old_intent(modules: dict[str, Any]) -> dict[str, bool]:
    first, first_scope, *_ = issue_authority(modules)
    first_scope.close_if_active()
    connector_module = importlib.import_module("tests.test_postgres_mssql_r1_source_schema_runtime")
    connector = connector_module.FakeCatalogConnector()
    connector.column_suffix = "_changed"
    second, second_scope, *_ = issue_authority(modules, connector=connector)
    second_scope.close_if_active()

    class ExactSealedIntentConsumer:
        """Fake downstream port retaining the exact authority digest it sealed."""

        def __init__(self, source_schema_digest: bytes) -> None:
            self.source_schema_digest = source_schema_digest

        def admit_retry(self, observed_authority: object) -> None:
            if (
                type(observed_authority) is not type(first)
                or getattr(observed_authority, "digest", None) != self.source_schema_digest
            ):
                raise modules["authority"].PostgresMssqlSourceSchemaAuthorityErrorV1("authority_splice")

    error = _raises(lambda: ExactSealedIntentConsumer(first.digest).admit_retry(second))
    recovery = getattr(error, "recovery", None)
    return {
        "schema_digest_changed": first.digest != second.digest,
        "old_operation_effect_key_rejected": getattr(error, "reason", "") == "authority_splice",
        "new_generation_required": getattr(recovery, "value", None) == "operator_intervention",
    }


bind_behavior_scenario("runtime.exact-bundle", _exact_bundle)
bind_behavior_scenario("runtime.old-intent-reuse-blocked", _old_intent)


def test_constant_source_renderer_is_detected_by_independent_goldens(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _module("dpone.runtime.sources.postgres_mssql_source_schema_projection")
    original = module.render_postgres_declared_type
    monkeypatch.setattr(module, "render_postgres_declared_type", lambda _shape: "constant")
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _SOURCE_GOLDENS, _all_source_shapes

    assert tuple(module.render_postgres_declared_type(shape) for shape in _all_source_shapes()) != _SOURCE_GOLDENS
    monkeypatch.setattr(module, "render_postgres_declared_type", original)


def test_constant_codec_renderer_is_detected_by_independent_goldens(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _module("dpone.runtime.sources.postgres_mssql_source_schema_projection")
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _CODEC_GOLDENS

    monkeypatch.setattr(module, "render_transfer_representation", lambda _codec: "constant")
    assert any(module.render_transfer_representation(codec) != expected for codec, expected in _CODEC_GOLDENS.items())


def test_forged_exact_query_profile_rejects_before_catalog_query() -> None:
    import hashlib
    import json

    from dpone.runtime.extraction_lifecycle import ExtractionLifecycleAuthority
    from dpone.runtime.sources.postgres_source_authority import PostgresSourceAuthorityVerifier
    from tests.test_postgres_mssql_r1_source_schema_runtime import FakeCatalogConnector

    modules = {
        key: _module(name)
        for key, name in {
            "snapshot": "dpone.runtime.sources.postgres_verified_relation_snapshot",
            "issuer": "dpone.runtime.sources.postgres_mssql_source_schema_issuer",
            "observation": "dpone.runtime.sources.postgres_mssql_source_schema_observation",
        }.items()
    }
    verifier = PostgresSourceAuthorityVerifier(_source_authority())
    selected = verifier.select_for(_load_config())
    issuer = modules["issuer"].PostgresMssqlRelationSchemaAuthorityIssuerV1(type_policy_authority=_policy())
    valid = issuer.bind_query_profile(selected_source_authority=selected)
    connector = FakeCatalogConnector()
    scope = (
        modules["snapshot"]
        .PostgresVerifiedRelationSnapshotIssuerV1()
        .open(
            connector=connector,
            lifecycle=ExtractionLifecycleAuthority(),
            selected_source_authority=selected,
            query_profile=valid.generic_relation_profile,
        )
    )
    verified = scope.require_active(connector)

    def execution_digest(items: tuple[Any, ...]) -> bytes:
        document = {
            "contract_version": "dpone-postgres-mssql-source-schema-query-profile-1",
            "items": [item.to_document() for item in items],
        }
        payload = json.dumps(document, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
        return hashlib.sha256(b"dpone-postgres-mssql-source-schema-query-profile-v1\0" + payload).digest()

    first = valid.ordered_items[0]
    changed_sql = replace(first, sql_text=first.sql_text + " /* forged */")
    changed_items = (changed_sql, *valid.ordered_items[1:])
    alias_item = replace(first, statement_id="forged_statement_alias")
    alias_items = (alias_item, *valid.ordered_items[1:])
    cardinality_item = replace(valid.ordered_items[-1], cardinality="exactly_one")
    cardinality_items = (*valid.ordered_items[:-1], cardinality_item)
    field_item = valid.ordered_items[-1]
    forged_field = replace(field_item.result_fields[0], name="forged_relation_oid")
    field_items = (
        *valid.ordered_items[:-1],
        replace(field_item, result_fields=(forged_field, *field_item.result_fields[1:])),
    )
    substitute_generic = replace(valid.generic_relation_profile)
    forgeries = (
        replace(valid, ordered_items=changed_items),
        replace(valid, ordered_items=changed_items, query_execution_profile_sha256=execution_digest(changed_items)),
        replace(valid, generic_relation_profile=substitute_generic),
        replace(valid, ordered_items=alias_items),
        replace(valid, ordered_items=tuple(reversed(valid.ordered_items))),
        replace(valid, ordered_items=cardinality_items),
        replace(valid, ordered_items=field_items),
        replace(valid, query_execution_profile_sha256=b"x" * 32),
        replace(valid, query_template_profile_sha256=b"y" * 32),
    )
    observations: list[tuple[str | None, bool, bool]] = []
    for forged in forgeries:
        before = tuple(connector.events)
        caught = None
        try:
            issuer.issue(connector=connector, verified_relation=verified, query_profile=forged)
        except BaseException as error:
            caught = error
        observations.append(
            (
                getattr(caught, "reason", None),
                tuple(connector.events) == before,
                forged.generic_relation_profile is valid.generic_relation_profile,
            )
        )
    scope.close_if_active()

    assert all(reason in {"exact_type_violation", "internal_invariant_violation"} for reason, _, _ in observations)
    assert all(transcript_unchanged for _, transcript_unchanged, _ in observations)
    assert observations[2][2] is False


def test_generic_fake_observation_with_wrong_python_type_rejects_without_coercion() -> None:
    from dpone.runtime.extraction_lifecycle import ExtractionLifecycleAuthority
    from dpone.runtime.sources.postgres_source_authority import PostgresSourceAuthorityVerifier
    from tests.test_postgres_mssql_r1_source_schema_runtime import FakeCatalogConnector

    issuer_module = _module("dpone.runtime.sources.postgres_mssql_source_schema_issuer")
    snapshot_module = _module("dpone.runtime.sources.postgres_verified_relation_snapshot")
    verifier = PostgresSourceAuthorityVerifier(_source_authority())
    selected = verifier.select_for(_load_config())
    issuer = issuer_module.PostgresMssqlRelationSchemaAuthorityIssuerV1(type_policy_authority=_policy())
    profile = issuer.bind_query_profile(selected_source_authority=selected)
    connector = FakeCatalogConnector()
    original_rows = connector._rows

    def wrong_rows(sql: str):
        rows = original_rows(sql)
        if "snapshot_witness" in sql:
            rows[0]["database_oid"] = True
        return rows

    connector._rows = wrong_rows  # type: ignore[method-assign]
    error = _raises(
        lambda: snapshot_module.PostgresVerifiedRelationSnapshotIssuerV1().open(
            connector=connector,
            lifecycle=ExtractionLifecycleAuthority(),
            selected_source_authority=selected,
            query_profile=profile.generic_relation_profile,
        )
    )
    assert getattr(error, "reason", "") == "source_authority_mismatch"


def test_wrong_catalog_behavior_branch_cannot_be_relabelled_as_column_count() -> None:
    modules = {"dummy": None}
    for key, name in {
        "models": "dpone.contracts.postgres_mssql_source_schema_models",
        "authority": "dpone.contracts.postgres_mssql_source_schema_authority",
        "observation": "dpone.runtime.sources.postgres_mssql_source_schema_observation",
        "issuer": "dpone.runtime.sources.postgres_mssql_source_schema_issuer",
        "projection": "dpone.runtime.sources.postgres_mssql_source_schema_projection",
        "snapshot": "dpone.runtime.sources.postgres_verified_relation_snapshot",
        "runtime": "dpone.runtime.postgres_mssql_source_schema_runtime",
        "boundary": "dpone.runtime.sources.strategies.postgres.postgres_prepared_source_boundary",
    }.items():
        modules[key] = _module(name)
    runtime_support = importlib.import_module("tests.test_postgres_mssql_r1_source_schema_runtime")
    connector = runtime_support.FakeCatalogConnector(columns=0)
    connector.relation_present = False
    error = _raises(lambda: issue_authority(modules, connector=connector))
    assert getattr(error, "reason", "") == "source_authority_mismatch"


def test_type_authority_import_has_no_hidden_source_schema_cycle() -> None:
    import ast

    result = _run_isolated_type_authority_import()
    violations: list[str] = []
    if result.returncode != 0:
        violations.append(f"isolated type-authority import failed: {result.stderr}")
    elif Path(result.stdout.strip()) != (_CHECKOUT_SRC / "dpone" / "contracts" / "postgres_mssql_type_authority.py"):
        violations.append("isolated type-authority import resolved outside checkout")

    paths = {
        "generic_observation": _CHECKOUT_SRC / "dpone/runtime/sources/postgres_verified_relation_observation.py",
        "generic_snapshot": _CHECKOUT_SRC / "dpone/runtime/sources/postgres_verified_relation_snapshot.py",
        "generic_cleanup": _CHECKOUT_SRC / "dpone/runtime/sources/postgres_verified_relation_snapshot_cleanup.py",
        "route_observation": _CHECKOUT_SRC / "dpone/runtime/sources/postgres_mssql_source_schema_observation.py",
        "projection": _CHECKOUT_SRC / "dpone/runtime/sources/postgres_mssql_source_schema_projection.py",
        "postgres_source": _CHECKOUT_SRC / "dpone/runtime/sources/postgres.py",
        "file_export": _CHECKOUT_SRC / "dpone/runtime/sources/strategies/postgres/postgres_file_export_mixin.py",
        "value_admission": _CHECKOUT_SRC / "dpone/contracts/postgres_mssql_value_admission.py",
    }
    capability_present = paths["route_observation"].is_file()

    def dependencies(path: Path) -> set[str]:
        if not path.is_file():
            return set()
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        module_name = ".".join(path.relative_to(_CHECKOUT_SRC).with_suffix("").parts)
        package = module_name.rpartition(".")[0]
        found: set[str] = set()
        import_module_aliases = {"import_module"}
        import_aliases = {"__import__"}
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "importlib":
                        import_module_aliases.add(alias.asname or alias.name)
            elif isinstance(node, ast.ImportFrom) and node.module == "importlib":
                for alias in node.names:
                    if alias.name == "import_module":
                        import_module_aliases.add(alias.asname or alias.name)
            elif isinstance(node, ast.ImportFrom) and node.module == "builtins":
                for alias in node.names:
                    if alias.name == "__import__":
                        import_aliases.add(alias.asname or alias.name)

        def resolved(name: str, *, explicit_package: str | None = None, level: int = 0) -> str:
            relative = f"{'.' * level}{name}" if level else name
            if relative.startswith("."):
                return importlib.util.resolve_name(relative, explicit_package or package)
            return relative

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                found.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported_from = resolved(node.module or "", level=node.level)
                found.add(imported_from)
                found.update(
                    f"{imported_from}.{alias.name}" if imported_from else alias.name
                    for alias in node.names
                    if alias.name != "*"
                )
            elif (
                isinstance(node, ast.Call)
                and isinstance(node.func, (ast.Name, ast.Attribute))
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
                and (
                    (isinstance(node.func, ast.Name) and node.func.id in import_module_aliases.union(import_aliases))
                    or (
                        isinstance(node.func, ast.Attribute)
                        and node.func.attr == "import_module"
                        and isinstance(node.func.value, ast.Name)
                        and node.func.value.id in import_module_aliases
                    )
                )
            ):
                explicit_package = next(
                    (
                        keyword.value.value
                        for keyword in node.keywords
                        if keyword.arg == "package"
                        and isinstance(keyword.value, ast.Constant)
                        and isinstance(keyword.value.value, str)
                    ),
                    None,
                )
                if (
                    explicit_package is None
                    and len(node.args) > 1
                    and isinstance(node.args[1], ast.Constant)
                    and isinstance(node.args[1].value, str)
                ):
                    explicit_package = node.args[1].value
                level = next(
                    (
                        keyword.value.value
                        for keyword in node.keywords
                        if keyword.arg == "level"
                        and isinstance(keyword.value, ast.Constant)
                        and isinstance(keyword.value.value, int)
                    ),
                    0,
                )
                found.add(resolved(node.args[0].value, explicit_package=explicit_package, level=level))
        return found

    observed = {name: dependencies(path) for name, path in paths.items()}
    if capability_present:
        for name in ("generic_observation", "generic_snapshot", "generic_cleanup"):
            if not paths[name].is_file():
                violations.append(f"required generic implementation module missing: {paths[name].name}")
            elif any("postgres_mssql" in dependency for dependency in observed[name]):
                violations.append(f"{name} has an outward postgres_mssql dependency")
        if "dpone.runtime.sources.postgres_verified_relation_observation" not in observed["route_observation"]:
            violations.append("route observation does not depend inward on generic observation")
        forbidden = {
            "projection": "dpone.runtime.sources.strategies.postgres.postgres_prepared_source_boundary",
            "postgres_source": "dpone.runtime.postgres_mssql_source_schema_runtime",
            "file_export": "dpone.runtime.sources.strategies.postgres.postgres_prepared_source_boundary",
        }
        for name, dependency in forbidden.items():
            if dependency in observed[name]:
                violations.append(f"{name} imports forbidden concrete dependency {dependency}")

    def closed_literal_exports(source: str) -> tuple[set[str], bool]:
        tree = ast.parse(source)

        def literal_exports(value: ast.expr | None) -> set[str] | None:
            if value is None:
                return None
            try:
                candidate = ast.literal_eval(value)
            except (ValueError, TypeError, SyntaxError, MemoryError, RecursionError):
                return None
            if not isinstance(candidate, (tuple, list, set)) or not all(isinstance(item, str) for item in candidate):
                return None
            return set(candidate)

        declarations: list[tuple[ast.Name, ast.expr | None]] = []
        for node in tree.body:
            if (
                isinstance(node, ast.Assign)
                and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id == "__all__"
            ):
                declarations.append((node.targets[0], node.value))
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id == "__all__":
                declarations.append((node.target, node.value))
        if len(declarations) != 1:
            return set(), False
        declaration_target, declaration_value = declarations[0]
        exports = literal_exports(declaration_value)
        if exports is None:
            return set(), False

        all_name_occurrences = [
            walked for walked in ast.walk(tree) if isinstance(walked, ast.Name) and walked.id == "__all__"
        ]
        if all_name_occurrences != [declaration_target]:
            return exports, False
        if any(isinstance(walked, ast.Constant) and walked.value == "__all__" for walked in ast.walk(tree)):
            return exports, False

        def mentions_all(node: ast.AST) -> bool:
            return any(isinstance(child, ast.Name) and child.id == "__all__" for child in ast.walk(node))

        for walked in ast.walk(tree):
            if (
                isinstance(walked, ast.Name)
                and walked.id == "__all__"
                and isinstance(walked.ctx, (ast.Store, ast.Del))
                and walked is not declaration_target
            ):
                return exports, False
            if (
                isinstance(walked, ast.Subscript)
                and isinstance(walked.ctx, (ast.Store, ast.Del))
                and mentions_all(walked.value)
            ):
                return exports, False
            if isinstance(walked, ast.Call):
                receiver_mutation = isinstance(walked.func, ast.Attribute) and mentions_all(walked.func.value)
                unknown_argument_mutation = any(mentions_all(argument) for argument in walked.args) or any(
                    mentions_all(keyword.value) for keyword in walked.keywords
                )
                if receiver_mutation or unknown_argument_mutation:
                    return exports, False
        return exports, True

    synthetic_mutations = {
        "insert": "__all__ = ('safe',)\n__all__.insert(0, 'admit_binary_value')\n",
        "nested_append": "__all__ = ('safe',)\nif enabled:\n    __all__.append('admit_binary_value')\n",
        "subscript_store": "__all__ = ['safe']\n__all__[0] = 'admit_binary_value'\n",
        "unknown_mutator": "__all__ = ('safe',)\nmutate_exports(__all__)\n",
        "alias_mutator": "__all__ = ('safe',)\nalias = __all__\nalias.append('admit_binary_value')\n",
        "dynamic_globals_mutator": ("__all__ = ('safe',)\nglobals()['__all__'].append('admit_binary_value')\n"),
    }
    for scenario, source in synthetic_mutations.items():
        _exports, accepted = closed_literal_exports(source)
        if accepted:
            violations.append(f"__all__ mutation scanner accepted {scenario}")

    if capability_present and paths["value_admission"].is_file():
        exports, all_closed = closed_literal_exports(paths["value_admission"].read_text(encoding="utf-8"))
        if not all_closed:
            violations.append("value admission requires an explicit closed __all__")
        if "admit_binary_value" in exports:
            violations.append("admit_binary_value leaked into __all__")

    assert violations == []


def test_external_installed_copy_cannot_satisfy_type_authority_import_probe(tmp_path: Path) -> None:
    external_src = tmp_path / "external-site-packages"
    external_contracts = external_src / "dpone" / "contracts"
    external_contracts.mkdir(parents=True)
    (external_src / "dpone" / "__init__.py").write_text("", encoding="utf-8")
    (external_contracts / "__init__.py").write_text("", encoding="utf-8")
    (external_contracts / "postgres_mssql_type_authority.py").write_text(
        "EXTERNAL_COPY_WITHOUT_CYCLE = True\n",
        encoding="utf-8",
    )

    result = _run_isolated_type_authority_import(import_src=external_src)

    assert result.returncode != 0
    assert "external import origin cannot satisfy checkout probe" in result.stderr


def test_optional_runtime_port_is_either_absent_or_consumed_by_checkout_composition() -> None:
    script = textwrap.dedent(
        """
        import importlib.util
        import inspect
        import sys
        from pathlib import Path

        checkout_src = Path(sys.argv[1]).resolve(strict=True)
        sys.path.insert(0, str(checkout_src))
        port_name = "dpone.ports.postgres_mssql_source_schema_runtime"
        if importlib.util.find_spec(port_name) is None:
            raise SystemExit(0)
        import dpone.runtime.bootstrap_hydrator as hydrator
        assert port_name in sys.modules, "runtime port exists but composition does not consume it"
        signature = inspect.signature(hydrator.DefaultRuntimeHydrator)
        parameter = signature.parameters.get("postgres_mssql_source_schema_runtime_factory")
        assert parameter is not None, "runtime port exists without a composition injection point"
        annotation = repr(parameter.annotation)
        assert "postgres_mssql_source_schema_runtime" in annotation
        assert Path(hydrator.__file__).resolve().is_relative_to(checkout_src)
        """
    )
    result = subprocess.run(
        [sys.executable, "-I", "-c", script, str(_CHECKOUT_SRC)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
