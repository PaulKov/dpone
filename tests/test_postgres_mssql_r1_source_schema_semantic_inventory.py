"""Behavioral denominator for the PostgreSQL R1 source-schema authority.

The case registry selects node IDs and declares coverage, but it is never an
oracle.  Each scenario below owns an independent golden outcome.  A semantic
observation is emitted only after every constituent assertion completed.
"""

from __future__ import annotations

import importlib
import json
from collections.abc import Callable
from dataclasses import fields
from pathlib import Path
from typing import Any, Literal

import pytest

from dpone.contracts.postgres_mssql_type_authority import PostgresMssqlTypePolicyAuthorityV1
from dpone.contracts.postgres_mssql_type_derivation import derive_type_decision
from dpone.contracts.postgres_mssql_type_target_enums import (
    MssqlR1TargetScalarFamilyV1,
    PostgresMssqlCodecV1,
    PostgresMssqlLengthKindV1,
    PostgresMssqlSourceScalarFamilyV1,
)
from dpone.contracts.postgres_mssql_type_target_shapes import (
    PostgresMssqlSourceScalarShapeV1,
)
from dpone.contracts.postgres_source_authority import (
    PostgresNamedOidPin,
    PostgresRelationAuthorityPin,
    SelectedPostgresSourceAuthority,
)

ObservedClass = Literal[
    "byte_identical",
    "valid_distinct",
    "typed_rejection",
    "caller_cancelled",
    "invariant_preserved",
]

_REGISTRY_PATH = Path("docs/schemas/evidence/postgres-mssql-r1-source-schema-authority-v1-cases.json")
_REGISTRY = json.loads(_REGISTRY_PATH.read_text(encoding="utf-8"))
_CASES = tuple(_REGISTRY["cases"])
_CASE_IDS = tuple(item["case_id"] for item in _CASES)
_COVERAGE = {item["case_id"]: tuple(item["coverage"]) for item in _CASES}
_SELECTED_AUTHORITY_GOLDENS = {
    1: (
        b'{"database":{"canonical_name":"warehouse","oid":16384},"dialect":"postgres",'
        b'"principals":{"effective":{"canonical_name":"dpone_reader","oid":17001},'
        b'"session":{"canonical_name":"dpone_login","oid":17002}},'
        b'"relation":{"namespace_oid":2200,"relation":"orders","relation_oid":22001,"schema":"sales"},'
        b'"role":"source","system_identifier":"7272727272727272727","timeline_id":7,'
        b'"topology_role":"primary","version":1}'
    ),
    2: (
        b'{"database":{"canonical_name":"warehouse","oid":16384},"dialect":"postgres",'
        b'"principals":{"effective":{"canonical_name":"dpone_reader","oid":17001},'
        b'"session":{"canonical_name":"dpone_login","oid":17002}},'
        b'"relation":{"namespace_oid":2200,"relation":"orders","relation_oid":22001,"schema":"sales"},'
        b'"role":"source","topology_role":"primary","verification_profile":"catalog_identity","version":2}'
    ),
}
_SCENARIO_PROVIDER_MODULES = (
    "tests.test_postgres_mssql_r1_source_schema_runtime",
    "tests.test_postgres_mssql_r1_source_schema_authority_contract",
    "tests.test_postgres_mssql_r1_source_schema_authority_mutation",
    "tests.test_postgres_mssql_prepared_source_boundary",
    "tests.test_runtime_connection_composition_root",
)


class _ConstituentProof:
    """Require one independent assertion for every registered constituent."""

    def __init__(self, case_id: str) -> None:
        self.case_id = case_id
        self.required = _COVERAGE[case_id]
        self.observed: list[str] = []

    def prove(self, constituent: str, predicate: bool) -> None:
        assert constituent in self.required, f"unregistered constituent: {constituent}"
        assert constituent not in self.observed, f"duplicate constituent: {constituent}"
        assert predicate, f"behavior did not prove {self.case_id}/{constituent}"
        self.observed.append(constituent)

    def finish(self, observed_class: ObservedClass) -> ObservedClass:
        assert tuple(self.observed) == self.required
        return observed_class


@pytest.fixture
def semantic_case_observer(request: pytest.FixtureRequest) -> Callable[[str, ObservedClass], None]:
    called = False

    def observe(case_id: str, observed_class: ObservedClass) -> None:
        nonlocal called
        assert not called
        assert case_id in _CASE_IDS
        assert observed_class in {
            "byte_identical",
            "valid_distinct",
            "typed_rejection",
            "caller_cancelled",
            "invariant_preserved",
        }
        request.node.user_properties.append(("source_schema_semantic_observation", (case_id, observed_class)))
        called = True

    return observe


def _feature_modules() -> dict[str, Any]:
    names = {
        "models": "dpone.contracts.postgres_mssql_source_schema_models",
        "authority": "dpone.contracts.postgres_mssql_source_schema_authority",
        "observation": "dpone.runtime.sources.postgres_mssql_source_schema_observation",
        "issuer": "dpone.runtime.sources.postgres_mssql_source_schema_issuer",
        "projection": "dpone.runtime.sources.postgres_mssql_source_schema_projection",
        "snapshot": "dpone.runtime.sources.postgres_verified_relation_snapshot",
        "runtime": "dpone.runtime.postgres_mssql_source_schema_runtime",
        "boundary": "dpone.runtime.sources.strategies.postgres.postgres_prepared_source_boundary",
    }
    loaded: dict[str, Any] = {}
    for key, name in names.items():
        try:
            loaded[key] = importlib.import_module(name)
        except ModuleNotFoundError as exc:
            pytest.fail(f"approved source-schema implementation missing: {name}", pytrace=False)
            raise AssertionError from exc
    return loaded


def _selected(*, version: int = 1, relation_oid: int = 22001, nfd: bool = False) -> SelectedPostgresSourceAuthority:
    schema = "cafe\u0301" if nfd else "sales"
    relation = "orde\u0301rs" if nfd else "orders"
    return SelectedPostgresSourceAuthority(
        system_identifier="7272727272727272727" if version == 1 else None,
        timeline_id=7 if version == 1 else None,
        verification_profile="physical_cluster" if version == 1 else "catalog_identity",
        topology_role="primary",
        database=PostgresNamedOidPin("warehouse", 16384),
        effective_principal=PostgresNamedOidPin("dpone_reader", 17001),
        session_principal=PostgresNamedOidPin("dpone_login", 17002),
        relation=PostgresRelationAuthorityPin(schema, relation, 2200, relation_oid),
        authored_schema=schema,
        authored_relation=relation,
        version=version,
    )


def _shape(
    family: PostgresMssqlSourceScalarFamilyV1,
    oid: int,
    typmod: int = -1,
    *,
    precision: int | None = None,
    scale: int | None = None,
    maximum_characters: int | None = None,
) -> PostgresMssqlSourceScalarShapeV1:
    length = (
        PostgresMssqlLengthKindV1.BOUNDED
        if family is PostgresMssqlSourceScalarFamilyV1.VARCHAR
        else PostgresMssqlLengthKindV1.MAXIMUM
        if family is PostgresMssqlSourceScalarFamilyV1.TEXT
        else PostgresMssqlLengthKindV1.NOT_APPLICABLE
    )
    return PostgresMssqlSourceScalarShapeV1(family, oid, typmod, length, precision, scale, maximum_characters)


def _all_source_shapes() -> tuple[PostgresMssqlSourceScalarShapeV1, ...]:
    source = PostgresMssqlSourceScalarFamilyV1
    return (
        _shape(source.BOOL, 16),
        _shape(source.INT2, 21),
        _shape(source.INT4, 23),
        _shape(source.INT8, 20),
        _shape(source.NUMERIC, 1700, ((18 << 16) | 4) + 4, precision=18, scale=4),
        _shape(source.NUMERIC, 1700, ((18 << 16) | (-2 & 0x7FF)) + 4, precision=18, scale=-2),
        _shape(source.FLOAT4, 700),
        _shape(source.FLOAT8, 701),
        _shape(source.UUID, 2950),
        _shape(source.DATE, 1082),
        _shape(source.TIME, 1083, -1, precision=6),
        _shape(source.TIME, 1083, 3, precision=3),
        _shape(source.TIMESTAMP, 1114, -1, precision=6),
        _shape(source.TIMESTAMP, 1114, 3, precision=3),
        _shape(source.TIMESTAMPTZ, 1184, -1, precision=6),
        _shape(source.TIMESTAMPTZ, 1184, 3, precision=3),
        _shape(source.TEXT, 25),
        _shape(source.VARCHAR, 1043, 36, maximum_characters=32),
        _shape(source.BYTEA, 17),
    )


def _policy(shapes: tuple[PostgresMssqlSourceScalarShapeV1, ...] | None = None) -> PostgresMssqlTypePolicyAuthorityV1:
    owned = shapes or (_shape(PostgresMssqlSourceScalarFamilyV1.INT8, 20),)
    decisions = tuple(derive_type_decision(shape, maximum_input_bytes=1_048_576) for shape in owned)
    return PostgresMssqlTypePolicyAuthorityV1.create(decisions, owned)


def _raises_typed(call: Callable[[], object]) -> BaseException:
    try:
        call()
    except Exception as exc:  # noqa: BLE001 - exact exception is asserted by scenarios.
        assert type(exc) is not AssertionError
        return exc
    raise AssertionError("typed rejection required")


def _selected_preimage(case_id: str, proof: _ConstituentProof) -> ObservedClass:
    version = 1 if case_id.endswith("v1") else 2
    selected = _selected(version=version)
    expected = _SELECTED_AUTHORITY_GOLDENS[version]
    observed = getattr(selected, "authority_document_utf8", None)
    proof.prove(f"v{version}.authority_document_utf8", type(observed) is bytes and observed == expected)
    import hashlib

    proof.prove(
        f"v{version}.authority_sha256", selected.authority_sha256 == "sha256:" + hashlib.sha256(expected).hexdigest()
    )
    return proof.finish("byte_identical")


def _selected_nfd(proof: _ConstituentProof) -> ObservedClass:
    selected = _selected(nfd=True)
    payload = selected.authority_document_utf8
    proof.prove(
        "relation.schema.NFD", "cafe\\u0301" not in payload.decode("utf-8") and "cafe\u0301" in payload.decode("utf-8")
    )
    proof.prove("relation.relation.NFD", "orde\u0301rs" in payload.decode("utf-8"))
    proof.prove("no_unicode_normalization", b"\xcc\x81" in payload)
    return proof.finish("byte_identical")


def _closed_selected_document(case_id: str, proof: _ConstituentProof, modules: dict[str, Any]) -> ObservedClass:
    version = 1 if case_id.endswith("v1") else 2
    decoder = modules["models"].PostgresSelectedRelationAuthorityDocumentV1
    valid = _selected(version=version).authority_document_utf8
    decoded = decoder.from_authority_document_utf8(valid)
    document = json.loads(valid)
    proof.prove("version", decoded.version == version)
    proof.prove("dialect", decoded.dialect == "postgres")
    proof.prove("role", decoded.role == "source")
    proof.prove("topology_role", decoded.topology_role == "primary")
    proof.prove("database.canonical_name", decoded.database.canonical_name == "warehouse")
    proof.prove("database.oid", decoded.database.oid == 16384)
    proof.prove("principals.effective.canonical_name", decoded.effective_principal.canonical_name == "dpone_reader")
    proof.prove("principals.effective.oid", decoded.effective_principal.oid == 17001)
    proof.prove("principals.session.canonical_name", decoded.session_principal.canonical_name == "dpone_login")
    proof.prove("principals.session.oid", decoded.session_principal.oid == 17002)
    proof.prove("relation.schema", decoded.relation.schema == "sales")
    proof.prove("relation.relation", decoded.relation.relation == "orders")
    proof.prove("relation.namespace_oid", decoded.relation.namespace_oid == 2200)
    proof.prove("relation.relation_oid", decoded.relation.relation_oid == 22001)
    if version == 1:
        proof.prove("system_identifier", decoded.system_identifier == "7272727272727272727")
        proof.prove("timeline_id", decoded.timeline_id == 7)
        bad = dict(document, verification_profile="catalog_identity")
        proof.prove("verification_profile.forbidden", _raises_typed(lambda: decoder.from_document(bad)) is not None)
    else:
        proof.prove("verification_profile", decoded.verification_profile == "catalog_identity")
        proof.prove(
            "system_identifier.forbidden",
            _raises_typed(lambda: decoder.from_document(dict(document, system_identifier="1"))) is not None,
        )
        proof.prove(
            "timeline_id.forbidden",
            _raises_typed(lambda: decoder.from_document(dict(document, timeline_id=1))) is not None,
        )
    proof.prove("unknown_keys", _raises_typed(lambda: decoder.from_document(dict(document, extra=True))) is not None)
    missing = dict(document)
    missing.pop("database")
    proof.prove("missing_keys", _raises_typed(lambda: decoder.from_document(missing)) is not None)
    bad_oid = json.loads(valid)
    bad_oid["database"]["oid"] = True
    proof.prove("bool_is_not_oid", _raises_typed(lambda: decoder.from_document(bad_oid)) is not None)
    return proof.finish("typed_rejection")


def _source_projection_vectors() -> tuple[tuple[str, PostgresMssqlSourceScalarShapeV1, str], ...]:
    labels = (
        "bool",
        "int2",
        "int4",
        "int8",
        "numeric.scale_nonnegative",
        "numeric.scale_negative",
        "float4",
        "float8",
        "uuid",
        "date",
        "time.typmod_omitted",
        "time.typmod_explicit",
        "timestamp.typmod_omitted",
        "timestamp.typmod_explicit",
        "timestamptz.typmod_omitted",
        "timestamptz.typmod_explicit",
        "text",
        "varchar.bounded",
        "bytea",
    )
    tokens = (
        "boolean",
        "smallint",
        "integer",
        "bigint",
        "numeric(18,4)",
        "numeric(18,-2)",
        "real",
        "double precision",
        "uuid",
        "date",
        "time without time zone",
        "time(3) without time zone",
        "timestamp without time zone",
        "timestamp(3) without time zone",
        "timestamp with time zone",
        "timestamp(3) with time zone",
        "text",
        "character varying(32)",
        "bytea",
    )
    return tuple(zip(labels, _all_source_shapes(), tokens, strict=True))


_SOURCE_GOLDENS = tuple(token for _constituent, _shape_value, token in _source_projection_vectors())


def _projection_source_tokens(proof: _ConstituentProof, modules: dict[str, Any]) -> ObservedClass:
    renderer = modules["projection"].render_postgres_declared_type
    vectors = _source_projection_vectors()
    assert tuple(constituent for constituent, _shape_value, _token in vectors) == proof.required
    observed = tuple(renderer(shape) for _constituent, shape, _token in vectors)
    assert observed == _SOURCE_GOLDENS
    assert len(set(observed)) == len(observed)
    for (constituent, _shape_value, expected), actual in zip(vectors, observed, strict=True):
        proof.prove(constituent, actual == expected)
    return proof.finish("valid_distinct")


def _projection_target_tokens(proof: _ConstituentProof, modules: dict[str, Any]) -> ObservedClass:
    return _observe_target_projection_tokens(
        proof,
        modules["projection"].render_mssql_target_type,
        _target_projection_vectors(),
    )


def _target_projection_vectors() -> tuple[tuple[str, Any, str], ...]:
    """Build the exact independent target-facet matrix in frozen coverage order."""

    source = PostgresMssqlSourceScalarFamilyV1
    source_vectors: list[tuple[str, PostgresMssqlSourceScalarShapeV1, str]] = [
        ("bit", _shape(source.BOOL, 16), "bit"),
        ("smallint", _shape(source.INT2, 21), "smallint"),
        ("int", _shape(source.INT4, 23), "int"),
        ("bigint", _shape(source.INT8, 20), "bigint"),
        (
            "decimal.standard_scale",
            _shape(source.NUMERIC, 1700, ((18 << 16) | 4) + 4, precision=18, scale=4),
            "decimal(18,4)",
        ),
        (
            "decimal.negative_source_scale",
            _shape(
                source.NUMERIC,
                1700,
                ((18 << 16) | (-2 & 0x7FF)) + 4,
                precision=18,
                scale=-2,
            ),
            "decimal(20,0)",
        ),
        (
            "decimal.scale_greater_than_precision",
            _shape(source.NUMERIC, 1700, ((4 << 16) | 8) + 4, precision=4, scale=8),
            "decimal(8,8)",
        ),
        ("real", _shape(source.FLOAT4, 700), "real"),
        ("float_53", _shape(source.FLOAT8, 701), "float(53)"),
        ("uniqueidentifier", _shape(source.UUID, 2950), "uniqueidentifier"),
        ("date", _shape(source.DATE, 1082), "date"),
    ]
    for target_name, family, oid in (
        ("time", source.TIME, 1083),
        ("datetime2", source.TIMESTAMP, 1114),
        ("datetimeoffset", source.TIMESTAMPTZ, 1184),
    ):
        for precision in range(7):
            source_vectors.append(
                (
                    f"{target_name}.precision_{precision}",
                    _shape(family, oid, precision, precision=precision),
                    f"{target_name}({precision})",
                )
            )
    source_vectors.extend(
        (
            (
                "nvarchar.bounded",
                _shape(source.VARCHAR, 1043, 36, maximum_characters=32),
                "nvarchar(64)",
            ),
            ("nvarchar.max", _shape(source.TEXT, 25), "nvarchar(max)"),
            ("varbinary.max", _shape(source.BYTEA, 17), "varbinary(max)"),
        )
    )
    return tuple(
        (
            constituent,
            derive_type_decision(shape, maximum_input_bytes=1_048_576).target_shape,
            expected,
        )
        for constituent, shape, expected in source_vectors
    )


def _observe_target_projection_tokens(
    proof: _ConstituentProof,
    renderer: Callable[[Any], str],
    vectors: tuple[tuple[str, Any, str], ...],
) -> ObservedClass:
    assert tuple(constituent for constituent, _shape_value, _expected in vectors) == proof.required
    for constituent, target_shape, expected in vectors:
        proof.prove(constituent, renderer(target_shape) == expected)
    return proof.finish("valid_distinct")


def _independent_target_renderer(target_shape: Any) -> str:
    """Test-only complete renderer proving the frozen oracle has a valid model."""

    family = target_shape.family
    fixed = {
        MssqlR1TargetScalarFamilyV1.BIT: "bit",
        MssqlR1TargetScalarFamilyV1.SMALLINT: "smallint",
        MssqlR1TargetScalarFamilyV1.INT: "int",
        MssqlR1TargetScalarFamilyV1.BIGINT: "bigint",
        MssqlR1TargetScalarFamilyV1.REAL: "real",
        MssqlR1TargetScalarFamilyV1.FLOAT_53: "float(53)",
        MssqlR1TargetScalarFamilyV1.UNIQUEIDENTIFIER: "uniqueidentifier",
        MssqlR1TargetScalarFamilyV1.DATE: "date",
    }
    if family in fixed:
        return fixed[family]
    if family is MssqlR1TargetScalarFamilyV1.DECIMAL:
        return f"decimal({target_shape.precision},{target_shape.scale})"
    if family in {
        MssqlR1TargetScalarFamilyV1.TIME,
        MssqlR1TargetScalarFamilyV1.DATETIME2,
        MssqlR1TargetScalarFamilyV1.DATETIMEOFFSET,
    }:
        return f"{family.value}({target_shape.precision})"
    if family is MssqlR1TargetScalarFamilyV1.NVARCHAR:
        suffix = (
            str(target_shape.maximum_utf16_units)
            if target_shape.length_kind is PostgresMssqlLengthKindV1.BOUNDED
            else "max"
        )
        return f"nvarchar({suffix})"
    if family is MssqlR1TargetScalarFamilyV1.VARBINARY:
        return "varbinary(max)"
    raise AssertionError(f"unmodelled target family: {family!r}")


def test_target_token_observer_has_a_complete_independent_model_and_rejects_bad_value_or_order() -> None:
    case_id = "projection.target-token-arms"
    vectors = _target_projection_vectors()
    assert (
        _observe_target_projection_tokens(_ConstituentProof(case_id), _independent_target_renderer, vectors)
        == "valid_distinct"
    )
    with pytest.raises(AssertionError):
        _observe_target_projection_tokens(_ConstituentProof(case_id), lambda _shape_value: "constant", vectors)
    with pytest.raises(AssertionError):
        _observe_target_projection_tokens(_ConstituentProof(case_id), _independent_target_renderer, vectors[::-1])


def test_all_direct_renderer_observers_bind_exact_frozen_constituent_order() -> None:
    assert tuple(item[0] for item in _source_projection_vectors()) == _COVERAGE["projection.source-token-arms"]
    assert tuple(item[0] for item in _target_projection_vectors()) == _COVERAGE["projection.target-token-arms"]
    assert tuple(codec.value for codec in _CODEC_GOLDENS) == _COVERAGE["projection.codec-arms"]


_CODEC_GOLDENS = {
    PostgresMssqlCodecV1.BOOL_ASCII_V1: "0/1 text",
    PostgresMssqlCodecV1.SIGNED_INTEGER_ASCII_V1: "integer text",
    PostgresMssqlCodecV1.DECIMAL_FIXED_ASCII_V1: "decimal text",
    PostgresMssqlCodecV1.RYU_BINARY32_SHORTEST_ASCII_V1: "float text",
    PostgresMssqlCodecV1.RYU_BINARY64_SHORTEST_ASCII_V1: "float text",
    PostgresMssqlCodecV1.UUID_LOWER_ASCII_V1: "uuid text",
    PostgresMssqlCodecV1.ISO_DATE_ASCII_V1: "ISO date text",
    PostgresMssqlCodecV1.ISO_TIME_ASCII_V1: "time text",
    PostgresMssqlCodecV1.ISO_TIMESTAMP_ASCII_V1: "timestamp text",
    PostgresMssqlCodecV1.ISO_UTC_TIMESTAMP_ASCII_V1: "offset timestamp text",
    PostgresMssqlCodecV1.UTF8_TO_UTF16LE_V1: "BulkTextCodec text",
    PostgresMssqlCodecV1.RAW_BINARY_V1: "hex text via character BCP",
}


def _projection_codecs(proof: _ConstituentProof, modules: dict[str, Any]) -> ObservedClass:
    renderer = modules["projection"].render_transfer_representation
    observed = {codec.value: renderer(codec) for codec in _CODEC_GOLDENS}
    assert len(observed) == 12
    for constituent in proof.required:
        codec = PostgresMssqlCodecV1(constituent)
        proof.prove(constituent, observed[constituent] == _CODEC_GOLDENS[codec])
    return proof.finish("valid_distinct")


def _field_closure(case_id: str, proof: _ConstituentProof, modules: dict[str, Any]) -> ObservedClass:
    class_name = {
        "observed-column.field-closure": "PostgresMssqlObservedSourceColumnV1",
        "authority.field-closure": "PostgresMssqlSelectedRelationSchemaAuthorityV1",
    }[case_id]
    owner = modules["models"] if case_id.startswith("observed") else modules["authority"]
    names = tuple(field.name for field in fields(getattr(owner, class_name)))
    assert names == proof.required
    for constituent in proof.required:
        proof.prove(constituent, names.count(constituent) == 1)
    return proof.finish("typed_rejection")


def _required_behavior_probe(case_id: str, proof: _ConstituentProof, modules: dict[str, Any]) -> ObservedClass:
    """Invoke the approved black-box case driver supplied by the test rig.

    The driver is test-owned, receives concrete production modules and fake
    protocol objects, and returns constituent booleans.  It is deliberately not
    a production semantic-case switch and cannot read the registry oracle.
    """

    _ensure_behavior_scenarios_bound()
    result = _BEHAVIOR_SCENARIOS[case_id](modules)
    assert set(result) == set(proof.required)
    for constituent in proof.required:
        proof.prove(constituent, result[constituent] is True)
    return proof.finish(_OBSERVED_CLASSES[case_id])


def _not_yet_bound(_: dict[str, Any]) -> dict[str, bool]:
    pytest.fail("behavioral scenario requires the approved implementation", pytrace=False)
    return {}


_OBSERVED_CLASSES: dict[str, ObservedClass] = {
    "catalog-shape.exact-resolution": "invariant_preserved",
    "catalog-shape.missing": "typed_rejection",
    "catalog-shape.ambiguous": "typed_rejection",
    "observed-column.exact-types": "typed_rejection",
    "observed-column.domain-version": "typed_rejection",
    "observed-column.identifier-domain": "typed_rejection",
    "observed-column.catalog-identity": "typed_rejection",
    "authority.domain-version": "typed_rejection",
    "authority.canonical-roundtrip": "byte_identical",
    "authority.relation-splice": "typed_rejection",
    "authority.policy-splice": "typed_rejection",
    "authority.reference-splice": "typed_rejection",
    "authority.column-order": "typed_rejection",
    "authority.column-count": "typed_rejection",
    "failure.closed-reason-recovery-message": "invariant_preserved",
    "runtime.lock-before-snapshot": "invariant_preserved",
    "runtime.single-active-reservation": "typed_rejection",
    "runtime.session-incarnation": "typed_rejection",
    "runtime.revalidation": "invariant_preserved",
    "runtime.relation-profile": "typed_rejection",
    "runtime.catalog-observation": "invariant_preserved",
    "runtime.permission-translation": "typed_rejection",
    "runtime.caller-cancellation": "caller_cancelled",
    "runtime.database-cancellation-57014": "typed_rejection",
    "runtime.cleanup-idempotence": "invariant_preserved",
    "runtime.physical-quarantine": "invariant_preserved",
    "runtime.exact-bundle": "typed_rejection",
    "runtime.default-activation-blocked": "typed_rejection",
    "runtime.same-scope-byte-identity": "byte_identical",
    "runtime.ended-or-wrong-scope": "typed_rejection",
    "runtime.new-scope-unchanged-identity": "byte_identical",
    "runtime.post-ddl-digest-change": "valid_distinct",
    "runtime.transient-observation-retry": "invariant_preserved",
    "runtime.old-intent-reuse-blocked": "typed_rejection",
    "projection.provenance-fields": "invariant_preserved",
    "projection.column-fields": "invariant_preserved",
    "projection.aggregate-fields": "invariant_preserved",
    "projection.no-legacy-policy": "invariant_preserved",
    "prepared.from-only": "invariant_preserved",
    "prepared.whole-file-only": "typed_rejection",
    "prepared.bare-lease-blocked": "typed_rejection",
    "prepared.exact-boundary-admission": "typed_rejection",
    "prepared.scope-before-copy": "invariant_preserved",
    "prepared.artifact-receipt": "typed_rejection",
    "prepared.artifact-cleanup": "invariant_preserved",
    "prepared.legacy-boundary-compatible": "invariant_preserved",
}

# These scenario functions are replaced below by concrete fake-protocol
# drivers in the focused runtime/contract modules through normal test imports.
# A missing binding is an assertion failure, never a collection/import failure.
_BEHAVIOR_SCENARIOS: dict[str, Callable[[dict[str, Any]], dict[str, bool]]] = {
    case_id: _not_yet_bound for case_id in _OBSERVED_CLASSES
}


def bind_behavior_scenario(case_id: str, scenario: Callable[[dict[str, Any]], dict[str, bool]]) -> None:
    """Bind one test-owned black-box scenario before parametrized execution."""

    assert case_id in _BEHAVIOR_SCENARIOS
    assert _BEHAVIOR_SCENARIOS[case_id] is _not_yet_bound
    _BEHAVIOR_SCENARIOS[case_id] = scenario


def _ensure_behavior_scenarios_bound() -> None:
    """Load test-owned providers so every frozen node is order-independent."""

    for module_name in _SCENARIO_PROVIDER_MODULES:
        importlib.import_module(module_name)
    assert all(scenario is not _not_yet_bound for scenario in _BEHAVIOR_SCENARIOS.values())


@pytest.mark.parametrize("case_id", _CASE_IDS, ids=_CASE_IDS)
def test_source_schema_semantic_case(
    case_id: str,
    semantic_case_observer: Callable[[str, ObservedClass], None],
) -> None:
    proof = _ConstituentProof(case_id)
    modules = _feature_modules()
    if case_id in {"selected-source.preimage-v1", "selected-source.preimage-v2"}:
        observed = _selected_preimage(case_id, proof)
    elif case_id == "selected-source.nfd-byte-identity":
        observed = _selected_nfd(proof)
    elif case_id in {"selected-source.closed-document-v1", "selected-source.closed-document-v2"}:
        observed = _closed_selected_document(case_id, proof, modules)
    elif case_id in {"observed-column.field-closure", "authority.field-closure"}:
        observed = _field_closure(case_id, proof, modules)
    elif case_id == "projection.source-token-arms":
        observed = _projection_source_tokens(proof, modules)
    elif case_id == "projection.target-token-arms":
        observed = _projection_target_tokens(proof, modules)
    elif case_id == "projection.codec-arms":
        observed = _projection_codecs(proof, modules)
    else:
        observed = _required_behavior_probe(case_id, proof, modules)
    semantic_case_observer(case_id, observed)


def test_registry_is_exactly_56_unique_behavioral_nodes() -> None:
    assert _REGISTRY["contract_version"] == "dpone-postgres-mssql-source-schema-case-registry-1"
    assert len(_CASES) == 56
    assert len(set(_CASE_IDS)) == 56
    assert len({item["nodeid"] for item in _CASES}) == 56
    assert set(_OBSERVED_CLASSES) | {
        "selected-source.preimage-v1",
        "selected-source.preimage-v2",
        "selected-source.nfd-byte-identity",
        "selected-source.closed-document-v1",
        "selected-source.closed-document-v2",
        "observed-column.field-closure",
        "authority.field-closure",
        "projection.source-token-arms",
        "projection.target-token-arms",
        "projection.codec-arms",
    } == set(_CASE_IDS)


def test_selected_preimage_oracle_rejects_corrupted_to_document(monkeypatch: pytest.MonkeyPatch) -> None:
    selected = _selected(version=1)
    monkeypatch.setattr(type(selected), "to_document", lambda _self: {"corrupted": True})
    corrupted = json.dumps(
        selected.to_document(),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    assert corrupted != _SELECTED_AUTHORITY_GOLDENS[1]
    with pytest.raises(AssertionError):
        proof = _ConstituentProof("selected-source.preimage-v1")
        _selected_preimage("selected-source.preimage-v1", proof)


def test_selected_document_rejects_lone_surrogate_stably() -> None:
    modules = _feature_modules()
    decoder = modules["models"].PostgresSelectedRelationAuthorityDocumentV1
    malformed = json.loads(_SELECTED_AUTHORITY_GOLDENS[1])
    malformed["relation"]["schema"] = "\ud800"
    first = _raises_typed(lambda: decoder.from_document(malformed))
    second = _raises_typed(lambda: decoder.from_document(malformed))
    assert type(first) is type(second)
    assert first.args == second.args
    assert first.__cause__ is first.__context__ is None
    assert second.__cause__ is second.__context__ is None
