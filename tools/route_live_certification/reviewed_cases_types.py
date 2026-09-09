"""Reviewed type and boundary matrices for PostgreSQL→MSSQL."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from dpone.type_system.source_sink.certification_suites_postgres_mssql import (
    postgres_mssql_suite,
)

from .reviewed_case import ReviewedCase, ReviewedSuite, case

_POSTGIS_CASES = frozenset({"geometry", "geography", "postgis_raster", "postgis_catalog"})


@dataclass(frozen=True, slots=True)
class _AutoBoundary:
    """One executable boundary contract with an honest authority boundary."""

    value_case: str
    action: str
    outcome: str
    mutation: bool


def boundary_type_suite() -> ReviewedSuite:
    """Auto-mapped aliases, nominal values, boundaries, and rejected values."""

    suite_id = "boundary_types"
    cases: list[ReviewedCase] = []
    for type_case in postgres_mssql_suite().cases:
        if type_case.decision_category != "auto_inferred":
            continue
        base = {
            "source_type": type_case.source_type,
            "canonical_type": type_case.expected_canonical_type,
            "target_type": type_case.expected_target_type,
            "transport": type_case.expected_transport,
        }
        for boundary in _auto_boundaries(type_case.expected_canonical_type, type_case.source_type):
            cases.append(
                case(
                    suite_id,
                    f"{type_case.name}__{boundary.value_case}",
                    {
                        **base,
                        "value_case": boundary.value_case,
                        "comparison": _comparison(type_case.expected_canonical_type, type_case.source_type),
                    },
                    action=boundary.action,
                    outcome=boundary.outcome,
                    mutation=boundary.mutation,
                )
            )
    return ReviewedSuite.create(suite_id, "dpone.postgres_mssql.boundary_types.v1", cases)


def explicit_type_suite() -> ReviewedSuite:
    """Every policy-required family, including contract-negative partitions."""

    suite_id = "explicit_types"
    cases: list[ReviewedCase] = []
    for type_case in postgres_mssql_suite().cases:
        if type_case.decision_category != "incompatible_requires_policy" or type_case.name in _POSTGIS_CASES:
            continue
        base = {
            "source_type": type_case.source_type,
            "canonical_type": type_case.expected_canonical_type,
            "target_type": "nvarchar(max)",
            "comparison": "exact_postgres_cast_text_utf8",
        }
        for contract_case, contract in (
            ("missing_contract", None),
            ("physical_only", {"physical_target_type": "nvarchar(max)"}),
            ("wrong_logical_type", {"logical_type": "integer"}),
        ):
            cases.append(
                case(
                    suite_id,
                    f"{type_case.name}__{contract_case}",
                    {**base, "contract": contract},
                    action="type_contract_preflight",
                    outcome="typed_reject_before_source_copy",
                    mutation=False,
                )
            )
        for value_case in _explicit_boundaries(
            type_case.expected_canonical_type,
            type_case.source_type,
        ):
            cases.append(
                case(
                    suite_id,
                    f"{type_case.name}__string_contract__{value_case}",
                    {
                        **base,
                        "contract": {"logical_type": "string", "physical_target_type": "nvarchar(max)"},
                        "value_case": value_case,
                    },
                    action="standard_etl_explicit_text_roundtrip",
                    outcome="exact_postgres_cast_text_roundtrip",
                    mutation=True,
                )
            )
    return ReviewedSuite.create(suite_id, "dpone.postgres_mssql.explicit_types.v1", cases)


def postgis_type_suite() -> ReviewedSuite:
    """Pinned PostGIS geometry/geography/raster real-vendor contract."""

    suite_id = "postgis_types"
    cases: list[ReviewedCase] = []
    source_cases = [value for value in postgres_mssql_suite().cases if value.name in _POSTGIS_CASES]
    for type_case in source_cases:
        base = {
            "source_type": type_case.source_type,
            "canonical_type": type_case.expected_canonical_type,
            "extension": "postgis",
            "comparison": "exact_postgis_cast_text_utf8",
        }
        for contract_case, contract in (
            ("missing_contract", None),
            ("physical_only", {"physical_target_type": "nvarchar(max)"}),
            ("wrong_logical_type", {"logical_type": "binary"}),
        ):
            cases.append(
                case(
                    suite_id,
                    f"{type_case.name}__{contract_case}",
                    {**base, "contract": contract},
                    action="postgis_contract_preflight",
                    outcome="typed_reject_before_source_copy",
                    mutation=False,
                )
            )
        for value_case in _postgis_boundaries(type_case.name):
            cases.append(
                case(
                    suite_id,
                    f"{type_case.name}__string_contract__{value_case}",
                    {
                        **base,
                        "contract": {"logical_type": "string", "physical_target_type": "nvarchar(max)"},
                        "value_case": value_case,
                    },
                    action="standard_etl_postgis_roundtrip",
                    outcome="exact_postgis_text_and_catalog_roundtrip",
                    mutation=True,
                )
            )
    return ReviewedSuite.create(suite_id, "dpone.postgres_mssql.postgis_types.v1", cases)


def _auto_boundaries(canonical: str, source_type: str) -> tuple[_AutoBoundary, ...]:
    normalized_source = source_type.strip().lower()
    shared: list[_AutoBoundary] = [_roundtrip("null"), _roundtrip("nominal")]
    if canonical == "integer":
        shared += [
            _roundtrip("source_minimum"),
            _roundtrip("source_maximum"),
            _source_reject("below_source_minimum"),
            _source_reject("above_source_maximum"),
        ]
    elif canonical == "decimal":
        shared += [
            _roundtrip("source_minimum_exact"),
            _roundtrip("source_maximum_exact"),
            _roundtrip("midpoint_exact"),
            _normalization_roundtrip("source_typmod_scale_normalization"),
            _source_reject("source_precision_overflow"),
            _route_reject("nan"),
            _source_reject("positive_infinity"),
            _source_reject("negative_infinity"),
        ]
    elif canonical == "float":
        binary32 = normalized_source in {"real", "float4"}
        shared += [
            _roundtrip("positive_zero"),
            # SQL Server character-to-float materialization normalizes the
            # IEEE sign bit of -0. Its binary64 parser also underflows the
            # smallest PostgreSQL float8 value, while binary32 subnormal bits
            # survive exactly in ``real`` (proved through varbinary(4)).
            _route_reject("negative_zero"),
            *(
                (
                    _roundtrip("minimum_positive_subnormal"),
                    _roundtrip("maximum_negative_subnormal"),
                )
                if binary32
                else (
                    _route_reject("minimum_positive_subnormal"),
                    _route_reject("maximum_negative_subnormal"),
                )
            ),
            _roundtrip("maximum_finite"),
            _roundtrip("minimum_finite"),
            _route_reject("nan"),
            _route_reject("positive_infinity"),
            _route_reject("negative_infinity"),
            _source_reject("source_overflow"),
        ]
    elif canonical == "boolean":
        shared += [_roundtrip("false"), _roundtrip("true")]
    elif canonical == "string":
        shared += [
            _roundtrip("empty"),
            _roundtrip("u0020"),
            _roundtrip("tab"),
            _roundtrip("lf"),
            _roundtrip("cr"),
            _roundtrip("backslash"),
            _roundtrip("codec_marker_prefix"),
            _roundtrip("unicode_bmp"),
            _roundtrip("unicode_nonbmp"),
        ]
        declared_length = _string_declared_length(normalized_source)
        if declared_length != 1:
            shared.append(_roundtrip("codec_marker_prefix_doubled"))
        if declared_length is not None:
            shared += [_roundtrip("max_declared_length"), _source_reject("declared_length_plus_one")]
        else:
            shared.append(_roundtrip("large_1mib"))
    elif canonical == "uuid":
        shared += [_roundtrip("all_zero"), _roundtrip("all_one")]
    elif canonical == "binary":
        shared += [
            _roundtrip("empty"),
            _roundtrip("nul"),
            _roundtrip("all_byte_values"),
            _roundtrip("large_1mib"),
            _source_reject("invalid_hex_constructor"),
        ]
    elif canonical == "date":
        shared += [
            _roundtrip("mssql_minimum"),
            _roundtrip("mssql_maximum"),
            _route_reject("before_mssql_minimum"),
            _route_reject("postgres_infinity"),
            _route_reject("postgres_negative_infinity"),
        ]
    elif canonical == "timestamp":
        shared += [
            _roundtrip("mssql_minimum_at_source_scale"),
            _roundtrip("mssql_maximum_at_source_scale"),
            _roundtrip("dst_overlap_local_time"),
            _roundtrip("pre_epoch"),
            _roundtrip("post_epoch"),
            _route_reject("before_mssql_minimum"),
            _route_reject("postgres_infinity"),
            _route_reject("postgres_negative_infinity"),
            _normalization_roundtrip("source_typmod_100ns_normalization"),
        ]
    elif canonical == "offset_timestamp":
        shared += [
            _roundtrip("mssql_minimum_at_source_scale"),
            _roundtrip("mssql_maximum_at_source_scale"),
            _roundtrip("dst_overlap_instant"),
            _roundtrip("offset_positive"),
            _roundtrip("offset_negative"),
            _route_reject("before_mssql_minimum"),
            _route_reject("postgres_infinity"),
            _route_reject("postgres_negative_infinity"),
            _normalization_roundtrip("source_typmod_100ns_normalization"),
        ]
    elif canonical == "time":
        shared += [
            _roundtrip("minimum"),
            _roundtrip("maximum_at_source_scale"),
            _route_reject("midnight_24h"),
            _normalization_roundtrip("source_typmod_fraction_normalization"),
        ]
    elif canonical == "json":
        shared += [
            _roundtrip("empty_object"),
            _roundtrip("nested_unicode_controls"),
            _roundtrip("large_document"),
            _roundtrip("json_null"),
        ]
    by_id = {boundary.value_case: boundary for boundary in shared}
    if len(by_id) != len(shared):  # pragma: no cover - reviewed authoring invariant.
        raise AssertionError(f"duplicate boundary id for {canonical}:{source_type}")
    return tuple(shared)


def _roundtrip(value_case: str) -> _AutoBoundary:
    return _AutoBoundary(
        value_case,
        "standard_etl_roundtrip",
        "exact_value_and_catalog_roundtrip",
        True,
    )


def _normalization_roundtrip(value_case: str) -> _AutoBoundary:
    return _AutoBoundary(
        value_case,
        "postgres_typmod_normalization_roundtrip",
        "exact_normalized_source_value_and_catalog_roundtrip",
        True,
    )


def _route_reject(value_case: str) -> _AutoBoundary:
    return _AutoBoundary(
        value_case,
        "standard_etl_value_guard",
        "typed_reject_before_business_dml",
        False,
    )


def _source_reject(value_case: str) -> _AutoBoundary:
    return _AutoBoundary(
        value_case,
        "postgres_source_domain_reject",
        "postgres_typed_value_rejected_before_etl",
        False,
    )


def _string_declared_length(source_type: str) -> int | None:
    if source_type in {"char", "character"}:
        return 1
    if "(" not in source_type:
        return None
    raw = source_type.rsplit("(", 1)[1].rstrip(")").strip()
    return int(raw)


def _explicit_boundaries(canonical: str, source_type: str) -> tuple[str, ...]:
    normalized_source = source_type.strip().lower()
    if canonical == "decimal" and normalized_source in {"numeric(38,-1)", "numeric(3,39)"}:
        # PostgreSQL rejects ±Infinity for finite precision NUMERIC typmods;
        # NaN remains representable and is certified independently.
        return ("null", "nominal", "finite_large", "nan")
    if canonical == "network":
        network_cases = {
            "inet": (
                "ipv4_host",
                "ipv4_network",
                "ipv6_host",
                "ipv6_network",
                "minimum_prefix",
                "maximum_prefix",
            ),
            "cidr": ("ipv4_network", "ipv6_network", "minimum_prefix", "maximum_prefix"),
            "macaddr": ("all_zero", "all_one", "multicast", "local_administered", "boundary"),
            "macaddr8": ("all_zero", "all_one", "multicast", "local_administered", "eui64"),
        }
        return ("null", "nominal", *network_cases[normalized_source])
    if canonical == "bit_string" and normalized_source in {"bit", "bit(8)"}:
        # Fixed-width BIT cannot represent the empty varying value.  Casting an
        # empty literal merely pads zeros and must not be certified as empty.
        return ("null", "nominal", "all_zero", "all_one", "declared_boundary")
    by_family = {
        "array": ("empty", "multidimensional_non_one_bounds", "null_element", "quoted_unicode"),
        "enum": ("escaped_unicode", "delimiter_text"),
        # The reviewed catalog representative is a constrained numeric domain;
        # text-domain boundaries would be a different physical source type.
        "domain": ("minimum", "maximum"),
        "composite": ("null_attribute", "quoted_delimiter", "escaped_unicode"),
        "range": ("empty", "unbounded", "inclusive_exclusive", "infinite_bound"),
        "range_builtin": ("empty", "unbounded", "inclusive_exclusive"),
        "multirange": ("empty", "disjoint", "unbounded"),
        "multirange_builtin": ("empty", "disjoint", "unbounded"),
        "bit_string": ("empty_varying", "all_zero", "all_one", "declared_boundary"),
        "network": ("ipv4", "ipv6", "network_prefix", "mac_boundary"),
        "interval": ("zero", "mixed_sign", "months_days_microseconds"),
        "time": ("positive_offset", "negative_offset", "maximum_microsecond"),
        "geometric": ("degenerate", "negative_coordinates", "maximum_finite"),
        "xml": ("empty_element", "unicode_attributes", "escaped_entities"),
        "jsonpath": ("quoted_unicode", "predicate_expression"),
        "decimal": ("finite_large", "nan", "positive_infinity", "negative_infinity"),
    }
    return ("null", "nominal", *by_family.get(canonical, ("boundary",)))


def _postgis_boundaries(name: str) -> tuple[str, ...]:
    if name in {"geometry", "postgis_catalog"}:
        return ("null", "point_srid_4326", "empty", "negative_boundary", "z_dimension")
    if name == "geography":
        return ("null", "point_srid_4326", "antimeridian", "polar_boundary", "empty")
    return ("null", "one_pixel", "empty", "nodata", "multi_band")


def _comparison(canonical: str, source_type: str = "") -> str:
    if canonical == "string" and source_type.strip().lower().split("(", 1)[0] in {"char", "character"}:
        return "postgres_bpchar_text_semantics"
    if canonical == "binary":
        return "exact_bytes"
    if canonical in {"float", "decimal"}:
        return "canonical_numeric_bits_or_decimal"
    if canonical in {"date", "time", "timestamp", "offset_timestamp"}:
        return "canonical_temporal_instant_and_scale"
    if canonical == "json":
        return "postgres_textual_json_semantics"
    return "exact_typed_value"


def _names(values: Iterable[ReviewedCase]) -> tuple[str, ...]:
    """Test-only helper retained to make registry audits readable."""

    return tuple(value.case_id for value in values)


__all__ = ["boundary_type_suite", "explicit_type_suite", "postgis_type_suite"]
