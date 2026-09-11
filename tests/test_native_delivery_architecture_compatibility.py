"""Reflection and canonical identities survive compatible delivery refactoring."""

import inspect
import pickle
from typing import get_type_hints

from dpone.contracts.mssql_native_chunks import EncodedNativeFile
from dpone.contracts.postgres_source_authority import PostgresSourceAuthority, SelectedPostgresSourceAuthority
from dpone.contracts.runtime_connection import ResolvedBindingConnection
from dpone.ports.native_delivery_observer import NativeDeliveryObserver
from dpone.runtime import mssql_native_chunks, native_delivery_observations
from dpone.runtime.sinks import mssql_native_prepared_insert, mssql_native_switch
from dpone.runtime.sinks.mssql_native_switch.catalog import NativeSwitchCatalog
from dpone.runtime.sinks.mssql_native_switch.executor import execute_native_switch
from dpone.runtime.sinks.mssql_native_switch.planner import plan_native_switch
from dpone.runtime.sinks.strategies.mssql.mssql_native_lineage import MssqlNativeLineageProjection
from dpone.runtime.sinks.strategies.mssql.mssql_native_schema import ResolvedMssqlNativeSchema
from dpone.runtime.sources import postgres_source_authority


def test_postgres_authority_keeps_raw_connection_annotation_and_canonical_resolution():
    verifier = postgres_source_authority.PostgresSourceAuthorityVerifier
    constructor = verifier.from_connection
    assert inspect.get_annotations(constructor, eval_str=False) == {
        "connection": "ResolvedBindingConnection",
        "return": "PostgresSourceAuthorityVerifier",
    }
    namespace = {**vars(postgres_source_authority), "ResolvedBindingConnection": ResolvedBindingConnection}
    assert get_type_hints(constructor, globalns=namespace) == {
        "connection": ResolvedBindingConnection,
        "return": verifier,
    }
    fields = get_type_hints(verifier)
    assert fields["authority"] is PostgresSourceAuthority
    assert fields["_selected_by_route"] == dict[tuple[str, str], SelectedPostgresSourceAuthority]


def test_recorder_keeps_raw_annotations_and_canonical_namespace_resolution():
    constructor = native_delivery_observations.NativeDeliveryRecorder.__init__
    assert inspect.get_annotations(constructor, eval_str=False) == {
        "observer": "NativeDeliveryObserver | None",
        "clock": "Callable[[], int]",
        "clock_domain": "str",
        "process_id": "int",
        "worker_id": "str",
        "diagnostics": "Callable[[dict[str, Any]], None] | None",
        "return": "None",
    }
    namespace = {**vars(native_delivery_observations), "NativeDeliveryObserver": NativeDeliveryObserver}
    resolved = get_type_hints(constructor, globalns=namespace)
    assert resolved["observer"] == NativeDeliveryObserver | None
    signature = inspect.signature(constructor)
    assert signature.parameters["observer"].default is None
    assert signature.parameters["clock_domain"].kind is inspect.Parameter.KEYWORD_ONLY


def test_prepared_insert_keeps_raw_annotations_and_canonical_namespace_resolution():
    builder = mssql_native_prepared_insert.build_prepared_insert
    assert inspect.get_annotations(builder, eval_str=False) == {
        "target_sql": "str",
        "source_sql": "str",
        "business_schema": "Sequence[tuple[str, str]]",
        "resolved": "ResolvedMssqlNativeSchema",
        "lineage": "MssqlNativeLineageProjection",
        "quote_identifier": "Callable[[str], str]",
        "return": "str",
    }
    namespace = {
        **vars(mssql_native_prepared_insert),
        "ResolvedMssqlNativeSchema": ResolvedMssqlNativeSchema,
        "MssqlNativeLineageProjection": MssqlNativeLineageProjection,
    }
    resolved = get_type_hints(builder, globalns=namespace)
    assert resolved["resolved"] is ResolvedMssqlNativeSchema
    assert resolved["lineage"] is MssqlNativeLineageProjection
    assert all(p.kind is inspect.Parameter.KEYWORD_ONLY for p in inspect.signature(builder).parameters.values())


def test_runtime_dataclass_fields_keep_ordinary_reflection():
    fields = get_type_hints(mssql_native_chunks._Work)
    assert fields["file"] == EncodedNativeFile | None


def test_switch_supported_exports_keep_canonical_and_pickle_identity():
    expected = {
        "NativeSwitchCatalog": NativeSwitchCatalog,
        "execute_native_switch": execute_native_switch,
        "plan_native_switch": plan_native_switch,
    }
    assert set(mssql_native_switch.__all__) == expected.keys()
    for name, canonical in expected.items():
        exported = getattr(mssql_native_switch, name)
        assert exported is canonical
        assert pickle.loads(pickle.dumps(exported)) is canonical
