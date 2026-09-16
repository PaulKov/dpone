"""Historical registration imports, pickle identities and finite wire projection."""

import hashlib
import importlib
import inspect
import pickle
import subprocess
import sys
from dataclasses import dataclass, fields
from types import FunctionType

import pytest

from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_registration_codec import encode_physical_runtime_registration
from dpone.contracts.dbt_mssql_physical_registration_values import RegisteredLimits
from tests.support.dbt_mssql_physical_registration import registration_inputs

OWNER = "dpone.contracts.dbt_mssql_physical_registration"
HISTORICAL_OBJECTS = {
    OWNER + "_values": (
        "PhysicalRegistrationError",
        "PlatformSelection",
        "ProgramAuthority",
        "RegisteredLimits",
        "DatabasePrincipal",
        "DatabaseRoleMapping",
        "DedicatedObserver",
        "SharedObserver",
        "RegisteredPrincipals",
        "reference_payload",
        "platform_subject_payload",
        "require_registration_digest",
    ),
    OWNER + "_codec": (
        "encode_physical_runtime_registration",
        "decode_physical_runtime_registration",
        "physical_runtime_registration_digest",
    ),
}


@pytest.mark.parametrize("module,names", HISTORICAL_OBJECTS.items())
def test_historical_public_objects_retain_module_and_pickle_identity(module, names):
    historical = importlib.import_module(module)
    for name in names:
        value = getattr(historical, name)
        assert value.__module__ == module
        assert pickle.loads(pickle.dumps(value)) is value
        historical_pickle = f"c{module}\n{name}\n.".encode("ascii")
        assert pickle.loads(historical_pickle) is value
        if isinstance(value, type):
            for method in vars(value).values():
                if isinstance(method, FunctionType) and method.__qualname__.startswith(value.__qualname__ + "."):
                    assert method.__module__ == module
                    assert pickle.loads(pickle.dumps(method)) is method


def test_dataclass_support_functions_keep_their_own_identity():
    assert RegisteredLimits.__getstate__.__module__ == "dataclasses"
    assert RegisteredLimits.__setstate__.__module__ == "dataclasses"


@pytest.mark.parametrize("module,names", HISTORICAL_OBJECTS.items())
def test_canonical_and_historical_exports_are_identical(module, names):
    canonical = importlib.import_module(OWNER)
    historical = importlib.import_module(module)
    for name in names:
        assert getattr(canonical, name) is getattr(historical, name)


@pytest.mark.parametrize("first", [OWNER, *HISTORICAL_OBJECTS])
def test_import_order_and_pickle_in_a_fresh_process(first):
    script = f"""
import importlib
import pickle
import typing
importlib.import_module({first!r})
owner = importlib.import_module({OWNER!r})
assert typing.get_type_hints(owner.PlatformSelection)["reference"] is owner.OriginalRef
assert typing.get_type_hints(owner.MssqlPhysicalRuntimeRegistration)["limits"] is owner.RegisteredLimits
for module, names in {HISTORICAL_OBJECTS!r}.items():
    historical = importlib.import_module(module)
    for name in names:
        value = getattr(historical, name)
        assert getattr(owner, name) is value
        assert value.__module__ == module
        assert pickle.loads(pickle.dumps(value)) is value
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr


def test_registration_instance_pickle_and_golden_canonical_bytes():
    registration = MssqlPhysicalRuntimeRegistration(**registration_inputs())
    restored = pickle.loads(pickle.dumps(registration))
    assert type(restored) is MssqlPhysicalRuntimeRegistration
    assert restored == registration
    assert type(restored.limits) is RegisteredLimits
    assert encode_physical_runtime_registration(restored) == encode_physical_runtime_registration(registration)
    assert hashlib.sha256(encode_physical_runtime_registration(restored)).hexdigest() == (
        "a01bc11d3c97354b18e818a81e593d19e1dda6fb8a1dcf54827d31a5acd20c57"
    )


def test_historical_eager_and_postponed_annotations_remain_distinct():
    values = importlib.import_module(OWNER + "_values")
    codec = importlib.import_module(OWNER + "_codec")
    assert RegisteredLimits.__annotations__["max_columns"] == "int"
    assert inspect.signature(RegisteredLimits).parameters["max_columns"].annotation == "int"
    assert values.reference_payload.__annotations__["value"] == "OriginalRef"
    assert inspect.signature(RegisteredLimits.to_dict).return_annotation == "dict[str, NativeJsonValue]"
    assert MssqlPhysicalRuntimeRegistration.__annotations__["limits"] is RegisteredLimits
    assert inspect.signature(MssqlPhysicalRuntimeRegistration).parameters["limits"].annotation is RegisteredLimits
    assert codec.encode_physical_runtime_registration.__annotations__["return"] is bytes
    assert codec.decode_physical_runtime_registration.__annotations__["return"] is MssqlPhysicalRuntimeRegistration


def test_limits_projection_is_detached_ordered_and_excludes_subclass_fields():
    @dataclass(frozen=True, slots=True)
    class ExtendedLimits(RegisteredLimits):
        extra: str = "not part of the registration wire schema"

    original = registration_inputs()["limits"]
    values = {field.name: getattr(original, field.name) for field in fields(RegisteredLimits)}
    limits = ExtendedLimits(**values)
    projection = limits.to_dict()
    assert list(projection) == [
        "max_metadata_bytes",
        "max_generation_bytes",
        "max_catalog_rows",
        "max_definition_utf16_bytes",
        "max_dependency_rows",
        "max_columns",
    ]
    assert projection == values
    projection["max_columns"] = 1
    assert limits.max_columns == 256
    assert "extra" not in limits.to_dict()
