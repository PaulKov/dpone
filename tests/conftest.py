import importlib.util
import os
import sys
from pathlib import Path

import pytest


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

# When uv installs the editable wheel (CI default), importing from src/ and from
# site-packages duplicates modules and breaks isinstance/Enum identity under xdist.
# Set DPONE_TEST_USE_INSTALLED_PACKAGE=1 in CI (see .github/workflows/ci.yml).

if not _truthy(os.getenv("DPONE_TEST_USE_INSTALLED_PACKAGE")) and str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# The Airflow compatibility matrix intentionally installs only the dependency-
# light scheduler packages and proves that the heavy ``dpone`` runtime is not
# importable. Route-live recording is a runtime-only pytest capability, so do
# not load its plugin in that scheduler environment.
pytest_plugins = (
    ("tools.route_live_certification.pytest_plugin",) if importlib.util.find_spec("dpone") is not None else ()
)


@pytest.fixture
def replayable_doctor_import_surface(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep unit probes independent from pytest's runtime import-rewrite hook."""

    import dpone.readiness.python_import_health as import_health

    monkeypatch.setattr(import_health, "startup_import_surface_replayable", lambda: True)


@pytest.fixture(scope="session")
def clean_doctor_probe_python(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Provide one same-version interpreter without executable site hooks."""

    from tests.doctor_import_test_support import _probe_clean_venv_python

    return _probe_clean_venv_python(tmp_path_factory.mktemp("doctor-clean-python"))


try:
    import psycopg  # noqa: F401
except ModuleNotFoundError as exc:  # pragma: no cover - lightweight test env fallback
    if exc.name != "psycopg":
        raise
    import importlib.machinery
    import types

    def _fake_module(name: str, *, is_package: bool = False) -> types.ModuleType:
        module = types.ModuleType(name)
        module.__spec__ = importlib.machinery.ModuleSpec(name, loader=None, is_package=is_package)
        module.__package__ = name if is_package else name.rpartition(".")[0]
        if is_package:
            module.__path__ = []
        return module

    class _FakeSqlObject:
        def __init__(self, value: object = ""):
            self.value = value

        def format(self, *args, **kwargs):
            rendered = str(self)
            for value in args:
                rendered = rendered.replace("{}", str(value), 1)
            for key, value in kwargs.items():
                rendered = rendered.replace("{" + str(key) + "}", str(value))
            return _FakeSqlObject(rendered)

        def join(self, seq):
            return _FakeSqlObject(str(self).join(str(item) for item in seq))

        def __add__(self, other):
            return _FakeSqlObject(str(self) + str(other))

        def __str__(self):
            if isinstance(self.value, list):
                return "".join(str(item) for item in self.value)
            return str(self.value)

        def __repr__(self):
            return f"FakeSql({self.value!r})"

    fake_sql = _fake_module("psycopg.sql")
    setattr(fake_sql, "SQL", lambda value="": _FakeSqlObject(value))
    setattr(fake_sql, "Identifier", lambda value: _FakeSqlObject(str(value)))
    setattr(fake_sql, "Literal", lambda value: _FakeSqlObject(str(value)))
    setattr(fake_sql, "Composable", _FakeSqlObject)
    setattr(fake_sql, "Composed", _FakeSqlObject)
    fake_psycopg = _fake_module("psycopg", is_package=True)
    setattr(fake_psycopg, "sql", fake_sql)
    fake_rows = _fake_module("psycopg.rows")
    setattr(fake_rows, "dict_row", object())
    sys.modules.setdefault("psycopg", fake_psycopg)
    sys.modules.setdefault("psycopg.sql", fake_sql)
    sys.modules.setdefault("psycopg.rows", fake_rows)
