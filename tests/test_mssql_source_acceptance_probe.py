"""MSSQL table sources must answer required acceptance metrics."""

from dpone.runtime.sources.mssql import MSSQLSource


def test_mssql_source_exposes_an_acceptance_metric_probe():
    source = MSSQLSource(connector=object(), logger=None)

    assert callable(source.acceptance_metric_probe.collect)
