from __future__ import annotations

import importlib


def test_lazy_facades_preserve_public_imports() -> None:
    from dpone.connector_sdk import ConnectorSdkScaffoldService
    from dpone.manifest import ManifestLoader
    from dpone.observability import RuntimeMetricsExportService
    from dpone.ops import OpsServiceCatalog
    from dpone.runtime.cdc import CDCReplayPlanner
    from dpone.runtime.sinks.staging import BigQueryStagingManager, PostgresStagingManager
    from dpone.storage import ObjectStorageProvider
    from dpone.supply_chain import SupplyChainAttestationService
    from dpone.type_system import TypeInferenceOptions

    assert ManifestLoader is not None
    assert OpsServiceCatalog is not None
    assert TypeInferenceOptions is not None
    assert RuntimeMetricsExportService is not None
    assert SupplyChainAttestationService is not None
    assert ConnectorSdkScaffoldService is not None
    assert CDCReplayPlanner is not None
    assert ObjectStorageProvider is not None
    assert BigQueryStagingManager is not None
    assert PostgresStagingManager is not None


def test_etl_logging_facade_keeps_singleton_object_after_submodule_import() -> None:
    import dpone.runtime.etl_logging as package

    importlib.import_module("dpone.runtime.etl_logging.etl_logger")

    from dpone.runtime.etl_logging import etl_logger

    assert hasattr(etl_logger, "info")
    assert package.etl_logger is etl_logger
