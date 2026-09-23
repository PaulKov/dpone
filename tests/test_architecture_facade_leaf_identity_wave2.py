import importlib
import pickle

SURFACES = (
    (
        "dpone.runtime.connectors.api.parallel",
        "dpone.runtime.connectors.api.parallel_execution",
        ("TaskResult", "ParallelTaskExecutor", "BoundedParallelStreamExecutor"),
    ),
    (
        "dpone.orchestration.run",
        "dpone.orchestration.run_impl",
        ("OrchestratedRunRequest", "OrchestratedRunReport", "OrchestratedRunService"),
    ),
    (
        "dpone.services.airflow_connection_secret_gc_policy",
        "dpone.services.airflow_connection_secret_gc_policy_impl",
        ("AirflowConnectionSecretGcCandidate", "AirflowConnectionSecretGcClassification"),
    ),
    (
        "dpone.ops.routes.certify_release_finalizer_policy",
        "dpone.ops.routes.certify_release_finalizer_policy_impl",
        ("RouteCertificationReleaseFinalizerDecision", "RouteCertificationReleaseFinalizerPolicy"),
    ),
    (
        "dpone.ports.bounded_window",
        "dpone.ports.bounded_window_contracts",
        (
            "WindowSource",
            "WindowTarget",
            "WindowStore",
            "WindowProgressJournal",
            "WindowExecutor",
            "ExclusiveWindowWriterGuard",
            "WindowBinaryIngest",
            "WindowMetadataStore",
        ),
    ),
)


def test_facades_preserve_class_identity_and_legacy_pickle_globals() -> None:
    for facade_name, leaf_name, names in SURFACES:
        facade = importlib.import_module(facade_name)
        leaf = importlib.import_module(leaf_name)
        for name in names:
            value = getattr(leaf, name)
            assert getattr(facade, name) is value
            assert pickle.loads(f"c{facade_name}\n{name}\n.".encode()) is value


def test_function_facades_preserve_identity() -> None:
    pairs = (
        (
            "dpone.services.airflow_connection_secret_gc_policy",
            "dpone.services.airflow_connection_secret_gc_policy_impl",
            ("classify_airflow_connection_secret_inventory", "iso_utc", "require_aware_utc"),
        ),
        ("dpone.metrics.import_rule_catalog", "dpone.metrics.import_rule_catalog_impl", ("default_import_rules",)),
    )
    for facade_name, leaf_name, names in pairs:
        facade = importlib.import_module(facade_name)
        leaf = importlib.import_module(leaf_name)
        for name in names:
            assert getattr(facade, name) is getattr(leaf, name)
