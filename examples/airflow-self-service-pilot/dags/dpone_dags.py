"""Load declarative dpone DAGs from the bounded deployment cache."""

from airflow.providers.dpone import load_and_acknowledge_dpone_dags

try:
    from platform_semantic_refresh_runtime import application as semantic_refresh_application
except ModuleNotFoundError as exc:
    if exc.name != "platform_semantic_refresh_runtime":
        raise
    semantic_refresh_application = None

if semantic_refresh_application is None:
    loaded = load_and_acknowledge_dpone_dags(
        globals(),
        index_path="/opt/airflow/.dpone-cache/current/airflow-index.json",
        ack_path="/opt/airflow/.dpone-ack/loader-ack.json",
        ack_root="/opt/airflow/.dpone-ack",
    )
else:
    loaded = semantic_refresh_application.load_dags(
        globals(),
        index_path="/opt/airflow/.dpone-cache/current/airflow-index.json",
        ack_path="/opt/airflow/.dpone-ack/loader-ack.json",
        ack_root="/opt/airflow/.dpone-ack",
    )
if loaded.report.fatal:
    error_code = (
        loaded.report.errors[0].get("code", "DPONE_AIRFLOW_INDEX_INVALID")
        if loaded.report.errors
        else "DPONE_AIRFLOW_INDEX_INVALID"
    )
    raise RuntimeError(f"{error_code}: dpone Airflow deployment index could not be loaded")
