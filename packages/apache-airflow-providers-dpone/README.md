# apache-airflow-providers-dpone

The formal Apache Airflow provider facade for dpone self-service DAGs.

## Install

Use a supported Airflow matrix cell. This example uses Python 3.12,
Airflow 3.2.0, and the CNCF Kubernetes provider 10.14.0. Use Apache Airflow's
official constraints for the Airflow install only, then pin Airflow again while
adding this provider:

```bash
AIRFLOW_VERSION=3.2.0
PYTHON_VERSION=3.12
DPONE_VERSION=X.Y.Z
AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

python3 -m pip install \
  "apache-airflow[cncf.kubernetes]==${AIRFLOW_VERSION}" \
  --constraint "${AIRFLOW_CONSTRAINTS_URL}"
python3 -m pip install \
  "apache-airflow==${AIRFLOW_VERSION}" \
  "apache-airflow-providers-cncf-kubernetes==10.14.0" \
  "apache-airflow-providers-dpone==${DPONE_VERSION}"
python3 -m pip check
python3 -c "from airflow.providers.dpone import load_dpone_dags; print(load_dpone_dags.__module__)"
```

The provider installs the same-version `dpone-airflow-pack`; it does not require the
full dpone runtime or connector drivers in the scheduler image.

Use only a version already published on PyPI. For a release candidate, install
the exact provider and pack wheels built from the same reviewed commit instead
of using a not-yet-existing tag or floating branch.

## Load DAGs

```python
from airflow.providers.dpone import load_and_acknowledge_dpone_dags

index_path = "/opt/airflow/.dpone-cache/current/airflow-index.json"
loaded = load_and_acknowledge_dpone_dags(
    globals(),
    index_path=index_path,
    ack_path="/opt/airflow/.dpone-ack/loader-ack.json",
    ack_root="/opt/airflow/.dpone-ack",
)
if loaded.report.fatal:
    error_code = (
        loaded.report.errors[0].get("code", "DPONE_AIRFLOW_INDEX_INVALID")
        if loaded.report.errors
        else "DPONE_AIRFLOW_INDEX_INVALID"
    )
    raise RuntimeError(
        f"{error_code}: dpone Airflow deployment index could not be loaded"
    )
```

`load_and_acknowledge_dpone_dags(...)` holds one shared cache lease across the
parse and acknowledgement. It performs bounded local filesystem I/O only and
atomically binds the completed report to the exact deployment and index digest.
Mount `/opt/airflow/.dpone-cache` read-only in the parser container and provide
the separate `/opt/airflow/.dpone-ack` volume as its only writable evidence
channel. The older cache-local `status/` destination remains supported when
`ack_root` is omitted, but it requires a writable cache mount and is not the
recommended Kubernetes layout.

The distribution owns Airflow provider discovery and the typed canonical
namespace. The dependency-light `dpone-airflow-pack` package owns static pack
reading and DAG construction. Importing the provider performs no remote cache,
Airflow metadata, Connection, Variable, Vault, Kubernetes, or database I/O.

The deployment index read is bounded to 8 MiB and each listed artifact to
64 MiB by default. An index-level error returns `fatal=True`; do not represent
that as a successful empty parse. After a trusted index is loaded, DAG spec and
workload-pack size/SHA-256 failures are isolated to the affected DAG under the
default `skip_and_report` policy. Recover by rematerializing the immutable
release/deployment from trusted storage, never by editing cache artifacts in
place.

`DponeDag.from_spec(...)` and `DponeTaskGroup.from_pack(...)` are typed escape
hatches for hybrid DAGs. The recommended production path is
`load_and_acknowledge_dpone_dags(...)`; `load_dpone_dags(...)` remains the
parse-only lower-level API.

## Operations

Strict runtime tasks set `get_logs=true` and
`on_finish_action=delete_succeeded_pod` so Airflow streams `base` container
stdout after pod start. Structured XCom/runtime evidence remains the task
outcome authority; a cluster log collector is still required for post-delete
forensics. The full dpone distribution ships the runtime Pod
retention control; the parse-safe provider facade does not execute it during DAG
import. Before production, deploy and certify its label-scoped
plan/apply CronJob through the
[runtime pod retention runbook](https://paulkov.github.io/dpone/airflow-runtime-pod-retention/),
and deploy the exact cache through the
[Kubernetes cache runbook](https://paulkov.github.io/dpone/airflow-cache-kubernetes-deployment/).
The stock JSONL publisher reports `process_ordered`, not crash durability;
production apply requires a certified publisher/sink that reports
`durable_acknowledged` and retains the operation, intent, outcome and completion
chain.
