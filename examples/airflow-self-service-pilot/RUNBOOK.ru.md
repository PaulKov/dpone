# Пилот: декларативный Airflow для marketing

Краткий runbook для миграции домена `marketing` с рукописного DAG на
declarative self-service.

**Candidate:** `dpone==0.73.2`,
`apache-airflow-providers-dpone==0.73.2`, runtime image
`ghcr.io/paulkov/dpone-runtime:0.73.2`.

## Цель

- заменить ~50 строк Python DAG одним блоком `dags:` в domain YAML;
- оставить один loader-файл `dags/dpone_dags.py`;
- сохранить GitOps-контракт pack + dag-spec + cache.

## Предусловия (platform)

- CI runner / локальная authoring-среда с `uv` и Python 3.11–3.12.
- Для authoring, plan и статических проверок установите базовый `dpone` без
  connector extras:

```bash
uv tool install "dpone==0.73.2"
# или в venv проекта DAG-ов:
uv venv --python 3.12 .venv
. .venv/bin/activate
uv pip install "dpone==0.73.2"
dpone --version
```

- Scheduler/DAG-processor image использует проверенную matrix-ячейку:
  Python 3.12, Airflow 3.2.0,
  `apache-airflow-providers-cncf-kubernetes==10.14.0`. Сначала установите
  Airflow с официальным constraints-файлом, затем formal dpone provider,
  повторно зафиксировав Airflow:

```bash
AIRFLOW_VERSION=3.2.0
PYTHON_VERSION=3.12
AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

python -m pip install \
  "apache-airflow[cncf.kubernetes]==${AIRFLOW_VERSION}" \
  --constraint "${AIRFLOW_CONSTRAINTS_URL}"
python -m pip install \
  "apache-airflow==${AIRFLOW_VERSION}" \
  "apache-airflow-providers-cncf-kubernetes==10.14.0" \
  "apache-airflow-providers-dpone==0.73.2"
python -m pip check
python -c "from airflow.providers.dpone import load_dpone_dags; print(load_dpone_dags.__module__)"
```

- KPO runtime image — отдельно от scheduler image:
  `ghcr.io/paulkov/dpone-runtime:0.73.2`. Не устанавливайте `dpone[full]` и
  database drivers в scheduler image.

## Шаги

1. Скопируйте `examples/airflow-self-service-pilot/` в репозиторий DAG-ов.
2. Создайте workload через plan-first init (или wizard на TTY):

```bash
dpone workload init marketing/sample_web_sync \
  --source clickhouse --sink mssql --strategy full_refresh \
  --layout batch
# либо: dpone workload init marketing/sample_web_sync --wizard
```

3. Проверьте diff, затем `--apply`.
4. Локально соберите plan:

```bash
dpone gitops airflow pack \
  --workload-set dpone_workloads/gitops/gitops.yaml \
  --mode plan
```

5. Проверьте граф зависимостей:

```bash
dpone gitops airflow deps \
  --workload-set dpone_workloads/gitops/gitops.yaml \
  --format md
```

6. Откройте MR в `example-workloads`. После merge CI публикует release/deployment;
   выполните первый promotion с allowlist и явным CAS-guard:

   Этот dev promotion является parse canary, пока для точного candidate image
   не настроен attestation verifier и не сохранено live Kubernetes evidence.
   Не запускайте production task из такой проекции. Для исполняемого pilot
   используйте явно собранный `non_production` deployment в dev.

```bash
: "${DPONE_SCHEDULER_CACHE_ROOT:=/opt/airflow/.dpone-cache}"
: "${DPONE_DEPLOYMENT_DIR:?укажите проверенное имя каталога sha256-...}"

dpone airflow cache-sync \
  --cache-root "${DPONE_SCHEDULER_CACHE_ROOT}" \
  --deployment-dir "${DPONE_SCHEDULER_CACHE_ROOT}/deployments/dev/${DPONE_DEPLOYMENT_DIR}" \
  --environment dev \
  --promoted-by ci://example-workloads/dpone \
  --allowed-promoter ci://example-workloads/dpone \
  --expect-current-absent \
  --confirm-promote
```

   Для повторного promotion сначала выполните
   `dpone airflow cache-recovery-plan --environment dev --format json` и
   замените `--expect-current-absent` на
   `--expected-current-deployment-id "${DPONE_CURRENT_DEPLOYMENT_ID}"`, где
   переменная содержит проверенный canonical SHA-256 из свежего плана.

7. Удалите legacy `DAG__marketing__sample_web_sync__sync.py` после успешного parse
   loader-а в dev/stage.

## Откат

- loader пропускает `dag_id`, уже присутствующие в `globals()` — legacy DAG
  можно оставить до cutover;
- не редактируйте `current`, `airflow-index.json` или immutable artifacts
  вручную. Сначала получите read-only план:

```bash
dpone airflow cache-recovery-plan \
  --cache-root /opt/airflow/.dpone-cache \
  --environment dev \
  --format json
```

- применяйте только сгенерированное планом действие с проверенным
  `deployment_id`, actor allowlist и свежим CAS-guard.

## Диагностика

- `dpone dag explain-edge --dag-spec --dag-id DAG__marketing__sample_web_sync__sync`
- если generated loader сообщает
  `<DPONE_AIRFLOW_INDEX_*>: dpone Airflow deployment index could not be loaded`,
  это fatal index failure: DAG-и из этого index не считаются успешно
  загруженными. Запустите `cache-recovery-plan`; не отключайте fatal guard;
- ошибка отдельного DAG/pack с `fatal: false` изолирована, но повреждённый
  immutable artifact нужно rematerialize из trusted release;
- error catalog: `docs/errors/DPONE_WORKLOAD_INIT_*.md`
