# DataOps checklist: bump dpone runtime 0.73.32

Чеклист для платформенной команды перед/после pilot MR в `example-workloads`.

## 1. Образ scheduler/worker

| Item | Значение |
| --- | --- |
| Image | `ghcr.io/paulkov/dpone-runtime:0.73.32` |
| Verify | `docker run --rm ghcr.io/paulkov/dpone-runtime:0.73.32 --version` |
| Native accel | `docker run --rm ghcr.io/paulkov/dpone-runtime:0.73.32 runtime native-accel doctor --format json` |

Обновите Helm values / manifest там, где задаётся dpone runtime image для Airflow
(scheduler, worker, triggerer — по вашей topology).

## 2. CI runner (pack build)

Убедитесь, что job сборки GitOps pack использует:

```bash
uv pip install "dpone[full]==0.73.32" "apache-airflow-providers-dpone==0.73.32"
```

Или pin в `uv.lock` / constraints файле CI.

## 3. Cache promotion (production path)

Promotion только через идентифицируемый CI actor:

```bash
dpone airflow cache-sync \
  --allowed-promoter <platform-ci-identity> \
  --promoted-by <platform-ci-identity> \
  ... # см. docs/airflow-cache-sync.md
```

Recovery/retention — только с reviewed plan + allowlist.

## 4. Smoke после deploy

| Check | Command / signal |
| --- | --- |
| Release verify | `uvx --from dpone==0.73.32 dpone ops release-verify --release v0.73.32` |
| Index readable | Airflow loader parse без `dpone_spec_error` quarantine DAGs |
| Deployment index | `current/airflow-index.json` fingerprints match release-set |
| One workload | manual trigger marketing pilot DAG в dev |

## 5. Rollback

1. Helm/image → предыдущий tag (`0.72.1` или last known good).
2. Cache pointer → предыдущий deployment (`cache-recovery-plan` / manual CAS).
3. Legacy Python DAG — re-enable если cutover откатили.

## Evidence

- Release artifact: `test_artifacts/release/v0.73.32/release_verify.json` в dpone repo
- GitHub Release: https://github.com/PaulKov/dpone/releases/tag/v0.73.32
