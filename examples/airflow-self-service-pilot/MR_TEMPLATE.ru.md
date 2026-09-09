# MR template: pilot declarative Airflow (marketing)

Скопируйте в описание MR в `example-workloads` после переноса layout из
`examples/airflow-self-service-pilot/`.

## Summary

- Миграция домена `marketing/sample_web_sync` на declarative self-service dpone **0.73.32**.
- Один loader `dags/dpone_dags.py` вместо рукописного `DAG__marketing__sample_web_sync__sync.py`.
- GitOps: domain catalog `dags:` + manifest + CI pack/dag-spec.

## Изменения

| Область | Действие |
| --- | --- |
| `dags/dpone_dags.py` | добавить loader (`load_dpone_dags`) |
| `dpone_workloads/gitops/` | domain catalog + workload-set root |
| `workloads/marketing/dpone/manifests/` | manifest pipeline |
| Legacy `DAG__marketing__sample_web_sync__sync.py` | удалить после cutover в dev |

## Версии (обязательно)

- PyPI: `dpone[full]==0.73.32`, `apache-airflow-providers-dpone==0.73.32`
- Runtime image: `ghcr.io/paulkov/dpone-runtime:0.73.32` (см. DATAOPS checklist)

## Pre-merge checklist (author)

- [ ] `dpone workload init marketing/sample_web_sync --apply` (или wizard) — plan reviewed
- [ ] `dpone gitops airflow pack --workload-set dpone_workloads/gitops/gitops.yaml --mode plan` — PASS
- [ ] `dpone gitops airflow deps --workload-set dpone_workloads/gitops/gitops.yaml --format md` — reviewed
- [ ] CI собирает pack + dag-spec artifacts для workload-set
- [ ] Нет секретов/credentials в diff

## Post-merge checklist (DataOps)

- [ ] CI promotion: `dpone airflow cache-sync` с `--allowed-promoter` / `--promoted-by`
- [ ] Airflow scheduler parse: новый DAG visible, legacy DAG paused/removed
- [ ] `dpone dag explain-edge --dag-spec --dag-id DAG__marketing__sample_web_sync__sync` при необходимости
- [ ] Rollback plan: restore cache `current` pointer на предыдущий deployment

## Test plan

1. Dev: дождаться parse loader-а, проверить DAG в UI (paused/unpaused по политике).
2. Trigger manual run одного interval — outcome gate / evidence без false success.
3. Stage: повторить после promotion того же release-set.

## Rollback

- Вернуть legacy Python DAG (loader пропускает `dag_id` уже в `globals()` — можно временно coexist).
- `dpone airflow cache-recovery-plan` → review → `cache-recovery-apply` при повреждённом cache.

## Links

- dpone release: https://github.com/PaulKov/dpone/releases/tag/v0.73.32
- Golden path: https://paulkov.github.io/dpone/airflow-self-service/
- Runbook: `examples/airflow-self-service-pilot/RUNBOOK.ru.md`
