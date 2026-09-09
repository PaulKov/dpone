# CBR rollout bundle

Этот каталог содержит конкретный rollout package для CBR в Airflow/Argo.

## Состав

- `values.base.yaml` — общие значения/команды/volume mounts
- `values.dev.yaml` — dev overlay c `DPONE_PACKAGE_SPEC` (snapshot install)
- `values.prod.yaml` — prod overlay c baked package
- `runtime.dev.env.example` / `runtime.prod.env.example` — env examples для pod runtime
- `live-smoke.dev.env.example` / `live-smoke.prod.env.example` — env examples для public smoke

## Канонический manifest

`/opt/airflow/dags/repo/examples/batch/landing_cbr_api.batch.yaml`

## Канонический schedule

`10 7 * * *`

## Канонические selectors

- `app.xml_daily_asp`

## Как использовать

1. Берёте `values.base.yaml` + `values.dev.yaml` или `values.prod.yaml`.
2. Настраиваете Secret для `DPONE_PACKAGE_INDEX_URL` (dev) и доступ до BigQuery/Vault sink credentials.
3. Применяете rollout.
4. Прогоняете public smoke по `deploy/argo/cbr/live-smoke.*.env.example`.
5. Дальше включаете schedule / снимаете pause.

Подробный чек-лист: `docs/CBR_ROLLOUT.md`.
