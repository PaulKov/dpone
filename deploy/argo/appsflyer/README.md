# AppsFlyer rollout bundle

Этот каталог содержит **конкретный rollout package** для AppsFlyer в Airflow/Argo.

## Состав

- `values.base.yaml` — общие значения/команды/volume mounts
- `values.dev.yaml` — dev overlay c `DPONE_PACKAGE_SPEC` (snapshot install)
- `values.prod.yaml` — prod overlay c baked package
- `runtime.dev.env.example` / `runtime.prod.env.example` — env examples для pod runtime
- `live-smoke.dev.env.example` / `live-smoke.prod.env.example` — env examples для manual smoke

## Канонический manifest

`/opt/airflow/dags/repo/examples/batch/landing_appsflyer_api.batch.yaml`

## Канонический schedule

`1 4 * * *`

## Канонические selectors

- `app.installs_report`
- `app.in_app_events_report`
- `app.uninstall_events_report`
- `app.installs_retarget`
- `app.in_app_events_retarget`
- `app.organic_installs_report`
- `app.organic_in_app_events_report`
- `app.organic_uninstall_events_report`

## Как использовать

1. Берёте `values.base.yaml` + `values.dev.yaml` или `values.prod.yaml`.
2. Настраиваете Secret для `DPONE_PACKAGE_INDEX_URL` (dev) и Vault access.
3. Применяете rollout.
4. Прогоняете live smoke по `deploy/argo/appsflyer/live-smoke.*.env.example`.
5. Дальше включаете schedule / снимаете pause.

Подробный чек-лист: `docs/APPSFLYER_ROLLOUT.md`.
