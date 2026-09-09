# Fasttrack rollout bundle

Этот каталог содержит конкретный rollout package для Fasttrack в Airflow/Argo.

## Состав

- `values.base.yaml` — общие значения/команды/volume mounts
- `values.dev.yaml` — dev overlay c `DPONE_PACKAGE_SPEC` (snapshot install)
- `values.prod.yaml` — prod overlay c baked package
- `runtime.dev.env.example` / `runtime.prod.env.example` — env examples для pod runtime
- `live-smoke.dev.env.example` / `live-smoke.prod.env.example` — env examples для manual smoke

## Канонический manifest

`/opt/airflow/dags/repo/examples/batch/landing_fasttrack_api.batch.yaml`

## Канонический schedule

`0 1 * * *`

## Канонические selectors

- `default.cascade_transactions`
- `default.chat_sessions`
- `default.flex_cms_ratings`

## Как использовать

1. Берёте `values.base.yaml` + `values.dev.yaml` или `values.prod.yaml`.
2. Настраиваете Secret для `DPONE_PACKAGE_INDEX_URL` (dev) и Vault access.
3. Применяете rollout.
4. Прогоняете live smoke по `deploy/argo/fasttrack/live-smoke.*.env.example`.
5. Дальше включаете schedule / снимаете pause.

Подробный чек-лист: `docs/FASTTRACK_ROLLOUT.md`.
