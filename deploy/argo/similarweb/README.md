# SimilarWeb rollout bundle

Этот каталог содержит concrete rollout package для SimilarWeb Website Keywords в Airflow/Argo.

## Состав

- `values.base.yaml` — общие значения, manifest path и runtime wrapper commands
- `values.dev.yaml` — dev overlay со snapshot install
- `values.prod.yaml` — prod overlay c baked package
- `runtime.dev.env.example` / `runtime.prod.env.example` — env examples для pod runtime
- `live-smoke.dev.env.example` / `live-smoke.prod.env.example` — env examples для manual smoke

## Канонический manifest

`/opt/airflow/dags/repo/examples/batch/landing_similarweb_api.batch.yaml`

## Канонический schedule

`0 3 3 * *`

## Канонические selectors

- `default.keywords`

## Как использовать

1. Берёте `values.base.yaml` + `values.dev.yaml` или `values.prod.yaml`.
2. Настраиваете доступ до Vault и BigQuery sink secret.
3. Применяете rollout.
4. Прогоняете manual smoke через `deploy/argo/similarweb/live-smoke.*.env.example`.
5. После валидации включаете schedule.

Подробный checklist: `docs/SIMILARWEB_ROLLOUT.md`.
