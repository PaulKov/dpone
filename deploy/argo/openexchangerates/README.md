# OpenExchangeRates rollout bundle

Этот каталог содержит concrete rollout package для OpenExchangeRates Historical Rates в Airflow/Argo.

## Состав

- `values.base.yaml` — общие значения, manifest path и runtime wrapper commands
- `values.dev.yaml` — dev overlay со snapshot install
- `values.prod.yaml` — prod overlay c baked package
- `runtime.dev.env.example` / `runtime.prod.env.example` — env examples для pod runtime
- `live-smoke.dev.env.example` / `live-smoke.prod.env.example` — env examples для manual smoke

## Канонический manifest

`/opt/airflow/dags/repo/examples/batch/landing_openexchangerates_api.batch.yaml`

## Канонический schedule

`5 4 * * *`

## Канонические selectors

- `default.historical_rates_daily`

Подробный checklist: `docs/OPENEXCHANGERATES_ROLLOUT.md`.
