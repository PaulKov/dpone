# Batch manifests examples

`dpone.batch.v1` (Variant C) позволяет описывать множество процессов в одном YAML:

- общие настройки (credentials, стратегии) находятся в `defaults:`
- таблицы перечисляются в `schemas.*.tables:`
- имена датасетов/таблиц/тасков собираются из `vars:` + `naming:` шаблонов

Step 8 добавляет `convention:` — preset, который может поставить `vars/naming/validation` по умолчанию.

Step 9 добавляет `registry:` / `--registry` — источник реестра, который подставляет vars (например host/type)
по (src_system, src_database), чтобы не дублировать эти значения в каждом манифесте.

Полезные команды:

```bash
dpone manifest list examples/batch/landing_postgres_to_bq.batch.yaml
dpone manifest render examples/batch/landing_postgres_to_bq.batch.yaml --selector public.core_city
dpone manifest validate examples/batch/landing_postgres_to_bq.batch.yaml --profile landing_raw_v1 --registry examples/registry/sources.yaml
```

> Пример `landing_postgres_to_bq.batch.yaml` использует `convention: landing_raw_v1` (landing/raw стандарт),
> но механика `convention/vars/naming/overrides` универсальна и может быть адаптирована под любой стандарт.

- `landing_appsflyer_api.batch.yaml` — пример Variant C batch manifest для AppsFlyer → landing__appsflyer__api.
- `landing_cbr_api.batch.yaml` — пример Variant C batch manifest для публичного XML API ЦБ РФ → landing__cbr__api.

- `landing_mindbox_api.batch.yaml` — пример Variant C batch manifest для Mindbox Pull API → landing__mindbox__api.
- `landing_fasttrack_api.batch.yaml` — пример Variant C batch manifest для Fasttrack pull API → landing__fasttrack__api.
- `landing_similarweb_api.batch.yaml` — пример Variant C batch manifest для SimilarWeb Website Keywords → landing__similarweb__api.
- `landing_openexchangerates_api.batch.yaml` — пример Variant C batch manifest для OpenExchangeRates historical daily rates → landing__openexchangerates__api.
- `landing_google_ads_api.batch.yaml` — пример Variant C batch manifest для Google Ads statistics → landing__google_ads__api.
- `landing_google_sheets_api.batch.yaml` — пример Variant C batch manifest для Google Sheets → landing__google_sheets__api.
- `landing_yandex_webmaster_api.batch.yaml` — пример Variant C batch manifest для Yandex Webmaster → landing__yandex_webmaster__api.


Package smoke по умолчанию теперь покрывает примеры для Postgres, AppsFlyer, CBR, Mindbox, Fasttrack, SimilarWeb, OpenExchangeRates, Google Ads, Google Sheets и Yandex Webmaster через `tools/package_smoke.py`.
