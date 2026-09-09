# Yandex Webmaster live smoke examples

```bash
# Connector-only live smoke from source tree
export ENV_CODE=dev
export DPONE_IT_YANDEX_WEBMASTER_VAULT_PATH="api/yandex_webmaster"
export DPONE_IT_YANDEX_WEBMASTER_HOST_ID="https:travel.example.com:443"

python tools/yandex_webmaster_live_smoke.py \
  --project-root . \
  --mode connector \
  --format json
```

```bash
# End-to-end manifest run from source tree
export DPONE_PROJECT_DIR=$(pwd)
export ENV_CODE=dev
export DPONE_IT_YANDEX_WEBMASTER_HOST_ID="https:travel.example.com:443"
export DPONE_IT_YANDEX_WEBMASTER_SELECTORS="app.search_queries_history_daily"
export DPONE_IT_YANDEX_WEBMASTER_RESOURCE="search_queries_history_daily"
export DPONE_IT_YANDEX_WEBMASTER_DAYS_BACK="14"

python tools/yandex_webmaster_live_smoke.py \
  --project-root "$DPONE_PROJECT_DIR" \
  --mode manifest-run \
  --format json
```

```bash
# End-to-end manifest-run smoke through installed snapshot package
source examples/dev_install/yandex_webmaster_snapshot.env.example

# replace placeholders before use
export DPONE_PROJECT_DIR=$(pwd)

dpone-runtime-exec python tools/yandex_webmaster_live_smoke.py \
  --project-root "$DPONE_PROJECT_DIR" \
  --mode manifest-run \
  --selector app.query_analytics_by_region_daily \
  --resource query_analytics_by_region_daily \
  --region-ids 225,1 \
  --format json
```
