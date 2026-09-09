# Google Ads live smoke examples

```bash
# Connector-only live smoke from source tree
export ENV_CODE=dev
export DPONE_IT_GA_VAULT_PATH="api/google_ads"

python tools/google_ads_live_smoke.py \
  --project-root . \
  --mode connector \
  --format json
```

```bash
# End-to-end manifest run from source tree
export DPONE_PROJECT_DIR=$(pwd)
export ENV_CODE=dev
export DPONE_IT_GA_SELECTORS="app.ads_stats"

python tools/google_ads_live_smoke.py \
  --project-root "$DPONE_PROJECT_DIR" \
  --mode manifest-run \
  --format json
```

```bash
# End-to-end manifest-run smoke through installed snapshot package
source examples/dev_install/google_ads_snapshot.env.example

# replace placeholders before use
export DPONE_PROJECT_DIR=$(pwd)

dpone-runtime-exec python tools/google_ads_live_smoke.py \
  --project-root "$DPONE_PROJECT_DIR" \
  --mode manifest-run \
  --selector app.ads_stats \
  --format json
```
