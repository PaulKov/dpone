# SimilarWeb live smoke examples

```bash
# Dry-run runtime wrapper
dpone-runtime-exec --dry-run -- airflow scheduler
```

```bash
# Connector-only live smoke
export ENV_CODE=dev
export DPONE_IT_SW_VAULT_PATH="api/similarweb"
export DPONE_IT_SW_DOMAINS="travel.example.com"

python tools/similarweb_live_smoke.py \
  --project-root . \
  --mode connector \
  --format json
```

```bash
# End-to-end manifest run from source tree
export DPONE_PROJECT_DIR=$(pwd)
export ENV_CODE=dev
export DPONE_IT_SW_DOMAINS="travel.example.com"
export DPONE_IT_SW_SELECTORS="default.keywords"

python tools/similarweb_live_smoke.py \
  --project-root "$DPONE_PROJECT_DIR" \
  --mode manifest-run \
  --format json
```

```bash
# End-to-end manifest-run smoke through installed snapshot package
source examples/dev_install/similarweb_snapshot.env.example

# replace placeholders before use
export DPONE_PROJECT_DIR=$(pwd)

dpone-runtime-exec python tools/similarweb_live_smoke.py \
  --project-root "$DPONE_PROJECT_DIR" \
  --mode manifest-run \
  --selector default.keywords \
  --format json
```
