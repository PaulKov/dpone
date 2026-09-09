# Fasttrack live smoke examples

```bash
# Dry-run runtime wrapper
dpone-runtime-exec --dry-run -- airflow scheduler
```

```bash
# Connector-only live smoke
export ENV_CODE=dev
export DPONE_IT_FT_VAULT_PATH="api/fasttrack"
export DPONE_IT_FT_RESOURCE="flex_cms_ratings"

python tools/fasttrack_live_smoke.py \
  --project-root . \
  --mode connector \
  --format json
```

```bash
# End-to-end manifest run from source tree
export DPONE_PROJECT_DIR=$(pwd)
export ENV_CODE=dev
export DPONE_IT_FT_SELECTORS="default.flex_cms_ratings"

python tools/fasttrack_live_smoke.py \
  --project-root "$DPONE_PROJECT_DIR" \
  --mode manifest-run \
  --format json
```

```bash
# End-to-end manifest-run smoke through installed snapshot package
source examples/dev_install/fasttrack_snapshot.env.example

# replace placeholders before use
export DPONE_PROJECT_DIR=$(pwd)

dpone-runtime-exec python tools/fasttrack_live_smoke.py \
  --project-root "$DPONE_PROJECT_DIR" \
  --mode manifest-run \
  --selector default.flex_cms_ratings \
  --format json
```
