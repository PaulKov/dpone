# AppsFlyer live smoke examples

## Connector-only smoke against real Vault/Appsflyer

```bash
export DPONE_IT_AF_APP_IDS="com.example.travel,id1234567890"
export DPONE_IT_AF_VAULT_PATH="api/appsflyer"
export ENV_CODE=dev

python tools/appsflyer_live_smoke.py \
  --project-root . \
  --mode connector \
  --days-back 2
```

## End-to-end manifest run from source tree

```bash
export DPONE_PROJECT_DIR=$(pwd)
export ENV_CODE=dev
export DPONE_IT_AF_APP_IDS="com.example.travel,id1234567890"
export DPONE_IT_AF_SELECTORS="app.installs_report"

python tools/appsflyer_live_smoke.py \
  --project-root . \
  --mode manifest-run \
  --days-back 2
```

## End-to-end manifest run via snapshot overlay (`DPONE_PACKAGE_SPEC`)

```bash
source examples/dev_install/appsflyer_snapshot.env.example

# replace placeholders before use
export DPONE_PROJECT_DIR=$(pwd)

# baked image / base venv already contains dpone-runtime-exec
# overlay installs the snapshot package into DPONE_PACKAGE_TARGET

dpone-runtime-exec python tools/appsflyer_live_smoke.py \
  --project-root "$DPONE_PROJECT_DIR" \
  --mode manifest-run \
  --format json
```
