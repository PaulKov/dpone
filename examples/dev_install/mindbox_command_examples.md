# Mindbox live smoke examples

## Connector-only smoke against real Vault/Mindbox

```bash
export DPONE_IT_MB_VAULT_PATH="api/mindbox"
export DPONE_IT_MB_RESOURCE="getactions"
export DPONE_IT_MB_EXPORT_TIMEOUT="3000"
export ENV_CODE=dev

python tools/mindbox_live_smoke.py \
  --project-root . \
  --mode connector \
  --days-back 1
```

## Connector smoke for an exact 1-hour UTC window

```bash
export DPONE_IT_MB_VAULT_PATH="api/mindbox"
export DPONE_IT_MB_RESOURCE="getmailings"
export DPONE_IT_MB_EXPORT_TIMEOUT="3000"
export ENV_CODE=dev

python tools/mindbox_live_smoke.py \
  --project-root . \
  --mode connector \
  --since-datetime-utc 2026-03-10T09:00:00Z \
  --till-datetime-utc 2026-03-10T10:00:00Z
```

## End-to-end manifest run from source tree

```bash
export DPONE_PROJECT_DIR=$(pwd)
export ENV_CODE=dev
export DPONE_IT_MB_SELECTORS="app.getactions"
export DPONE_IT_MB_EXPORT_TIMEOUT="3000"

python tools/mindbox_live_smoke.py \
  --project-root . \
  --mode manifest-run \
  --days-back 1
```

## End-to-end manifest run via snapshot overlay (`DPONE_PACKAGE_SPEC`)

```bash
source examples/dev_install/mindbox_snapshot.env.example

# replace placeholders before use
export DPONE_PROJECT_DIR=$(pwd)

dpone-runtime-exec python tools/mindbox_live_smoke.py \
  --project-root "$DPONE_PROJECT_DIR" \
  --mode manifest-run \
  --format json
```
