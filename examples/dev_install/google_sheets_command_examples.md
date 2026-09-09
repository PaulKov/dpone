# Google Sheets live smoke examples

```bash
# Connector-only live smoke from source tree
export ENV_CODE=dev
export DPONE_IT_GS_VAULT_PATH="api/google_sheets"
export DPONE_IT_GS_SPREADSHEET_URL="https://docs.google.com/spreadsheets/d/1EXAMPLE1234567890/edit#gid=0"

python tools/google_sheets_live_smoke.py \
  --project-root . \
  --mode connector \
  --format json
```

```bash
# End-to-end manifest run from source tree
export DPONE_PROJECT_DIR=$(pwd)
export ENV_CODE=dev
export DPONE_IT_GS_SPREADSHEET_URL="https://docs.google.com/spreadsheets/d/1EXAMPLE1234567890/edit#gid=0"
export DPONE_IT_GS_SELECTORS="app.worksheet_rows"

python tools/google_sheets_live_smoke.py \
  --project-root "$DPONE_PROJECT_DIR" \
  --mode manifest-run \
  --format json
```

```bash
# End-to-end manifest-run smoke through installed snapshot package
source examples/dev_install/google_sheets_snapshot.env.example

# replace placeholders before use
export DPONE_PROJECT_DIR=$(pwd)

dpone-runtime-exec python tools/google_sheets_live_smoke.py \
  --project-root "$DPONE_PROJECT_DIR" \
  --mode manifest-run \
  --selector app.worksheet_rows \
  --format json
```
