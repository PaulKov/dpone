# OpenExchangeRates snapshot commands

```bash
python -m venv /tmp/venv
. /tmp/venv/bin/activate
pip install --upgrade pip
pip install "dpone[vault,gcp]==<snapshot-version>" --extra-index-url https://pypi.org/simple
```

```bash
export DPONE_PROJECT_DIR=/path/to/dpone
export ENV_CODE=dev
export VAULT_ADDR=https://vault.example.com
export VAULT_AUTH_METHOD=jwt
export VAULT_ID_TOKEN=<jwt>
export VAULT_AUTH_ROLE_DEV=dpone-viewer-dev
export DPONE_IT_OXR_VAULT_PATH=api/openexchangerates
export DPONE_IT_OXR_SELECTORS=default.historical_rates_daily
export DPONE_IT_OXR_SYMBOLS=ARS,RUB,EUR
dpone-runtime-exec python tools/openexchangerates_live_smoke.py --project-root "$DPONE_PROJECT_DIR" --mode connector --format json
```
