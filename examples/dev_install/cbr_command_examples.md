# CBR public smoke examples

## Connector-only smoke against public CBR API

```bash
python tools/cbr_public_smoke.py \
  --project-root . \
  --mode connector \
  --days-back 3
```

## End-to-end manifest run from source tree

```bash
export DPONE_PROJECT_DIR=$(pwd)
export ENV_CODE=dev

python tools/cbr_public_smoke.py \
  --project-root . \
  --mode manifest-run \
  --days-back 3
```

## Public smoke via snapshot overlay (`DPONE_PACKAGE_SPEC`)

```bash
source examples/dev_install/cbr_snapshot.env.example

# replace placeholders before use
export DPONE_PROJECT_DIR=$(pwd)

dpone-runtime-exec python tools/cbr_public_smoke.py \
  --project-root "$DPONE_PROJECT_DIR" \
  --mode connector \
  --format json
```
