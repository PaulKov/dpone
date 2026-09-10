# Change-aware validation plan

Categories: cli, docs, python

## Commands
- `uv run ruff check .`
- `uv run ruff format --check .`
- `uv run mypy --config-file mypy.ini`
- `uv run dpone docs check-import-rules`
- `uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json`
- `HEAD_SHA="$(git rev-parse HEAD)"; BASE_SHA="$(git merge-base "$HEAD_SHA" origin/master)"; if [ "$BASE_SHA" = "$HEAD_SHA" ]; then BASE_SHA="$(git rev-parse "${HEAD_SHA}^")"; fi; uv run dpone docs check-module-size --baseline docs/module_size_baseline.json --base-ref "$BASE_SHA" --head-ref "$HEAD_SHA"`
- `uv run pytest -k "cli or command" -q`
- `uv run dpone docs check-docs`
- `uv run dpone docs check-generated-references`
- `uv run pytest tests/test_docs_language_contracts.py -q`
- `uv run mkdocs build --strict`
- `uv run pytest -m "not integration_live" -n auto --dist loadfile`

## Potential release gates
- R1 CLI correctness and UX
- R2 run CLI/Python parity
- R6 documentation and CJM

Live checks require an approved environment; SKIP/UNVERIFIED is not PASS.
