# dpone Studio Responsive Polish QA Artifact

- Artifact date: 2026-06-03
- Tested by: Codex
- Framework repo: `<workspace>/dpone`
- Studio repo: `<workspace>/dpone-studio`
- Framework API bridge: `http://127.0.0.1:8769`
- Studio preview: `http://127.0.0.1:3039`
- Scope: visual polish, responsive layout, no overflow, no long skinny panels, low/no-code accessibility labels, API-backed quality checks with custom unique key

## Fixes applied

- Mid-width overflow fixed for desktop widths where sidebar leaves a narrower workspace.
- Maturity cards switch to a better 2-column layout before they become too narrow.
- Long operational tokens now wrap safely instead of forcing horizontal scroll.
- Extra-small mobile padding is tightened so panels fit content without feeling crushed.
- Code-heavy GitOps/YAML panels are constrained on very small screens.
- Low/no-code and maturity form controls now expose explicit accessible names:
  - `Pipeline source`
  - `Pipeline sink`
  - `Pipeline unique key`
  - `Schema source`
  - `Schema sink`
  - `Reconciliation unique key`
- Studio API generated quality checks now use the selected unique key instead of hard-coded `id`.
- Quality status now distinguishes `pending`, `passed`, and `failed`.

## Interaction proof

The QA script changed the pipeline sink to `clickhouse`, changed unique key to `event_id`, clicked `Validate draft`, and verified the API-backed UI state.

```json
{
  "selectedSink": "clickhouse",
  "uniqueKey": "event_id",
  "qualityText": [
    "Quality: passed",
    "Plan: dry-run ready",
    "Bulk: clickhouse_native_or_http_bulk"
  ],
  "manifestHasEventId": true,
  "noApiError": true,
  "noOverflow": true,
  "scrollWidth": 1366,
  "clientWidth": 1366
}
```

## Viewport matrix

All checked viewports passed: no horizontal overflow, no API error, no framework overlay, no skinny cards from 360px upward, no tall-narrow panels, and no huge empty cards.

| Viewport | Overflow | API error | Overlay | Cards | Maturity | Skinny | Tall narrow | Huge empty |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 320x900 | false | false | false | 9 | 1 | 0 | 0 | 0 |
| 360x900 | false | false | false | 9 | 1 | 0 | 0 | 0 |
| 390x1000 | false | false | false | 9 | 1 | 0 | 0 | 0 |
| 414x1000 | false | false | false | 9 | 1 | 0 | 0 | 0 |
| 540x1000 | false | false | false | 9 | 1 | 0 | 0 | 0 |
| 768x1000 | false | false | false | 9 | 1 | 0 | 0 | 0 |
| 1024x1000 | false | false | false | 9 | 1 | 0 | 0 | 0 |
| 1265x900 | false | false | false | 9 | 1 | 0 | 0 | 0 |
| 1366x900 | false | false | false | 9 | 1 | 0 | 0 | 0 |
| 1440x1000 | false | false | false | 9 | 1 | 0 | 0 | 0 |
| 1680x1000 | false | false | false | 9 | 1 | 0 | 0 | 0 |

## Browser evidence

Screenshots captured during final QA:

- `/tmp/dpone-studio-final-interaction-1366.png`
- `/tmp/dpone-studio-final-1440.png`
- `/tmp/dpone-studio-final-768.png`
- `/tmp/dpone-studio-final-320.png`

## Build and test gates

- `npm run test`: PASS
- `npm run typecheck`: PASS
- `npm run build`: PASS
- `uv run ruff check .`: PASS
- `uv run ruff format --check .`: PASS
- `uv run mypy --config-file mypy.ini`: PASS
- `uv run pytest tests/test_studio_api_v1_contracts.py -q`: PASS

## Result

PASS.

The UI is now responsive across phone, tablet, laptop and desktop widths. The layout avoids horizontal scroll, avoids narrow/tall operational panels, preserves readable spacing, and keeps low/no-code controls accessible and API-backed.
