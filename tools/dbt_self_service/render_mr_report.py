"""Render one portable dbt compile report as a review-friendly Markdown summary."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from pathlib import Path

from dpone.services.dbt_ci_report import render_dbt_ci_report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("report")
    parser.add_argument("--output", required=True)
    parser.add_argument("--airflow-base-url")
    args = parser.parse_args()
    payload = json.loads(Path(args.report).read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise SystemExit("compile report must be a JSON object")
    rendered = render_dbt_ci_report(
        payload,
        airflow_base_url=args.airflow_base_url,
    )
    Path(args.output).write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
