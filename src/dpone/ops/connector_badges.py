"""Connector badge matrix generation for docs and release artifacts."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dpone.ops.marketplace import ConnectorMarketplaceService


@dataclass(frozen=True, slots=True)
class ConnectorBadgeEntry:
    connector: str
    marketplace_status: str
    history_status: str
    badge: str
    capabilities: tuple[str, ...]
    docs: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class StrategyBadgeRow:
    source: str
    sink: str
    strategy: str
    status: str
    case_id: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ConnectorBadgeReport:
    release: str | None
    passed: bool
    entries: tuple[ConnectorBadgeEntry, ...]
    strategy_rows: tuple[StrategyBadgeRow, ...]
    output_dir: str

    def to_dict(self) -> dict[str, object]:
        return {
            "release": self.release,
            "passed": self.passed,
            "output_dir": self.output_dir,
            "entries": [entry.to_dict() for entry in self.entries],
            "strategy_rows": [row.to_dict() for row in self.strategy_rows],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone connector badges",
            "",
            f"- Release: `{self.release or 'unversioned'}`",
            f"- Passed: `{self.passed}`",
            "",
            "## Connectors",
            "",
            "| connector | badge | marketplace | history | capabilities | docs |",
            "|---|---|---|---|---|---|",
        ]
        for entry in self.entries:
            caps = ", ".join(f"`{capability}`" for capability in entry.capabilities)
            lines.append(
                f"| `{entry.connector}` | `{entry.badge}` | `{entry.marketplace_status}` | "
                f"`{entry.history_status}` | {caps} | {entry.docs} |"
            )
        if self.strategy_rows:
            lines.extend(
                [
                    "",
                    "## Source -> sink strategy changes",
                    "",
                    "| source | sink | strategy | status | case |",
                    "|---|---|---|---|---|",
                ]
            )
            for row in self.strategy_rows:
                lines.append(f"| `{row.source}` | `{row.sink}` | `{row.strategy}` | `{row.status}` | `{row.case_id}` |")
        return "\n".join(lines) + "\n"


class ConnectorBadgeService:
    """Generates docs-ready connector badge matrices from marketplace and history."""

    def __init__(self, marketplace: ConnectorMarketplaceService | None = None) -> None:
        self._marketplace = marketplace or ConnectorMarketplaceService.default()

    def generate(
        self,
        *,
        output_dir: str | Path,
        history_index_path: str | Path | None = None,
    ) -> ConnectorBadgeReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        latest = self._latest_release(Path(history_index_path)) if history_index_path else {}
        strategy_rows = self._strategy_rows(latest)
        impacted = {
            connector
            for row in strategy_rows
            if row.status in {"new_failure", "unchanged_failure"}
            for connector in (row.source, row.sink)
        }
        entries = self._entries(impacted=impacted, latest=latest)
        report = ConnectorBadgeReport(
            release=str(latest["release"]) if latest.get("release") else None,
            passed=not impacted,
            entries=entries,
            strategy_rows=strategy_rows,
            output_dir=str(directory),
        )
        (directory / "connector_badges.json").write_text(report.to_json(), encoding="utf-8")
        (directory / "connector_badges.md").write_text(report.to_markdown(), encoding="utf-8")
        return report

    def _entries(
        self,
        *,
        impacted: set[str],
        latest: Mapping[str, Any],
    ) -> tuple[ConnectorBadgeEntry, ...]:
        catalog = self._marketplace.catalog()
        history_status = str(latest.get("status", "unknown"))
        entries: list[ConnectorBadgeEntry] = []
        for connector, entry in sorted(catalog.connectors.items()):
            connector_history_status = "regression" if connector in impacted else history_status
            badge = "regression" if connector in impacted else entry.badge
            entries.append(
                ConnectorBadgeEntry(
                    connector=connector,
                    marketplace_status=entry.status,
                    history_status=connector_history_status,
                    badge=badge,
                    capabilities=entry.capabilities,
                    docs=entry.docs,
                )
            )
        return tuple(entries)

    def _strategy_rows(self, latest: Mapping[str, Any]) -> tuple[StrategyBadgeRow, ...]:
        rows: list[StrategyBadgeRow] = []
        for field, status in (
            ("new_failures", "new_failure"),
            ("fixed_failures", "fixed"),
            ("unchanged_failures", "unchanged_failure"),
        ):
            rows.extend(self._rows_from_cases(latest.get(field, []), status=status))
        return tuple(rows)

    def _rows_from_cases(self, case_ids: object, *, status: str) -> tuple[StrategyBadgeRow, ...]:
        if not isinstance(case_ids, list):
            return tuple()
        rows: list[StrategyBadgeRow] = []
        for case_id in [str(item) for item in case_ids]:
            parsed = self._parse_case_id(case_id)
            if parsed is None:
                continue
            source, sink, strategy = parsed
            rows.append(StrategyBadgeRow(source=source, sink=sink, strategy=strategy, status=status, case_id=case_id))
        return tuple(rows)

    @staticmethod
    def _parse_case_id(case_id: str) -> tuple[str, str, str] | None:
        if "__" not in case_id or "_to_" not in case_id:
            return None
        pair, strategy = case_id.split("__", 1)
        source, sink = pair.split("_to_", 1)
        return source, sink, strategy

    @staticmethod
    def _latest_release(path: Path) -> Mapping[str, Any]:
        if not path.exists():
            return {}
        payload = json.loads(path.read_text(encoding="utf-8"))
        releases = payload.get("releases", []) if isinstance(payload, Mapping) else []
        valid = [item for item in releases if isinstance(item, Mapping)]
        return dict(valid[-1]) if valid else {}
