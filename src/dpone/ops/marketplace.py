"""Connector marketplace and certification badge generation."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.readiness.capability_discovery_protocols import (
        CapabilitySnapshotProtocol,
    )


@dataclass(frozen=True, slots=True)
class ConnectorCatalogEntry:
    connector: str
    status: str
    capabilities: tuple[str, ...]
    docs: str
    badge: str
    maturity: str = "experimental"
    release_phase: str = ""

    def __post_init__(self) -> None:
        if not self.release_phase:
            object.__setattr__(self, "release_phase", self.status)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ConnectorCatalog:
    connectors: dict[str, ConnectorCatalogEntry]
    snapshot_id: str | None = None
    issues: tuple[dict[str, str], ...] = ()

    @property
    def passed(self) -> bool:
        return not self.issues

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "dpone.connector-marketplace.v2",
            "passed": self.passed,
            "snapshot_id": self.snapshot_id,
            "connectors": {name: entry.to_dict() for name, entry in self.connectors.items()},
            "issues": [dict(item) for item in self.issues],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone connector marketplace",
            "",
            f"- Passed: `{self.passed}`",
            f"- Capability snapshot: `{self.snapshot_id or 'unavailable'}`",
            "",
            "| connector | status | capabilities | docs |",
            "|---|---|---|---|",
        ]
        for name, entry in sorted(self.connectors.items()):
            caps = ", ".join(f"`{item}`" for item in entry.capabilities)
            lines.append(f"| {name} | {entry.badge} | {caps} | {entry.docs} |")
        if self.issues:
            lines.extend(
                [
                    "",
                    "## Capability issues",
                    "",
                    *(f"- `{item['code']}`: {item['message']}" for item in self.issues),
                ]
            )
        return "\n".join(lines) + "\n"


class ConnectorMarketplaceService:
    """Builds the public connector capability catalog."""

    def __init__(
        self,
        entries: dict[str, ConnectorCatalogEntry],
        *,
        snapshot_id: str | None = None,
        issues: tuple[dict[str, str], ...] = (),
    ) -> None:
        self.entries = entries
        self._snapshot_id = snapshot_id
        self._issues = issues

    @classmethod
    def default(
        cls,
        snapshot: CapabilitySnapshotProtocol | None = None,
    ) -> ConnectorMarketplaceService:
        """Build the compatibility catalog from the canonical capability snapshot."""

        if snapshot is None:
            from dpone.readiness.capability_discovery_composition import (
                build_capability_discovery_service,
            )

            snapshot = build_capability_discovery_service(root=Path.cwd()).snapshot()
        labels = {
            "bigquery": "BigQuery",
            "clickhouse": "ClickHouse",
            "kafka": "Kafka",
            "mssql": "MSSQL",
            "mysql": "MySQL",
            "postgres": "PostgreSQL",
            "rest": "REST API",
        }
        return cls(
            {
                connector.id: ConnectorCatalogEntry(
                    connector=connector.id,
                    status=connector.release_phase,
                    capabilities=connector.capability_ids,
                    docs=(f"[{labels.get(connector.id, connector.id)}]({connector.docs_link.removeprefix('docs/')})"),
                    badge=connector.release_phase,
                    maturity=connector.maturity,
                    release_phase=connector.release_phase,
                )
                for connector in snapshot.connectors
            },
            snapshot_id=snapshot.snapshot_id,
            issues=tuple(item.to_dict() for item in snapshot.issues),
        )

    def catalog(self) -> ConnectorCatalog:
        return ConnectorCatalog(
            connectors=dict(self.entries),
            snapshot_id=self._snapshot_id,
            issues=self._issues,
        )
