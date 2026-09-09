"""OpenLineage event builder for parsed dbt artifact graphs."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime

from dpone.ops.dbt_artifacts import FAILING_DBT_STATUSES, MODEL_RESOURCE_TYPES, DbtLineageNode
from dpone.ops.openlineage_export import OPENLINEAGE_PRODUCER, OPENLINEAGE_SCHEMA_URL


class DbtOpenLineageEventBuilder:
    """Builds OpenLineage-compatible events for dbt dataset-producing nodes."""

    def build_events(
        self,
        *,
        nodes: Mapping[str, DbtLineageNode],
        run_id: str,
        invocation_id: str,
        namespace: str,
    ) -> list[dict[str, object]]:
        events: list[dict[str, object]] = []
        for node in sorted(nodes.values(), key=lambda item: item.unique_id):
            if node.resource_type not in MODEL_RESOURCE_TYPES:
                continue
            events.append(
                self._event(
                    node=node,
                    nodes=nodes,
                    run_id=run_id,
                    invocation_id=invocation_id,
                    namespace=namespace,
                )
            )
        return events

    def _event(
        self,
        *,
        node: DbtLineageNode,
        nodes: Mapping[str, DbtLineageNode],
        run_id: str,
        invocation_id: str,
        namespace: str,
    ) -> dict[str, object]:
        return {
            "eventType": "FAIL" if node.status in FAILING_DBT_STATUSES else "COMPLETE",
            "eventTime": datetime.now(UTC).isoformat(),
            "producer": OPENLINEAGE_PRODUCER,
            "schemaURL": OPENLINEAGE_SCHEMA_URL,
            "run": {
                "runId": run_id,
                "facets": {
                    "dbt": {
                        "_producer": OPENLINEAGE_PRODUCER,
                        "_schemaURL": "https://github.com/PaulKov/dpone/blob/master/docs/dbt.md",
                        "invocation_id": invocation_id,
                        "unique_id": node.unique_id,
                        "status": node.status,
                    }
                },
            },
            "job": {"namespace": namespace, "name": f"dbt.{node.name}", "facets": {}},
            "inputs": [
                self._dataset(namespace, nodes[upstream].relation_name)
                for upstream in node.depends_on
                if upstream in nodes
            ],
            "outputs": [self._dataset(namespace, node.relation_name)],
        }

    @staticmethod
    def _dataset(namespace: str, name: str) -> dict[str, object]:
        return {"namespace": namespace, "name": name, "facets": {}}
