"""OpenTelemetry-compatible JSON rendering.

The renderer emits an OTLP-shaped JSON artifact without depending on the
OpenTelemetry SDK. This keeps local CI and package installs lightweight while
preserving an easy bridge to collectors and vendor pipelines.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from dpone.observability.metrics import MetricPoint


class OpenTelemetryJsonRenderer:
    """Render runtime metrics as an OTLP-like JSON resource payload."""

    def render(
        self,
        points: Iterable[MetricPoint],
        *,
        service_name: str,
        namespace: str,
        resource_attributes: Mapping[str, str] | None = None,
        data_point_attributes: Mapping[str, str] | None = None,
    ) -> dict[str, object]:
        return {
            "resourceMetrics": [
                {
                    "resource": {"attributes": self._resource_attributes(service_name, namespace, resource_attributes)},
                    "scopeMetrics": [
                        {
                            "scope": {"name": "dpone.observability", "version": "1"},
                            "metrics": [self._metric(point, data_point_attributes) for point in points],
                        }
                    ],
                }
            ]
        }

    def _metric(
        self,
        point: MetricPoint,
        data_point_attributes: Mapping[str, str] | None,
    ) -> dict[str, object]:
        attributes = dict(point.labels)
        attributes.update({key: str(value) for key, value in (data_point_attributes or {}).items()})
        return {
            "name": point.name,
            "description": point.description,
            "unit": point.unit,
            "gauge": {
                "dataPoints": [
                    {
                        "asDouble": point.value,
                        "attributes": [self._attribute(key, value) for key, value in sorted(attributes.items())],
                    }
                ]
            },
        }

    def _resource_attributes(
        self,
        service_name: str,
        namespace: str,
        resource_attributes: Mapping[str, str] | None,
    ) -> list[dict[str, object]]:
        attributes = {
            "service.name": service_name,
            "service.namespace": namespace,
        }
        for key, value in sorted((resource_attributes or {}).items()):
            if key and key not in attributes:
                attributes[key] = str(value)
        return [self._attribute(key, value) for key, value in sorted(attributes.items())]

    @staticmethod
    def _attribute(key: str, value: str) -> dict[str, object]:
        return {"key": key, "value": {"stringValue": value}}
