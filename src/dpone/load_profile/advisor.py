"""Load profile recommendation engine."""

from __future__ import annotations

from math import ceil
from typing import Any

from dpone.load_profile.models import LoadProfileDecision, ProfileAdviceRequest


class LoadProfileAdvisor:
    """Chooses a route profile and manifest patch from connector-neutral facts."""

    def advise(self, request: ProfileAdviceRequest) -> LoadProfileDecision:
        route = _route_family(request.route_id)
        max_rows = _recommended_max_chunk_rows(request)
        warnings = _warnings(request, max_rows)
        patch = _recommended_patch(request, max_rows)
        window_count = _window_count_estimate(request, max_rows)
        return LoadProfileDecision(
            selected_profile=_selected_profile(route, request),
            selected_route=route,
            execution_mode=request.execution_mode,
            expected_rps_range=_expected_rps(route, request.source_shape.width_class),
            confidence="medium" if request.source_shape.estimated_bytes_per_row else "low",
            warnings=tuple(warnings),
            blockers=(),
            recommendations=tuple(_recommendations(request, max_rows, warnings)),
            rejected_routes=tuple(_rejected_routes(route)),
            recommended_patch=patch,
            details={
                "source_type": request.source_type,
                "sink_type": request.sink_type,
                "optimization_goal": _goal(request),
                "source_shape": request.source_shape.to_dict(),
                "worker_profile": request.worker_profile.to_dict(),
                "target_chunk_bytes": request.target_chunk_bytes,
                "current_max_chunk_rows": request.current_max_chunk_rows,
                "rows_for_target_chunk_bytes": _rows_for_target(request),
                "window_count_estimate": window_count,
            },
        )


def _route_family(route_id: str) -> str:
    normalized = route_id.strip().lower()
    if normalized.startswith("object_storage_pull"):
        return "object_storage_pull"
    if normalized.startswith("direct_push_columnar"):
        return "direct_push_columnar"
    if normalized.startswith("typed_binary"):
        return "typed_binary_streaming"
    if normalized.startswith("typed_raw"):
        return "typed_raw_streaming"
    return "object_storage_pull" if normalized in {"", "auto"} else normalized


def _rows_for_target(request: ProfileAdviceRequest) -> int:
    return max(1, int(request.target_chunk_bytes / max(request.source_shape.bytes_per_row, 1)))


def _recommended_max_chunk_rows(request: ProfileAdviceRequest) -> int:
    target_rows = _rows_for_target(request)
    memory_rows = int((request.worker_profile.memory_bytes * 0.40) / max(request.source_shape.bytes_per_row * 6, 1))
    safe_rows = max(1_000_000, min(target_rows, memory_rows, _worker_row_ceiling(request.worker_profile.name)))
    return max(request.current_max_chunk_rows, _round_rows(safe_rows))


def _selected_profile(route: str, request: ProfileAdviceRequest) -> str:
    suffix = "" if _goal(request) == "balanced" else f".{_goal(request)}"
    return f"{route}.{request.execution_mode}.{request.worker_profile.name}{suffix}"


def _goal(request: ProfileAdviceRequest) -> str:
    goal = (request.optimization_goal or "balanced").strip().lower()
    if goal in {"fast", "speed", "throughput"}:
        return "performance"
    if goal in {"safe", "safety", "reliability"}:
        return "safety"
    if goal in {"cost", "cheap"}:
        return "cost"
    return "performance" if goal == "performance" else "balanced"


def _window_count_estimate(request: ProfileAdviceRequest, recommended_rows: int) -> dict[str, int | None]:
    row_count = request.source_shape.row_count
    if not row_count or row_count <= 0:
        return {"current": None, "recommended": None, "reduction": None}
    current = ceil(row_count / max(request.current_max_chunk_rows, 1))
    recommended = ceil(row_count / max(recommended_rows, 1))
    return {"current": current, "recommended": recommended, "reduction": max(0, current - recommended)}


def _worker_row_ceiling(name: str) -> int:
    if name == "weak_worker":
        return 5_000_000
    if name == "throughput":
        return 20_000_000
    return 10_000_000


def _round_rows(value: int) -> int:
    if value <= 1_000_000:
        return 1_000_000
    step = 500_000
    return int(ceil(value / step) * step)


def _warnings(request: ProfileAdviceRequest, recommended_rows: int) -> list[str]:
    warnings: list[str] = []
    target_rows = _rows_for_target(request)
    if target_rows > request.current_max_chunk_rows and recommended_rows > request.current_max_chunk_rows:
        warnings.append("row_cap_limits_target_chunk_bytes")
    if request.execution_mode == "file" and request.worker_profile.name == "weak_worker":
        warnings.append("file_mode_not_recommended_for_weak_worker")
    return warnings


def _recommendations(request: ProfileAdviceRequest, recommended_rows: int, warnings: list[str]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = [
        {
            "code": "use_governed_chunked_execution",
            "severity": "info",
            "message": "Use chunked execution with bounded inflight windows and eager cleanup.",
            "patch_path": "source.options.native_transfer.snapshot.columnar_fast_path.execution.mode",
            "value": "chunked",
        }
    ]
    if "row_cap_limits_target_chunk_bytes" in warnings:
        items.append(
            {
                "code": "raise_columnar_max_chunk_rows",
                "severity": "warning",
                "message": "Current row cap prevents target_chunk_bytes from forming larger windows on a narrow table.",
                "patch_path": "source.options.native_transfer.snapshot.columnar_fast_path.execution.max_chunk_rows",
                "value": recommended_rows,
            }
        )
    if _goal(request) == "performance":
        items.append(
            {
                "code": "performance_profile_checks_mssql_impact",
                "severity": "warning",
                "message": (
                    "Performance profile may increase MSSQL source pressure; certify with live source-impact checks "
                    "before making it the workload default."
                ),
                "patch_path": "source.options.native_transfer.snapshot.columnar_fast_path.execution.max_inflight_chunks",
                "value": request.worker_profile.max_inflight_chunks,
            }
        )
    return items


def _rejected_routes(selected_route: str) -> list[dict[str, str]]:
    rejected: list[dict[str, str]] = []
    for route in ("typed_binary_streaming", "typed_raw_streaming", "native_file"):
        if route != selected_route:
            rejected.append({"route_id": route, "reason": "lower_expected_throughput_or_weaker_columnar_contract"})
    return rejected


def _recommended_patch(request: ProfileAdviceRequest, max_rows: int) -> dict[str, Any]:
    return {
        "source": {
            "options": {
                "native_transfer": {
                    "snapshot": {
                        "columnar_fast_path": {
                            "execution": {
                                "mode": "chunked",
                                "target_chunk_bytes": request.target_chunk_bytes,
                                "max_chunk_rows": max_rows,
                                "max_inflight_chunks": request.worker_profile.max_inflight_chunks,
                                "cleanup_policy": "eager",
                            }
                        }
                    }
                }
            }
        }
    }


def _expected_rps(route: str, width_class: str) -> tuple[int, int]:
    if route == "object_storage_pull":
        return (150_000, 900_000) if width_class == "narrow" else (40_000, 250_000)
    if route == "direct_push_columnar":
        return (80_000, 350_000)
    if route == "typed_binary_streaming":
        return (15_000, 120_000)
    return (10_000, 90_000)


__all__ = ["LoadProfileAdvisor"]
