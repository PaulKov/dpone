from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dpone.strategy_intelligence.replay_adapters import ReplayAdapterRegistry
from dpone.strategy_intelligence.replay_evidence import ReplayEvidenceWriter


@dataclass(frozen=True, slots=True)
class ReplayExecutionRequest:
    action: str
    run_id: str
    source_type: str
    sink_type: str
    strategy_mode: str
    failed_stage: str = "finalize"
    partitions: tuple[str, ...] = ()
    yes: bool = False


@dataclass(frozen=True, slots=True)
class ReplayExecutionResult:
    action: str
    run_id: str
    mode: str
    executed: bool
    status: str
    failed_stage: str
    artifact_path: Path
    commands: tuple[str, ...]
    operations: tuple[str, ...] = ()
    state_committed: bool = False

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["artifact_path"] = str(self.artifact_path)
        payload["commands"] = list(self.commands)
        payload["operations"] = list(self.operations)
        return payload


class ReplayExecutionService:
    """Create safe replay/resync execution artifacts.

    v1 intentionally records an execution contract and requires --yes before marking
    replay as executed. Live DB mutation is left to injected runtime adapters in a
    later pass.
    """

    def __init__(
        self,
        artifact_dir: str | Path = ".dpone/replay",
        *,
        adapter_registry: ReplayAdapterRegistry | None = None,
    ) -> None:
        self._artifact_dir = Path(artifact_dir)
        self._adapter_registry = adapter_registry or ReplayAdapterRegistry()

    def execute(self, request: ReplayExecutionRequest) -> ReplayExecutionResult:
        mode = "execute" if request.yes else "plan"
        command = self._command(request)
        adapter_result = self._adapter_registry.resolve(request.sink_type).execute(request) if request.yes else None
        executed = request.yes and adapter_result is not None and adapter_result.status == "executed"
        status = adapter_result.status if adapter_result is not None else "planned"
        operations = adapter_result.operations if adapter_result is not None else ()
        state_committed = bool(adapter_result.state_committed) if adapter_result is not None else False
        artifact_path = self._write_artifact(
            request,
            mode=mode,
            executed=executed,
            status=status,
            command=command,
            operations=operations,
            state_committed=state_committed,
            diagnostics=adapter_result.to_dict() if adapter_result is not None else None,
        )
        ReplayEvidenceWriter(self._artifact_dir).write(
            action=request.action,
            run_id=request.run_id,
            service=request.sink_type,
            source_type=request.source_type,
            strategy_mode=request.strategy_mode,
            mode=mode,
            executed=executed,
            status=status,
            artifact_path=artifact_path,
            commands=(command,),
            operations=operations,
            state_committed=state_committed,
            adapter_result=adapter_result,
        )
        return ReplayExecutionResult(
            action=request.action,
            run_id=request.run_id,
            mode=mode,
            executed=executed,
            status=status,
            failed_stage=request.failed_stage,
            artifact_path=artifact_path,
            commands=(command,),
            operations=operations,
            state_committed=state_committed,
        )

    def _command(self, request: ReplayExecutionRequest) -> str:
        if request.action == "resume":
            return f"dpone resume {request.run_id} --from-stage {request.failed_stage} --yes"
        partitions = ",".join(request.partitions) if request.partitions else "from-run-artifact"
        return f"dpone resync --run-id {request.run_id} --partitions {partitions} --yes"

    def _write_artifact(
        self,
        request: ReplayExecutionRequest,
        *,
        mode: str,
        executed: bool,
        status: str,
        command: str,
        operations: tuple[str, ...],
        state_committed: bool,
        diagnostics: dict[str, Any] | None,
    ) -> Path:
        self._artifact_dir.mkdir(parents=True, exist_ok=True)
        path = self._artifact_dir / f"{request.action}_{_safe_name(request.run_id)}.json"
        payload = {
            "action": request.action,
            "run_id": request.run_id,
            "mode": mode,
            "executed": executed,
            "status": status,
            "state_committed": state_committed,
            "source_type": request.source_type,
            "sink_type": request.sink_type,
            "strategy_mode": request.strategy_mode,
            "failed_stage": request.failed_stage,
            "partitions": list(request.partitions),
            "created_at": datetime.now(UTC).isoformat(),
            "commands": [command],
            "operations": list(operations),
            "diagnostics": diagnostics or {},
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return path


def _safe_name(value: str) -> str:
    return "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in value)
