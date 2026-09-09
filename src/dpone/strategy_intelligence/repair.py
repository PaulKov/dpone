from __future__ import annotations

from dataclasses import dataclass

from dpone.strategy_intelligence.models import RepairPlan, RepairStep


@dataclass(frozen=True, slots=True)
class RepairRequest:
    run_id: str
    source_type: str
    sink_type: str
    strategy_mode: str
    failed_stage: str
    partition_values: tuple[str, ...] = ()


class RepairPlanService:
    """Build deterministic repair/resume plans for failed load runs."""

    def plan(self, request: RepairRequest) -> RepairPlan:
        strategy = request.strategy_mode.strip().lower()
        steps = [
            RepairStep(
                action="inspect_run_artifacts",
                description="Read run report, load ids, staging artifacts, state before/after and failed stage.",
                command=f"dpone run-report {request.run_id} --format md",
            ),
            RepairStep(
                action="validate_staging_artifacts",
                description="Validate staging files/tables and row counts before any replay.",
                command=f"dpone state inspect --identity {request.run_id} --state-type run",
            ),
        ]
        if strategy == "partition_replace":
            values = ",".join(request.partition_values) if request.partition_values else "from-run-artifact"
            replay_command = f"dpone resync --run-id {request.run_id} --partitions {values} --plan"
            steps.append(
                RepairStep(
                    action="replay_partition_replace",
                    description="Replay only affected partitions through staging-first partition replacement.",
                    command=replay_command,
                )
            )
            safe = False
        else:
            replay_command = f"dpone resume {request.run_id} --from-stage {request.failed_stage} --plan"
            steps.append(
                RepairStep(
                    action="resume_from_failed_stage",
                    description="Resume from the failed idempotent stage after validating staging artifacts.",
                    command=replay_command,
                )
            )
            safe = request.failed_stage in {"extract", "stage"}
        steps.extend(
            [
                RepairStep(
                    action="quality_reconcile",
                    description="Run source-target count/checksum reconciliation before state commit.",
                ),
                RepairStep(
                    action="commit_state_after_success",
                    description="Advance cursor/CDC/Kafka state only after finalizer and quality gates succeed.",
                ),
            ]
        )
        replay_commands = tuple(
            step.command
            for step in steps
            if step.command and step.action in {"replay_partition_replace", "resume_from_failed_stage"}
        )
        diagnostic_commands = tuple(
            step.command
            for step in steps
            if step.command and step.action not in {"replay_partition_replace", "resume_from_failed_stage"}
        )
        commands = replay_commands + diagnostic_commands
        return RepairPlan(run_id=request.run_id, safe_to_auto_resume=safe, steps=tuple(steps), commands=commands)
