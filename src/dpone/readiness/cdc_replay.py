"""CDC replay planning and offset commit gates."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.readiness.cdc import CDCBackend, CDCOffset


@dataclass(frozen=True, slots=True)
class CDCReplayStep:
    name: str
    description: str
    required: bool = True

    def to_dict(self) -> dict[str, object]:
        return {"name": self.name, "description": self.description, "required": self.required}


@dataclass(frozen=True, slots=True)
class CDCReplayPlan:
    backend: CDCBackend
    pipeline_name: str
    source_schema: str
    source_table: str
    stored_offset: CDCOffset | None
    replay_from: CDCOffset
    replay_to: CDCOffset | None
    retention_min: CDCOffset | None
    high_watermark: CDCOffset | None
    artifact_uri: str | None
    allow_rewind: bool
    steps: tuple[CDCReplayStep, ...]
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]

    @property
    def safe_to_execute(self) -> bool:
        return not self.blockers

    def to_dict(self) -> dict[str, object]:
        return {
            "safe_to_execute": self.safe_to_execute,
            "backend": self.backend.value,
            "pipeline_name": self.pipeline_name,
            "source_schema": self.source_schema,
            "source_table": self.source_table,
            "stored_offset": self.stored_offset.to_state() if self.stored_offset else None,
            "replay_from": self.replay_from.to_state(),
            "replay_to": self.replay_to.to_state() if self.replay_to else None,
            "retention_min": self.retention_min.to_state() if self.retention_min else None,
            "high_watermark": self.high_watermark.to_state() if self.high_watermark else None,
            "artifact_uri": self.artifact_uri,
            "allow_rewind": self.allow_rewind,
            "steps": [step.to_dict() for step in self.steps],
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
        }

    def to_markdown(self) -> str:
        blockers = "\n".join(f"- `{item}`" for item in self.blockers) if self.blockers else "- none"
        warnings = "\n".join(f"- `{item}`" for item in self.warnings) if self.warnings else "- none"
        steps = "\n".join(f"{idx}. `{step.name}`: {step.description}" for idx, step in enumerate(self.steps, 1))
        return "\n".join(
            [
                "# CDC replay plan",
                "",
                f"- Safe to execute: `{self.safe_to_execute}`",
                f"- Backend: `{self.backend.value}`",
                f"- Pipeline: `{self.pipeline_name}`",
                f"- Source: `{self.source_schema}.{self.source_table}`",
                f"- Replay from: `{self.replay_from.token}`",
                f"- Replay to: `{self.replay_to.token if self.replay_to else ''}`",
                f"- Artifact URI: `{self.artifact_uri or ''}`",
                "",
                "## Steps",
                "",
                steps,
                "",
                "## Blockers",
                "",
                blockers,
                "",
                "## Warnings",
                "",
                warnings,
                "",
            ]
        )


@dataclass(frozen=True, slots=True)
class CDCCommitDecision:
    can_commit: bool
    next_offset: CDCOffset | None
    blockers: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "can_commit": self.can_commit,
            "next_offset": self.next_offset.to_state() if self.next_offset else None,
            "blockers": list(self.blockers),
        }


class CDCOffsetTokenComparator:
    """Compare source-specific CDC tokens for planning checks."""

    def compare(self, left: str, right: str) -> int:
        left_value = self._numeric_value(left)
        right_value = self._numeric_value(right)
        if left_value is None or right_value is None:
            return (left > right) - (left < right)
        return (left_value > right_value) - (left_value < right_value)

    def _numeric_value(self, token: str) -> int | None:
        value = token.strip()
        if not value:
            return None
        try:
            if "/" in value:
                high, low = value.split("/", 1)
                return (int(high, 16) << 32) + int(low, 16)
            if value.lower().startswith("0x"):
                return int(value, 16)
            return int(value)
        except ValueError:
            return None


class CDCReplayPlanner:
    """Build safe operator plans for bounded CDC replay."""

    def __init__(self, *, comparator: CDCOffsetTokenComparator | None = None) -> None:
        self._comparator = comparator or CDCOffsetTokenComparator()

    def plan(
        self,
        *,
        backend: CDCBackend,
        pipeline_name: str,
        source_schema: str,
        source_table: str,
        stored_offset: CDCOffset | None,
        replay_from: CDCOffset,
        replay_to: CDCOffset | None = None,
        retention_min: CDCOffset | None = None,
        high_watermark: CDCOffset | None = None,
        artifact_uri: str | None = None,
        allow_rewind: bool = False,
    ) -> CDCReplayPlan:
        blockers: list[str] = []
        warnings: list[str] = []
        self._validate_backend("stored_offset", backend, stored_offset, blockers)
        self._validate_backend("replay_from", backend, replay_from, blockers)
        self._validate_backend("replay_to", backend, replay_to, blockers)
        self._validate_backend("retention_min", backend, retention_min, blockers)
        self._validate_backend("high_watermark", backend, high_watermark, blockers)

        if replay_to and self._compare(replay_from, replay_to) > 0:
            blockers.append("replay.invalid_window")
        if retention_min and self._compare(replay_from, retention_min) < 0:
            blockers.append("replay.start_before_retention")
        if high_watermark and replay_to and self._compare(replay_to, high_watermark) > 0:
            blockers.append("replay.to_after_high_watermark")
        if stored_offset and self._compare(replay_from, stored_offset) < 0:
            warnings.append("replay.rewinds_stored_offset")
            if not allow_rewind:
                blockers.append("replay.requires_allow_rewind")
            if backend == CDCBackend.POSTGRES_LOGICAL and not artifact_uri:
                blockers.append("replay.artifact_required_for_consumed_postgres_slot")

        return CDCReplayPlan(
            backend=backend,
            pipeline_name=pipeline_name,
            source_schema=source_schema,
            source_table=source_table,
            stored_offset=stored_offset,
            replay_from=replay_from,
            replay_to=replay_to,
            retention_min=retention_min,
            high_watermark=high_watermark,
            artifact_uri=artifact_uri,
            allow_rewind=allow_rewind,
            steps=self._steps(),
            blockers=tuple(dict.fromkeys(blockers)),
            warnings=tuple(dict.fromkeys(warnings)),
        )

    def _compare(self, left: CDCOffset, right: CDCOffset) -> int:
        return self._comparator.compare(left.token, right.token)

    @staticmethod
    def _validate_backend(
        field_name: str,
        expected: CDCBackend,
        offset: CDCOffset | None,
        blockers: list[str],
    ) -> None:
        if offset is not None and offset.backend != expected:
            blockers.append(f"offset.backend_mismatch.{field_name}")

    @staticmethod
    def _steps() -> tuple[CDCReplayStep, ...]:
        return (
            CDCReplayStep("pause_pipeline", "Stop the scheduled CDC consumer for this pipeline."),
            CDCReplayStep(
                "verify_retention", "Confirm source retention or replay artifact covers the requested window."
            ),
            CDCReplayStep("set_start_offset", "Use replay_from as the bounded read start offset."),
            CDCReplayStep(
                "load_idempotent_delta", "Load CDC rows with deterministic event ids and target idempotency keys."
            ),
            CDCReplayStep("compare_target", "Run reconciliation and data-quality checks before offset advancement."),
            CDCReplayStep(
                "commit_offset_after_sink_commit", "Persist next_offset only after sink load status is committed."
            ),
        )


class CDCCommitGate:
    """Decide whether a CDC offset may be durably advanced."""

    _SUCCESS_STATUSES = {"success", "succeeded", "completed", "committed"}

    def evaluate(
        self,
        *,
        next_offset: CDCOffset | None,
        expected_backend: CDCBackend,
        sink_status: str,
        load_status: str,
        idempotency_passed: bool,
    ) -> CDCCommitDecision:
        blockers: list[str] = []
        if next_offset is None:
            blockers.append("offset_missing")
        elif next_offset.backend != expected_backend:
            blockers.append("offset_backend_mismatch")
        if sink_status.strip().lower() not in self._SUCCESS_STATUSES:
            blockers.append("sink_not_success")
        if load_status.strip().lower() != "committed":
            blockers.append("load_not_committed")
        if not idempotency_passed:
            blockers.append("idempotency_failed")
        return CDCCommitDecision(can_commit=not blockers, next_offset=next_offset, blockers=tuple(blockers))
