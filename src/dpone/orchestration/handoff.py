"""Scheduler handoff snippets for orchestrated dpone runs."""

from __future__ import annotations

import shlex
from dataclasses import asdict, dataclass


@dataclass(frozen=True, slots=True)
class SchedulerHandoff:
    cron_command: str
    airflow_task: str
    dagster_asset: str
    kubernetes_cronjob: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


class SchedulerHandoffBuilder:
    """Builds copy-paste scheduler handoff snippets without owning scheduling."""

    def build(
        self,
        *,
        manifest_path: str,
        selector: str | None,
        run_id: str | None,
        retry_attempts: int,
        retry_backoff_seconds: float,
        concurrency_key: str | None = None,
        lock_dir: str | None = None,
        state_dir: str | None = None,
        output_dir: str | None = None,
        lock_ttl_seconds: int | None = None,
        resume_policy: str | None = None,
    ) -> SchedulerHandoff:
        command = self._command(
            manifest_path=manifest_path,
            selector=selector,
            run_id=run_id,
            retry_attempts=retry_attempts,
            retry_backoff_seconds=retry_backoff_seconds,
            concurrency_key=concurrency_key,
            lock_dir=lock_dir,
            state_dir=state_dir,
            output_dir=output_dir,
            lock_ttl_seconds=lock_ttl_seconds,
            resume_policy=resume_policy,
        )
        return SchedulerHandoff(
            cron_command=command,
            airflow_task=self._airflow(command),
            dagster_asset=self._dagster(command),
            kubernetes_cronjob=self._kubernetes_cronjob(command),
        )

    @staticmethod
    def _command(
        *,
        manifest_path: str,
        selector: str | None,
        run_id: str | None,
        retry_attempts: int,
        retry_backoff_seconds: float,
        concurrency_key: str | None,
        lock_dir: str | None,
        state_dir: str | None,
        output_dir: str | None,
        lock_ttl_seconds: int | None,
        resume_policy: str | None,
    ) -> str:
        parts = ["dpone", "orchestrate", "run", "--manifest", manifest_path]
        if selector:
            parts.extend(["--selector", selector])
        if run_id:
            parts.extend(["--run-id", run_id])
        if retry_attempts:
            parts.extend(["--retry-attempts", str(retry_attempts)])
        if retry_backoff_seconds:
            parts.extend(["--retry-backoff-seconds", str(retry_backoff_seconds)])
        if concurrency_key:
            parts.extend(["--concurrency-key", concurrency_key])
        if lock_dir:
            parts.extend(["--lock-dir", lock_dir])
        if state_dir:
            parts.extend(["--state-dir", state_dir])
        if output_dir:
            parts.extend(["--output-dir", output_dir])
        if lock_ttl_seconds:
            parts.extend(["--lock-ttl-seconds", str(lock_ttl_seconds)])
        if resume_policy:
            parts.extend(["--resume-policy", resume_policy])
        parts.extend(["--format", "json"])
        return " ".join(shlex.quote(part) for part in parts)

    @staticmethod
    def _airflow(command: str) -> str:
        return f"BashOperator(task_id='dpone_run', bash_command={command!r})"

    @staticmethod
    def _dagster(command: str) -> str:
        return f"@asset\ndef dpone_run(context):\n    context.log.info({command!r})"

    @staticmethod
    def _kubernetes_cronjob(command: str) -> str:
        return "\n".join(
            [
                "apiVersion: batch/v1",
                "kind: CronJob",
                "metadata:",
                "  name: dpone-run",
                "spec:",
                '  schedule: "0 * * * *"',
                "  jobTemplate:",
                "    spec:",
                "      template:",
                "        spec:",
                "          restartPolicy: Never",
                "          containers:",
                "            - name: dpone",
                "              image: your-dpone-runtime:latest",
                '              command: ["/bin/sh", "-lc"]',
                f"              args: [{command!r}]",
            ]
        )
