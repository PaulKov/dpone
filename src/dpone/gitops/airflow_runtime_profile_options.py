from __future__ import annotations

from collections.abc import Iterable, Mapping

from dpone.gitops.models import GitOpsIssue
from dpone.gitops.paths import GitOpsPathValidationError, safe_relative_path


class GitOpsAirflowRuntimeProfileOptions:
    def __init__(
        self,
        *,
        image: str,
        image_digest: str | None,
        namespace: str,
        service_account: str,
        resource_requests: dict[str, str],
        resource_limits: dict[str, str],
        artifact_sink_kind: str,
        artifact_sink_path: str | None,
        runner_policy: str,
        env: tuple[dict[str, str], ...],
        labels: dict[str, str],
        annotations: dict[str, str],
        outcome_mode: str,
    ) -> None:
        self.image = image
        self.image_digest = image_digest
        self.namespace = namespace
        self.service_account = service_account
        self.resource_requests = resource_requests
        self.resource_limits = resource_limits
        self.artifact_sink_kind = artifact_sink_kind
        self.artifact_sink_path = artifact_sink_path
        self.runner_policy = runner_policy
        self.env = env
        self.labels = labels
        self.annotations = annotations
        self.outcome_mode = outcome_mode


def parse_runtime_profile_options(
    args: object,
) -> tuple[GitOpsAirflowRuntimeProfileOptions, tuple[GitOpsIssue, ...]]:
    labels, label_blockers = _parse_key_values(getattr(args, "label", ()), source="--label")
    annotations, annotation_blockers = _parse_key_values(getattr(args, "annotation", ()), source="--annotation")
    env, env_blockers = _parse_env(getattr(args, "env", ()))
    artifact_sink_kind, artifact_sink_path, artifact_blockers = _artifact_sink(args)
    resource_requests, resource_limits = _resources(args)
    parsed = GitOpsAirflowRuntimeProfileOptions(
        image=_text(getattr(args, "image", None)),
        image_digest=_optional_text(getattr(args, "image_digest", None)),
        namespace=_text(getattr(args, "namespace", None)) or "default",
        service_account=_text(getattr(args, "service_account", None)) or "default",
        resource_requests=resource_requests,
        resource_limits=resource_limits,
        artifact_sink_kind=artifact_sink_kind,
        artifact_sink_path=artifact_sink_path,
        runner_policy=_text(getattr(args, "runner_policy", None)) or "advisory",
        env=env,
        labels=labels,
        annotations=annotations,
        outcome_mode=_text(getattr(args, "outcome_mode", None)) or "strict_fail",
    )
    image_blockers = (
        (
            GitOpsIssue(
                code="image_required",
                message="Airflow runtime profile requires --image",
                path="--image",
                source="dpone gitops airflow runtime-profile",
            ),
        )
        if not parsed.image
        else ()
    )
    return parsed, (*label_blockers, *annotation_blockers, *env_blockers, *artifact_blockers, *image_blockers)


def _resources(args: object) -> tuple[dict[str, str], dict[str, str]]:
    requests = _clean_map(
        {
            "cpu": _optional_text(getattr(args, "cpu_request", None)),
            "memory": _optional_text(getattr(args, "memory_request", None)),
        }
    )
    limits = _clean_map(
        {
            "cpu": _optional_text(getattr(args, "cpu_limit", None)),
            "memory": _optional_text(getattr(args, "memory_limit", None)),
        }
    )
    return requests, limits


def _artifact_sink(args: object) -> tuple[str, str | None, tuple[GitOpsIssue, ...]]:
    kind = _text(getattr(args, "artifact_sink_kind", None)) or "local"
    raw_path = getattr(args, "artifact_sink_path", None)
    if raw_path is None or not str(raw_path).strip():
        return kind, None, ()
    try:
        path = safe_relative_path(raw_path, source="--artifact-sink-path")
    except GitOpsPathValidationError as exc:
        label = str(raw_path or "")
        return (
            kind,
            label,
            (GitOpsIssue(code="invalid_path", message=str(exc), path=label, source="--artifact-sink-path"),),
        )
    return kind, "." if path.as_posix() == "." else path.as_posix(), ()


def _parse_key_values(raw_values: object, *, source: str) -> tuple[dict[str, str], tuple[GitOpsIssue, ...]]:
    values: dict[str, str] = {}
    blockers: list[GitOpsIssue] = []
    for raw_value in _iterable(raw_values):
        text = str(raw_value or "").strip()
        if "=" not in text:
            blockers.append(_kv_issue(source=source, value=text))
            continue
        key, value = text.split("=", 1)
        key = key.strip()
        if not key:
            blockers.append(_kv_issue(source=source, value=text))
            continue
        values[key] = value.strip()
    return values, tuple(blockers)


def _parse_env(raw_values: object) -> tuple[tuple[dict[str, str], ...], tuple[GitOpsIssue, ...]]:
    values, blockers = _parse_key_values(raw_values, source="--env")
    return tuple({"name": key, "value": value} for key, value in values.items()), blockers


def _kv_issue(*, source: str, value: str) -> GitOpsIssue:
    return GitOpsIssue(
        code="runtime_profile_key_value_invalid",
        message=f"{source} values must use NAME=value syntax",
        path=value,
        source="dpone gitops airflow runtime-profile",
    )


def _iterable(value: object) -> Iterable[object]:
    if isinstance(value, str) or value is None:
        return (value,) if value else ()
    if isinstance(value, Iterable):
        return value
    return (value,)


def _clean_map(values: Mapping[str, str | None]) -> dict[str, str]:
    return {key: value for key, value in values.items() if value}


def _text(value: object) -> str:
    return str(value or "").strip()


def _optional_text(value: object) -> str | None:
    text = _text(value)
    return text or None


__all__ = [
    "GitOpsAirflowRuntimeProfileOptions",
    "parse_runtime_profile_options",
]
