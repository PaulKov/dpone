from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

DEFAULT_ARTIFACT_DIR = ".dpone/gitops/airflow"
KPO_KWARGS_FILE = "kpo-kwargs.json"
POD_SPEC_FILE = "pod-spec.yaml"
POD_CONTRACT_FILE = "pod-contract.json"

_CONTRACT_OWNED_KPO_FIELDS = frozenset({"pod_template_file", "do_xcom_push", "cmds", "arguments"})


class DponeAirflowArtifactError(RuntimeError):
    """Base error for Airflow DAG-side dpone artifact loading."""


class DponeAirflowContractError(DponeAirflowArtifactError):
    """Raised when generated dpone Airflow artifacts are missing or unsafe."""

    def __init__(self, message: str, *, blockers: tuple[dict[str, str], ...]) -> None:
        super().__init__(message)
        self.blockers = blockers


def load_dpone_kpo_kwargs(
    artifact_dir: str | Path = DEFAULT_ARTIFACT_DIR,
    *,
    task_id: str = "dpone_gitops_runtime",
    name: str | None = None,
    labels: Mapping[str, Any] | None = None,
    annotations: Mapping[str, Any] | None = None,
    operator_overrides: Mapping[str, Any] | None = None,
    validate: bool = True,
) -> dict[str, Any]:
    """Load generated KubernetesPodOperator kwargs and apply DAG-local fields.

    The generated `kpo-kwargs.json` remains the canonical contract. This helper
    only rewrites the local `pod_template_file`, keeps final XCom enabled, and
    applies caller-owned DAG metadata such as task id, labels, annotations,
    retries, pools, and queues.
    """

    root = Path(artifact_dir)
    overrides = _operator_overrides(operator_overrides)
    if validate:
        validate_dpone_artifacts(root)
    kwargs = _load_json_mapping(root / KPO_KWARGS_FILE, kind="kpo_kwargs")
    kwargs["task_id"] = task_id
    kwargs["name"] = name or task_id
    kwargs["pod_template_file"] = str(root / POD_SPEC_FILE)
    kwargs["do_xcom_push"] = True
    kwargs["labels"] = _merge_mapping(kwargs.get("labels"), labels)
    kwargs["annotations"] = _merge_mapping(kwargs.get("annotations"), annotations)
    kwargs.update(overrides)
    return kwargs


def build_dpone_gitops_task_from_artifacts(
    *,
    dag: Any,
    artifact_dir: str | Path = DEFAULT_ARTIFACT_DIR,
    task_id: str = "dpone_gitops_runtime",
    name: str | None = None,
    labels: Mapping[str, Any] | None = None,
    annotations: Mapping[str, Any] | None = None,
    operator_overrides: Mapping[str, Any] | None = None,
) -> Any:
    """Build a KubernetesPodOperator from generated dpone artifacts."""

    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

    kwargs = load_dpone_kpo_kwargs(
        artifact_dir,
        task_id=task_id,
        name=name,
        labels=labels,
        annotations=annotations,
        operator_overrides=operator_overrides,
    )
    return KubernetesPodOperator(dag=dag, **kwargs)


def validate_dpone_artifacts(
    artifact_dir: str | Path = DEFAULT_ARTIFACT_DIR,
    *,
    require_pod_contract: bool = True,
) -> dict[str, Any]:
    """Validate a generated dpone Airflow artifact directory.

    This is intentionally a parse-time safety check for DAG repositories. It
    does not parse manifests, discover sparse paths, mutate Kubernetes specs, or
    make network calls.
    """

    root = Path(artifact_dir)
    entries = _artifact_entries(root=root, require_pod_contract=require_pod_contract)
    blockers = _missing_required_blockers(entries)
    kpo_kwargs: Mapping[str, Any] = {}
    pod_spec: Mapping[str, Any] = {}
    pod_contract: Mapping[str, Any] = {}
    if not blockers:
        kpo_kwargs, pod_spec, pod_contract, parse_blockers = _load_artifacts(
            root=root,
            require_pod_contract=require_pod_contract,
        )
        blockers.extend(parse_blockers)
    if not blockers:
        blockers.extend(
            _contract_blockers(
                root=root,
                kpo_kwargs=kpo_kwargs,
                pod_spec=pod_spec,
                pod_contract=pod_contract,
                require_pod_contract=require_pod_contract,
            )
        )
    report = {
        "kind": "dpone.airflow_runtime_artifacts",
        "artifact_dir": str(root),
        "entries": entries,
        "warnings": [],
        "blockers": blockers,
    }
    if blockers:
        message = "dpone Airflow artifact directory is not ready: " + "; ".join(
            f"{item['code']} at {item['path']}" for item in blockers
        )
        raise DponeAirflowContractError(message, blockers=tuple(blockers))
    return report


def _load_artifacts(
    *,
    root: Path,
    require_pod_contract: bool,
) -> tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any], list[dict[str, str]]]:
    blockers: list[dict[str, str]] = []
    kpo_kwargs = _load_json_mapping_or_blocker(root / KPO_KWARGS_FILE, kind="kpo_kwargs", blockers=blockers)
    pod_spec = _load_yaml_mapping_or_blocker(root / POD_SPEC_FILE, blockers=blockers)
    pod_contract: Mapping[str, Any] = {}
    if require_pod_contract:
        pod_contract = _load_json_mapping_or_blocker(root / POD_CONTRACT_FILE, kind="pod_contract", blockers=blockers)
    return kpo_kwargs, pod_spec, pod_contract, blockers


def _load_json_mapping(path: Path, *, kind: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise DponeAirflowContractError(
            f"Required dpone Airflow artifact is missing: {path}",
            blockers=(_blocker(f"{kind}_missing", str(path), "Required artifact does not exist"),),
        ) from exc
    except json.JSONDecodeError as exc:
        raise DponeAirflowContractError(
            f"Required dpone Airflow artifact is not valid JSON: {path}",
            blockers=(_blocker(f"{kind}_json_invalid", str(path), exc.msg),),
        ) from exc
    if not isinstance(payload, dict):
        raise DponeAirflowContractError(
            f"Required dpone Airflow artifact must be a JSON object: {path}",
            blockers=(_blocker(f"{kind}_json_invalid", str(path), "JSON artifact must be an object"),),
        )
    return payload


def _load_json_mapping_or_blocker(
    path: Path,
    *,
    kind: str,
    blockers: list[dict[str, str]],
) -> Mapping[str, Any]:
    try:
        return _load_json_mapping(path, kind=kind)
    except DponeAirflowContractError as exc:
        blockers.extend(exc.blockers)
    return {}


def _load_yaml_mapping_or_blocker(path: Path, *, blockers: list[dict[str, str]]) -> Mapping[str, Any]:
    try:
        import yaml

        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        blockers.append(_blocker("pod_spec_missing", str(path), "Required pod spec artifact does not exist"))
        return {}
    except Exception as exc:  # noqa: BLE001 - parser differences should become contract blockers
        blockers.append(_blocker("pod_spec_yaml_invalid", str(path), f"Pod spec YAML could not be parsed: {exc}"))
        return {}
    if not isinstance(payload, Mapping):
        blockers.append(_blocker("pod_spec_yaml_invalid", str(path), "Pod spec YAML must be an object"))
        return {}
    return payload


def _artifact_entries(*, root: Path, require_pod_contract: bool) -> list[dict[str, Any]]:
    entries = [
        _entry(root / KPO_KWARGS_FILE, kind="kpo_kwargs", required=True),
        _entry(root / POD_SPEC_FILE, kind="pod_spec", required=True),
    ]
    entries.append(_entry(root / POD_CONTRACT_FILE, kind="pod_contract", required=require_pod_contract))
    return entries


def _entry(path: Path, *, kind: str, required: bool) -> dict[str, Any]:
    return {
        "path": str(path),
        "kind": kind,
        "required": required,
        "exists": path.exists(),
        "reason": "dpone Airflow runtime artifact",
    }


def _missing_required_blockers(entries: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        _blocker(f"{entry['kind']}_missing", str(entry["path"]), "Required artifact does not exist")
        for entry in entries
        if entry["required"] and not entry["exists"]
    ]


def _contract_blockers(
    *,
    root: Path,
    kpo_kwargs: Mapping[str, Any],
    pod_spec: Mapping[str, Any],
    pod_contract: Mapping[str, Any],
    require_pod_contract: bool,
) -> list[dict[str, str]]:
    blockers: list[dict[str, str]] = []
    _append_kpo_blockers(root=root, kpo_kwargs=kpo_kwargs, pod_contract=pod_contract, blockers=blockers)
    _append_pod_spec_blockers(root=root, pod_spec=pod_spec, blockers=blockers)
    if require_pod_contract:
        _append_pod_contract_blockers(root=root, pod_contract=pod_contract, blockers=blockers)
    return blockers


def _append_kpo_blockers(
    *,
    root: Path,
    kpo_kwargs: Mapping[str, Any],
    pod_contract: Mapping[str, Any],
    blockers: list[dict[str, str]],
) -> None:
    path = str(root / KPO_KWARGS_FILE)
    if kpo_kwargs.get("do_xcom_push") is not True:
        blockers.append(_blocker("kpo_xcom_push_required", path, "kpo-kwargs.json must set do_xcom_push=true"))
    if not kpo_kwargs.get("cmds"):
        blockers.append(_blocker("kpo_cmds_required", path, "kpo-kwargs.json must include runtime cmds"))
    if not kpo_kwargs.get("arguments"):
        blockers.append(_blocker("kpo_arguments_required", path, "kpo-kwargs.json must include runtime arguments"))
    pod_template = str(kpo_kwargs.get("pod_template_file") or "").strip()
    expected = str(pod_contract.get("pod_spec_path") or "").strip()
    if expected and pod_template != expected:
        blockers.append(
            _blocker("kpo_pod_template_mismatch", path, "kpo-kwargs.json pod_template_file must match pod-contract")
        )
    if not expected and not pod_template.endswith(POD_SPEC_FILE):
        blockers.append(_blocker("kpo_pod_template_required", path, "kpo-kwargs.json must reference pod-spec.yaml"))


def _append_pod_spec_blockers(
    *,
    root: Path,
    pod_spec: Mapping[str, Any],
    blockers: list[dict[str, str]],
) -> None:
    path = str(root / POD_SPEC_FILE)
    if pod_spec.get("kind") != "Pod":
        blockers.append(_blocker("pod_spec_kind_invalid", path, "pod-spec.yaml kind must be Pod"))
    containers = _mapping(pod_spec.get("spec")).get("containers")
    first_container = containers[0] if isinstance(containers, list) and containers else {}
    if _mapping(first_container).get("name") != "base":
        blockers.append(_blocker("pod_spec_base_container_missing", path, "pod-spec.yaml first container must be base"))


def _append_pod_contract_blockers(
    *,
    root: Path,
    pod_contract: Mapping[str, Any],
    blockers: list[dict[str, str]],
) -> None:
    path = str(root / POD_CONTRACT_FILE)
    if pod_contract.get("kind") != "gitops.airflow_pod_contract":
        blockers.append(
            _blocker("pod_contract_kind_invalid", path, "pod-contract.json kind must be gitops.airflow_pod_contract")
        )
    xcom = _mapping(pod_contract.get("xcom"))
    if xcom.get("return_path") != "/airflow/xcom/return.json" or xcom.get("enabled") is not True:
        blockers.append(_blocker("pod_contract_xcom_invalid", path, "pod contract must enable final XCom return.json"))


def _operator_overrides(overrides: Mapping[str, Any] | None) -> dict[str, Any]:
    resolved = dict(overrides or {})
    forbidden = sorted(_CONTRACT_OWNED_KPO_FIELDS.intersection(resolved))
    if forbidden:
        fields = ", ".join(forbidden)
        raise ValueError(f"operator_overrides cannot replace dpone contract-owned KPO fields: {fields}")
    return resolved


def _merge_mapping(base: object, extra: Mapping[str, Any] | None) -> dict[str, Any]:
    merged = dict(base) if isinstance(base, Mapping) else {}
    merged.update(dict(extra or {}))
    return merged


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _blocker(code: str, path: str, message: str) -> dict[str, str]:
    return {"code": code, "path": path, "message": message, "source": "dpone.airflow.runtime_adapter"}


__all__ = [
    "DEFAULT_ARTIFACT_DIR",
    "DponeAirflowArtifactError",
    "DponeAirflowContractError",
    "build_dpone_gitops_task_from_artifacts",
    "load_dpone_kpo_kwargs",
    "validate_dpone_artifacts",
]
