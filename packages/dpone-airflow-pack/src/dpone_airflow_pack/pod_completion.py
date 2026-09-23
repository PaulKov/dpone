"""Status-only completion polling when Kubernetes live logs are unavailable."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


def await_base_container_completion(
    operator: Any,
    *,
    pod: Any,
    read_pod: Callable[..., Any],
    sleep: Callable[[float], None],
) -> None:
    """Wait for base termination without hanging on a pod that cannot start it.

    Keep the provider's read retries, configured polling interval and caller's
    execution timeout/cancellation. Publish every fresh snapshot to the operator
    before making a decision so its cleanup sees the real pod state. A terminated
    base still delegates outcome/exit-code and XCom policy to the provider.

    Diagnostics contain only fixed labels and bounded numeric exit codes: pod
    names, termination messages, reasons and specifications are never rendered.
    """

    container_name = str(getattr(operator, "base_container_name", None) or "base")
    polling_time = getattr(operator, "base_container_status_polling_interval", 1)
    while True:
        remote_pod = read_pod(pod=pod)
        operator.remote_pod = remote_pod
        status = getattr(remote_pod, "status", None)
        phase = getattr(status, "phase", None)
        for container in getattr(status, "container_statuses", None) or ():
            if container.name == container_name and getattr(container.state, "terminated", None) is not None:
                return
        _raise_failed_init(remote_pod)
        if phase in {"Failed", "Succeeded"}:
            raise RuntimeError(f"DPONE_KPO_POD_TERMINATED phase={phase} base_terminated=false")
        sleep(polling_time)


def _raise_failed_init(pod: Any) -> None:
    """Reject failed one-shot init containers, allowing Kubernetes-managed retries."""

    status = getattr(pod, "status", None)
    spec = getattr(pod, "spec", None)
    if getattr(status, "phase", None) != "Failed" and getattr(spec, "restart_policy", None) != "Never":
        return
    restartable = {
        container.name
        for container in getattr(spec, "init_containers", None) or ()
        if getattr(container, "restart_policy", None) == "Always"
    }
    for container in getattr(status, "init_container_statuses", None) or ():
        terminated = getattr(getattr(container, "state", None), "terminated", None)
        exit_code = getattr(terminated, "exit_code", None)
        if container.name in restartable or terminated is None or exit_code == 0:
            continue
        bounded_code = (
            str(exit_code) if type(exit_code) is int and -2147483648 <= exit_code <= 2147483647 else "unknown"
        )
        raise RuntimeError(f"DPONE_KPO_INIT_CONTAINER_FAILED exit_code={bounded_code}")
