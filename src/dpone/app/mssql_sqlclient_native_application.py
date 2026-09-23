"""Explicit process bootstrap for the opt-in SqlClient native route."""

from collections.abc import Callable

from dpone.app.mssql_sqlclient_native_deployment_composition import (
    SqlClientNativeDeployment,
    compose_sqlclient_native_application,
)
from dpone.ports.process_runner import register_process_runner
from dpone.runtime.bootstrap_runner import DefaultProcessRunner

SqlClientNativeDeploymentFactory = Callable[[object], SqlClientNativeDeployment]


def compose_sqlclient_native_process_runner(
    deployment_factory: SqlClientNativeDeploymentFactory,
) -> DefaultProcessRunner:
    """Bind explicit deployment DI to the ordinary ETL process runner."""
    if not callable(deployment_factory):
        raise ValueError("mssql_native.sqlclient_deployment_factory_invalid")

    def runtime_factory(process_config: object):
        deployment = deployment_factory(process_config)
        if type(deployment) is not SqlClientNativeDeployment:
            raise ValueError("mssql_native.sqlclient_deployment_factory_invalid")
        return compose_sqlclient_native_application(deployment)

    return DefaultProcessRunner(native_runtime_factory=runtime_factory)


def install_sqlclient_native_process_runner(deployment_factory: SqlClientNativeDeploymentFactory) -> None:
    """Explicitly activate SqlClient for ordinary ``ETLProcess.run`` calls."""
    register_process_runner(compose_sqlclient_native_process_runner(deployment_factory))


__all__ = (
    "SqlClientNativeDeploymentFactory",
    "compose_sqlclient_native_application",
    "compose_sqlclient_native_process_runner",
    "install_sqlclient_native_process_runner",
)
