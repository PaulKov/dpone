"""Lazy, metadata-only Kubernetes inventory and conditional-delete adapter."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol
from urllib.parse import quote

from dpone.adapters.kubernetes_metadata_values import bounded_text, metadata_timestamp, string_mapping
from dpone.ports.kubernetes_metadata import (
    KubernetesMetadataApiError,
    KubernetesMetadataSnapshot,
    KubernetesMetadataTransport,
)

PARTIAL_METADATA_ACCEPT = "application/json;as=PartialObjectMetadataList;g=meta.k8s.io;v=v1"
_MAX_PAGES = 10_000
KUBERNETES_METADATA_MAX_ITEMS = 10_000
KUBERNETES_METADATA_MAX_PAGE_BYTES = 8 * 1024 * 1024
KUBERNETES_METADATA_MAX_TOTAL_BYTES = 32 * 1024 * 1024
_MAX_CONTINUE_TOKEN_BYTES = 4 * 1024
_REQUEST_TIMEOUT = (5, 30)
_FORBIDDEN_RESPONSE_KEYS = frozenset({"data", "stringData", "spec", "status"})


@dataclass(slots=True)
class KubernetesMetadataReadBudget:
    """One shared item/byte budget across related metadata queries."""

    remaining_items: int = KUBERNETES_METADATA_MAX_ITEMS
    remaining_bytes: int = KUBERNETES_METADATA_MAX_TOTAL_BYTES

    def consume(self, *, items: int, encoded_bytes: int) -> None:
        if items < 0 or encoded_bytes < 0 or items > self.remaining_items or encoded_bytes > self.remaining_bytes:
            raise invalid_inventory()
        self.remaining_items -= items
        self.remaining_bytes -= encoded_bytes


class _KubernetesApiClient(Protocol):
    def call_api(self, resource_path: str, method: str, **kwargs: object) -> object: ...


class _KubernetesCoreApi(Protocol):
    def delete_namespaced_secret(self, *, name: str, namespace: str, body: object, **kwargs: object) -> object: ...

    def delete_namespaced_pod(self, *, name: str, namespace: str, body: object, **kwargs: object) -> object: ...


class KubernetesPartialMetadataClient:
    """Perform one bounded metadata-only list with optional expiry restart."""

    def __init__(self, *, transport: KubernetesMetadataTransport) -> None:
        self._transport = transport

    def list_metadata(
        self,
        *,
        resource: str,
        namespace: str,
        label_selector: str,
        field_selector: str | None,
        page_size: int,
        restart_on_expired: bool = False,
        budget: KubernetesMetadataReadBudget | None = None,
    ) -> tuple[KubernetesMetadataSnapshot, ...]:
        read_budget = budget or KubernetesMetadataReadBudget()
        restarts = 1 if restart_on_expired else 0
        while True:
            try:
                return self._list_once(
                    resource=resource,
                    namespace=namespace,
                    label_selector=label_selector,
                    field_selector=field_selector,
                    page_size=page_size,
                    budget=read_budget,
                )
            except KubernetesMetadataApiError as exc:
                if exc.status == 410 and restarts:
                    restarts -= 1
                    continue
                raise

    def _list_once(
        self,
        *,
        resource: str,
        namespace: str,
        label_selector: str,
        field_selector: str | None,
        page_size: int,
        budget: KubernetesMetadataReadBudget,
    ) -> tuple[KubernetesMetadataSnapshot, ...]:
        snapshots: list[KubernetesMetadataSnapshot] = []
        token: str | None = None
        seen_tokens: set[str] = set()
        for _ in range(_MAX_PAGES):
            if budget.remaining_items <= 0 or budget.remaining_bytes <= 0:
                raise invalid_inventory()
            page_limit = min(page_size, budget.remaining_items)
            kwargs: dict[str, object] = {
                "resource": resource,
                "namespace": namespace,
                "label_selector": label_selector,
                "limit": page_limit,
                "continue_token": token,
                "accept": PARTIAL_METADATA_ACCEPT,
            }
            if field_selector is not None:
                kwargs["field_selector"] = field_selector
            try:
                page = self._transport.list_metadata_page(**kwargs)  # type: ignore[arg-type]
            except KubernetesMetadataApiError:
                raise
            except Exception as exc:  # noqa: BLE001 - vendor errors are redacted here.
                raise api_error("list", exc) from None
            page_bytes = encoded_mapping_size(page)
            parsed = parse_metadata_page(page, resource=resource)
            if len(parsed) > page_limit:
                raise invalid_inventory()
            budget.consume(items=len(parsed), encoded_bytes=page_bytes)
            snapshots.extend(parsed)
            token = continuation_token(page)
            if token is None:
                return tuple(snapshots)
            if token in seen_tokens:
                raise invalid_inventory()
            seen_tokens.add(token)
        raise invalid_inventory()


class KubernetesApiMetadataTransport:
    """Thin raw Kubernetes SDK transport used by metadata cleanup adapters."""

    def __init__(
        self,
        *,
        api_client: _KubernetesApiClient,
        core_api: _KubernetesCoreApi,
        credential_mode: str = "external",
    ) -> None:
        self._api_client = api_client
        self._core_api = core_api
        self.credential_mode = credential_mode

    def list_metadata_page(
        self,
        *,
        resource: str,
        namespace: str,
        label_selector: str,
        limit: int,
        continue_token: str | None,
        accept: str,
        field_selector: str | None = None,
    ) -> dict[str, object]:
        query_params: list[tuple[str, object]] = [("labelSelector", label_selector), ("limit", limit)]
        if continue_token is not None:
            query_params.append(("continue", continue_token))
        if field_selector is not None:
            query_params.append(("fieldSelector", field_selector))
        try:
            response = self._api_client.call_api(
                f"/api/v1/namespaces/{quote(namespace, safe='')}/{resource}",
                "GET",
                query_params=query_params,
                header_params={"Accept": accept},
                response_type="object",
                auth_settings=["BearerToken"],
                _return_http_data_only=True,
                _preload_content=False,
                _request_timeout=_REQUEST_TIMEOUT,
            )
        except Exception as exc:  # noqa: BLE001 - vendor errors are redacted here.
            raise api_error("list", exc) from None
        return response_mapping(response)

    def delete_secret(self, *, namespace: str, name: str, uid: str, resource_version: str) -> None:
        self._delete(
            resource="secret",
            namespace=namespace,
            name=name,
            uid=uid,
            resource_version=resource_version,
        )

    def delete_pod(self, *, namespace: str, name: str, uid: str, resource_version: str) -> None:
        self._delete(
            resource="pod",
            namespace=namespace,
            name=name,
            uid=uid,
            resource_version=resource_version,
        )

    def _delete(self, *, resource: str, namespace: str, name: str, uid: str, resource_version: str) -> None:
        try:
            from kubernetes.client import V1DeleteOptions, V1Preconditions

            body = V1DeleteOptions(preconditions=V1Preconditions(uid=uid, resource_version=resource_version))
            method = (
                self._core_api.delete_namespaced_secret
                if resource == "secret"
                else self._core_api.delete_namespaced_pod
            )
            method(name=name, namespace=namespace, body=body, _request_timeout=_REQUEST_TIMEOUT)
        except Exception as exc:  # noqa: BLE001 - vendor errors are redacted here.
            raise api_error("delete", exc) from None


def build_kubernetes_metadata_transport(
    *,
    auth_mode: str = "auto",
    kube_context: str | None = None,
) -> KubernetesApiMetadataTransport:
    if auth_mode not in {"auto", "in-cluster", "kubeconfig"}:
        raise ValueError("kube_auth must be auto, in-cluster, or kubeconfig")
    if auth_mode == "in-cluster" and kube_context is not None:
        raise ValueError("kube_context cannot be used with in-cluster authentication")
    try:
        from kubernetes import client, config
        from kubernetes.config.config_exception import ConfigException
    except ModuleNotFoundError:
        raise KubernetesMetadataApiError(operation="configure", status=None, reason="sdk_unavailable") from None
    try:
        resolved_mode = auth_mode
        if auth_mode == "in-cluster":
            config.load_incluster_config()
        elif auth_mode == "kubeconfig" or kube_context is not None:
            config.load_kube_config(context=kube_context)
            resolved_mode = "kubeconfig"
        else:
            try:
                config.load_incluster_config()
                resolved_mode = "in-cluster"
            except ConfigException:
                config.load_kube_config(context=kube_context)
                resolved_mode = "kubeconfig"
        api_client = client.ApiClient()
        return KubernetesApiMetadataTransport(
            api_client=api_client,
            core_api=client.CoreV1Api(api_client),
            credential_mode=resolved_mode,
        )
    except KubernetesMetadataApiError:
        raise
    except Exception as exc:  # noqa: BLE001 - auth/config errors are redacted.
        raise api_error("configure", exc) from None


def parse_metadata_page(
    page: Mapping[str, object],
    *,
    resource: str,
) -> tuple[KubernetesMetadataSnapshot, ...]:
    if page.get("kind") != "PartialObjectMetadataList" or contains_forbidden_object_fields(page):
        raise invalid_inventory()
    items = page.get("items")
    if not isinstance(items, list):
        raise invalid_inventory()
    snapshots: list[KubernetesMetadataSnapshot] = []
    for item in items:
        if not isinstance(item, Mapping) or item.get("kind") != "PartialObjectMetadata":
            raise invalid_inventory()
        metadata = item.get("metadata")
        if not isinstance(metadata, Mapping):
            raise invalid_inventory()
        try:
            snapshots.append(
                KubernetesMetadataSnapshot(
                    resource=resource,
                    name=bounded_text(metadata.get("name"), max_bytes=253),
                    uid=bounded_text(metadata.get("uid"), max_bytes=256),
                    resource_version=bounded_text(metadata.get("resourceVersion"), max_bytes=256),
                    creation_timestamp=metadata_timestamp(metadata, "creationTimestamp"),
                    labels=string_mapping(metadata.get("labels")),
                    annotations=string_mapping(metadata.get("annotations")),
                    deletion_timestamp=metadata_timestamp(metadata, "deletionTimestamp"),
                )
            )
        except ValueError:
            raise invalid_inventory() from None
    return tuple(snapshots)


def continuation_token(page: Mapping[str, object]) -> str | None:
    metadata = page.get("metadata")
    if not isinstance(metadata, Mapping):
        raise invalid_inventory()
    token = metadata.get("continue")
    if token in {None, ""}:
        return None
    if not isinstance(token, str) or len(token.encode("utf-8")) > _MAX_CONTINUE_TOKEN_BYTES:
        raise invalid_inventory()
    return token


def contains_forbidden_object_fields(page: Mapping[str, object]) -> bool:
    if any(key in _FORBIDDEN_RESPONSE_KEYS for key in page):
        return True
    items = page.get("items")
    return isinstance(items, list) and any(
        isinstance(item, Mapping) and any(key in _FORBIDDEN_RESPONSE_KEYS for key in item) for item in items
    )


def response_mapping(response: object) -> dict[str, object]:
    if isinstance(response, Mapping):
        payload = dict(response)
        if encoded_mapping_size(payload) > KUBERNETES_METADATA_MAX_PAGE_BYTES:
            raise invalid_inventory()
        return payload
    reader = getattr(response, "read", None)
    raw = reader(KUBERNETES_METADATA_MAX_PAGE_BYTES + 1) if callable(reader) else getattr(response, "data", response)
    close = getattr(response, "release_conn", None)
    if callable(close):
        close()
    if isinstance(raw, bytes):
        if len(raw) > KUBERNETES_METADATA_MAX_PAGE_BYTES:
            raise invalid_inventory()
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        if len(raw.encode("utf-8")) > KUBERNETES_METADATA_MAX_PAGE_BYTES:
            raise invalid_inventory()
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            raise invalid_inventory() from None
        if isinstance(payload, Mapping):
            return dict(payload)
    raise invalid_inventory()


def encoded_mapping_size(value: Mapping[str, object]) -> int:
    try:
        return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    except (TypeError, ValueError):
        raise invalid_inventory() from None


def invalid_inventory() -> KubernetesMetadataApiError:
    return KubernetesMetadataApiError(operation="list", status=None, reason="invalid_metadata_response")


def api_error(operation: str, exc: BaseException) -> KubernetesMetadataApiError:
    status = safe_http_status(exc)
    if status in {401, 403}:
        reason = "access_denied"
    elif status == 406:
        reason = "metadata_only_unsupported"
    elif status == 410:
        reason = "inventory_expired"
    else:
        reason = "api_failure"
    return KubernetesMetadataApiError(operation=operation, status=status, reason=reason)


def safe_http_status(exc: BaseException) -> int | None:
    value = getattr(exc, "status", None)
    if isinstance(value, int) and not isinstance(value, bool) and 100 <= value <= 599:
        return value
    return None


__all__ = [
    "KubernetesMetadataReadBudget",
    "KUBERNETES_METADATA_MAX_ITEMS",
    "KUBERNETES_METADATA_MAX_PAGE_BYTES",
    "KUBERNETES_METADATA_MAX_TOTAL_BYTES",
    "PARTIAL_METADATA_ACCEPT",
    "KubernetesApiMetadataTransport",
    "KubernetesPartialMetadataClient",
    "build_kubernetes_metadata_transport",
]
