from __future__ import annotations

import csv
import io
from typing import Any


def get_google_credentials(connector: Any, auth_loader: Any) -> Any:
    if connector._google_credentials is not None:
        return connector._google_credentials

    Request, service_account, UserCredentials = auth_loader()
    del Request

    creds = connector.credentials
    if creds.auth_type == "published_csv":
        return None
    if creds.auth_type == "service_account":
        info = creds.credentials_json or {
            "type": "service_account",
            "project_id": creds.project_id,
            "private_key_id": creds.private_key_id,
            "private_key": creds.private_key,
            "client_email": creds.client_email,
            "token_uri": creds.token_uri or "https://oauth2.googleapis.com/token",
        }
        connector._google_credentials = service_account.Credentials.from_service_account_info(info, scopes=creds.scopes)
        return connector._google_credentials
    if creds.auth_type == "oauth_refresh_token":
        connector._google_credentials = UserCredentials(
            token=None,
            refresh_token=creds.refresh_token,
            token_uri=creds.token_uri or "https://oauth2.googleapis.com/token",
            client_id=creds.client_id,
            client_secret=creds.client_secret,
            scopes=creds.scopes,
        )
        return connector._google_credentials
    raise ValueError(
        f"Unsupported Google Sheets auth_type='{creds.auth_type}'. "
        "Supported: service_account, oauth_refresh_token, published_csv"
    )


def get_access_token(connector: Any, auth_loader: Any) -> str:
    if connector.credentials.auth_type == "published_csv":
        raise ValueError("Access token is not used for auth_type='published_csv'")

    override = _access_token_override(connector)
    if override is not None:
        return str(override())

    Request, _, _ = auth_loader()
    creds = get_google_credentials(connector, auth_loader)
    if not getattr(creds, "valid", False) or getattr(creds, "token", None) is None:
        creds.refresh(Request())

    token = getattr(creds, "token", None)
    if not token:
        raise ValueError("Google access token was not obtained")
    return token


def _access_token_override(connector: Any) -> Any | None:
    candidate = getattr(connector, "_get_access_token", None)
    if not callable(candidate):
        return None
    class_method = getattr(type(connector), "_get_access_token", None)
    if getattr(candidate, "__func__", None) is class_method:
        return None
    return candidate


def request_json(
    connector: Any,
    method: str,
    url: str,
    *,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    connector._apply_rate_limit()
    auth_loader = connector.google_auth_loader
    headers = {"Accept": "application/json", "Authorization": f"Bearer {get_access_token(connector, auth_loader)}"}
    response = connector.session.request(
        method=method,
        url=url,
        params=params,
        headers=headers,
        timeout=connector.timeout,
    )
    response.raise_for_status()
    return response.json()


def download_published_csv(connector: Any, spreadsheet_id: str, worksheet_gid: str | int | None) -> list[list[Any]]:
    if worksheet_gid is None:
        raise ValueError("Для auth_type='published_csv' необходимо передать worksheet_gid")
    connector._apply_rate_limit()
    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={worksheet_gid}"
    response = connector.session.get(url, timeout=connector.timeout)
    response.raise_for_status()
    content = response.content.decode("utf-8")
    return [list(row) for row in csv.reader(io.StringIO(content))]
