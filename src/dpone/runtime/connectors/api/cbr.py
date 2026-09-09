"""Public XML connector for the Central Bank of Russia (CBR) API."""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from typing import Any

import requests

from dpone.runtime.support.cbr_xml_parser import parse_xml_daily, resolve_date_option


class CbrConnector:
    """HTTP client for the public XML API of the Central Bank of Russia.

    The API does not require authentication, so this connector is instantiated
    directly (no Vault integration).
    """

    BASE_URL = "https://www.cbr.ru/scripts"
    DAILY_ENDPOINT = "XML_daily.asp"

    def __init__(
        self,
        *,
        base_url: str = BASE_URL,
        timeout: int = 60,
        retries: int = 3,
        retry_delay: float = 2.0,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.retry_delay = retry_delay
        self._session = session
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
        return self._session

    def close(self) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None

    def _build_url(self, endpoint: str = DAILY_ENDPOINT) -> str:
        return f"{self.base_url}/{endpoint.lstrip('/')}"

    def get_xml_daily(self, day: date | None = None) -> str:
        """Fetches raw XML for a given requested day."""

        params: dict[str, str] = {}
        if day is not None:
            params["date_req"] = day.strftime("%d/%m/%Y")

        last_exc: BaseException | None = None
        for attempt in range(1, self.retries + 1):
            try:
                response = self.session.get(
                    self._build_url(),
                    params=params,
                    timeout=self.timeout,
                )
                if response.status_code != 200:
                    raise RuntimeError(
                        f"CBR API returned HTTP {response.status_code} for day {day}. Body: {response.text[:200]}"
                    )
                return response.text
            except RuntimeError:
                raise
            except Exception as exc:  # pragma: no cover - defensive/retry path
                last_exc = exc
                if attempt < self.retries:
                    time.sleep(self.retry_delay * attempt)
        raise RuntimeError(f"CBR API is unavailable after {self.retries} attempts. Last error: {last_exc}")

    def health_check(self) -> bool:
        try:
            self.get_xml_daily()
            return True
        except Exception as exc:  # pragma: no cover - defensive
            self.logger.warning("CBR health_check failed: %s", exc)
            return False

    def get_resources(
        self,
        resource_type: str,
        filters: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Iterator[dict[str, Any]]:
        """Yields normalized rows for ``xml_daily_asp``.

        Supports either a single day (``day``) or a date range
        (``start_date`` / ``end_date``).
        """

        if resource_type != "xml_daily_asp":
            raise ValueError(
                f"CBR connector does not support resource={resource_type!r}. Supported resource: 'xml_daily_asp'"
            )

        params = dict(filters or {})
        params.update(kwargs)
        day = resolve_date_option(params.pop("day", None))
        start_date = resolve_date_option(params.pop("start_date", None))
        end_date = resolve_date_option(params.pop("end_date", None))
        if day is not None and (start_date is not None or end_date is not None):
            raise ValueError("Use either 'day' or 'start_date'/'end_date' for CBR resource requests")
        if start_date is not None:
            final_end = end_date or datetime.now(UTC).date()
            if start_date > final_end:
                raise ValueError(f"CBR date range is invalid: {start_date} > {final_end}")
            current = start_date
            while current <= final_end:
                yield from parse_xml_daily(self.get_xml_daily(current), loaded_at_utc=datetime.now(UTC))
                current += timedelta(days=1)
            return
        yield from parse_xml_daily(self.get_xml_daily(day), loaded_at_utc=datetime.now(UTC))


__all__ = ["CbrConnector"]
