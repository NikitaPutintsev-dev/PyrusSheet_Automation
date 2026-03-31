from __future__ import annotations

import logging
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

AUTH_URL = "https://accounts.pyrus.com/api/v4/auth"
DEFAULT_API_BASE = "https://api.pyrus.com/v4/"


class PyrusApiError(Exception):
    def __init__(self, message: str, status_code: int | None = None, body: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


def _is_retryable_status(status: int) -> bool:
    return status == 429 or status >= 500


class PyrusClient:
    def __init__(
        self,
        login: str,
        security_key: str,
        api_base: str | None = None,
        timeout: float = 30.0,
    ):
        self._login = login
        self._security_key = security_key
        self._api_base = (api_base or DEFAULT_API_BASE).rstrip("/") + "/"
        self._timeout = timeout
        self._token: str | None = None
        self._client = httpx.Client(timeout=timeout)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> PyrusClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def _auth(self) -> None:
        r = self._client.post(
            AUTH_URL,
            json={"login": self._login, "security_key": self._security_key},
            headers={"Content-Type": "application/json"},
        )
        if r.status_code >= 400:
            raise PyrusApiError(f"auth failed: {r.text}", status_code=r.status_code, body=r.text)
        data = r.json()
        self._token = data["access_token"]
        if "api_url" in data and data["api_url"]:
            base = str(data["api_url"]).rstrip("/") + "/"
            self._api_base = base
        logger.info("Pyrus auth ok, api_base=%s", self._api_base)

    def _headers(self) -> dict[str, str]:
        if not self._token:
            self._auth()
        return {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    def create_task(self, body: dict[str, Any], max_attempts: int = 3) -> dict[str, Any]:
        url = f"{self._api_base}tasks"
        last_err: Exception | None = None
        for attempt in range(max_attempts):
            delay = 2**attempt
            try:
                r = self._client.post(url, json=body, headers=self._headers())
            except httpx.RequestError as e:
                last_err = e
                logger.warning("Pyrus request error attempt %s: %s", attempt + 1, e)
                if attempt < max_attempts - 1:
                    time.sleep(delay)
                continue

            if r.status_code == 401:
                logger.info("Pyrus token rejected, re-authenticating")
                self._token = None
                try:
                    r = self._client.post(url, json=body, headers=self._headers())
                except httpx.RequestError as e:
                    last_err = e
                    if attempt < max_attempts - 1:
                        time.sleep(delay)
                    continue

            if r.status_code < 400:
                return r.json()

            if _is_retryable_status(r.status_code) and attempt < max_attempts - 1:
                logger.warning(
                    "Pyrus HTTP %s, retry in %ss: %s", r.status_code, delay, r.text[:500]
                )
                time.sleep(delay)
                last_err = PyrusApiError(r.text, status_code=r.status_code, body=r.text)
                continue

            raise PyrusApiError(r.text, status_code=r.status_code, body=r.text)

        if last_err:
            if isinstance(last_err, PyrusApiError):
                raise last_err
            raise PyrusApiError(str(last_err)) from last_err
        raise PyrusApiError("create_task failed with no response")
