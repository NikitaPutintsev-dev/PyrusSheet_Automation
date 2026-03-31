from unittest.mock import MagicMock, patch

import pytest

from pyrus_sheet_sync.pyrus import PyrusApiError, PyrusClient


def _ok_json(data):
    r = MagicMock()
    r.status_code = 200
    r.json.return_value = data
    r.text = ""
    return r


def _err_json(status: int, text: str = "bad"):
    r = MagicMock()
    r.status_code = status
    r.text = text
    return r


def test_create_task_success_first_call() -> None:
    auth = _ok_json(
        {"access_token": "tok", "api_url": "https://api.pyrus.com/v4/"}
    )
    task = _ok_json({"task": {"id": 777}})

    with patch("pyrus_sheet_sync.pyrus.httpx.Client") as Cls:
        http = MagicMock()
        Cls.return_value = http
        http.post.side_effect = [auth, task]

        with PyrusClient("bot@x", "secret") as c:
            out = c.create_task({"form_id": 1, "fields": []})

        assert out["task"]["id"] == 777
        assert http.post.call_count == 2


def test_create_task_retries_on_500() -> None:
    auth = _ok_json({"access_token": "tok", "api_url": "https://api.pyrus.com/v4/"})
    fail = _err_json(500, "srv")
    ok = _ok_json({"task": {"id": 1}})

    with patch("pyrus_sheet_sync.pyrus.httpx.Client") as Cls:
        with patch("pyrus_sheet_sync.pyrus.time.sleep", autospec=True):
            http = MagicMock()
            Cls.return_value = http
            http.post.side_effect = [auth, fail, ok]

            with PyrusClient("bot@x", "secret") as c:
                out = c.create_task({"form_id": 1, "fields": []})

            assert out["task"]["id"] == 1
            assert http.post.call_count == 3


def test_create_task_no_retry_on_422() -> None:
    auth = _ok_json({"access_token": "tok", "api_url": "https://api.pyrus.com/v4/"})
    bad = _err_json(422, "validation")

    with patch("pyrus_sheet_sync.pyrus.httpx.Client") as Cls:
        http = MagicMock()
        Cls.return_value = http
        http.post.side_effect = [auth, bad]

        with PyrusClient("bot@x", "secret") as c:
            with pytest.raises(PyrusApiError) as ei:
                c.create_task({"form_id": 1, "fields": []})
            assert ei.value.status_code == 422

        assert http.post.call_count == 2
