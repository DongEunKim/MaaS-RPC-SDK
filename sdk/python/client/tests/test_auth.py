"""HttpTokenSource 단위 테스트."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from maas_client.auth import HttpTokenSource


def test_http_token_source_parses_access_token() -> None:
    body = json.dumps({"access_token": "jwt-here"}).encode("utf-8")
    mock_resp = MagicMock()
    mock_resp.read.return_value = body
    mock_cm = MagicMock()
    mock_cm.__enter__.return_value = mock_resp
    mock_cm.__exit__.return_value = None

    with patch("maas_client.auth.urllib.request.urlopen", return_value=mock_cm):
        src = HttpTokenSource("https://at.example/token", payload=b"{}")
        assert src.acquire() == "jwt-here"


def test_http_token_source_missing_field_raises() -> None:
    body = json.dumps({"token": "x"}).encode("utf-8")
    mock_resp = MagicMock()
    mock_resp.read.return_value = body
    mock_cm = MagicMock()
    mock_cm.__enter__.return_value = mock_resp
    mock_cm.__exit__.return_value = None

    with patch("maas_client.auth.urllib.request.urlopen", return_value=mock_cm):
        src = HttpTokenSource("https://at.example/token")
        with pytest.raises(RuntimeError, match="access_token"):
            src.acquire()


def test_http_token_source_callable_alias() -> None:
    body = json.dumps({"access_token": "z"}).encode("utf-8")
    mock_resp = MagicMock()
    mock_resp.read.return_value = body
    mock_cm = MagicMock()
    mock_cm.__enter__.return_value = mock_resp
    mock_cm.__exit__.return_value = None

    with patch("maas_client.auth.urllib.request.urlopen", return_value=mock_cm):
        src = HttpTokenSource("https://at.example/token")
        assert src() == "z"
