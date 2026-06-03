"""
MQTT 연결용 토큰 획득 추상화 (stdlib만 사용).

AT(토큰 발급) HTTP 계약은 배포 환경에 맞게 ``HttpTokenSource`` 의
요청 본문·헤더를 조정한다.
"""

from __future__ import annotations

import json
import logging
import ssl
import urllib.error
import urllib.request
from typing import Any, Callable, Optional, Protocol, Union

logger = logging.getLogger(__name__)


class TokenSource(Protocol):
    """연결 시점에 MQTT용 비밀 문자열(JWT 등)을 돌려주는 소스."""

    def acquire(self) -> str:
        """브로커 username 등에 실을 토큰 문자열."""


class HttpTokenSource:
    """
    HTTP POST로 AT 등에서 토큰(JSON 응답의 ``access_token`` 필드)을 받아온다.

    응답 예: ``{"access_token": "..."}`` — 다른 스키마면 서브클래스에서
    ``_parse_token_body`` 를 오버라이드한다.

    ``token_provider`` 인자에 인스턴스를 넘기면 ``__call__`` 이 ``acquire`` 를 호출하므로
    ``MaasClient`` / ``MaasClientAsync`` 와 그대로 호환된다.
    """

    def __init__(
        self,
        url: str,
        *,
        payload: Optional[bytes] = None,
        headers: Optional[dict[str, str]] = None,
        timeout: float = 30.0,
        ssl_context: Optional[ssl.SSLContext] = None,
    ) -> None:
        """
        Args:
            url: 토큰 발급 엔드포인트 (https 권장).
            payload: POST 본문. None이면 빈 본문.
            headers: 추가 HTTP 헤더 (예: Authorization, Content-Type).
            timeout: 요청 타임아웃(초).
            ssl_context: TLS 검증 옵션 등. None이면 기본 컨텍스트.
        """
        self._url = url
        self._payload = payload
        self._headers = dict(headers or {})
        self._timeout = timeout
        self._ssl_context = ssl_context

    def acquire(self) -> str:
        """HTTP POST 후 응답 본문에서 토큰 문자열을 추출한다."""
        req = urllib.request.Request(
            self._url,
            data=self._payload,
            method="POST",
            headers=self._headers,
        )
        try:
            with urllib.request.urlopen(
                req, timeout=self._timeout, context=self._ssl_context
            ) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as exc:
            logger.warning("토큰 발급 HTTP 오류: %s", exc.code)
            raise RuntimeError(f"토큰 발급 HTTP 오류: {exc.code}") from exc
        except urllib.error.URLError as exc:
            logger.warning("토큰 발급 네트워크 오류: %s", exc.reason)
            raise RuntimeError(f"토큰 발급 실패: {exc.reason}") from exc
        return self._parse_token_body(raw)

    def _parse_token_body(self, raw: bytes) -> str:
        """기본: JSON ``access_token`` 필드."""
        try:
            data: Any = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise RuntimeError("토큰 응답 JSON 파싱 실패") from exc
        if isinstance(data, dict) and "access_token" in data:
            token = data["access_token"]
            if isinstance(token, str):
                return token
        raise RuntimeError(
            "토큰 응답에 access_token 문자열이 없습니다. 스키마에 맞게 서브클래스를 사용하세요."
        )

    def __call__(self) -> str:
        """``token_provider`` 콜백과 동일한 사용."""
        return self.acquire()


# ``token_provider`` 에 넘길 수 있는 형태: 콜백 또는 ``HttpTokenSource`` 등 ``__call__`` 제공자
TokenProvider = Union[Callable[[], str], HttpTokenSource]
