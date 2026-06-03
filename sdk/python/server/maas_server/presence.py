"""
클라이언트 단절 모니터 (LWT 기반).

클라이언트가 패턴 C/E 진입 시 ``.../offline`` 토픽을 LWT(will)로 등록하고,
비정상 단절 시 브로커가 이 토픽으로 메시지를 발행한다. 서버는 이를 감지해
독점 세션 강제 해제·스트리밍 구독 취소 콜백을 호출한다.
"""

from __future__ import annotations

import json
import logging
from typing import Callable, Optional

from . import topics as _topic_utils

logger = logging.getLogger(__name__)

DisconnectCallback = Callable[[str], None]


class OfflineMonitor:
    """
    LWT(Last Will and Testament) 기반 단절 모니터.

    클라이언트가 패턴 C/E 진입 시 ``WMT/{tt}/{svc}/{vin}/{client_id}/offline`` 을
    will로 등록하고, 비정상 단절 시 브로커가 이 토픽으로 LWT 메시지를 발행한다.
    서버는 이를 파싱해 client_id별 단절 콜백을 호출한다.
    """

    def __init__(self) -> None:
        self._on_offline_callbacks: list[DisconnectCallback] = []

    def on_offline(self, callback: DisconnectCallback) -> None:
        """단절 콜백 등록. ``callback(client_id)``."""
        self._on_offline_callbacks.append(callback)

    def matches(self, topic: str) -> bool:
        """수신 토픽이 LWT(offline) 토픽인지."""
        return _topic_utils.parse_offline(topic) is not None

    def handle_message(self, topic: str, payload: bytes) -> Optional[str]:
        """
        LWT 메시지 처리. client_id를 추출(토픽 우선, payload ``clientId`` 폴백)하고
        콜백을 호출한다. 처리한 client_id를 반환, 아니면 None.
        """
        client_id = _topic_utils.parse_offline(topic)
        if not client_id:
            try:
                data = json.loads(payload.decode("utf-8"))
                client_id = data.get("clientId", "")
            except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
                client_id = ""
        if not client_id:
            return None
        logger.debug("LWT 단절 감지: client_id=%s", client_id)
        for cb in self._on_offline_callbacks:
            try:
                cb(client_id)
            except Exception:
                logger.exception("LWT 단절 콜백 오류: client_id=%s", client_id)
        return client_id
