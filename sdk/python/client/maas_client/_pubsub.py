"""
Pub/Sub 레이어 (내부 모듈).

임의 MQTT 토픽에 대한 발행/구독을 관리한다.
"""

from __future__ import annotations

import logging
from typing import Callable, Optional

from .connection import (
    IncomingMessage,
    Mqtt5Connection,
    build_publish_properties,
    decode_payload,
    encode_payload,
)
from .models import Message

logger = logging.getLogger(__name__)

MessageHandler = Callable[[Message], None]


class PubSubManager:
    """
    단순 pub/sub 관리자.

    RPC 레이어와 독립적으로 임의 토픽에 발행하거나 구독한다.
    """

    def __init__(self, conn: Mqtt5Connection) -> None:
        self._conn = conn
        # topic → 콜백 리스트
        self._handlers: dict[str, list[MessageHandler]] = {}

    def handle_incoming(self, msg: IncomingMessage) -> bool:
        """
        수신 메시지를 등록된 pub/sub 핸들러로 라우팅.

        Returns:
            처리된 경우 True, 등록된 핸들러가 없으면 False.
        """
        handlers = self._handlers.get(msg.topic)
        if not handlers:
            # 와일드카드 토픽 구독도 처리
            for pattern, callbacks in self._handlers.items():
                if _topic_matches(pattern, msg.topic):
                    handlers = callbacks
                    break
        if not handlers:
            return False

        message = Message(
            topic=msg.topic,
            payload=msg.payload,
            qos=msg.qos,
            user_properties=list(msg.user_props.items()),
        )
        for callback in handlers:
            try:
                callback(message)
            except Exception:
                logger.exception("pub/sub 핸들러 오류: topic=%s", msg.topic)
        return True

    async def publish(
        self,
        topic: str,
        payload: object,
        qos: int = 0,
        message_expiry: Optional[int] = None,
    ) -> None:
        """임의 토픽에 메시지 발행."""
        raw = encode_payload(payload)
        props = build_publish_properties(message_expiry=message_expiry)
        await self._conn.publish(topic, raw, qos=qos, properties=props)

    async def subscribe(self, topic: str, callback: MessageHandler, qos: int = 1) -> None:
        """토픽 구독 및 콜백 등록."""
        if topic not in self._handlers:
            self._handlers[topic] = []
            await self._conn.subscribe(topic, qos=qos)
        self._handlers[topic].append(callback)

    async def unsubscribe(self, topic: str) -> None:
        """토픽 구독 해제."""
        self._handlers.pop(topic, None)
        await self._conn.unsubscribe(topic)


def _topic_matches(pattern: str, topic: str) -> bool:
    """
    MQTT 와일드카드 토픽 패턴 매칭.

    '+': 단일 레벨, '#': 다중 레벨.
    """
    pattern_parts = pattern.split("/")
    topic_parts = topic.split("/")

    for i, pp in enumerate(pattern_parts):
        if pp == "#":
            return True
        if i >= len(topic_parts):
            return False
        if pp != "+" and pp != topic_parts[i]:
            return False

    return len(pattern_parts) == len(topic_parts)
