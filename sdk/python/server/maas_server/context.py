"""
RpcContext: 핸들러에 전달되는 요청 컨텍스트.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class RpcContext:
    """
    RPC 핸들러에 전달되는 요청 컨텍스트.

    핸들러는 이 객체를 통해 요청 정보를 읽고
    필요 시 직접 응답을 발행할 수 있다.
    """

    thing_type: str
    """토픽의 {ThingType} (예: CGU)."""

    service: str
    """토픽의 {Service} (예: viss, diagnostics)."""

    action: str
    """페이로드에서 추출된 action 값."""

    vin: str
    """토픽의 {VIN}. 대상 장비 식별자."""

    client_id: str
    """토픽의 {ClientId}. 응답 라우팅에 사용."""

    payload: Any
    """action 필드가 제거된 나머지 페이로드 (dict 또는 bytes)."""

    correlation_id: Optional[bytes]
    """MQTT5 Correlation Data. 응답 발행 시 그대로 반환해야 한다."""

    response_topic: Optional[str]
    """MQTT5 Response Topic. SDK가 자동으로 응답을 발행하므로 직접 사용할 필요 없음."""

    user_props: dict[str, str] = field(default_factory=dict)
    """수신 메시지의 MQTT5 User Properties."""

    response_props: dict[str, str] = field(default_factory=dict)
    """핸들러/미들웨어가 응답 User Property에 추가할 키-값. SDK가 응답 발행 시 병합."""

    subscription_id: Optional[str] = None
    """스트리밍 핸들러에서 사용할 구독 세션 ID. SDK가 자동 생성하여 주입."""

    cancel_event: Optional[asyncio.Event] = None
    """구독 취소 신호. cancel_event.is_set() 이 True이면 스트리밍 핸들러가 종료해야 한다."""

    received_at: float = 0.0
    """디스패처가 요청 수신 시 주입하는 monotonic 시각(초). 응답 Message Expiry 계산에 사용."""

    sent_at_ms: Optional[int] = None
    """요청 User Property ``sent_at`` (unix ms). 패턴 D 시한 검증에 사용."""

    timeout_s: Optional[float] = None
    """요청 User Property ``timeout`` (초). 응답 Message Expiry 및 시한 검증에 사용."""

    request_qos: str = "1"
    """요청 User Property ``qos``. 단일 응답 QoS 미러링에 사용. 미포함 시 기본 '1'."""

    def is_expired(self, clock_tolerance_ms: int = 500) -> bool:
        """
        패턴 D: 요청 ``sent_at``·``timeout`` 기준으로 시한 만료 여부를 판단한다.

        ``sent_at_ms`` 또는 ``timeout_s`` 가 없으면 만료로 보지 않는다(False).
        """
        if self.sent_at_ms is None or self.timeout_s is None:
            return False
        now_ms = time.time() * 1000.0
        return (now_ms - self.sent_at_ms) > (self.timeout_s * 1000.0 + clock_tolerance_ms)
