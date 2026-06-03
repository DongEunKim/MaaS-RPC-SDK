"""
어댑터 추상화 레이어.

MqttClientAdapter Protocol: paho-mqtt 및 Greengrass IPC 모두 이 인터페이스를 구현한다.
MqttProperties: MQTT5 메시지 메타데이터 (전송 계층 중립).
encode_payload: 페이로드 직렬화 유틸리티.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Protocol, runtime_checkable


@dataclass
class MqttProperties:
    """
    MQTT5 메시지 메타데이터 (전송 계층 중립).

    어댑터 경계에서 paho Properties 또는 IPC 헤더로 변환된다.
    """

    correlation_data: Optional[bytes] = None
    response_topic: Optional[str] = None
    user_properties: list[tuple[str, str]] = field(default_factory=list)
    message_expiry_interval: Optional[int] = None


MessageCallback = Callable[["IncomingMessage"], None]  # forward ref


@runtime_checkable
class MqttClientAdapter(Protocol):
    """
    전송 계층 어댑터 Protocol.

    구현체: PahoMqttAdapter (CCU/pure MQTT), GreengrassIpcAdapter (CGU/Greengrass IPC).
    모든 메서드는 동기(threading 친화적). 비동기 컨텍스트에서는 asyncio.to_thread로 감싼다.
    """

    def connect(self) -> None:
        """브로커/IPC에 연결. 블로킹."""
        ...

    def disconnect(self) -> None:
        """연결 종료."""
        ...

    def publish(
        self,
        topic: str,
        payload: bytes,
        qos: int = 1,
        props: Optional[MqttProperties] = None,
    ) -> None:
        """메시지 발행. 블로킹."""
        ...

    def subscribe(self, topic: str, qos: int = 1) -> None:
        """토픽 구독. 블로킹."""
        ...

    def set_message_callback(self, callback: MessageCallback) -> None:
        """수신 메시지 콜백 등록."""
        ...

    def get_vin(self) -> str:
        """
        이 어댑터가 담당하는 VIN(Thing Name) 반환.

        Greengrass IPC 어댑터는 Thing Name API로 취득.
        paho 어댑터는 생성 시 주입된 값을 반환.
        """
        ...


def encode_payload(payload: Any) -> bytes:
    """페이로드를 bytes로 직렬화."""
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, str):
        return payload.encode("utf-8")
    if payload is None:
        return b""
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")
