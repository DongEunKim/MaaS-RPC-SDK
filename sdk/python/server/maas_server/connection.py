"""
MQTT 5.0 서버 연결 관리.

paho-mqtt 2.x 기반. TCP 또는 WSS 전송을 지원한다.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from typing import Any, Callable, Optional

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from .exceptions import ConnectionError as MaasConnectionError

logger = logging.getLogger(__name__)

_KEEPALIVE = 60
_CONNECT_TIMEOUT = 30.0

UP_REASON_CODE = "reason_code"
UP_ERROR_DETAIL = "error_detail"
UP_IS_EOF = "is_EOF"
UP_CANCEL_REASON = "cancel_reason"
# 요청 측 규약
UP_QOS = "qos"
UP_TIMEOUT = "timeout"
UP_SENT_AT = "sent_at"
# 응답 측 규약
UP_SUBSCRIPTION_ID = "subscription_id"
UP_UNSUBSCRIBE_ACTION = "unsubscribe_action"
UP_SESSION_ID = "session_id"


def _user_props_to_dict(props: Properties) -> dict[str, str]:
    """MQTT5 UserProperty 리스트를 dict로 변환."""
    result: dict[str, str] = {}
    raw = getattr(props, "UserProperty", None)
    if raw:
        for key, val in raw:
            result[key] = val
    return result


class IncomingMessage:
    """수신된 MQTT 5.0 메시지 DTO."""

    __slots__ = (
        "topic",
        "payload",
        "qos",
        "correlation_data",
        "response_topic",
        "user_props",
    )

    def __init__(self, msg: Any) -> None:
        self.topic: str = msg.topic
        self.payload: bytes = msg.payload or b""
        self.qos: int = msg.qos
        props: Properties = getattr(msg, "properties", None) or Properties(
            PacketTypes.PUBLISH
        )
        self.correlation_data: Optional[bytes] = getattr(
            props, "CorrelationData", None
        )
        self.response_topic: Optional[str] = getattr(props, "ResponseTopic", None)
        self.user_props: dict[str, str] = _user_props_to_dict(props)

    @classmethod
    def from_raw(
        cls,
        topic: str,
        payload: bytes,
        qos: int,
        correlation_data: Optional[bytes],
        response_topic: Optional[str],
        user_props: dict[str, str],
    ) -> "IncomingMessage":
        """paho msg 없이 IncomingMessage 생성 (Greengrass IPC 어댑터용)."""
        obj = object.__new__(cls)
        obj.topic = topic
        obj.payload = payload
        obj.qos = qos
        obj.correlation_data = correlation_data
        obj.response_topic = response_topic
        obj.user_props = user_props
        return obj


MessageCallback = Callable[[IncomingMessage], None]


class _Mqtt5ServerConnection:
    """
    내부 전용. 직접 사용하지 말 것.

    MQTT 5.0 서버 연결 클라이언트.

    TCP(기본) 또는 WSS 전송을 지원한다.
    paho 콜백 스레드와 asyncio 루프를 call_soon_threadsafe로 연결한다.
    """

    def __init__(
        self,
        endpoint: str,
        client_id: str,
        port: int = 8883,
        use_wss: bool = False,
        *,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Args:
            endpoint: MQTT 브로커 엔드포인트.
            client_id: MQTT 클라이언트 ID.
            port: 브로커 포트 (기본 8883 TLS, WSS는 443).
            use_wss: True이면 WSS 전송 사용.
            logger: 로거 인스턴스.
        """
        self._endpoint = endpoint
        self._client_id = client_id
        self._port = port
        self._use_wss = use_wss
        self._log = logger or logging.getLogger(__name__)

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._mqtt: Optional[mqtt.Client] = None
        self._connected = False
        self._closed = False

        self._connect_event = threading.Event()
        self._connect_rc: int = -1
        self._disconnect_event = threading.Event()

        self._message_callback: Optional[MessageCallback] = None

    def set_message_callback(self, callback: MessageCallback) -> None:
        """수신 메시지 콜백 등록."""
        self._message_callback = callback

    async def connect(self) -> None:
        """MQTT 5.0 브로커에 연결."""
        self._loop = asyncio.get_running_loop()
        self._connect_event.clear()
        self._disconnect_event.clear()
        self._closed = False

        transport = "websockets" if self._use_wss else "tcp"
        self._mqtt = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=self._client_id,
            protocol=mqtt.MQTTv5,
            transport=transport,
        )

        if self._use_wss:
            self._mqtt.ws_set_options(path="/mqtt")
            self._mqtt.tls_set()

        self._mqtt.on_connect = self._on_connect
        self._mqtt.on_disconnect = self._on_disconnect
        self._mqtt.on_message = self._on_message

        conn_props = Properties(PacketTypes.CONNECT)

        try:
            await asyncio.to_thread(
                self._mqtt.connect,
                self._endpoint,
                self._port,
                keepalive=_KEEPALIVE,
                clean_start=True,
                properties=conn_props,
            )
            self._mqtt.loop_start()
        except Exception as exc:
            raise MaasConnectionError(f"MQTT 연결 시도 실패: {exc}") from exc

        connected = await asyncio.to_thread(
            self._connect_event.wait, _CONNECT_TIMEOUT
        )
        if not connected or self._connect_rc != 0:
            raise MaasConnectionError(
                f"MQTT 연결 실패: rc={self._connect_rc}"
            )

    async def disconnect(self) -> None:
        """연결 종료."""
        self._closed = True
        if self._mqtt:
            await asyncio.to_thread(self._mqtt.loop_stop)
            await asyncio.to_thread(self._mqtt.disconnect)
            self._mqtt = None
        self._connected = False

    async def publish(
        self,
        topic: str,
        payload: bytes,
        qos: int = 1,
        properties: Optional[Properties] = None,
    ) -> None:
        """MQTT 5.0 PUBLISH."""
        if not self._mqtt or not self._connected:
            raise MaasConnectionError("연결되지 않은 상태에서 publish 호출")
        await asyncio.to_thread(
            self._mqtt.publish, topic, payload, qos, False, properties
        )

    async def subscribe(self, topic: str, qos: int = 1) -> None:
        """토픽 구독."""
        if not self._mqtt:
            raise MaasConnectionError("연결되지 않은 상태에서 subscribe 호출")
        sub_props = Properties(PacketTypes.SUBSCRIBE)
        await asyncio.to_thread(
            self._mqtt.subscribe, topic, qos, properties=sub_props
        )
        self._log.debug("구독: %s (QoS %d)", topic, qos)

    async def unsubscribe(self, topic: str) -> None:
        """토픽 구독 해제."""
        if self._mqtt:
            await asyncio.to_thread(self._mqtt.unsubscribe, topic)

    @property
    def is_connected(self) -> bool:
        """연결 상태."""
        return self._connected and not self._closed

    def _on_connect(
        self,
        client: Any,
        userdata: Any,
        connect_flags: Any,
        reason_code: Any,
        properties: Any,
    ) -> None:
        rc = int(reason_code) if hasattr(reason_code, "__int__") else reason_code
        if rc == 0:
            self._connected = True
            self._connect_rc = 0
            self._log.debug("MQTT 연결 성공: %s", self._endpoint)
        else:
            self._connect_rc = rc
            self._log.warning("MQTT 연결 실패: rc=%s", rc)
        self._connect_event.set()

    def _on_disconnect(
        self,
        client: Any,
        userdata: Any,
        disconnect_flags: Any,
        reason_code: Any,
        properties: Any,
    ) -> None:
        self._connected = False
        self._disconnect_event.set()
        if not self._closed:
            self._log.info("MQTT 연결 끊김: rc=%s", reason_code)

    def _on_message(
        self,
        client: Any,
        userdata: Any,
        message: Any,
    ) -> None:
        if not self._message_callback or not self._loop:
            return
        try:
            incoming = IncomingMessage(message)
        except Exception:
            self._log.exception("수신 메시지 파싱 오류")
            return
        self._loop.call_soon_threadsafe(self._message_callback, incoming)


def build_response_properties(
    correlation_data: Optional[bytes],
    reason_code: int = 0,
    error_detail: str = "",
    is_eof: bool = False,
) -> Properties:
    """응답 PUBLISH용 MQTT5 Properties 빌더."""
    props = Properties(PacketTypes.PUBLISH)
    if correlation_data:
        props.CorrelationData = correlation_data
    user_props: list[tuple[str, str]] = [
        (UP_REASON_CODE, str(reason_code)),
    ]
    if error_detail:
        user_props.append((UP_ERROR_DETAIL, error_detail))
    if is_eof:
        user_props.append((UP_IS_EOF, "true"))
    props.UserProperty = user_props
    return props


def encode_payload(payload: Any) -> bytes:
    """페이로드를 bytes로 직렬화."""
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, str):
        return payload.encode("utf-8")
    if payload is None:
        return b""
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")
