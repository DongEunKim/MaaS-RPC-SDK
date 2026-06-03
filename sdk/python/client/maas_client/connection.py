"""
MQTT 5.0 연결 관리 (WSS+TLS 또는 로컬 TCP).

paho-mqtt 2.x 기반. paho 내부 스레드에서 실행되는 콜백을
asyncio 이벤트 루프로 안전하게 브리지한다.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import uuid
from typing import Any, Callable, Optional

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from .exceptions import ConnectionError as MaasConnectionError

logger = logging.getLogger(__name__)

_KEEPALIVE = 30
_CONNECT_TIMEOUT = 30.0

# User Property 키 상수
UP_REASON_CODE = "reason_code"
UP_ERROR_DETAIL = "error_detail"
UP_IS_EOF = "is_EOF"
UP_CANCEL_REASON = "cancel_reason"
UP_CONTENT_TYPE = "content-type"
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
    """수신된 MQTT 5.0 메시지를 담는 내부 DTO."""

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


MessageCallback = Callable[[IncomingMessage], None]


class Mqtt5Connection:
    """
    MQTT 5.0 연결 클라이언트.

    기본은 WSS+TLS(WebSocket Secure)이며, ``use_wss=False`` 이면 비암호화 TCP
    (로컬 Mosquitto 등)에 연결할 수 있다.
    JWT 토큰은 연결 시 username으로 전달한다.
    paho 콜백 스레드와 asyncio 루프를 call_soon_threadsafe로 연결한다.
    """

    def __init__(
        self,
        endpoint: str,
        client_id: str,
        token: Optional[str] = None,
        port: int = 443,
        *,
        use_wss: bool = True,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Args:
            endpoint: 브로커 호스트명 (포트 제외).
            client_id: MQTT 클라이언트 ID.
            token: JWT 인증 토큰. 브로커 설정에 따라 username 등으로 전달.
            port: 브로커 포트 (WSS 기본 443, TCP 예: 1883).
            use_wss: True면 WebSocket+TLS, False면 일반 TCP(로컬 개발용).
            logger: 로거 인스턴스.
        """
        self._endpoint = endpoint
        self._client_id = client_id
        self._token = token
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

        # LWT(will). connect() 직전에 paho에 등록된다.
        self._will_topic: Optional[str] = None
        self._will_payload: Optional[bytes] = None
        self._will_qos: int = 1

    def set_message_callback(self, callback: MessageCallback) -> None:
        """수신 메시지 콜백 등록."""
        self._message_callback = callback

    def set_will(
        self,
        topic: Optional[str],
        payload: Optional[bytes] = None,
        qos: int = 1,
    ) -> None:
        """
        LWT(Last Will and Testament)를 등록한다.

        paho는 connect() 이전에만 will을 설정할 수 있으므로, 여기서는 값만
        보관하고 다음 ``connect()`` 시점에 paho에 적용된다. ``topic=None`` 이면
        will을 해제한다.
        """
        self._will_topic = topic
        self._will_payload = payload
        self._will_qos = qos

    def set_token(self, token: Optional[str]) -> None:
        """
        연결 직전 MQTT username으로 쓸 토큰을 설정한다.

        ``connect()`` 호출 전에 갱신하면 매 연결마다 최신 JWT 등을 반영할 수 있다.
        """
        self._token = token

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

        if self._token:
            self._mqtt.username_pw_set(self._token, password="")

        if self._will_topic:
            will_props = Properties(PacketTypes.WILLMESSAGE)
            self._mqtt.will_set(
                self._will_topic,
                payload=self._will_payload or b"",
                qos=self._will_qos,
                retain=False,
                properties=will_props,
            )

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
        qos: int = 0,
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
        if not self._mqtt:
            return
        await asyncio.to_thread(self._mqtt.unsubscribe, topic)
        self._log.debug("구독 해제: %s", topic)

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


def build_publish_properties(
    response_topic: Optional[str] = None,
    correlation_data: Optional[bytes] = None,
    message_expiry: Optional[int] = None,
    user_props: Optional[list[tuple[str, str]]] = None,
) -> Properties:
    """PUBLISH용 MQTT5 Properties 빌더."""
    props = Properties(PacketTypes.PUBLISH)
    if response_topic:
        props.ResponseTopic = response_topic
    if correlation_data:
        props.CorrelationData = correlation_data
    if message_expiry is not None:
        props.MessageExpiryInterval = message_expiry
    if user_props:
        props.UserProperty = user_props
    return props


def new_correlation_id() -> bytes:
    """새 UUID v4를 correlation_data bytes로 반환."""
    return uuid.uuid4().bytes


def encode_payload(payload: Any) -> bytes:
    """페이로드를 bytes로 직렬화. dict→JSON, bytes→그대로."""
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, str):
        return payload.encode("utf-8")
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def decode_payload(raw: bytes) -> Any:
    """bytes 페이로드를 역직렬화. JSON 파싱 시도 후 실패 시 bytes 반환."""
    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return raw
