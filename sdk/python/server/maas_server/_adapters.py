"""
MQTT 클라이언트 어댑터 구현체.

PahoMqttAdapter: paho-mqtt 2.x 기반 (CCU/pure MQTT).
GreengrassIpcAdapter: AWS Greengrass Core IPC 기반 (CGU).

awsiotsdk는 GreengrassIpcAdapter 내부에서만 지연 import한다.
파일 최상위에서 절대 import하지 않는다.
"""
from __future__ import annotations

import logging
import threading
from typing import Optional

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
import paho.mqtt.properties as paho_props

from .connection import IncomingMessage
from ._adapter import MqttClientAdapter, MqttProperties, MessageCallback
from .exceptions import ConnectionError as MaasConnectionError

_KEEPALIVE = 60
_CONNECT_TIMEOUT = 30.0


def _to_paho_props(props: Optional[MqttProperties]) -> Optional[paho_props.Properties]:
    """MqttProperties → paho Properties 변환."""
    if props is None:
        return None
    p = paho_props.Properties(PacketTypes.PUBLISH)
    if props.correlation_data:
        p.CorrelationData = props.correlation_data
    if props.response_topic:
        p.ResponseTopic = props.response_topic
    if props.user_properties:
        p.UserProperty = list(props.user_properties)
    if props.message_expiry_interval is not None:
        p.MessageExpiryInterval = props.message_expiry_interval
    return p


class PahoMqttAdapter:
    """paho-mqtt 2.x 기반 MqttClientAdapter 구현체."""

    def __init__(
        self,
        endpoint: str,
        client_id: str,
        vin: str,
        port: int = 8883,
        use_wss: bool = False,
        *,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._endpoint = endpoint
        self._client_id = client_id
        self._vin = vin
        self._port = port
        self._use_wss = use_wss
        self._log = logger or logging.getLogger(__name__)

        self._mqtt: Optional[mqtt.Client] = None
        self._connected = False
        self._closed = False

        self._connect_event = threading.Event()
        self._connect_rc: int = -1
        self._disconnect_event = threading.Event()

        self._message_callback: Optional[MessageCallback] = None

    def connect(self) -> None:
        """MQTT 5.0 브로커에 연결. 블로킹."""
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

        conn_props = paho_props.Properties(PacketTypes.CONNECT)

        try:
            self._mqtt.connect(
                self._endpoint,
                self._port,
                keepalive=_KEEPALIVE,
                clean_start=True,
                properties=conn_props,
            )
            self._mqtt.loop_start()
        except Exception as exc:
            raise MaasConnectionError(f"MQTT 연결 시도 실패: {exc}") from exc

        connected = self._connect_event.wait(_CONNECT_TIMEOUT)
        if not connected or self._connect_rc != 0:
            raise MaasConnectionError(
                f"MQTT 연결 실패: rc={self._connect_rc}"
            )

    def disconnect(self) -> None:
        """연결 종료."""
        self._closed = True
        if self._mqtt:
            self._mqtt.loop_stop()
            self._mqtt.disconnect()
            self._mqtt = None
        self._connected = False

    def publish(
        self,
        topic: str,
        payload: bytes,
        qos: int = 1,
        props: Optional[MqttProperties] = None,
    ) -> None:
        """MQTT 5.0 PUBLISH. 블로킹."""
        if not self._mqtt or not self._connected:
            raise MaasConnectionError("연결되지 않은 상태에서 publish 호출")
        paho_properties = _to_paho_props(props)
        self._mqtt.publish(topic, payload, qos, False, paho_properties)

    def subscribe(self, topic: str, qos: int = 1) -> None:
        """토픽 구독. 블로킹."""
        if not self._mqtt:
            raise MaasConnectionError("연결되지 않은 상태에서 subscribe 호출")
        sub_props = paho_props.Properties(PacketTypes.SUBSCRIBE)
        self._mqtt.subscribe(topic, qos, properties=sub_props)
        self._log.debug("구독: %s (QoS %d)", topic, qos)

    def set_message_callback(self, callback: MessageCallback) -> None:
        """수신 메시지 콜백 등록."""
        self._message_callback = callback

    def get_vin(self) -> str:
        """이 어댑터가 담당하는 VIN 반환."""
        return self._vin

    def _on_connect(
        self,
        client: object,
        userdata: object,
        connect_flags: object,
        reason_code: object,
        properties: object,
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
        client: object,
        userdata: object,
        disconnect_flags: object,
        reason_code: object,
        properties: object,
    ) -> None:
        self._connected = False
        self._disconnect_event.set()
        if not self._closed:
            self._log.info("MQTT 연결 끊김: rc=%s", reason_code)

    def _on_message(
        self,
        client: object,
        userdata: object,
        message: object,
    ) -> None:
        if not self._message_callback:
            return
        try:
            incoming = IncomingMessage(message)
        except Exception:
            self._log.exception("수신 메시지 파싱 오류")
            return
        self._message_callback(incoming)


class _IpcMessageStreamHandler:
    """IPC 수신 메시지를 IncomingMessage로 변환하는 핸들러."""

    def __init__(
        self,
        topic_pattern: str,
        callback: Optional[MessageCallback],
        log: logging.Logger,
    ) -> None:
        self._topic_pattern = topic_pattern
        self._callback = callback
        self._log = log

    def on_stream_event(self, event: object) -> None:
        if not self._callback:
            return
        try:
            msg = _ipc_event_to_incoming(event)
            self._callback(msg)
        except Exception:
            self._log.exception("IPC 메시지 변환 오류")


def _ipc_event_to_incoming(event: object) -> IncomingMessage:
    """
    awsiot IoTCoreMessage → IncomingMessage 변환.

    IPC 메시지의 MQTT5 속성(CorrelationData, ResponseTopic, UserProperties)을
    IncomingMessage 필드로 매핑한다.
    """
    msg = event.message  # type: ignore[attr-defined]
    topic: str = msg.topic_name
    payload: bytes = msg.payload or b""
    if not isinstance(payload, bytes):
        payload = bytes(payload)

    correlation_data = getattr(msg, "correlation_data", None)
    if correlation_data is not None and not isinstance(correlation_data, bytes):
        correlation_data = bytes(correlation_data)

    response_topic: Optional[str] = getattr(msg, "response_topic", None)
    user_props_raw = getattr(msg, "user_properties", None) or []
    user_props: dict[str, str] = {up.key: up.value for up in user_props_raw}

    return IncomingMessage.from_raw(
        topic=topic,
        payload=payload,
        qos=1,
        correlation_data=correlation_data,
        response_topic=response_topic,
        user_props=user_props,
    )


class GreengrassIpcAdapter:
    """
    AWS Greengrass Core IPC 기반 MqttClientAdapter 구현체.

    awsiotsdk는 이 클래스 내부에서만 지연 import한다.
    최상위 파일 스코프에서는 절대 import하지 않는다.
    """

    def __init__(
        self,
        vin: Optional[str] = None,
        *,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._log = logger or logging.getLogger(__name__)
        self._vin = vin
        self._ipc_client = None
        self._message_callback: Optional[MessageCallback] = None
        self._subscriptions: list = []

    def connect(self) -> None:
        """Greengrass IPC 클라이언트 초기화."""
        try:
            import awsiot.greengrasscoreipc as gg_ipc  # noqa: F401
            import awsiot.greengrasscoreipc.clientV2 as gg_v2
        except ImportError as exc:
            raise ImportError(
                "Greengrass IPC 어댑터를 사용하려면 awsiotsdk가 필요합니다. "
                "pip install maas-server-sdk[aws] 로 설치하세요."
            ) from exc

        self._ipc_client = gg_v2.GreengrassCoreIPCClientV2()

        if self._vin is None:
            self._vin = self._get_thing_name()

    def _get_thing_name(self) -> str:
        """환경변수 AWS_IOT_THING_NAME에서 Thing Name 취득."""
        import os
        thing_name = os.environ.get("AWS_IOT_THING_NAME", "")
        if not thing_name:
            raise RuntimeError(
                "VIN을 지정하지 않았고 AWS_IOT_THING_NAME 환경변수도 없습니다."
            )
        return thing_name

    def disconnect(self) -> None:
        """연결 종료."""
        if self._ipc_client:
            self._ipc_client.close()
            self._ipc_client = None

    def publish(
        self,
        topic: str,
        payload: bytes,
        qos: int = 1,
        props: Optional[MqttProperties] = None,
    ) -> None:
        """
        Greengrass IPC publish_to_iot_core.

        MqttProperties → IPC publish 인자로 변환.
        correlation_data, response_topic, user_properties는
        IPC publish API에서 MQTT5 속성으로 지정한다.
        """
        if not self._ipc_client:
            raise RuntimeError("GreengrassIpcAdapter가 연결되지 않았습니다")
        from awsiot.greengrasscoreipc.model import (  # type: ignore[import]
            PublishToIoTCoreRequest,
            QOS,
            MqttUserProperty,
        )

        ipc_qos = QOS.AT_LEAST_ONCE if qos >= 1 else QOS.AT_MOST_ONCE

        request = PublishToIoTCoreRequest(
            topic_name=topic,
            qos=ipc_qos,
            payload=payload,
        )
        if props:
            if props.user_properties:
                request.user_properties = [
                    MqttUserProperty(key=k, value=v)
                    for k, v in props.user_properties
                ]
            if props.correlation_data:
                request.correlation_data = props.correlation_data
            if props.response_topic:
                request.response_topic = props.response_topic
            if props.message_expiry_interval is not None:
                try:
                    request.message_expiry_interval_sec = props.message_expiry_interval
                except AttributeError:
                    pass  # SDK 버전에 따라 없을 수 있음

        self._ipc_client.publish_to_iot_core(request)

    def subscribe(self, topic: str, qos: int = 1) -> None:
        """Greengrass IPC subscribe_to_iot_core."""
        if not self._ipc_client:
            raise RuntimeError("GreengrassIpcAdapter가 연결되지 않았습니다")

        from awsiot.greengrasscoreipc.model import QOS  # type: ignore[import]

        ipc_qos = QOS.AT_LEAST_ONCE if qos >= 1 else QOS.AT_MOST_ONCE

        if self._message_callback is None:
            self._log.warning(
                "subscribe() 호출 시점에 메시지 콜백이 설정되지 않았습니다. "
                "set_message_callback()을 먼저 호출했는지 확인하세요. topic=%s",
                topic,
            )
        handler = _IpcMessageStreamHandler(topic, self._message_callback, self._log)
        _, op = self._ipc_client.subscribe_to_iot_core(
            topic_name=topic,
            qos=ipc_qos,
            on_stream_event=handler.on_stream_event,
        )
        self._subscriptions.append(op)

    def set_message_callback(self, callback: MessageCallback) -> None:
        """수신 메시지 콜백 등록."""
        self._message_callback = callback

    def get_vin(self) -> str:
        """이 어댑터가 담당하는 VIN(Thing Name) 반환."""
        if not self._vin:
            raise RuntimeError("VIN이 설정되지 않았습니다. connect() 후에 사용하세요.")
        return self._vin
