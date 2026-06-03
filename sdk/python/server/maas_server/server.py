"""
MaasServer: MQTT 5.0 서비스 서버 (기본 인터페이스).

asyncio 이벤트 루프에서 MQTT를 구동하고,
@action 데코레이터로 RPC 핸들러를 등록한 뒤 run()으로 실행한다.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Callable, Optional

from ._adapter import MqttClientAdapter, MqttProperties, encode_payload
from ._adapters import PahoMqttAdapter, GreengrassIpcAdapter
from .connection import IncomingMessage
from ._dispatcher import Dispatcher, DEFAULT_UNSUBSCRIBE_ACTION
from .session import ExclusiveSessionManager
from .presence import OfflineMonitor
from .context import RpcContext
from . import topics as topic_utils

logger = logging.getLogger(__name__)


class MaasServer:
    """
    MQTT 5.0 RPC 서비스 서버.

    토픽의 ``{ThingType}``, ``{Service}``, ``{VIN}`` 을 고정하고
    기본적으로 페이로드의 ``route_key`` 필드(기본 이름 ``action``)로 핸들러를 고른다.
    ``route_key=None`` 이면 페이로드를 분해하지 않고 ``@server.default`` 한 개만 둘 수 있다.

    Example::

        server = MaasServer(
            thing_type="CGU",
            service_name="viss",
            vin="VIN-123456",
            endpoint="mqtt.example.com",
        )

        @server.action("get")
        def get_datapoint(ctx: RpcContext):
            return {"value": read_sensor(ctx.payload.get("path"))}

        server.run()  # 블로킹
    """

    def __init__(
        self,
        thing_type: str,
        service_name: str,
        vin: Optional[str] = None,
        endpoint: Optional[str] = None,
        port: int = 8883,
        use_wss: bool = False,
        client_id: Optional[str] = None,
        route_key: Optional[str] = "action",
        mode: str = "mqtt",
        heartbeat_interval: float = 10.0,
        *,
        exclusive_service: bool = False,
        clock_tolerance_ms: int = 500,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Args:
            thing_type: 토픽의 {ThingType} (예: CGU).
            service_name: 이 서비스의 이름. 토픽의 {Service}에 해당.
            vin: 이 서비스가 담당하는 장비 VIN. 토픽의 {VIN}.
                mode='mqtt'에서는 필수. mode='greengrass'에서는 None이면 connect() 후 자동 취득.
            endpoint: MQTT 브로커 엔드포인트. mode='mqtt'에서는 필수.
            port: 브로커 포트 (TLS 기본 8883, WSS는 443).
            use_wss: True이면 WSS 전송 사용.
            client_id: MQTT 클라이언트 ID. None이면 service_name 기반으로 자동 생성.
            route_key: 페이로드에서 라우팅에 쓸 필드명. 기본 ``action``.
                ``None``이면 필드를 제거하지 않으며 ``@server.default`` 만 등록 가능.
            mode: 전송 계층 모드. 'mqtt' (paho-mqtt, 기본값) 또는 'greengrass' (Greengrass IPC).
            heartbeat_interval: 서버 Heartbeat 발행 간격(초). 0 이하면 Heartbeat 비활성화.
            logger: 로거 인스턴스.
        """
        if route_key is not None and route_key == "":
            raise ValueError("route_key는 None 이거나 비어 있지 않은 문자열이어야 합니다")
        self._thing_type = thing_type
        self._service_name = service_name
        self._vin = vin
        self._log = logger or logging.getLogger(__name__)

        self._client_id = client_id or f"{service_name}-{vin or 'greengrass'}"

        if mode == "mqtt":
            if not endpoint:
                raise ValueError("mode='mqtt' 에서는 endpoint가 필수입니다.")
            if not vin:
                raise ValueError("mode='mqtt' 에서는 vin이 필수입니다.")
            adapter: MqttClientAdapter = PahoMqttAdapter(
                endpoint=endpoint,
                client_id=self._client_id,
                vin=vin,
                port=port,
                use_wss=use_wss,
                logger=self._log,
            )
        elif mode == "greengrass":
            adapter = GreengrassIpcAdapter(vin=vin, logger=self._log)
        else:
            raise ValueError(
                f"알 수 없는 mode: {mode!r}. 'mqtt' 또는 'greengrass'를 지정하세요."
            )

        self._adapter = adapter
        self._exclusive_service = exclusive_service
        self._exclusive_mgr: Optional[ExclusiveSessionManager] = (
            ExclusiveSessionManager() if exclusive_service else None
        )
        self._offline_monitor = OfflineMonitor()
        self._dispatcher = Dispatcher(
            conn=self._adapter,
            thing_type=thing_type,
            service_name=service_name,
            vin=vin or "",
            route_key=route_key,
            exclusive_mgr=self._exclusive_mgr,
            clock_tolerance_ms=clock_tolerance_ms,
        )
        self._heartbeat_interval = heartbeat_interval

        # 단절 콜백: LWT(offline) 경로
        self._offline_monitor.on_offline(self._on_client_offline)

        # 패턴 C/E 단절 콜백 등록자
        self._on_session_lost_callbacks: list[Callable] = []
        self._on_subscription_lost_callbacks: list[Callable] = []
        self._unsubscribe_registered_by_user = False

        self._adapter.set_message_callback(self._dispatch)

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop_event: Optional[asyncio.Event] = None

    @classmethod
    def from_env(
        cls,
        thing_type: str,
        service_name: str,
        *,
        vin_env: str = "THING_VIN",
        endpoint_env: str = "MQTT_ENDPOINT",
        mode: str = "mqtt",
        **kwargs: Any,
    ) -> "MaasServer":
        """
        환경변수에서 VIN과 엔드포인트를 읽어 서버를 생성한다.

        컨테이너·엣지 배포에서 환경변수로 설정을 주입할 때 편리하다.
        mode='greengrass'에서는 endpoint가 필요 없으므로 환경변수가 없어도 된다.
        mode='mqtt'에서 vin/endpoint가 None이면 __init__에서 ValueError가 발생한다.
        """
        vin = os.environ.get(vin_env)
        endpoint = os.environ.get(endpoint_env)
        return cls(
            thing_type=thing_type,
            service_name=service_name,
            vin=vin,
            endpoint=endpoint,
            mode=mode,
            **kwargs,
        )

    @staticmethod
    def get_required_iot_core_permissions(
        thing_type: str,
        service_name: str,
        vin: str,
    ) -> dict:
        """
        Greengrass recipe accessControl 패턴에 필요한 IoT Core 권한 dict 반환.

        Returns:
            Greengrass component recipe accessControl 섹션에 삽입 가능한 dict.
        """
        sub_topic = f"WMT/{thing_type}/{service_name}/{vin}/+/request"
        pub_topic = f"WMO/{thing_type}/{service_name}/{vin}/+/response"
        return {
            "aws.greengrass.ipc.mqttproxy": {
                f"maas:{thing_type}:{service_name}:subscribe": {
                    "policyDescription": f"Allow subscribe to {sub_topic}",
                    "operations": ["aws.greengrass#SubscribeToIoTCore"],
                    "resources": [sub_topic],
                },
                f"maas:{thing_type}:{service_name}:publish": {
                    "policyDescription": f"Allow publish to {pub_topic}",
                    "operations": ["aws.greengrass#PublishToIoTCore"],
                    "resources": [pub_topic],
                },
            }
        }

    def use_middleware(self, func) -> None:
        """
        서버 미들웨어 등록. 등록 역순으로 실행된다.

        Example::

            @server.middleware
            async def logging_mw(ctx: RpcContext, call_next):
                logger.info("action=%s", ctx.action)
                await call_next(ctx)
        """
        self._dispatcher.add_middleware(func)

    @property
    def middleware(self):
        """데코레이터 형식으로 미들웨어 등록."""
        def decorator(func):
            self.use_middleware(func)
            return func
        return decorator

    def cancel_subscription(self, subscription_id: str, reason: Optional[str] = None) -> bool:
        """
        진행 중인 스트리밍 구독을 취소한다.

        스트리밍 핸들러의 cancel_event를 set하여 핸들러가 루프를 종료할 수 있게 한다.
        핸들러가 cancel_event.is_set()을 확인하지 않으면 효과가 없다.

        Args:
            subscription_id: ctx.subscription_id 또는 클라이언트에서 전달된 ID.
            reason: 취소 이유 (선택). 지정하면 클라이언트에서 SubscriptionCancelledError(reason) 발생.
                    unsubscribe 핸들러에서는 None (클라이언트 주도 취소).
                    서버 정책 취소(할당량, 리소스 등)에서는 reason 지정.

        Returns:
            True이면 취소 신호 전달 성공, False이면 해당 subscription_id 없음.
        """
        if not self._loop:
            return False
        future = asyncio.run_coroutine_threadsafe(
            self._dispatcher._registry.cancel(subscription_id, reason=reason),
            self._loop,
        )
        return future.result(timeout=5.0)

    def action(
        self,
        action_name: str,
        *,
        subscription: bool = False,
        qos: int = 0,
    ) -> Callable:
        """
        RPC 핸들러 등록 데코레이터.

        ``action_name`` 은 클라이언트가 보내는 페이로드의 ``route_key`` 필드 값과 같아야 한다
        (기본 ``route_key`` 는 ``"action"``).

        Args:
            action_name: 페이로드 라우팅 값과 동일한 식별자.
            subscription: True이면 패턴 C 스트리밍 구독 핸들러(generator/async generator).
            qos: 스트림 청크 발행 QoS (subscription 시). 기본 0.
        """

        def decorator(func: Callable) -> Callable:
            if action_name == DEFAULT_UNSUBSCRIBE_ACTION:
                self._unsubscribe_registered_by_user = True
            self._dispatcher.register(
                action_name,
                func,
                subscription=subscription,
                stream_qos=qos,
            )
            return func

        return decorator

    # ── 패턴 E: 독점 세션 ─────────────────────────────────────────────────────

    def acquire_session(self, client_id: str) -> Optional[str]:
        """
        독점 세션을 획득한다(핸들러 내부에서 호출). ``exclusive_service=True`` 필요.

        Returns:
            session_id(uuid). 다른 클라이언트 점유 중이면 None.
        """
        if self._exclusive_mgr is None:
            raise RuntimeError("exclusive_service=True 로 생성한 서버에서만 사용 가능합니다.")
        return self._exclusive_mgr.acquire(client_id)

    def release_session(self, client_id: str) -> bool:
        """독점 세션을 해제한다(핸들러 내부에서 호출)."""
        if self._exclusive_mgr is None:
            raise RuntimeError("exclusive_service=True 로 생성한 서버에서만 사용 가능합니다.")
        return self._exclusive_mgr.release(client_id)

    def get_session_id(self, client_id: str) -> Optional[str]:
        """해당 client_id가 보유한 session_id. 없으면 None."""
        if self._exclusive_mgr is None:
            return None
        return self._exclusive_mgr.get_session_id(client_id)

    def on_session_lost(self, func: Callable) -> Callable:
        """패턴 E: 세션 단절 콜백 데코레이터. ``func(client_id, session_id)``."""
        self._on_session_lost_callbacks.append(func)
        return func

    def on_subscription_lost(self, func: Callable) -> Callable:
        """패턴 C: 구독 단절 콜백 데코레이터. ``func(client_id, subscription_id)``."""
        self._on_subscription_lost_callbacks.append(func)
        return func

    def default(
        self,
        *,
        subscription: bool = False,
        qos: int = 0,
    ) -> Callable:
        """
        단일 기본 RPC 핸들러 등록 데코레이터.

        ``route_key`` 가 문자열일 때: 해당 필드가 없거나 빈 값이면 이 핸들러가 호출된다.
        ``route_key=None`` 일 때: 모든 RPC 요청이 이 핸들러로만 전달되며,
        페이로드에서 라우팅 필드를 제거하지 않는다. 이 경우 ``@server.action`` 은 사용할 수 없다.
        """

        def decorator(func: Callable) -> Callable:
            self._dispatcher.register_default(
                func,
                subscription=subscription,
                stream_qos=qos,
            )
            return func

        return decorator

    def subscribe(self, topic: str) -> Callable:
        """
        임의 토픽 구독 데코레이터.

        @server.subscribe("shadow/update/#")
        def on_shadow(topic: str, payload: bytes):
            ...
        """

        def decorator(func: Callable) -> Callable:
            self._dispatcher.register_pubsub(topic, func)
            return func

        return decorator

    def run(self) -> None:
        """서버를 시작하고 블로킹 실행한다."""
        asyncio.run(self._run_async())

    def stop(self) -> None:
        """실행 중인 서버를 종료한다."""
        if self._stop_event:
            if self._loop:
                self._loop.call_soon_threadsafe(self._stop_event.set)

    def publish(
        self,
        topic: str,
        payload: Any,
        qos: int = 1,
    ) -> None:
        """임의 토픽에 메시지 발행 (run() 실행 중에만 사용)."""
        if not self._loop:
            raise RuntimeError("서버가 실행 중이지 않습니다")
        asyncio.run_coroutine_threadsafe(
            asyncio.to_thread(self._adapter.publish, topic, encode_payload(payload), qos),
            self._loop,
        ).result(timeout=10.0)

    async def _run_async(self) -> None:
        """비동기 서버 실행 루프."""
        self._loop = asyncio.get_running_loop()
        self._stop_event = asyncio.Event()

        await asyncio.to_thread(self._adapter.connect)

        if self._vin is None:
            self._vin = self._adapter.get_vin()
            self._dispatcher.update_vin(self._vin)

        self._log.info(
            "MaasServer 시작: thing_type=%s, service=%s, vin=%s",
            self._thing_type,
            self._service_name,
            self._vin,
        )

        # 패턴 C 기본 unsubscribe 핸들러 자동 등록 (사용자가 직접 등록하지 않은 경우)
        self._maybe_register_default_unsubscribe()

        sub_topic = topic_utils.build_subscription(
            self._thing_type, self._service_name, self._vin
        )
        await asyncio.to_thread(self._adapter.subscribe, sub_topic, 1)
        self._log.info("요청 구독 완료: %s", sub_topic)

        # 패턴 C/E: LWT(offline) 와일드카드 자동 구독
        if self._exclusive_service or self._dispatcher.has_subscription_handler:
            offline_topic = topic_utils.build_offline_subscription(
                self._thing_type, self._service_name, self._vin
            )
            await asyncio.to_thread(self._adapter.subscribe, offline_topic, 1)
            self._log.info("LWT(offline) 구독 완료: %s", offline_topic)

        for topic in self._dispatcher._pubsub_handlers:
            await asyncio.to_thread(self._adapter.subscribe, topic, 1)

        self._log.info("서버 실행 중. 종료하려면 KeyboardInterrupt 또는 stop() 호출.")

        if self._heartbeat_interval > 0:
            hb_task = asyncio.create_task(self._heartbeat_loop())
        else:
            hb_task = None

        try:
            await self._stop_event.wait()
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        finally:
            if hb_task:
                hb_task.cancel()
                try:
                    await hb_task
                except asyncio.CancelledError:
                    pass
            await asyncio.to_thread(self._adapter.disconnect)
            self._log.info("MaasServer 종료")

    async def _heartbeat_loop(self) -> None:
        """서버 Heartbeat 발행 루프."""
        hb_topic = topic_utils.build_heartbeat(
            self._thing_type, self._service_name, self._vin or ""
        )
        while True:
            try:
                payload = encode_payload({
                    "interval": self._heartbeat_interval,
                    "ts": time.time(),
                })
                props = MqttProperties(
                    message_expiry_interval=int(self._heartbeat_interval)
                )
                await asyncio.to_thread(
                    self._adapter.publish, hb_topic, payload, 0, props
                )
            except Exception:
                self._log.debug("Heartbeat 발행 실패 (무시)", exc_info=True)
            await asyncio.sleep(self._heartbeat_interval)

    def _maybe_register_default_unsubscribe(self) -> None:
        """패턴 C: 구독 핸들러가 있고 사용자가 unsubscribe를 등록하지 않았으면 기본 핸들러를 등록한다."""
        if self._unsubscribe_registered_by_user:
            return
        if not self._dispatcher.has_subscription_handler:
            return
        if self._dispatcher._route_key is None:
            return
        if DEFAULT_UNSUBSCRIBE_ACTION in self._dispatcher._handlers:
            return

        registry = self._dispatcher._registry

        async def _default_unsubscribe(ctx: RpcContext) -> dict:
            sid = None
            if isinstance(ctx.payload, dict):
                sid = ctx.payload.get("subscriptionId")
            if sid:
                await registry.cancel(sid, reason=None)
            return {"status": "ok"}

        self._dispatcher.register(DEFAULT_UNSUBSCRIBE_ACTION, _default_unsubscribe)
        self._log.debug("기본 unsubscribe 핸들러 자동 등록")

    def _on_client_offline(self, client_id: str) -> None:
        """LWT(offline) 단절 처리: 세션 강제 해제 + 구독 취소 + 콜백."""
        # 패턴 E: 독점 세션 강제 해제
        if self._exclusive_mgr is not None:
            sid = self._exclusive_mgr.force_release_by_client(client_id)
            if sid:
                for cb in self._on_session_lost_callbacks:
                    try:
                        cb(client_id, sid)
                    except Exception:
                        logger.exception("on_session_lost 콜백 오류: client_id=%s", client_id)

        # 패턴 C: 구독 취소 + 콜백
        if self._loop:
            asyncio.run_coroutine_threadsafe(
                self._cancel_subscriptions_for(client_id),
                self._loop,
            )

    async def _cancel_subscriptions_for(self, client_id: str) -> None:
        """client_id의 모든 구독을 취소하고 on_subscription_lost 콜백을 호출한다."""
        sub_ids = await self._dispatcher._registry.cancel_by_client(client_id)
        for sid in sub_ids:
            for cb in self._on_subscription_lost_callbacks:
                try:
                    cb(client_id, sid)
                except Exception:
                    logger.exception(
                        "on_subscription_lost 콜백 오류: client_id=%s, sub_id=%s",
                        client_id, sid,
                    )

    def _dispatch(self, msg: IncomingMessage) -> None:
        """수신 메시지를 dispatcher로 라우팅."""
        if not self._loop:
            return

        # LWT(offline) 경로
        if self._offline_monitor.matches(msg.topic):
            self._offline_monitor.handle_message(msg.topic, msg.payload)
            return

        for pattern, callbacks in self._dispatcher._pubsub_handlers.items():
            if topic_utils.topic_matches(pattern, msg.topic):
                for cb in callbacks:
                    try:
                        cb(msg.topic, msg.payload)
                    except Exception:
                        logger.exception("pub/sub 핸들러 오류: topic=%s", msg.topic)
                return

        asyncio.run_coroutine_threadsafe(
            self._dispatcher.handle(msg), self._loop
        )


