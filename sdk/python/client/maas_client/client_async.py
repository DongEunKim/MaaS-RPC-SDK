"""
MaasClientAsync: MQTT 5.0 비동기 클라이언트 (고급 인터페이스).
"""

from __future__ import annotations

import logging
from typing import Any, AsyncIterator, Optional

from .auth import TokenProvider
from .connection import Mqtt5Connection, IncomingMessage
from ._rpc import RpcManager
from ._pubsub import PubSubManager, MessageHandler
from ._heartbeat import HeartbeatManager
from .models import (
    Response,
    RpcResponse,
    StreamEvent,
    Message,
    Session,
    Subscription,
    ServerWatcher,
)
from . import topics

logger = logging.getLogger(__name__)


class MaasClientAsync:
    """
    MQTT 5.0 비동기 클라이언트.

    기본은 WSS+TLS이며, ``use_wss=False`` 로 로컬 TCP 브로커에도 연결 가능하다.
    RPC 호출(call, stream, exclusive_session)과 단순 pub/sub를 지원한다.
    asyncio 환경에서 직접 사용하거나, MaasClient(동기)의 내부 구현으로 사용된다.

    생성자에 ``thing_type``, ``service``, ``vin`` 을 모두 넣으면
    ``call(action[, params])`` / ``stream(action[, params])`` /
    무인자 ``exclusive_session()`` 으로 짧게 호출할 수 있다.
    라우팅을 매번 지정하려면 ``call(thing_type, service, action, vin[, params])`` 형태를 쓴다.
    """

    def __init__(
        self,
        endpoint: str,
        client_id: str,
        token_provider: Optional[TokenProvider] = None,
        port: Optional[int] = None,
        *,
        use_wss: bool = True,
        thing_type: Optional[str] = None,
        service: Optional[str] = None,
        vin: Optional[str] = None,
        heartbeat_hint_interval: float = 10.0,
        heartbeat_timeout_multiplier: float = 3.0,
        first_heartbeat_timeout: float = 30.0,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Args:
            endpoint: 브로커 호스트명.
            client_id: MQTT 클라이언트 ID. 응답 토픽 라우팅에 사용.
            token_provider: 연결 시마다 호출되어 MQTT username 문자열을 반환.
                None이면 인증 없이 연결. ``HttpTokenSource`` 등 ``__call__`` 제공자 가능.
            port: 브로커 포트. None이면 ``use_wss`` 에 따라 443(WSS) 또는 1883(TCP).
            use_wss: True면 WebSocket+TLS, False면 TCP(로컬 Mosquitto 등).
            thing_type: 바인딩 시 토픽 ThingType. ``service``, ``vin`` 과 함께 세트로 지정.
            service: 바인딩 시 서비스 이름.
            vin: 바인딩 시 대상 VIN.
            heartbeat_hint_interval: Heartbeat 간격 힌트(초). 첫 HB 수신 전 타임아웃 계산에 사용.
            heartbeat_timeout_multiplier: HB 타임아웃 = interval × multiplier.
            first_heartbeat_timeout: 콜드 스타트 시 첫 HB 최대 대기 시간(초).
            logger: 로거 인스턴스.
        """
        bound = (thing_type, service, vin)
        if any(x is not None for x in bound) and not all(x is not None for x in bound):
            raise ValueError(
                "thing_type, service, vin 은 세 값 모두 생략하거나 모두 지정해야 합니다."
            )

        self._endpoint = endpoint
        self._client_id = client_id
        self._token_provider = token_provider
        eff_port = port if port is not None else (443 if use_wss else 1883)
        self._port = eff_port
        self._use_wss = use_wss
        self._thing_type = thing_type
        self._service = service
        self._vin = vin
        self._log = logger or logging.getLogger(__name__)

        self._conn = Mqtt5Connection(
            endpoint=endpoint,
            client_id=client_id,
            token=None,
            port=eff_port,
            use_wss=use_wss,
            logger=self._log,
        )
        self._rpc = RpcManager(self._conn, client_id)
        self._pubsub = PubSubManager(self._conn)
        self._hb_mgr = HeartbeatManager(
            conn=self._conn,
            hint_interval=heartbeat_hint_interval,
            timeout_multiplier=heartbeat_timeout_multiplier,
            first_hb_timeout=first_heartbeat_timeout,
        )
        self._rpc._hb_mgr = self._hb_mgr

        self._conn.set_message_callback(self._dispatch_message)

    def _is_bound(self) -> bool:
        """생성자에 thing_type, service, vin 이 모두 설정되었는지."""
        return (
            self._thing_type is not None
            and self._service is not None
            and self._vin is not None
        )

    def _bound_routing(self) -> tuple[str, str, str]:
        """바인딩된 thing_type, service, vin. 없으면 ValueError."""
        if not self._is_bound():
            raise ValueError(
                "생성자에 thing_type, service, vin을 모두 지정한 경우에만 "
                "call(action[, params]), stream(action[, params]), "
                "exclusive_session() 무인자 형태를 사용할 수 있습니다. "
                "그렇지 않으면 call(thing_type, service, action, vin[, params])처럼 "
                "네 축을 인자로 넘기세요."
            )
        return self._thing_type, self._service, self._vin

    def _parse_rpc_routing(
        self,
        *args,
        params: Any = None,
        for_stream: bool = False,
    ) -> tuple[str, str, str, str, Any]:
        """
        call/stream 공통: 인자 개수에 따라 바인딩 단축 형식과 전체 라우팅 형식을 구분한다.

        - 바인딩: ``(action,)`` 또는 ``(action, params_pos)``
        - 비바인딩/명시: ``(thing_type, service, action, vin)`` 또는
          ``(thing_type, service, action, vin, params_pos)``
        """
        n = len(args)
        if n in (1, 2):
            thing_type, service, vin = self._bound_routing()
            action = args[0]
            if n == 2:
                if params is not None:
                    raise TypeError(
                        "params를 두 번째 위치 인자와 keyword 동시에 지정할 수 없습니다."
                    )
                eff_params = args[1]
            else:
                eff_params = params
            return thing_type, service, action, vin, eff_params

        if n == 4:
            tt, sv, act, vn = args
            return tt, sv, act, vn, params

        if n == 5:
            if params is not None:
                raise TypeError(
                    "다섯 번째 위치 인자로 params를 넘기면 keyword params는 사용할 수 없습니다."
                )
            return args[0], args[1], args[2], args[3], args[4]

        ctx = "stream" if for_stream else "call"
        raise TypeError(
            f"{ctx}() 인자는 (action[, params]) — 생성자 바인딩 필요 — 또는 "
            f"(thing_type, service, action, vin[, params]) 여야 합니다. "
            f"지금은 인자가 {n}개입니다."
        )

    def enable_offline_will(
        self,
        thing_type: Optional[str] = None,
        service: Optional[str] = None,
        vin: Optional[str] = None,
    ) -> None:
        """
        패턴 C/E LWT(단절 will)를 등록한다. paho 특성상 다음 ``connect()`` 시 적용된다.

        인자 생략 시 생성자 바인딩 값을 사용한다. 라우팅 정보가 없으면 무시한다.
        """
        import json as _json

        eff_tt = thing_type or self._thing_type
        eff_svc = service or self._service
        eff_vin = vin or self._vin
        if not (eff_tt and eff_svc and eff_vin):
            return
        will_topic = topics.build_offline(eff_tt, eff_svc, eff_vin, self._client_id)
        will_payload = _json.dumps({"clientId": self._client_id}).encode("utf-8")
        self._conn.set_will(will_topic, will_payload, qos=1)

    async def connect(self) -> None:
        """MQTT 브로커에 연결하고 응답 토픽을 구독한다."""
        token: Optional[str] = None
        if self._token_provider is not None:
            token = self._token_provider()
        self._conn.set_token(token)
        # 바인딩된 클라이언트는 기본적으로 패턴 C/E LWT를 등록한다.
        if self._is_bound():
            self.enable_offline_will()
        await self._conn.connect()
        await self._rpc.setup_subscriptions()
        self._log.info(
            "MaasClientAsync 연결 완료: endpoint=%s, client_id=%s",
            self._endpoint,
            self._client_id,
        )

    async def disconnect(self) -> None:
        """연결을 종료한다."""
        await self._conn.disconnect()
        self._log.info("MaasClientAsync 연결 종료")

    async def __aenter__(self) -> "MaasClientAsync":
        await self.connect()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.disconnect()

    # ── RPC Layer ────────────────────────────────────────────────────────────

    async def call(
        self,
        *args,
        params: Any = None,
        qos: int = 1,
        timeout: float = 10.0,
        expiry: Optional[int] = None,
        sent_at: bool = False,
    ) -> Response:
        """
        단일 RPC 호출.

        **생성자 바인딩** (``thing_type``, ``service``, ``vin`` 모두 지정):

        - ``await client.call("get")``
        - ``await client.call("get", {"path": "..."})``
        - ``await client.call("get", params={...})``

        **명시 라우팅** (플릿 등):

        - ``await client.call("CGU", "viss", "get", "VIN-1", params={...})``
        - ``await client.call("CGU", "viss", "get", "VIN-1", {"path": "..."})`` (params 다섯 번째 위치)

        Args:
            args: 위 패턴 중 하나.
            params: RPC 인자. 네 축만 위치로 줄 때는 keyword로 전달. 위치 params와 동시 사용 불가.
            qos: MQTT QoS (0 또는 1).
            timeout: 응답 대기 타임아웃(초). QoS 1이면 PUBLISH의 Message Expiry
                Interval도 이 값과 맞춘다(초 올림, 최소 1).
            expiry: Message Expiry Interval(초). **QoS 0일 때만** PUBLISH에 넣는다.
                QoS 1에서는 무시되며 Expiry는 ``timeout``에서 유도된다. QoS 0은
                보통 비큐잉이라 Expiry 실효는 제한적이며, 시한성 제어는 QoS 1 권장.

        Returns:
            RpcResponse (응답 본문은 ``payload`` 필드, MQTT 페이로드와 용어 구분).

        Raises:
            TypeError: 인자 개수·조합이 맞지 않을 때.
            ValueError: 바인딩 단축 형식인데 생성자에 라우팅이 없을 때.
        """
        thing_type, service, action, vin, eff_params = self._parse_rpc_routing(
            *args, params=params, for_stream=False
        )
        return await self._rpc.call(
            thing_type=thing_type,
            service=service,
            action=action,
            vin=vin,
            params=eff_params,
            qos=qos,
            timeout=timeout,
            expiry=expiry,
            sent_at=sent_at,
        )

    # ── 패턴별 편의 메서드 (제네릭 call 위임) ─────────────────────────────────

    async def call_best_effort(
        self,
        *args,
        params: Any = None,
        timeout: float = 5.0,
        on_event: Optional[Any] = None,
        on_cancel: Optional[Any] = None,
    ) -> Response:
        """
        패턴 A/C: Best-Effort 호출 (QoS 0).

        서버가 스트리밍 구독(``is_EOF=false``)으로 응답하면 ``Response.is_subscription``
        이 True가 되며, ``on_event``/``on_cancel`` 콜백을 주면 논블로킹 소비를 시작한다.
        """
        resp = await self.call(*args, params=params, qos=0, timeout=timeout)
        if (on_event or on_cancel) and resp.is_subscription:
            if on_event:
                resp.on_event(on_event)
            if on_cancel:
                resp.on_cancel(on_cancel)
        return resp

    async def call_reliable(
        self,
        *args,
        params: Any = None,
        timeout: float = 10.0,
    ) -> Response:
        """패턴 B: Reliable 호출 (QoS 1, 결과 보장)."""
        return await self.call(*args, params=params, qos=1, timeout=timeout)

    async def call_timed(
        self,
        *args,
        params: Any = None,
        valid_for: float = 10.0,
    ) -> Response:
        """
        패턴 D: Time-bound 호출 (QoS 1, ``sent_at`` 포함).

        시한 ``valid_for`` 내에 응답이 없으면 ``RpcExpiredError`` 가 발생한다.
        """
        return await self.call(
            *args, params=params, qos=1, timeout=valid_for, sent_at=True
        )

    async def stream(
        self,
        *args,
        params: Any = None,
        qos: int = 1,
    ) -> AsyncIterator[StreamEvent]:
        """
        스트리밍 RPC 호출. async for 로 청크를 수신한다.

        인자 규칙은 ``call`` 과 동일 (바인딩: ``action[, params]``,
        명시: ``thing_type, service, action, vin[, params]``).

        서버는 청크와 완료 신호 모두 WMO/.../response 토픽으로 발행한다.
        User Property ``is_EOF=false``가 청크, ``is_EOF=true``가 완료 신호다.
        """
        thing_type, service, action, vin, eff_params = self._parse_rpc_routing(
            *args, params=params, for_stream=True
        )
        async for event in self._rpc.stream(
            thing_type=thing_type,
            service=service,
            action=action,
            vin=vin,
            params=eff_params,
            qos=qos,
        ):
            yield event

    async def open_subscription(
        self,
        *args,
        params: Any = None,
        qos: int = 1,
    ) -> Subscription:
        """
        패턴 C: 클라이언트-종료형 무한 구독.

        인자 규칙은 call/stream 과 동일.

        반환 Subscription:
          - subscription_id: 서버가 첫 이벤트에 포함한 UUID
          - events: 데이터 이벤트 async iterator
          - await sub.unsubscribe(): 클라이언트 주도 취소

        서버 주도 강제 취소 시 events에서 SubscriptionCancelledError 발생:
            try:
                async for event in sub.events:
                    ...
            except SubscriptionCancelledError as e:
                print(e.reason)
        """
        thing_type, service, action, vin, eff_params = self._parse_rpc_routing(
            *args, params=params, for_stream=True
        )
        return await self._rpc.open_subscription(
            thing_type=thing_type,
            service=service,
            action=action,
            vin=vin,
            params=eff_params,
            qos=qos,
        )

    def exclusive_session(
        self,
        *thing_svc_vin: str,
        acquire_action: str = "session_start",
        release_action: str = "session_stop",
        timeout: float = 15.0,
    ) -> "ExclusiveSessionContext":
        """
        독점 세션 컨텍스트 매니저 (패턴 E).

        인자 없음: 생성자에 지정한 thing_type, service, vin 사용.

        인자 세 개: (thing_type, service, vin) 명시 (고급).

        async with client.exclusive_session() as session:
            await session.call(action="ecu_reset", params={})
        """
        if len(thing_svc_vin) == 0:
            thing_type, service, vin = self._bound_routing()
        elif len(thing_svc_vin) == 3:
            thing_type, service, vin = (
                thing_svc_vin[0],
                thing_svc_vin[1],
                thing_svc_vin[2],
            )
        else:
            raise TypeError(
                "exclusive_session() 인자는 0개(생성자 바인딩) 또는 "
                "(thing_type, service, vin) 3개여야 합니다."
            )
        return ExclusiveSessionContext(
            client=self,
            thing_type=thing_type,
            service=service,
            vin=vin,
            acquire_action=acquire_action,
            release_action=release_action,
            timeout=timeout,
        )

    # ── Pub/Sub Layer ─────────────────────────────────────────────────────────

    async def publish(
        self,
        topic: str,
        payload: Any,
        qos: int = 0,
        message_expiry: Optional[int] = None,
    ) -> None:
        """임의 토픽에 메시지 발행."""
        await self._pubsub.publish(topic, payload, qos=qos, message_expiry=message_expiry)

    async def subscribe(
        self,
        topic: str,
        callback: MessageHandler,
        qos: int = 1,
    ) -> None:
        """임의 토픽 구독."""
        await self._pubsub.subscribe(topic, callback, qos=qos)

    async def unsubscribe(self, topic: str) -> None:
        """임의 토픽 구독 해제."""
        await self._pubsub.unsubscribe(topic)

    # ── Server Watcher ────────────────────────────────────────────────────────

    async def watch_server(
        self,
        thing_type: Optional[str] = None,
        service: Optional[str] = None,
        vin: Optional[str] = None,
    ) -> "ServerWatcher":
        """
        패턴 독립적인 서버 Heartbeat 감시.

        인자 생략 시 생성자 바인딩 값 사용.
        반환된 ServerWatcher의 on_offline(callback)으로 오프라인 콜백 등록.
        완료 시 await watcher.stop() 호출.

        Args:
            thing_type: 서버 ThingType. None이면 생성자 바인딩 값 사용.
            service: 서비스 이름. None이면 생성자 바인딩 값 사용.
            vin: 대상 VIN. None이면 생성자 바인딩 값 사용.

        Returns:
            ServerWatcher.

        Raises:
            ValueError: 유효한 라우팅 정보가 없을 때.
        """
        eff_tt = thing_type or self._thing_type
        eff_svc = service or self._service
        eff_vin = vin or self._vin
        if not (eff_tt and eff_svc and eff_vin):
            raise ValueError("watch_server: thing_type, service, vin 이 필요합니다.")

        hb_topic = topics.build_server_heartbeat(eff_tt, eff_svc, eff_vin)
        watcher = ServerWatcher(
            _hb_mgr=self._hb_mgr,
            _hb_topic=hb_topic,
            _watcher_id=-1,  # 아직 미등록
        )

        def _on_offline(exc: Exception) -> None:
            watcher._is_online = False
            for cb in watcher._offline_callbacks:
                try:
                    cb(exc)
                except Exception:
                    pass

        wid = await self._hb_mgr.add_callback_watcher(hb_topic, _on_offline)
        watcher._watcher_id = wid
        return watcher

    # ── Internal ──────────────────────────────────────────────────────────────

    def _dispatch_message(self, msg: IncomingMessage) -> None:
        """수신 메시지를 RPC 또는 pub/sub 레이어로 라우팅."""
        if not self._rpc.handle_incoming(msg):
            self._pubsub.handle_incoming(msg)

    @property
    def client_id(self) -> str:
        """클라이언트 ID."""
        return self._client_id

    @property
    def is_connected(self) -> bool:
        """연결 상태."""
        return self._conn.is_connected


class ExclusiveSessionContext:
    """
    독점 세션 비동기 컨텍스트 매니저.

    진입 시 acquire_action RPC를 호출하여 서버 측 Lock을 획득하고,
    종료 시 release_action RPC를 호출하여 Lock을 해제한다.
    """

    def __init__(
        self,
        client: MaasClientAsync,
        thing_type: str,
        service: str,
        vin: str,
        acquire_action: str,
        release_action: str,
        timeout: float,
    ) -> None:
        self._client = client
        self._thing_type = thing_type
        self._service = service
        self._vin = vin
        self._acquire_action = acquire_action
        self._release_action = release_action
        self._timeout = timeout

    async def __aenter__(self) -> "ExclusiveSessionContext":
        await self._client.call(
            self._thing_type,
            self._service,
            self._acquire_action,
            self._vin,
            qos=1,
            timeout=self._timeout,
        )
        return self

    async def __aexit__(self, exc_type: Any, *args: Any) -> None:
        try:
            await self._client.call(
                self._thing_type,
                self._service,
                self._release_action,
                self._vin,
                qos=1,
                timeout=self._timeout,
            )
        except Exception:
            logger.warning("세션 해제 RPC 실패", exc_info=True)

    async def call(
        self,
        action: str,
        params: Any = None,
        *,
        qos: int = 1,
        timeout: Optional[float] = None,
    ) -> RpcResponse:
        """세션 내에서 RPC 호출."""
        return await self._client.call(
            self._thing_type,
            self._service,
            action,
            self._vin,
            params=params,
            qos=qos,
            timeout=timeout or self._timeout,
        )
