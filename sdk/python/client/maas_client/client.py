"""
MaasClient: MQTT 5.0 동기 클라이언트 (기본 인터페이스).

asyncio 루프를 백그라운드 스레드에서 실행하며,
모든 메서드는 결과가 나올 때까지 블로킹한다.
"""

from __future__ import annotations

import asyncio
import logging
import queue
import threading
from typing import Any, Callable, Iterator, Optional

from .auth import TokenProvider
from .client_async import MaasClientAsync
from ._pubsub import MessageHandler
from .exceptions import RpcTimeoutError, SubscriptionCancelledError
from .models import Response, RpcResponse, StreamEvent, Message, ServerWatcher

logger = logging.getLogger(__name__)


class SyncResponse:
    """
    통합 ``Response`` 의 동기 래퍼. 단일/스트림/세션 응답을 블로킹 환경에서 노출한다.

    - 단일: ``resp.payload`` / ``resp.reason_code``.
    - 스트림: ``for ev in resp:`` (백그라운드 루프에서 큐로 수집) / ``resp.stop()``.
    - 세션: ``resp.session`` (활성 ``Session`` 또는 None).
    """

    def __init__(self, async_resp: Response, loop: Any) -> None:
        self._resp = async_resp
        self._loop = loop

    @property
    def payload(self) -> Any:
        return self._resp.payload

    @property
    def reason_code(self) -> int:
        return self._resp.reason_code

    @property
    def correlation_id(self) -> Optional[bytes]:
        return self._resp.correlation_id

    @property
    def is_subscription(self) -> bool:
        return self._resp.is_subscription

    @property
    def subscription_id(self) -> Optional[str]:
        return self._resp.subscription_id

    @property
    def session(self) -> Any:
        return self._resp.session

    def __iter__(self) -> Iterator[StreamEvent]:
        if not self._resp.is_subscription:
            return
        sync_queue: queue.Queue = queue.Queue()

        async def _collect() -> None:
            try:
                async for ev in self._resp:
                    sync_queue.put(ev)
            except Exception as exc:
                sync_queue.put(exc)
            finally:
                sync_queue.put(None)

        asyncio.run_coroutine_threadsafe(_collect(), self._loop)
        while True:
            item = sync_queue.get()
            if item is None:
                break
            if isinstance(item, Exception):
                raise item
            yield item

    def stop(self) -> Any:
        """구독을 취소한다 (블로킹)."""
        fut = asyncio.run_coroutine_threadsafe(self._resp.stop(), self._loop)
        return fut.result(timeout=15.0)


class SyncSubscription:
    """
    패턴 C 동기 구독 핸들. MaasClient.open_subscription() 반환값.
    """

    def __init__(
        self,
        subscription_id: Optional[str],
        rpc: Any,
        thing_type: str,
        service: str,
        vin: str,
        loop: Any,
    ) -> None:
        self.subscription_id = subscription_id
        self._rpc = rpc
        self._thing_type = thing_type
        self._service = service
        self._vin = vin
        self._loop = loop

    def unsubscribe(self, action: str = "unsubscribe") -> RpcResponse:
        """클라이언트 주도 취소 (블로킹). unsubscribe action을 별도 RPC로 호출."""
        future = asyncio.run_coroutine_threadsafe(
            self._rpc.call(
                self._thing_type,
                self._service,
                action,
                self._vin,
                params={"subscriptionId": self.subscription_id},
            ),
            self._loop,
        )
        return future.result(timeout=15.0)


class SyncServerWatcher:
    """ServerWatcher 동기 래퍼."""

    def __init__(self, async_watcher: ServerWatcher, loop: Any) -> None:
        self._watcher = async_watcher
        self._loop = loop

    @property
    def is_online(self) -> bool:
        """서버 온라인 여부."""
        return self._watcher.is_online

    def on_offline(self, callback: Callable) -> None:
        """오프라인 감지 시 호출될 콜백 등록. callback(exc: Exception) 형태."""
        self._watcher.on_offline(callback)

    def stop(self) -> None:
        """감시 중단 (블로킹)."""
        fut = asyncio.run_coroutine_threadsafe(self._watcher.stop(), self._loop)
        fut.result(timeout=5.0)


class MaasClient:
    """
    MQTT 5.0 동기 클라이언트 (기본 인터페이스).

    기본은 WSS+TLS이며, ``use_wss=False`` 로 로컬 TCP 브로커(Mosquitto 등)에도 연결할 수 있다.
    내부적으로 asyncio 루프를 전용 스레드에서 운영한다.
    모든 메서드는 블로킹 방식으로 동작하므로 일반 Python 스크립트,
    Flask 등 비동기 컨텍스트 없이 바로 사용할 수 있다.

    Example (생성자 바인딩 + 짧은 ``call``)::

        client = MaasClient(
            endpoint="mqtt.example.com",
            client_id="my-client",
            thing_type="CGU",
            service="viss",
            vin="VIN-001",
            token_provider=get_jwt,
        )
        client.connect()
        result = client.call("get", {"path": "Vehicle.Speed"})
        client.disconnect()

    Example (호출마다 라우팅 지정)::

        client = MaasClient(endpoint="...", client_id="...")
        client.connect()
        result = client.call("CGU", "viss", "get", "VIN-001", {"path": "Vehicle.Speed"})
        client.disconnect()
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
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Args:
            endpoint: 브로커 호스트명.
            client_id: MQTT 클라이언트 ID.
            token_provider: 연결 시마다 호출되어 MQTT username 문자열을 반환.
                None이면 인증 없이 연결.
            port: 브로커 포트. None이면 ``use_wss`` 에 따라 443(WSS) 또는 1883(TCP).
            use_wss: True면 WebSocket+TLS, False면 TCP(로컬 Mosquitto 등).
            thing_type: 바인딩 시 토픽 ThingType. ``service``, ``vin`` 과 함께 세트로 지정.
            service: 바인딩 시 서비스 이름.
            vin: 바인딩 시 대상 VIN.
            logger: 로거 인스턴스.
        """
        self._log = logger or logging.getLogger(__name__)
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever,
            name="maas-client-loop",
            daemon=True,
        )
        self._thread.start()

        self._async = MaasClientAsync(
            endpoint=endpoint,
            client_id=client_id,
            token_provider=token_provider,
            port=port,
            use_wss=use_wss,
            thing_type=thing_type,
            service=service,
            vin=vin,
            logger=self._log,
        )

    def _run(self, coro: Any, timeout: Optional[float] = None) -> Any:
        """코루틴을 백그라운드 루프에 제출하고 결과를 블로킹 대기."""
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result(timeout=timeout)

    def connect(self, timeout: float = 30.0) -> None:
        """MQTT 브로커에 연결한다."""
        self._run(self._async.connect(), timeout=timeout)

    def disconnect(self) -> None:
        """연결을 종료하고 백그라운드 루프를 중지한다."""
        try:
            self._run(self._async.disconnect(), timeout=10.0)
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=5.0)

    def __enter__(self) -> "MaasClient":
        self.connect()
        return self

    def __exit__(self, *args: Any) -> None:
        self.disconnect()

    # ── RPC Layer ────────────────────────────────────────────────────────────

    def call(
        self,
        *args,
        params: Any = None,
        qos: int = 1,
        timeout: float = 10.0,
        expiry: Optional[int] = None,
    ) -> RpcResponse:
        """
        단일 RPC 호출 (블로킹).

        ``MaasClientAsync.call`` 와 동일한 인자 규칙:
        바인딩 시 ``call(action[, params])``, 명시 시
        ``call(thing_type, service, action, vin[, params])``.

        ``timeout``·``expiry``·QoS 1에서의 Message Expiry 연동은
        ``MaasClientAsync.call`` 과 동일하다.

        Returns:
            RpcResponse.

        Raises:
            TypeError, ValueError: 인자 조합 오류.
            RpcTimeoutError: 타임아웃 초과.
            RpcServerError: 서버 오류 응답.
        """
        return self._run(
            self._async.call(
                *args,
                params=params,
                qos=qos,
                timeout=timeout,
                expiry=expiry,
            ),
            timeout=timeout + 5.0,
        )

    # ── 패턴별 편의 메서드 (블로킹) ───────────────────────────────────────────

    def call_best_effort(
        self,
        *args,
        params: Any = None,
        timeout: float = 5.0,
        on_event: Optional[Callable] = None,
        on_cancel: Optional[Callable] = None,
    ) -> SyncResponse:
        """패턴 A/C: Best-Effort 호출 (QoS 0). 스트림이면 SyncResponse로 반복/콜백 가능."""
        resp = self._run(
            self._async.call_best_effort(
                *args, params=params, timeout=timeout
            ),
            timeout=timeout + 5.0,
        )
        if (on_event or on_cancel) and resp.is_subscription:
            def _arm() -> None:
                if on_event:
                    resp.on_event(on_event)
                if on_cancel:
                    resp.on_cancel(on_cancel)
            self._loop.call_soon_threadsafe(_arm)
        return SyncResponse(resp, self._loop)

    def call_reliable(
        self,
        *args,
        params: Any = None,
        timeout: float = 10.0,
    ) -> SyncResponse:
        """패턴 B: Reliable 호출 (QoS 1)."""
        resp = self._run(
            self._async.call_reliable(*args, params=params, timeout=timeout),
            timeout=timeout + 5.0,
        )
        return SyncResponse(resp, self._loop)

    def call_timed(
        self,
        *args,
        params: Any = None,
        valid_for: float = 10.0,
    ) -> SyncResponse:
        """패턴 D: Time-bound 호출 (QoS 1). 시한 초과 시 RpcExpiredError."""
        resp = self._run(
            self._async.call_timed(*args, params=params, valid_for=valid_for),
            timeout=valid_for + 5.0,
        )
        return SyncResponse(resp, self._loop)

    def stream(
        self,
        *args,
        params: Any = None,
        qos: int = 1,
        chunk_timeout: float = 60.0,
    ) -> Iterator[StreamEvent]:
        """
        스트리밍 RPC 호출 (동기 이터레이터).

        인자 규칙은 ``call`` 과 동일.

        Args:
            chunk_timeout: 청크 간 최대 대기 시간(초).

        Yields:
            StreamEvent (is_eof=False인 청크들).

        Raises:
            RpcServerError: 서버 오류.
            ServerOfflineError: 연결 끊김(HB 감시).
        """
        sync_queue: queue.Queue = queue.Queue()

        async def _collect() -> None:
            try:
                async for event in self._async.stream(
                    *args,
                    params=params,
                    qos=qos,
                ):
                    sync_queue.put(event)
            except Exception as exc:
                sync_queue.put(exc)
            finally:
                sync_queue.put(None)  # 종료 sentinel

        asyncio.run_coroutine_threadsafe(_collect(), self._loop)

        while True:
            item = sync_queue.get(timeout=chunk_timeout)
            if item is None:
                break
            if isinstance(item, Exception):
                raise item
            if not item.is_eof:
                yield item

    def open_subscription(
        self,
        *args,
        params: Any = None,
        qos: int = 1,
        on_event: Optional[Callable] = None,
        on_cancel: Optional[Callable] = None,
        first_event_timeout: float = 30.0,
    ) -> SyncSubscription:
        """
        패턴 C 동기 래퍼. 첫 이벤트까지 블로킹 후 SyncSubscription 반환.

        이후 이벤트는 백그라운드 스레드에서 on_event 콜백으로 처리된다.

        Args:
            args: call/stream 과 동일한 인자 규칙.
            params: RPC 인자.
            qos: MQTT QoS.
            on_event: 이벤트 수신 시 호출되는 콜백 (StreamEvent).
            on_cancel: 서버 주도 취소 시 호출되는 콜백 (reason: str).
            first_event_timeout: 첫 이벤트 대기 타임아웃(초).

        Returns:
            SyncSubscription (subscription_id, unsubscribe()).
        """
        first_ready = threading.Event()
        sub_holder: list = [None]

        async def _run() -> None:
            sub = await self._async.open_subscription(*args, params=params, qos=qos)
            sub_holder[0] = sub
            first_ready.set()
            try:
                async for event in sub.events:
                    if on_event:
                        on_event(event)
            except SubscriptionCancelledError as e:
                if on_cancel:
                    on_cancel(e.reason)

        asyncio.run_coroutine_threadsafe(_run(), self._loop)
        if not first_ready.wait(timeout=first_event_timeout):
            raise RpcTimeoutError("open_subscription", "first_event", first_event_timeout)

        a_sub = sub_holder[0]
        return SyncSubscription(
            subscription_id=a_sub.subscription_id if a_sub else None,
            rpc=a_sub._rpc if a_sub else None,
            thing_type=a_sub._thing_type if a_sub else "",
            service=a_sub._service if a_sub else "",
            vin=a_sub._vin if a_sub else "",
            loop=self._loop,
        )

    def exclusive_session(
        self,
        *thing_svc_vin: str,
        acquire_action: str = "session_start",
        release_action: str = "session_stop",
        timeout: float = 15.0,
    ) -> "SyncExclusiveSessionContext":
        """
        독점 세션 컨텍스트 매니저 (패턴 E).

        인자 없음: 생성자 바인딩 사용.

        인자 세 개: (thing_type, service, vin) 명시.

        with client.exclusive_session() as session:
            session.call("ecu_reset", params={})
        """
        if len(thing_svc_vin) == 0:
            thing_type, service, vin = self._async._bound_routing()
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
        return SyncExclusiveSessionContext(
            client=self,
            thing_type=thing_type,
            service=service,
            vin=vin,
            acquire_action=acquire_action,
            release_action=release_action,
            timeout=timeout,
        )

    # ── Pub/Sub Layer ─────────────────────────────────────────────────────────

    def publish(
        self,
        topic: str,
        payload: Any,
        qos: int = 0,
        message_expiry: Optional[int] = None,
    ) -> None:
        """임의 토픽에 메시지 발행 (블로킹)."""
        self._run(
            self._async.publish(topic, payload, qos=qos, message_expiry=message_expiry)
        )

    def subscribe(self, topic: str, callback: MessageHandler, qos: int = 1) -> None:
        """임의 토픽 구독 (블로킹). 메시지 수신 시 callback이 호출된다."""
        self._run(self._async.subscribe(topic, callback, qos=qos))

    def unsubscribe(self, topic: str) -> None:
        """임의 토픽 구독 해제 (블로킹)."""
        self._run(self._async.unsubscribe(topic))

    # ── Server Watcher ────────────────────────────────────────────────────────

    def watch_server(
        self,
        thing_type: Optional[str] = None,
        service: Optional[str] = None,
        vin: Optional[str] = None,
    ) -> "SyncServerWatcher":
        """
        패턴 독립적인 서버 Heartbeat 감시 (블로킹).

        인자 생략 시 생성자 바인딩 값 사용.
        반환된 SyncServerWatcher의 on_offline(callback)으로 오프라인 콜백 등록.
        완료 시 watcher.stop() 호출.

        Returns:
            SyncServerWatcher.
        """
        fut = asyncio.run_coroutine_threadsafe(
            self._async.watch_server(thing_type, service, vin),
            self._loop,
        )
        async_watcher = fut.result(timeout=10.0)
        return SyncServerWatcher(async_watcher, self._loop)

    @property
    def client_id(self) -> str:
        """클라이언트 ID."""
        return self._async.client_id

    @property
    def is_connected(self) -> bool:
        """연결 상태."""
        return self._async.is_connected


class SyncExclusiveSessionContext:
    """MaasClient용 동기 독점 세션 컨텍스트 매니저."""

    def __init__(
        self,
        client: MaasClient,
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

    def __enter__(self) -> "SyncExclusiveSessionContext":
        self._client.call(
            self._thing_type,
            self._service,
            self._acquire_action,
            self._vin,
            qos=1,
            timeout=self._timeout,
        )
        return self

    def __exit__(self, exc_type: Any, *args: Any) -> None:
        try:
            self._client.call(
                self._thing_type,
                self._service,
                self._release_action,
                self._vin,
                qos=1,
                timeout=self._timeout,
            )
        except Exception:
            logger.warning("세션 해제 RPC 실패", exc_info=True)

    def call(
        self,
        action: str,
        params: Any = None,
        *,
        qos: int = 1,
        timeout: Optional[float] = None,
    ) -> RpcResponse:
        """세션 내에서 RPC 호출."""
        return self._client.call(
            self._thing_type,
            self._service,
            action,
            self._vin,
            params=params,
            qos=qos,
            timeout=timeout or self._timeout,
        )
