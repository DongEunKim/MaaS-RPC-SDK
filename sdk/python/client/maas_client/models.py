"""
maas-client-sdk 데이터 모델.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Callable, List, Optional

from .exceptions import SubscriptionCancelledError


@dataclass
class StreamEvent:
    """스트리밍 청크 이벤트 (WMO/.../response 수신)."""

    payload: Any
    """청크 페이로드. 구조는 서버-클라이언트 계약에 따름."""

    is_eof: bool = False
    """마지막 청크 여부. True이면 스트림 종료."""

    correlation_id: Optional[bytes] = None


@dataclass
class Session:
    """
    패턴 E: 독점 세션 핸들. 응답 User Property ``session_id`` 가 비어있지 않을 때 생성.

    ``on_lost(cb)`` 로 서버 단절(Heartbeat watchdog / LWT) 감지 시 콜백을 등록하고,
    ``stop()`` 으로 로컬 감시를 해제한다.
    """

    session_id: str

    _rpc: Any = field(default=None, repr=False)
    _thing_type: str = field(default="", repr=False)
    _service: str = field(default="", repr=False)
    _vin: str = field(default="", repr=False)
    _hb_topic: Optional[str] = field(default=None, repr=False)
    _watcher_id: Optional[int] = field(default=None, repr=False)
    _lost_callbacks: List[Callable] = field(default_factory=list, repr=False)

    def on_lost(self, callback: Callable) -> None:
        """세션 단절 감지 시 호출될 콜백 등록. ``callback(exc: Exception)`` 형태."""
        self._lost_callbacks.append(callback)

    def _notify_lost(self, exc: Exception) -> None:
        """내부: 단절 콜백 일괄 호출."""
        for cb in list(self._lost_callbacks):
            try:
                cb(exc)
            except Exception:
                pass

    async def stop(self) -> None:
        """로컬 세션 감시 중단."""
        if (
            self._hb_topic
            and self._watcher_id is not None
            and self._rpc is not None
            and getattr(self._rpc, "_hb_mgr", None) is not None
        ):
            try:
                await self._rpc._hb_mgr.remove_callback_watcher(
                    self._hb_topic, self._watcher_id
                )
            except Exception:
                pass


@dataclass
class Response:
    """
    통합 RPC 응답 객체 (단일 / 스트리밍 구독 / 독점 세션).

    - 단일 응답: ``payload`` / ``reason_code`` / ``correlation_id`` 로 접근.
    - 스트리밍 구독(``is_subscription=True``): ``async for ev in result`` 또는
      ``on_event`` 콜백으로 소비. ``await result.stop()`` 으로 구독 취소.
    - 독점 세션: ``result.session`` 이 ``Session`` (활성) 또는 ``None``.

    ``RpcResponse`` 는 이 클래스의 별칭이다.
    """

    payload: Any = None
    reason_code: int = 0
    correlation_id: Optional[bytes] = None
    is_subscription: bool = False
    subscription_id: Optional[str] = None
    session: Optional[Session] = None

    _rpc: Any = field(default=None, repr=False)
    _thing_type: str = field(default="", repr=False)
    _service: str = field(default="", repr=False)
    _vin: str = field(default="", repr=False)
    _corr_id: Optional[bytes] = field(default=None, repr=False)
    _queue: Optional[asyncio.Queue] = field(default=None, repr=False)
    _unsubscribe_action: str = field(default="unsubscribe", repr=False)
    _first_event: Optional[StreamEvent] = field(default=None, repr=False)
    _hb_topic: Optional[str] = field(default=None, repr=False)
    _consumed: bool = field(default=False, repr=False)
    _consumer_task: Any = field(default=None, repr=False)

    # ── 스트림 이터레이션 ─────────────────────────────────────────────────────

    async def _aiter_events(self) -> AsyncIterator[StreamEvent]:
        """내부: 첫 이벤트 포함 EOF까지 StreamEvent를 yield한다."""
        if self._consumed:
            return
        self._consumed = True

        if self._first_event is not None:
            first = self._first_event
            self._first_event = None
            yield first
            if first.is_eof:
                await self._cleanup_stream()
                return

        if self._queue is None:
            await self._cleanup_stream()
            return

        try:
            while True:
                item = await self._queue.get()
                if isinstance(item, Exception):
                    await self._cleanup_stream()
                    raise item
                yield item
                if item.is_eof:
                    break
        finally:
            await self._cleanup_stream()

    def __aiter__(self) -> AsyncIterator[StreamEvent]:
        return self._aiter_events()

    async def _cleanup_stream(self) -> None:
        """스트림 종료 시 _streams 및 HB 감시 정리."""
        if self._rpc is None or self._corr_id is None:
            return
        try:
            await self._rpc._unregister_stream(self._corr_id, self._hb_topic)
        except Exception:
            pass

    # ── 클라이언트 주도 취소 ───────────────────────────────────────────────────

    async def stop(self) -> "Response":
        """
        구독을 클라이언트 주도로 취소한다.

        ``is_subscription`` 이면 ``unsubscribe_action`` RPC를 호출하여 서버에 취소를
        요청한다(params ``{"subscriptionId": subscription_id}``). 비구독이면 no-op.
        """
        if not self.is_subscription or self._rpc is None:
            return self
        return await self._rpc.call(
            self._thing_type,
            self._service,
            self._unsubscribe_action,
            self._vin,
            params={"subscriptionId": self.subscription_id},
        )

    # ── 콜백 소비 (논블로킹) ───────────────────────────────────────────────────

    def on_event(self, callback: Callable) -> "Response":
        """이벤트 콜백 등록 후 백그라운드 소비 태스크를 시작한다. ``callback(StreamEvent)``."""
        self._start_consumer(on_event=callback)
        return self

    def on_cancel(self, callback: Callable) -> "Response":
        """서버 주도 취소 콜백 등록. ``callback(reason: Optional[str])``."""
        self._start_consumer(on_cancel=callback)
        return self

    def on_eof(self, callback: Callable) -> "Response":
        """자연 종료(EOF) 콜백 등록. ``callback()``."""
        self._start_consumer(on_eof=callback)
        return self

    def _start_consumer(
        self,
        on_event: Optional[Callable] = None,
        on_cancel: Optional[Callable] = None,
        on_eof: Optional[Callable] = None,
    ) -> None:
        """백그라운드 소비 태스크를 1회 시작하고 콜백을 누적 등록한다."""
        if not hasattr(self, "_cb_event"):
            self._cb_event: List[Callable] = []
            self._cb_cancel: List[Callable] = []
            self._cb_eof: List[Callable] = []
        if on_event:
            self._cb_event.append(on_event)
        if on_cancel:
            self._cb_cancel.append(on_cancel)
        if on_eof:
            self._cb_eof.append(on_eof)

        if not self.is_subscription:
            return
        if self._consumer_task is not None:
            return

        async def _consume() -> None:
            try:
                async for ev in self._aiter_events():
                    if ev.is_eof:
                        for cb in self._cb_eof:
                            try:
                                cb()
                            except Exception:
                                pass
                    else:
                        for cb in self._cb_event:
                            try:
                                cb(ev)
                            except Exception:
                                pass
            except SubscriptionCancelledError as exc:
                for cb in self._cb_cancel:
                    try:
                        cb(exc.reason)
                    except Exception:
                        pass

        self._consumer_task = asyncio.ensure_future(_consume())


# 별칭 (하위호환): 단일 응답 용례에서 RpcResponse 로 노출.
RpcResponse = Response


@dataclass
class Message:
    """단순 pub/sub 수신 메시지."""

    topic: str
    payload: bytes
    qos: int = 0
    user_properties: list[tuple[str, str]] = field(default_factory=list)


@dataclass
class Subscription:
    """
    패턴 C: 무한 구독 핸들. open_subscription() 반환값.

    subscription_id는 서버 핸들러가 첫 이벤트에 포함한 UUID.
    ``call(..., on_event=...)`` 으로 받는 통합 ``Response`` (``is_subscription=True``)와
    함께 패턴 C의 두 진입점을 이룬다.
    """

    subscription_id: Optional[str]
    events: AsyncIterator  # 첫 이벤트 이후 데이터 이벤트 (is_eof=True 포함)

    _rpc: Any = field(default=None, repr=False)
    _thing_type: str = field(default="", repr=False)
    _service: str = field(default="", repr=False)
    _vin: str = field(default="", repr=False)

    async def unsubscribe(self, action: str = "unsubscribe") -> "Response":
        """클라이언트 주도 취소. unsubscribe action을 별도 RPC로 호출."""
        return await self._rpc.call(
            self._thing_type, self._service, action, self._vin,
            params={"subscriptionId": self.subscription_id},
        )


@dataclass
class ServerWatcher:
    """
    패턴과 무관한 서버 연결 감시 핸들.
    watch_server() 반환값.
    """

    _hb_mgr: Any = field(repr=False)
    _hb_topic: str = field(repr=False)
    _watcher_id: int = field(repr=False)
    _is_online: bool = field(default=True, repr=False)
    _offline_callbacks: list = field(default_factory=list, repr=False)

    @property
    def is_online(self) -> bool:
        return self._is_online

    def on_offline(self, callback: Callable) -> None:
        """오프라인 감지 시 호출될 콜백 등록. callback(exc: Exception) 형태."""
        self._offline_callbacks.append(callback)

    async def stop(self) -> None:
        """감시 중단."""
        await self._hb_mgr.remove_callback_watcher(self._hb_topic, self._watcher_id)
