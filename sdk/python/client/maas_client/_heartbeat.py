"""
Heartbeat 관리자 (내부 모듈).

같은 Heartbeat 토픽을 여러 구독/ServerWatcher가 공유한다.
ref-count 기반으로 구독/구독해제를 자동 관리한다.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


@dataclass
class _HbTopicState:
    last_ts: Optional[float] = None         # 마지막 HB 수신 시각 (monotonic)
    known_interval: Optional[float] = None  # 페이로드에서 읽은 서버 발행 간격
    stream_watchers: dict = field(default_factory=dict)    # corr_id(bytes) → asyncio.Queue
    callback_watchers: dict = field(default_factory=dict)  # watcher_id(int) → Callable
    _next_watcher_id: int = 0
    watchdog_task: Optional[asyncio.Task] = None


class HeartbeatManager:
    """
    Heartbeat 구독 공유 관리자.

    같은 (thing_type/service/vin) 서버를 감시하는 여러 주체
    (open_subscription, watch_server)가 동일 MQTT 구독을 공유하도록 ref-count를 관리한다.
    """

    def __init__(
        self,
        conn: Any,  # Mqtt5Connection
        hint_interval: float = 10.0,
        timeout_multiplier: float = 3.0,
        first_hb_timeout: float = 30.0,
    ) -> None:
        self._conn = conn
        self._hint_interval = hint_interval
        self._timeout_multiplier = timeout_multiplier
        self._first_hb_timeout = first_hb_timeout
        self._topics: dict[str, _HbTopicState] = {}
        self._lock = asyncio.Lock()

    def handle_heartbeat(self, msg: Any) -> bool:
        """
        수신 메시지가 heartbeat suffix이면 처리. Returns True if handled.

        페이로드: {"interval": N, "ts": float}
        stale 방어: now - msg_ts > known_interval * 2 이면 무시.
        """
        topic: str = msg.topic
        suffix = topic.rsplit("/", 1)[-1] if "/" in topic else ""
        if suffix != "heartbeat":
            return False

        state = self._topics.get(topic)
        if state is None:
            return True  # 구독 중이지만 감시자 없음 (정리 직전) → 그냥 무시

        from .connection import decode_payload
        try:
            payload = decode_payload(msg.payload)
            msg_ts = payload.get("ts") if isinstance(payload, dict) else None
            srv_interval = payload.get("interval") if isinstance(payload, dict) else None
        except Exception:
            payload = {}
            msg_ts = None
            srv_interval = None

        # stale 필터: 페이로드 timestamp가 있고 너무 오래됐으면 무시
        eff_interval = (srv_interval or state.known_interval or self._hint_interval)
        if msg_ts is not None:
            age = time.time() - msg_ts
            if age > eff_interval * 2:
                logger.debug("Stale heartbeat 무시: age=%.1fs topic=%s", age, topic)
                return True

        state.last_ts = time.monotonic()
        if srv_interval:
            state.known_interval = float(srv_interval)
        return True

    async def watch_stream(
        self,
        hb_topic: str,
        corr_id: bytes,
        stream_queue: asyncio.Queue,
    ) -> None:
        """
        스트림 큐를 Heartbeat 감시 대상으로 등록.
        서버 오프라인 감지 시 stream_queue에 SubscriptionCancelledError("server_offline") 주입.
        첫 HB를 first_hb_timeout 내에 못 받으면 ServerOfflineError 주입.
        """
        async with self._lock:
            state = await self._ensure_subscribed(hb_topic)
            state.stream_watchers[corr_id] = stream_queue

    async def unwatch_stream(self, hb_topic: str, corr_id: bytes) -> None:
        """스트림 감시 해제. ref-count 0이면 구독 해제."""
        async with self._lock:
            state = self._topics.get(hb_topic)
            if state:
                state.stream_watchers.pop(corr_id, None)
            await self._maybe_unsubscribe(hb_topic)

    async def add_callback_watcher(
        self,
        hb_topic: str,
        on_offline: Callable,
    ) -> int:
        """콜백 감시자 추가. 반환된 watcher_id로 나중에 제거."""
        async with self._lock:
            state = await self._ensure_subscribed(hb_topic)
            wid = state._next_watcher_id
            state._next_watcher_id += 1
            state.callback_watchers[wid] = on_offline
            return wid

    async def remove_callback_watcher(self, hb_topic: str, watcher_id: int) -> None:
        """콜백 감시자 제거. ref-count 0이면 구독 해제."""
        async with self._lock:
            state = self._topics.get(hb_topic)
            if state:
                state.callback_watchers.pop(watcher_id, None)
            await self._maybe_unsubscribe(hb_topic)

    async def _ensure_subscribed(self, hb_topic: str) -> _HbTopicState:
        """토픽이 없으면 구독하고 state 생성. Lock 보유 상태에서 호출."""
        if hb_topic not in self._topics:
            self._topics[hb_topic] = _HbTopicState()
            await self._conn.subscribe(hb_topic, qos=0)
            task = asyncio.create_task(self._watchdog(hb_topic))
            self._topics[hb_topic].watchdog_task = task
        return self._topics[hb_topic]

    async def _maybe_unsubscribe(self, hb_topic: str) -> None:
        """감시자가 모두 없으면 구독 해제. Lock 보유 상태에서 호출."""
        state = self._topics.get(hb_topic)
        if state is None:
            return
        if state.stream_watchers or state.callback_watchers:
            return
        # 모두 없음 → 정리
        if state.watchdog_task:
            state.watchdog_task.cancel()
        del self._topics[hb_topic]
        try:
            await self._conn.unsubscribe(hb_topic)
        except Exception:
            pass

    async def _watchdog(self, hb_topic: str) -> None:
        """
        주기적으로 last_ts를 확인해 타임아웃 시 에러 주입/콜백 호출.

        콜드 스타트: first_hb_timeout 내 첫 HB 없으면 ServerOfflineError.
        운용 중: interval * multiplier 초 내 HB 없으면 SubscriptionCancelledError.
        """
        from .exceptions import SubscriptionCancelledError, ServerOfflineError

        # 파싱: "WMO/{tt}/{svc}/{vin}/heartbeat"
        parts = hb_topic.split("/")
        tt = parts[1] if len(parts) > 1 else ""
        svc = parts[2] if len(parts) > 2 else ""
        vin = parts[3] if len(parts) > 3 else ""

        start = time.monotonic()
        cold_start = True

        while True:
            await asyncio.sleep(1.0)  # 1초 간격으로 체크
            state = self._topics.get(hb_topic)
            if state is None:
                return  # 구독 해제됨

            now = time.monotonic()
            eff_interval = state.known_interval or self._hint_interval

            if cold_start:
                # 첫 HB 대기
                if state.last_ts is not None:
                    cold_start = False  # 첫 HB 도착
                    continue
                elapsed = now - start
                if elapsed >= self._first_hb_timeout:
                    # 콜드 스타트 타임아웃
                    exc: Exception = ServerOfflineError(tt, svc, vin)
                    await self._broadcast_error(hb_topic, state, exc)
                    self._broadcast_callbacks(state, exc)
                    return
            else:
                # 운용 중 타임아웃 체크
                if state.last_ts is not None:
                    elapsed_since_hb = now - state.last_ts
                    timeout = eff_interval * self._timeout_multiplier
                    if elapsed_since_hb >= timeout:
                        exc = SubscriptionCancelledError("server_offline")
                        await self._broadcast_error(hb_topic, state, exc)
                        self._broadcast_callbacks(state, exc)
                        return

    async def _broadcast_error(
        self, hb_topic: str, state: _HbTopicState, exc: Exception
    ) -> None:
        """모든 stream_watchers 큐에 에러 주입."""
        for q in list(state.stream_watchers.values()):
            try:
                await q.put(exc)
            except Exception:
                pass

    def _broadcast_callbacks(self, state: _HbTopicState, exc: Exception) -> None:
        """모든 callback_watchers 호출."""
        for cb in list(state.callback_watchers.values()):
            try:
                cb(exc)
            except Exception:
                pass
