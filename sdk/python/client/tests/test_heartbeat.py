"""
HeartbeatManager 단위 테스트.

실제 MQTT 연결 없이 Mock 기반으로 HeartbeatManager 동작을 검증한다.
"""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from maas_client._heartbeat import HeartbeatManager, _HbTopicState
from maas_client.exceptions import ServerOfflineError, SubscriptionCancelledError


HB_TOPIC = "WMO/T/S/VIN-1/heartbeat"


def _make_hb_msg(topic: str = HB_TOPIC, interval: float = 10.0, ts: float | None = None) -> Any:
    """heartbeat 메시지 Mock 생성."""
    import json
    payload_dict = {"interval": interval, "ts": ts if ts is not None else time.time()}
    msg = SimpleNamespace(
        topic=topic,
        payload=json.dumps(payload_dict).encode(),
        qos=0,
        properties=None,
    )
    return msg


def _make_conn() -> MagicMock:
    """Mqtt5Connection mock 생성."""
    conn = MagicMock()
    conn.subscribe = AsyncMock()
    conn.unsubscribe = AsyncMock()
    return conn


@pytest.mark.asyncio
async def test_heartbeat_updates_last_ts() -> None:
    """HB 수신 → state.last_ts 갱신."""
    conn = _make_conn()
    mgr = HeartbeatManager(conn=conn, hint_interval=10.0, first_hb_timeout=30.0)

    q: asyncio.Queue = asyncio.Queue()
    corr = b"\x01"
    await mgr.watch_stream(HB_TOPIC, corr, q)

    state = mgr._topics[HB_TOPIC]
    assert state.last_ts is None

    mgr.handle_heartbeat(_make_hb_msg())

    assert state.last_ts is not None


@pytest.mark.asyncio
async def test_heartbeat_updates_known_interval() -> None:
    """페이로드 interval → state.known_interval 갱신."""
    conn = _make_conn()
    mgr = HeartbeatManager(conn=conn, hint_interval=10.0, first_hb_timeout=30.0)

    q: asyncio.Queue = asyncio.Queue()
    corr = b"\x02"
    await mgr.watch_stream(HB_TOPIC, corr, q)

    mgr.handle_heartbeat(_make_hb_msg(interval=5.0))

    state = mgr._topics[HB_TOPIC]
    assert state.known_interval == 5.0


@pytest.mark.asyncio
async def test_stale_heartbeat_ignored() -> None:
    """ts가 너무 오래된 HB → last_ts 미갱신."""
    conn = _make_conn()
    mgr = HeartbeatManager(conn=conn, hint_interval=10.0, first_hb_timeout=30.0)

    q: asyncio.Queue = asyncio.Queue()
    corr = b"\x03"
    await mgr.watch_stream(HB_TOPIC, corr, q)

    state = mgr._topics[HB_TOPIC]

    # ts가 30초 전 (interval*2=20초 초과) → stale
    stale_ts = time.time() - 30.0
    mgr.handle_heartbeat(_make_hb_msg(interval=10.0, ts=stale_ts))

    assert state.last_ts is None


@pytest.mark.asyncio
async def test_watchdog_timeout_injects_error() -> None:
    """HB 없이 timeout 경과 → 스트림 큐에 SubscriptionCancelledError('server_offline')."""
    conn = _make_conn()
    # timeout = interval(1.0) * multiplier(2.0) = 2초, first_hb_timeout은 충분히 크게
    mgr = HeartbeatManager(
        conn=conn,
        hint_interval=1.0,
        timeout_multiplier=2.0,
        first_hb_timeout=100.0,
    )

    q: asyncio.Queue = asyncio.Queue()
    corr = b"\x04"
    await mgr.watch_stream(HB_TOPIC, corr, q)

    state = mgr._topics[HB_TOPIC]
    # 첫 HB를 받은 것처럼 설정 (cold_start 탈출)
    state.last_ts = time.monotonic() - 3.0  # 3초 전 = timeout(2초) 초과
    state.known_interval = 1.0

    # watchdog이 1초 간격으로 체크하므로 잠시 대기
    try:
        exc = await asyncio.wait_for(q.get(), timeout=3.0)
    except asyncio.TimeoutError:
        pytest.fail("watchdog이 timeout 내에 에러를 주입하지 않음")

    assert isinstance(exc, SubscriptionCancelledError)
    assert exc.reason == "server_offline"


@pytest.mark.asyncio
async def test_cold_start_timeout_injects_server_offline() -> None:
    """첫 HB 없이 first_hb_timeout 경과 → ServerOfflineError."""
    conn = _make_conn()
    mgr = HeartbeatManager(
        conn=conn,
        hint_interval=10.0,
        timeout_multiplier=3.0,
        first_hb_timeout=2.0,  # 2초
    )

    q: asyncio.Queue = asyncio.Queue()
    corr = b"\x05"
    await mgr.watch_stream(HB_TOPIC, corr, q)

    # last_ts가 None인 상태 = 첫 HB 미수신 (cold start)
    try:
        exc = await asyncio.wait_for(q.get(), timeout=4.0)
    except asyncio.TimeoutError:
        pytest.fail("watchdog이 cold start timeout 내에 에러를 주입하지 않음")

    assert isinstance(exc, ServerOfflineError)


@pytest.mark.asyncio
async def test_multiple_streams_all_notified() -> None:
    """같은 서버 복수 구독 → 모두 에러 주입."""
    conn = _make_conn()
    mgr = HeartbeatManager(
        conn=conn,
        hint_interval=1.0,
        timeout_multiplier=2.0,
        first_hb_timeout=100.0,
    )

    q1: asyncio.Queue = asyncio.Queue()
    q2: asyncio.Queue = asyncio.Queue()
    corr1 = b"\x10"
    corr2 = b"\x11"

    await mgr.watch_stream(HB_TOPIC, corr1, q1)
    await mgr.watch_stream(HB_TOPIC, corr2, q2)

    state = mgr._topics[HB_TOPIC]
    # cold start 탈출 + 운용 중 타임아웃 트리거
    state.last_ts = time.monotonic() - 3.0
    state.known_interval = 1.0

    exc1 = await asyncio.wait_for(q1.get(), timeout=3.0)
    exc2 = await asyncio.wait_for(q2.get(), timeout=3.0)

    assert isinstance(exc1, SubscriptionCancelledError)
    assert isinstance(exc2, SubscriptionCancelledError)


@pytest.mark.asyncio
async def test_shared_subscription() -> None:
    """같은 HB 토픽 2개 watch → HB 구독 1회만."""
    conn = _make_conn()
    mgr = HeartbeatManager(conn=conn, hint_interval=10.0, first_hb_timeout=30.0)

    q1: asyncio.Queue = asyncio.Queue()
    q2: asyncio.Queue = asyncio.Queue()

    await mgr.watch_stream(HB_TOPIC, b"\x20", q1)
    await mgr.watch_stream(HB_TOPIC, b"\x21", q2)

    # subscribe는 최초 1회만 호출됨
    conn.subscribe.assert_called_once_with(HB_TOPIC, qos=0)


@pytest.mark.asyncio
async def test_unsubscribe_on_last_unwatch() -> None:
    """마지막 watcher 제거 → HB 토픽 구독 해제."""
    conn = _make_conn()
    mgr = HeartbeatManager(conn=conn, hint_interval=10.0, first_hb_timeout=30.0)

    q: asyncio.Queue = asyncio.Queue()
    corr = b"\x30"
    await mgr.watch_stream(HB_TOPIC, corr, q)

    conn.unsubscribe.assert_not_called()

    await mgr.unwatch_stream(HB_TOPIC, corr)

    conn.unsubscribe.assert_called_once_with(HB_TOPIC)
    assert HB_TOPIC not in mgr._topics


@pytest.mark.asyncio
async def test_callback_watcher_called() -> None:
    """add_callback_watcher → 타임아웃 시 콜백 호출."""
    conn = _make_conn()
    mgr = HeartbeatManager(
        conn=conn,
        hint_interval=1.0,
        timeout_multiplier=2.0,
        first_hb_timeout=2.0,  # 2초 후 cold start timeout
    )

    called_with: list = []

    def on_offline(exc: Exception) -> None:
        called_with.append(exc)

    await mgr.add_callback_watcher(HB_TOPIC, on_offline)

    # cold start timeout 대기
    await asyncio.sleep(3.5)

    assert len(called_with) == 1
    assert isinstance(called_with[0], ServerOfflineError)


@pytest.mark.asyncio
async def test_watch_server_returns_watcher() -> None:
    """watch_server() → ServerWatcher 반환 및 on_offline 콜백 등록."""
    from maas_client.client_async import MaasClientAsync
    from maas_client.models import ServerWatcher
    from unittest.mock import patch, AsyncMock

    with patch("maas_client.client_async.Mqtt5Connection") as MockConn:
        mock_conn = MagicMock()
        mock_conn.subscribe = AsyncMock()
        mock_conn.unsubscribe = AsyncMock()
        mock_conn.set_message_callback = MagicMock()
        MockConn.return_value = mock_conn

        client = MaasClientAsync(
            endpoint="localhost",
            client_id="test-client",
            use_wss=False,
            thing_type="T",
            service="S",
            vin="VIN-1",
        )

    watcher = await client.watch_server()

    assert isinstance(watcher, ServerWatcher)
    assert watcher.is_online is True
    assert watcher._watcher_id >= 0

    # on_offline 콜백 등록
    received: list = []
    watcher.on_offline(lambda exc: received.append(exc))
    assert len(watcher._offline_callbacks) == 1

    # stop 후 구독 해제 확인
    await watcher.stop()
    assert watcher._hb_topic not in client._hb_mgr._topics
