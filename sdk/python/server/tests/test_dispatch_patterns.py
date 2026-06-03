"""서버 패턴 A~E 디스패처 단위 테스트: QoS 미러링, Expiry, session_id 신호, 패턴 D."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from maas_server._dispatcher import Dispatcher
from maas_server.connection import IncomingMessage
from maas_server.session import ExclusiveSessionManager


def _make_msg(action: str, *, user_props=None, payload_extra=None) -> IncomingMessage:
    props = Properties(PacketTypes.PUBLISH)
    props.ResponseTopic = "WMO/T/S/VIN-1/cid/response"
    props.CorrelationData = b"\x01\x02\x03\x04"
    if user_props:
        props.UserProperty = list(user_props)
    body = {"action": action}
    if payload_extra:
        body.update(payload_extra)
    return IncomingMessage(
        SimpleNamespace(
            topic="WMT/T/S/VIN-1/cid/request",
            payload=json.dumps(body).encode(),
            qos=1,
            properties=props,
        )
    )


def _make_dispatcher(exclusive=False):
    conn = MagicMock()
    published = []

    def fake_publish(topic, payload, qos, props):
        published.append((topic, payload, qos, props))

    conn.publish = fake_publish
    mgr = ExclusiveSessionManager() if exclusive else None
    d = Dispatcher(
        conn=conn,
        thing_type="T",
        service_name="S",
        vin="VIN-1",
        exclusive_mgr=mgr,
    )
    return d, published, mgr


@pytest.mark.asyncio
async def test_qos_mirroring_qos0() -> None:
    """요청 qos=0 → 응답 QoS 0."""
    d, published, _ = _make_dispatcher()
    d.register("ping", lambda ctx: {"ok": True})
    await d.handle(_make_msg("ping", user_props=[("qos", "0"), ("timeout", "5.0")]))
    assert published
    _, _, qos, _props = published[-1]
    assert qos == 0


@pytest.mark.asyncio
async def test_qos_mirroring_default_1() -> None:
    """qos User Property 없음 → 응답 QoS 1 (하위호환)."""
    d, published, _ = _make_dispatcher()
    d.register("ping", lambda ctx: {"ok": True})
    await d.handle(_make_msg("ping"))
    _, _, qos, _props = published[-1]
    assert qos == 1


@pytest.mark.asyncio
async def test_single_response_has_subscription_id() -> None:
    """단일 응답에도 subscription_id User Property가 들어간다."""
    d, published, _ = _make_dispatcher()
    d.register("ping", lambda ctx: {"ok": True})
    await d.handle(_make_msg("ping", user_props=[("qos", "1"), ("timeout", "5.0")]))
    _, _, _, props = published[-1]
    up = dict(props.user_properties)
    assert "subscription_id" in up


@pytest.mark.asyncio
async def test_response_message_expiry_qos1() -> None:
    """QoS 1 단일 응답에 Message Expiry가 설정된다."""
    d, published, _ = _make_dispatcher()
    d.register("ping", lambda ctx: {"ok": True})
    await d.handle(_make_msg("ping", user_props=[("qos", "1"), ("timeout", "10.0")]))
    _, _, _, props = published[-1]
    assert props.message_expiry_interval is not None
    assert 1 <= props.message_expiry_interval <= 10


@pytest.mark.asyncio
async def test_pattern_d_expired_no_reply() -> None:
    """sent_at이 timeout+tolerance를 초과하면 핸들러 미호출·응답 없음."""
    d, published, _ = _make_dispatcher()
    called = []
    d.register("act", lambda ctx: called.append(1) or {"ok": True})
    # sent_at = 1000ms 전, timeout=0.1s → 만료
    import time
    old_ms = int(time.time() * 1000) - 5000
    await d.handle(
        _make_msg("act", user_props=[("qos", "1"), ("timeout", "0.1"), ("sent_at", str(old_ms))])
    )
    assert not called
    assert not published


@pytest.mark.asyncio
async def test_exclusive_access_control_busy() -> None:
    """다른 client_id가 점유 중이면 0x8A 거부, 핸들러 미호출."""
    d, published, mgr = _make_dispatcher(exclusive=True)
    mgr.acquire("other-client")
    called = []
    d.register("act", lambda ctx: called.append(1) or {"ok": True})
    await d.handle(_make_msg("act", user_props=[("qos", "1"), ("timeout", "5.0")]))
    assert not called
    _, _, _, props = published[-1]
    up = dict(props.user_properties)
    assert up["reason_code"] == str(0x8A)


@pytest.mark.asyncio
async def test_session_id_signal_acquire_and_release() -> None:
    """핸들러가 acquire하면 응답 session_id=uuid, release하면 ''."""
    d, published, mgr = _make_dispatcher(exclusive=True)

    def start(ctx):
        mgr.acquire(ctx.client_id)
        return {"ok": True}

    def stop(ctx):
        mgr.release(ctx.client_id)
        return {"ok": True}

    d.register("start", start)
    d.register("stop", stop)

    await d.handle(_make_msg("start", user_props=[("qos", "1"), ("timeout", "5.0")]))
    _, _, _, props = published[-1]
    up = dict(props.user_properties)
    assert up.get("session_id") and up["session_id"] != ""

    await d.handle(_make_msg("stop", user_props=[("qos", "1"), ("timeout", "5.0")]))
    _, _, _, props2 = published[-1]
    up2 = dict(props2.user_properties)
    assert up2.get("session_id") == ""


def test_exclusive_manager_basic() -> None:
    mgr = ExclusiveSessionManager()
    sid = mgr.acquire("c1")
    assert sid
    assert mgr.acquire("c1") == sid  # 재획득 동일
    assert mgr.acquire("c2") is None  # 점유 중
    assert mgr.get_session_id("c1") == sid
    assert mgr.get_session_id("c2") is None
    assert mgr.force_release_by_client("c2") is None
    assert mgr.force_release_by_client("c1") == sid
    assert mgr.acquire("c2")  # 이제 가능
