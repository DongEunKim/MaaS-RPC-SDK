"""패턴 C: 서버 주도 구독 취소 — SubscriptionRegistry reason 지원 및 Dispatcher EOF 검증."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from maas_server.subscription import SubscriptionRegistry


# ── SubscriptionRegistry 단위 테스트 ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_cancel_with_reason_stored() -> None:
    """registry.cancel(id, reason) 후 get_cancel_reason(id)이 reason을 반환한다."""
    registry = SubscriptionRegistry()
    sid, _ = await registry.create()

    result = await registry.cancel(sid, reason="quota_exceeded")
    assert result is True

    reason = await registry.get_cancel_reason(sid)
    assert reason == "quota_exceeded"


@pytest.mark.asyncio
async def test_cancel_without_reason_is_none() -> None:
    """reason=None(기본값)으로 cancel() 시 get_cancel_reason()은 None을 반환한다."""
    registry = SubscriptionRegistry()
    sid, _ = await registry.create()

    await registry.cancel(sid)
    reason = await registry.get_cancel_reason(sid)
    assert reason is None


@pytest.mark.asyncio
async def test_remove_clears_reason() -> None:
    """remove(id) 후 get_cancel_reason(id)은 None을 반환한다."""
    registry = SubscriptionRegistry()
    sid, _ = await registry.create()

    await registry.cancel(sid, reason="resource_limit")
    await registry.remove(sid)

    reason = await registry.get_cancel_reason(sid)
    assert reason is None


@pytest.mark.asyncio
async def test_cancel_unknown_id_returns_false() -> None:
    """존재하지 않는 subscription_id에 cancel() 시 False 반환."""
    registry = SubscriptionRegistry()
    result = await registry.cancel("nonexistent-id", reason="test")
    assert result is False


@pytest.mark.asyncio
async def test_get_cancel_reason_unknown_id() -> None:
    """존재하지 않는 subscription_id에 get_cancel_reason() 시 None 반환."""
    registry = SubscriptionRegistry()
    reason = await registry.get_cancel_reason("nonexistent-id")
    assert reason is None


# ── Dispatcher EOF cancel_reason 통합 테스트 ─────────────────────────────────


@pytest.mark.asyncio
async def test_dispatcher_attaches_cancel_reason() -> None:
    """서버 강제 취소 시 EOF User Property에 cancel_reason이 포함된다."""
    from maas_server._dispatcher import Dispatcher, HandlerEntry
    from maas_server._adapter import MqttProperties
    from maas_server.connection import IncomingMessage as ServerIncomingMessage
    from paho.mqtt.packettypes import PacketTypes
    from paho.mqtt.properties import Properties

    published_props_list: list[MqttProperties] = []

    def fake_publish(topic, payload, qos, props):
        published_props_list.append(props)

    mock_conn = MagicMock()
    mock_conn.publish = fake_publish

    dispatcher = Dispatcher(
        conn=mock_conn,
        thing_type="T",
        service_name="S",
        vin="VIN-1",
    )

    async def subscribe_handler(ctx):
        # 첫 이벤트 yield (subscriptionId 포함)
        sid = ctx.subscription_id
        yield {"subscriptionId": sid}
        # 서버 강제 취소: 핸들러 내에서 직접 registry에 reason 설정
        await dispatcher._registry.cancel(sid, reason="quota_exceeded")
        # cancel_event가 set되었으므로 루프 종료
        if ctx.cancel_event.is_set():
            return

    dispatcher._handlers["subscribe"] = HandlerEntry(
        func=subscribe_handler,
        subscription=True,
    )

    # 가짜 RPC 요청 메시지 구성
    props = Properties(PacketTypes.PUBLISH)
    props.CorrelationData = b"corr-test-1"
    props.ResponseTopic = "WMO/T/S/VIN-1/cid-1/response"
    props.UserProperty = []

    class FakeMsg:
        topic = "WMT/T/S/VIN-1/cid-1/request"
        payload = b'{"action": "subscribe"}'
        qos = 1
        properties = props

    msg = ServerIncomingMessage(FakeMsg())

    await dispatcher.handle(msg)

    # EOF 메시지에서 cancel_reason User Property 확인
    # 마지막 publish = EOF
    assert len(published_props_list) >= 2, "첫 이벤트 + EOF 최소 2회 publish 필요"
    eof_props = published_props_list[-1]

    # MqttProperties의 user_properties 확인
    user_props_dict = dict(eof_props.user_properties)
    assert user_props_dict.get("is_EOF") == "true", "EOF 메시지에 is_EOF=true 필요"
    assert user_props_dict.get("cancel_reason") == "quota_exceeded", (
        f"cancel_reason이 없거나 잘못됨: {user_props_dict}"
    )
