"""
클라이언트 단절 → 서버 구독 취소 단위 테스트.

SubscriptionRegistry.cancel_by_client() 및 Dispatcher 스트리밍 client_id 전파 검증.
LWT(offline) 단절 경로는 test_lwt_offline.py 참고.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from maas_server.subscription import SubscriptionRegistry


@pytest.mark.asyncio
async def test_create_stores_client_id() -> None:
    """create(client_id='c1') → _clients에 저장됨."""
    registry = SubscriptionRegistry()
    sid, _ = await registry.create(client_id="c1")
    assert registry._clients[sid] == "c1"


@pytest.mark.asyncio
async def test_create_default_client_id_empty() -> None:
    """create() 기본값 → client_id=''로 저장됨 (하위 호환)."""
    registry = SubscriptionRegistry()
    sid, _ = await registry.create()
    assert registry._clients[sid] == ""


@pytest.mark.asyncio
async def test_cancel_by_client_cancels_matching() -> None:
    """cancel_by_client('c1') → c1 구독의 cancel_event가 set됨."""
    registry = SubscriptionRegistry()
    sid, event = await registry.create(client_id="c1")

    assert not event.is_set()
    await registry.cancel_by_client("c1")
    assert event.is_set()


@pytest.mark.asyncio
async def test_cancel_by_client_no_reason() -> None:
    """cancel_by_client 취소 후 get_cancel_reason() → None."""
    registry = SubscriptionRegistry()
    sid, event = await registry.create(client_id="c1")

    await registry.cancel_by_client("c1")
    reason = await registry.get_cancel_reason(sid)
    assert reason is None


@pytest.mark.asyncio
async def test_cancel_by_client_ignores_others() -> None:
    """cancel_by_client('c1') → 다른 client_id 구독은 영향 없음."""
    registry = SubscriptionRegistry()
    sid1, event1 = await registry.create(client_id="c1")
    sid2, event2 = await registry.create(client_id="c2")

    await registry.cancel_by_client("c1")

    assert event1.is_set()
    assert not event2.is_set()


@pytest.mark.asyncio
async def test_remove_clears_client_id() -> None:
    """remove(sid) 후 _clients에서 삭제됨."""
    registry = SubscriptionRegistry()
    sid, _ = await registry.create(client_id="c1")

    assert sid in registry._clients
    await registry.remove(sid)
    assert sid not in registry._clients


@pytest.mark.asyncio
async def test_cancel_by_client_already_set_skipped() -> None:
    """이미 set된 event는 cancel_by_client에서 중복 set하지 않음 (is_set 확인)."""
    registry = SubscriptionRegistry()
    sid, event = await registry.create(client_id="c1")

    # 먼저 cancel로 reason 설정
    await registry.cancel(sid, reason="quota_exceeded")
    assert event.is_set()

    # cancel_by_client는 이미 set된 경우 건너뜀
    await registry.cancel_by_client("c1")
    # reason은 여전히 quota_exceeded (덮어쓰지 않음)
    reason = await registry.get_cancel_reason(sid)
    assert reason == "quota_exceeded"


@pytest.mark.asyncio
async def test_dispatcher_passes_client_id() -> None:
    """_invoke_streaming mock → registry.create(client_id=...) 호출 확인."""
    from maas_server._dispatcher import Dispatcher
    from maas_server.connection import IncomingMessage
    import json
    from types import SimpleNamespace
    from paho.mqtt.packettypes import PacketTypes
    from paho.mqtt.properties import Properties

    mock_conn = MagicMock()
    mock_conn.publish = AsyncMock()
    dispatcher = Dispatcher(
        conn=mock_conn,
        thing_type="T",
        service_name="S",
        vin="VIN-1",
    )

    created_client_ids: list[str] = []

    original_create = dispatcher._registry.create

    async def mock_create(client_id: str = "") -> tuple:
        created_client_ids.append(client_id)
        return await original_create(client_id=client_id)

    dispatcher._registry.create = mock_create

    # Register a streaming handler directly
    from maas_server._dispatcher import HandlerEntry
    dispatcher._handlers["stream_action"] = HandlerEntry(
        func=_dummy_stream_handler,
        subscription=True,
    )

    # Build a fake request message
    props = Properties(PacketTypes.PUBLISH)
    props.ResponseTopic = "WMO/T/S/VIN-1/client-42/response"
    props.CorrelationData = b"\x01\x02\x03\x04"
    msg = SimpleNamespace(
        topic="WMT/T/S/VIN-1/client-42/request",
        payload=json.dumps({"action": "stream_action"}).encode(),
        qos=1,
        properties=props,
    )
    incoming = IncomingMessage(msg)

    await dispatcher.handle(incoming)

    assert "client-42" in created_client_ids


async def _dummy_stream_handler(ctx):
    """스트리밍 핸들러 더미 (즉시 종료)."""
    return
    yield
