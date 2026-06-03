"""패턴 C 스트리밍 — response 단일 토픽 통합 동작 검증."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from maas_client._rpc import RpcManager
from maas_client.connection import IncomingMessage

# 회귀 방지: 구 event 처리 메서드가 재도입되면 즉시 감지
assert not hasattr(RpcManager, "_handle_event"), (
    "RpcManager._handle_event가 재도입됨. event 토픽 처리는 삭제된 기능이다."
)


def _make_props(corr: bytes, *, is_eof: str | None = None) -> Properties:
    p = Properties(PacketTypes.PUBLISH)
    p.CorrelationData = corr
    user_props: list[tuple[str, str]] = [("reason_code", "0")]
    if is_eof is not None:
        user_props.append(("is_EOF", is_eof))
    p.UserProperty = user_props
    return p


def _make_msg(topic: str, corr: bytes, *, payload: bytes = b"{}", is_eof: str | None = None) -> IncomingMessage:
    msg = SimpleNamespace(
        topic=topic,
        payload=payload,
        qos=1,
        properties=_make_props(corr, is_eof=is_eof),
    )
    return IncomingMessage(msg)


@pytest.mark.asyncio
async def test_setup_subscriptions_response_only() -> None:
    """setup_subscriptions는 response 와일드카드만 구독하고 event 와일드카드는 구독하지 않는다."""
    mock_conn = MagicMock()
    mock_conn.subscribe = AsyncMock()
    rpc = RpcManager(mock_conn, "cid-1")

    await rpc.setup_subscriptions()

    subscribed_topics = [
        call.args[0] for call in mock_conn.subscribe.await_args_list
    ]
    assert any("response" in t for t in subscribed_topics), "response 와일드카드 구독 필요"
    assert not any("event" in t for t in subscribed_topics), "event 와일드카드 구독하면 안 됨"
    assert mock_conn.subscribe.await_count == 1


@pytest.mark.asyncio
async def test_stream_chunks_via_response_topic() -> None:
    """response 토픽으로 수신된 청크(is_EOF 없음)가 StreamEvent(is_eof=False)로 Queue에 들어간다."""
    mock_conn = MagicMock()
    mock_conn.publish = AsyncMock()
    rpc = RpcManager(mock_conn, "cid-1")

    events: list = []

    async def consume() -> None:
        async for ev in rpc.stream("T", "S", "act", "VIN-1"):
            events.append(ev)

    task = asyncio.create_task(consume())
    await asyncio.sleep(0)

    props = mock_conn.publish.await_args.kwargs["properties"]
    corr = props.CorrelationData
    response_topic = "WMO/T/S/VIN-1/cid-1/response"

    # 청크 1 (is_EOF 없음 → is_eof=False)
    rpc.handle_incoming(_make_msg(response_topic, corr, payload=b'"chunk1"'))
    await asyncio.sleep(0)

    # 청크 2 (is_EOF="false" → is_eof=False)
    rpc.handle_incoming(_make_msg(response_topic, corr, payload=b'"chunk2"', is_eof="false"))
    await asyncio.sleep(0)

    # EOF (is_EOF="true" → is_eof=True)
    rpc.handle_incoming(_make_msg(response_topic, corr, payload=b'null', is_eof="true"))
    await asyncio.sleep(0)

    await task

    assert len(events) == 3
    assert events[0].is_eof is False
    assert events[0].payload == "chunk1"
    assert events[1].is_eof is False
    assert events[1].payload == "chunk2"
    assert events[2].is_eof is True


@pytest.mark.asyncio
async def test_event_topic_ignored_in_stream() -> None:
    """suffix가 'event'인 메시지는 스트림 Queue에 적재되지 않는다."""
    mock_conn = MagicMock()
    mock_conn.publish = AsyncMock()
    rpc = RpcManager(mock_conn, "cid-1")

    events: list = []

    async def consume() -> None:
        async for ev in rpc.stream("T", "S", "act", "VIN-1"):
            events.append(ev)

    task = asyncio.create_task(consume())
    await asyncio.sleep(0)

    props = mock_conn.publish.await_args.kwargs["properties"]
    corr = props.CorrelationData
    event_topic = "WMO/T/S/VIN-1/cid-1/event"
    response_topic = "WMO/T/S/VIN-1/cid-1/response"

    # 구버전 event 토픽 메시지 — 무시되어야 한다
    rpc.handle_incoming(_make_msg(event_topic, corr, payload=b'"old_chunk"'))
    await asyncio.sleep(0)

    # EOF로 스트림 종료
    rpc.handle_incoming(_make_msg(response_topic, corr, payload=b'null', is_eof="true"))
    await asyncio.sleep(0)

    await task

    # event 토픽 메시지는 무시되고, EOF만 수신되어야 한다
    assert len(events) == 1
    assert events[0].is_eof is True
