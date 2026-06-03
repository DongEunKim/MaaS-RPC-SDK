"""패턴 C: open_subscription() API 단위 테스트."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from maas_client._rpc import RpcManager
from maas_client.connection import IncomingMessage
from maas_client.exceptions import SubscriptionCancelledError
from maas_client.models import Subscription


def _make_props(
    corr: bytes,
    *,
    is_eof: str | None = None,
    cancel_reason: str | None = None,
) -> Properties:
    p = Properties(PacketTypes.PUBLISH)
    p.CorrelationData = corr
    user_props: list[tuple[str, str]] = [("reason_code", "0")]
    if is_eof is not None:
        user_props.append(("is_EOF", is_eof))
    if cancel_reason is not None:
        user_props.append(("cancel_reason", cancel_reason))
    p.UserProperty = user_props
    return p


def _make_msg(
    topic: str,
    corr: bytes,
    *,
    payload: bytes = b"{}",
    is_eof: str | None = None,
    cancel_reason: str | None = None,
) -> IncomingMessage:
    msg = SimpleNamespace(
        topic=topic,
        payload=payload,
        qos=1,
        properties=_make_props(corr, is_eof=is_eof, cancel_reason=cancel_reason),
    )
    return IncomingMessage(msg)


RESPONSE_TOPIC = "WMO/T/S/VIN-1/cid-1/response"


@pytest.mark.asyncio
async def test_open_subscription_extracts_sub_id() -> None:
    """첫 이벤트 payload의 subscriptionId → sub.subscription_id"""
    mock_conn = MagicMock()
    mock_conn.publish = AsyncMock()
    rpc = RpcManager(mock_conn, "cid-1")

    sub_task = asyncio.create_task(
        rpc.open_subscription("T", "S", "subscribe", "VIN-1")
    )
    await asyncio.sleep(0)

    props = mock_conn.publish.await_args.kwargs["properties"]
    corr = props.CorrelationData

    # 첫 이벤트: subscriptionId 포함
    rpc.handle_incoming(
        _make_msg(RESPONSE_TOPIC, corr, payload=b'{"subscriptionId": "sub-uuid-123"}')
    )
    await asyncio.sleep(0)

    # EOF로 스트림 종료
    rpc.handle_incoming(_make_msg(RESPONSE_TOPIC, corr, payload=b"null", is_eof="true"))
    await asyncio.sleep(0)

    sub = await sub_task
    assert isinstance(sub, Subscription)
    assert sub.subscription_id == "sub-uuid-123"


@pytest.mark.asyncio
async def test_subscription_events_continue_after_first() -> None:
    """첫 이벤트 이후 데이터 이벤트가 sub.events에서 yield된다."""
    mock_conn = MagicMock()
    mock_conn.publish = AsyncMock()
    rpc = RpcManager(mock_conn, "cid-1")

    sub_task = asyncio.create_task(
        rpc.open_subscription("T", "S", "subscribe", "VIN-1")
    )
    await asyncio.sleep(0)

    props = mock_conn.publish.await_args.kwargs["properties"]
    corr = props.CorrelationData

    # 첫 이벤트 (subscriptionId)
    rpc.handle_incoming(
        _make_msg(RESPONSE_TOPIC, corr, payload=b'{"subscriptionId": "sub-abc"}')
    )
    await asyncio.sleep(0)

    sub = await sub_task
    assert sub.subscription_id == "sub-abc"

    # 이후 데이터 이벤트 수집
    received: list = []

    async def collect() -> None:
        async for event in sub.events:
            received.append(event)

    collect_task = asyncio.create_task(collect())
    await asyncio.sleep(0)

    rpc.handle_incoming(
        _make_msg(RESPONSE_TOPIC, corr, payload=b'{"value": 42}')
    )
    await asyncio.sleep(0)

    rpc.handle_incoming(
        _make_msg(RESPONSE_TOPIC, corr, payload=b'{"value": 99}')
    )
    await asyncio.sleep(0)

    # EOF로 종료
    rpc.handle_incoming(_make_msg(RESPONSE_TOPIC, corr, payload=b"null", is_eof="true"))
    await asyncio.sleep(0)

    await collect_task

    # EOF 포함 총 3개 (data×2 + EOF)
    assert len(received) == 3
    assert received[0].payload == {"value": 42}
    assert received[1].payload == {"value": 99}
    assert received[2].is_eof is True


@pytest.mark.asyncio
async def test_subscription_unsubscribe_calls_rpc() -> None:
    """sub.unsubscribe() 호출 시 올바른 params로 RPC 발행."""
    mock_conn = MagicMock()
    mock_conn.publish = AsyncMock()
    rpc = RpcManager(mock_conn, "cid-1")

    sub_task = asyncio.create_task(
        rpc.open_subscription("T", "S", "subscribe", "VIN-1")
    )
    await asyncio.sleep(0)

    props = mock_conn.publish.await_args.kwargs["properties"]
    corr = props.CorrelationData

    rpc.handle_incoming(
        _make_msg(RESPONSE_TOPIC, corr, payload=b'{"subscriptionId": "sub-xyz"}')
    )
    await asyncio.sleep(0)

    sub = await sub_task
    assert sub.subscription_id == "sub-xyz"

    # unsubscribe RPC 호출 준비: 응답 Future 세팅
    unsub_task = asyncio.create_task(
        sub.unsubscribe("unsubscribe")
    )
    await asyncio.sleep(0)

    # unsubscribe 요청에 대한 응답 시뮬레이션
    all_publish_calls = mock_conn.publish.await_args_list
    # 마지막 publish 호출의 corr_id로 응답
    last_call = all_publish_calls[-1]
    unsub_corr = last_call.kwargs["properties"].CorrelationData

    unsub_msg = SimpleNamespace(
        topic=RESPONSE_TOPIC,
        payload=b'{"status": "ok"}',
        qos=1,
        properties=_make_props(unsub_corr),
    )
    rpc.handle_incoming(IncomingMessage(unsub_msg))
    await asyncio.sleep(0)

    unsub_resp = await unsub_task
    assert unsub_resp.payload == {"status": "ok"}

    # publish된 페이로드 확인 (두 번째 publish = unsubscribe)
    import json
    second_publish = all_publish_calls[-1]
    raw_payload = second_publish.args[1]
    payload_dict = json.loads(raw_payload)
    assert payload_dict.get("action") == "unsubscribe"
    assert payload_dict.get("subscriptionId") == "sub-xyz"


@pytest.mark.asyncio
async def test_subscription_eof_terminates() -> None:
    """서버 EOF (cancel_reason 없음) → events 루프 자동 종료."""
    mock_conn = MagicMock()
    mock_conn.publish = AsyncMock()
    rpc = RpcManager(mock_conn, "cid-1")

    sub_task = asyncio.create_task(
        rpc.open_subscription("T", "S", "subscribe", "VIN-1")
    )
    await asyncio.sleep(0)

    props = mock_conn.publish.await_args.kwargs["properties"]
    corr = props.CorrelationData

    rpc.handle_incoming(
        _make_msg(RESPONSE_TOPIC, corr, payload=b'{"subscriptionId": "sub-eof"}')
    )
    await asyncio.sleep(0)

    sub = await sub_task

    received: list = []
    cancelled = False

    async def collect() -> None:
        nonlocal cancelled
        try:
            async for event in sub.events:
                received.append(event)
        except SubscriptionCancelledError:
            cancelled = True

    collect_task = asyncio.create_task(collect())
    await asyncio.sleep(0)

    # EOF without cancel_reason
    rpc.handle_incoming(_make_msg(RESPONSE_TOPIC, corr, payload=b"null", is_eof="true"))
    await asyncio.sleep(0)

    await collect_task

    assert cancelled is False
    assert len(received) == 1
    assert received[0].is_eof is True


@pytest.mark.asyncio
async def test_server_cancel_raises_error() -> None:
    """EOF + cancel_reason → SubscriptionCancelledError(reason) 발생."""
    mock_conn = MagicMock()
    mock_conn.publish = AsyncMock()
    rpc = RpcManager(mock_conn, "cid-1")

    sub_task = asyncio.create_task(
        rpc.open_subscription("T", "S", "subscribe", "VIN-1")
    )
    await asyncio.sleep(0)

    props = mock_conn.publish.await_args.kwargs["properties"]
    corr = props.CorrelationData

    rpc.handle_incoming(
        _make_msg(RESPONSE_TOPIC, corr, payload=b'{"subscriptionId": "sub-cancel"}')
    )
    await asyncio.sleep(0)

    sub = await sub_task

    caught: list[SubscriptionCancelledError] = []

    async def collect() -> None:
        try:
            async for _ in sub.events:
                pass
        except SubscriptionCancelledError as e:
            caught.append(e)

    collect_task = asyncio.create_task(collect())
    await asyncio.sleep(0)

    # EOF with cancel_reason
    rpc.handle_incoming(
        _make_msg(
            RESPONSE_TOPIC,
            corr,
            payload=b"null",
            is_eof="true",
            cancel_reason="quota_exceeded",
        )
    )
    await asyncio.sleep(0)

    await collect_task

    assert len(caught) == 1
    assert caught[0].reason == "quota_exceeded"


@pytest.mark.asyncio
async def test_no_sub_id_graceful() -> None:
    """첫 이벤트에 subscriptionId 없으면 subscription_id=None."""
    mock_conn = MagicMock()
    mock_conn.publish = AsyncMock()
    rpc = RpcManager(mock_conn, "cid-1")

    sub_task = asyncio.create_task(
        rpc.open_subscription("T", "S", "subscribe", "VIN-1")
    )
    await asyncio.sleep(0)

    props = mock_conn.publish.await_args.kwargs["properties"]
    corr = props.CorrelationData

    # 첫 이벤트에 subscriptionId 없음
    rpc.handle_incoming(
        _make_msg(RESPONSE_TOPIC, corr, payload=b'{"value": "no-sub-id"}')
    )
    await asyncio.sleep(0)

    # EOF로 종료
    rpc.handle_incoming(_make_msg(RESPONSE_TOPIC, corr, payload=b"null", is_eof="true"))
    await asyncio.sleep(0)

    sub = await sub_task
    assert sub.subscription_id is None
