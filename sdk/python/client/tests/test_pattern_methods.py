"""패턴 A~E 편의 메서드 및 User Property 규약·통합 Response 단위 테스트."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from maas_client._rpc import RpcManager
from maas_client.connection import IncomingMessage
from maas_client.exceptions import RpcExpiredError, RpcTimeoutError
from maas_client.models import Response, Session


RESPONSE_TOPIC = "WMO/T/svc/VIN-1/client-1/response"


def _props(
    corr: bytes,
    *,
    is_eof: str | None = None,
    subscription_id: str | None = None,
    unsubscribe_action: str | None = None,
    session_id: str | None = None,
    cancel_reason: str | None = None,
) -> Properties:
    p = Properties(PacketTypes.PUBLISH)
    p.CorrelationData = corr
    up: list[tuple[str, str]] = [("reason_code", "0")]
    if is_eof is not None:
        up.append(("is_EOF", is_eof))
    if subscription_id is not None:
        up.append(("subscription_id", subscription_id))
    if unsubscribe_action is not None:
        up.append(("unsubscribe_action", unsubscribe_action))
    if session_id is not None:
        up.append(("session_id", session_id))
    if cancel_reason is not None:
        up.append(("cancel_reason", cancel_reason))
    p.UserProperty = up
    return p


def _msg(corr: bytes, *, payload: bytes = b"{}", **kw) -> IncomingMessage:
    return IncomingMessage(
        SimpleNamespace(
            topic=RESPONSE_TOPIC, payload=payload, qos=1, properties=_props(corr, **kw)
        )
    )


@pytest.mark.asyncio
async def test_call_inserts_qos_timeout_user_props() -> None:
    """모든 요청에 qos/timeout User Property가 실린다."""
    conn = MagicMock()
    conn.publish = AsyncMock()
    rpc = RpcManager(conn, "client-1")

    task = asyncio.create_task(
        rpc.call("T", "svc", "act", "VIN-1", qos=0, timeout=5.0)
    )
    await asyncio.sleep(0)
    props = conn.publish.await_args.kwargs["properties"]
    up = dict(props.UserProperty)
    assert up["qos"] == "0"
    assert up["timeout"] == "5.0"
    assert "sent_at" not in up

    rpc.handle_incoming(_msg(props.CorrelationData))
    resp = await task
    assert isinstance(resp, Response)
    assert resp.is_subscription is False
    assert resp.reason_code == 0


@pytest.mark.asyncio
async def test_call_timed_adds_sent_at_and_raises_expired() -> None:
    """call(sent_at=True) 요청은 sent_at User Property를 포함하고, 미수신 시 RpcExpiredError."""
    conn = MagicMock()
    conn.publish = AsyncMock()
    rpc = RpcManager(conn, "client-1")

    task = asyncio.create_task(
        rpc.call("T", "svc", "act", "VIN-1", qos=1, timeout=0.05, sent_at=True)
    )
    await asyncio.sleep(0)
    props = conn.publish.await_args.kwargs["properties"]
    up = dict(props.UserProperty)
    assert "sent_at" in up

    with pytest.raises(RpcExpiredError):
        await task


def test_rpc_expired_is_timeout_subclass() -> None:
    err = RpcExpiredError("svc", "act", 1.0)
    assert isinstance(err, RpcTimeoutError)


@pytest.mark.asyncio
async def test_response_subscription_discrimination() -> None:
    """is_EOF=false 첫 응답 → Response.is_subscription True + subscription_id 추출."""
    conn = MagicMock()
    conn.publish = AsyncMock()
    rpc = RpcManager(conn, "client-1")

    task = asyncio.create_task(rpc.call("T", "svc", "subscribe", "VIN-1", qos=0, timeout=5.0))
    await asyncio.sleep(0)
    corr = conn.publish.await_args.kwargs["properties"].CorrelationData

    rpc.handle_incoming(
        _msg(corr, payload=b'{"v": 1}', is_eof="false", subscription_id="sub-1",
             unsubscribe_action="cancel_sub")
    )
    resp = await task
    assert resp.is_subscription is True
    assert resp.subscription_id == "sub-1"
    assert resp._unsubscribe_action == "cancel_sub"


@pytest.mark.asyncio
async def test_response_subscription_id_payload_fallback() -> None:
    """User Property subscription_id 없으면 payload subscriptionId 폴백."""
    conn = MagicMock()
    conn.publish = AsyncMock()
    rpc = RpcManager(conn, "client-1")

    task = asyncio.create_task(rpc.call("T", "svc", "subscribe", "VIN-1", qos=0, timeout=5.0))
    await asyncio.sleep(0)
    corr = conn.publish.await_args.kwargs["properties"].CorrelationData

    rpc.handle_incoming(
        _msg(corr, payload=b'{"subscriptionId": "from-payload"}', is_eof="false")
    )
    resp = await task
    assert resp.subscription_id == "from-payload"


@pytest.mark.asyncio
async def test_response_session_signal() -> None:
    """session_id 비어있지 않으면 Response.session 생성, ''이면 None."""
    conn = MagicMock()
    conn.publish = AsyncMock()
    rpc = RpcManager(conn, "client-1")

    task = asyncio.create_task(rpc.call("T", "svc", "session_start", "VIN-1", qos=1, timeout=5.0))
    await asyncio.sleep(0)
    corr = conn.publish.await_args.kwargs["properties"].CorrelationData

    rpc.handle_incoming(_msg(corr, payload=b'{"ok": true}', session_id="sess-uuid"))
    resp = await task
    assert isinstance(resp.session, Session)
    assert resp.session.session_id == "sess-uuid"

    # 빈 session_id → None
    task2 = asyncio.create_task(rpc.call("T", "svc", "session_stop", "VIN-1", qos=1, timeout=5.0))
    await asyncio.sleep(0)
    corr2 = conn.publish.await_args.kwargs["properties"].CorrelationData
    rpc.handle_incoming(_msg(corr2, payload=b'{"ok": true}', session_id=""))
    resp2 = await task2
    assert resp2.session is None


@pytest.mark.asyncio
async def test_response_stream_iteration_and_eof() -> None:
    """Response 이터레이터가 첫 이벤트 포함 EOF까지 yield한다."""
    conn = MagicMock()
    conn.publish = AsyncMock()
    rpc = RpcManager(conn, "client-1")

    task = asyncio.create_task(rpc.call("T", "svc", "subscribe", "VIN-1", qos=0, timeout=5.0))
    await asyncio.sleep(0)
    corr = conn.publish.await_args.kwargs["properties"].CorrelationData

    rpc.handle_incoming(_msg(corr, payload=b'{"n": 0}', is_eof="false", subscription_id="s"))
    resp = await task

    received: list = []

    async def consume() -> None:
        async for ev in resp:
            received.append(ev)

    ct = asyncio.create_task(consume())
    await asyncio.sleep(0)
    rpc.handle_incoming(_msg(corr, payload=b'{"n": 1}', is_eof="false", subscription_id="s"))
    await asyncio.sleep(0)
    rpc.handle_incoming(_msg(corr, payload=b"null", is_eof="true", subscription_id="s"))
    await asyncio.sleep(0)
    await ct

    # first(n=0) + n=1 + EOF = 3
    assert len(received) == 3
    assert received[0].payload == {"n": 0}
    assert received[-1].is_eof is True
