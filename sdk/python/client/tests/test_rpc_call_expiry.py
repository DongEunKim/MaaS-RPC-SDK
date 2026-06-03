"""QoS·timeout·Message Expiry 연동 단위 테스트."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from maas_client._rpc import RpcManager, _publish_message_expiry_for_call
from maas_client.connection import IncomingMessage


def test_publish_message_expiry_helper_qos1_from_timeout() -> None:
    """QoS 1이면 timeout에서 올림·최소 1초로 Expiry를 만든다."""
    assert _publish_message_expiry_for_call(1, 10.0, None) == 10
    assert _publish_message_expiry_for_call(1, 7.2, None) == 8
    assert _publish_message_expiry_for_call(1, 0.1, None) == 1
    assert _publish_message_expiry_for_call(1, 7.2, 999) == 8


def test_publish_message_expiry_helper_qos0_uses_expiry_arg() -> None:
    """QoS 0이면 호출자 expiry만 반영한다."""
    assert _publish_message_expiry_for_call(0, 10.0, None) is None
    assert _publish_message_expiry_for_call(0, 10.0, 42) == 42


def _success_response_props(corr: bytes) -> Properties:
    p = Properties(PacketTypes.PUBLISH)
    p.CorrelationData = corr
    p.UserProperty = [("reason_code", "0")]
    return p


@pytest.mark.asyncio
async def test_rpc_call_qos1_sets_message_expiry_from_timeout() -> None:
    """call(qos=1)의 PUBLISH Properties에 Message Expiry가 timeout과 맞는다."""
    mock_conn = MagicMock()
    mock_conn.publish = AsyncMock()
    rpc = RpcManager(mock_conn, "client-1")

    task = asyncio.create_task(
        rpc.call("T", "svc", "act", "VIN-1", qos=1, timeout=7.3)
    )
    await asyncio.sleep(0)
    mock_conn.publish.assert_awaited_once()
    props = mock_conn.publish.await_args.kwargs["properties"]
    assert props.MessageExpiryInterval == 8

    msg = SimpleNamespace(
        topic="WMO/T/svc/VIN-1/client-1/response",
        payload=b"{}",
        qos=1,
        properties=_success_response_props(props.CorrelationData),
    )
    rpc.handle_incoming(IncomingMessage(msg))

    resp = await task
    assert resp.reason_code == 0


@pytest.mark.asyncio
async def test_rpc_call_qos0_uses_explicit_expiry() -> None:
    """call(qos=0, expiry=n)이면 PUBLISH에 그 값이 들어간다."""
    mock_conn = MagicMock()
    mock_conn.publish = AsyncMock()
    rpc = RpcManager(mock_conn, "client-1")

    task = asyncio.create_task(
        rpc.call(
            "T",
            "svc",
            "act",
            "VIN-1",
            qos=0,
            timeout=10.0,
            expiry=99,
        )
    )
    await asyncio.sleep(0)
    props = mock_conn.publish.await_args.kwargs["properties"]
    assert props.MessageExpiryInterval == 99

    msg = SimpleNamespace(
        topic="WMO/T/svc/VIN-1/client-1/response",
        payload=b"{}",
        qos=1,
        properties=_success_response_props(props.CorrelationData),
    )
    rpc.handle_incoming(IncomingMessage(msg))
    await task


@pytest.mark.asyncio
async def test_rpc_call_qos0_no_expiry_omits_interval() -> None:
    """call(qos=0, expiry=None)이면 Message Expiry 속성을 넣지 않는다."""
    mock_conn = MagicMock()
    mock_conn.publish = AsyncMock()
    rpc = RpcManager(mock_conn, "client-1")

    task = asyncio.create_task(
        rpc.call("T", "svc", "act", "VIN-1", qos=0, timeout=10.0, expiry=None)
    )
    await asyncio.sleep(0)
    props = mock_conn.publish.await_args.kwargs["properties"]
    assert getattr(props, "MessageExpiryInterval", None) is None

    msg = SimpleNamespace(
        topic="WMO/T/svc/VIN-1/client-1/response",
        payload=b"{}",
        qos=1,
        properties=_success_response_props(props.CorrelationData),
    )
    rpc.handle_incoming(IncomingMessage(msg))
    await task
