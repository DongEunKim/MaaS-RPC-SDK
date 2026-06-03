"""RpcServerError.error_body 단위 테스트."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from maas_client._rpc import RpcManager
from maas_client.connection import IncomingMessage
from maas_client.exceptions import RpcServerError


def _make_error_props(corr: bytes, reason_code: int, error_body: bytes = b"") -> Properties:
    p = Properties(PacketTypes.PUBLISH)
    p.CorrelationData = corr
    p.UserProperty = [
        ("reason_code", str(reason_code)),
        ("error_detail", "test error"),
    ]
    return p


def _make_error_msg(
    topic: str, corr: bytes, reason_code: int, payload: bytes = b""
) -> IncomingMessage:
    msg = SimpleNamespace(
        topic=topic,
        payload=payload,
        qos=1,
        properties=_make_error_props(corr, reason_code),
    )
    return IncomingMessage(msg)


@pytest.mark.asyncio
async def test_rpc_error_carries_body() -> None:
    """서버 오류 응답에 페이로드가 있으면 RpcServerError.error_body에 담긴다."""
    mock_conn = MagicMock()
    mock_conn.publish = AsyncMock()
    rpc = RpcManager(mock_conn, "client-1")

    error_payload = {"error_code": "SENSOR_FAULT", "sensor_id": 42}
    error_payload_bytes = json.dumps(error_payload).encode("utf-8")

    task = asyncio.create_task(
        rpc.call("CGU", "viss", "get", "VIN-1", qos=1, timeout=5.0)
    )
    await asyncio.sleep(0)

    props = mock_conn.publish.await_args.kwargs["properties"]
    corr = props.CorrelationData

    msg = _make_error_msg(
        "WMO/CGU/viss/VIN-1/client-1/response",
        corr,
        reason_code=0x80,
        payload=error_payload_bytes,
    )
    rpc.handle_incoming(msg)

    with pytest.raises(RpcServerError) as exc_info:
        await task

    err = exc_info.value
    assert err.reason_code == 0x80
    assert err.error_body == error_payload


@pytest.mark.asyncio
async def test_rpc_error_empty_payload_has_none_body() -> None:
    """서버 오류 응답 페이로드가 비어 있으면 error_body가 None이다."""
    mock_conn = MagicMock()
    mock_conn.publish = AsyncMock()
    rpc = RpcManager(mock_conn, "client-1")

    task = asyncio.create_task(
        rpc.call("CGU", "viss", "get", "VIN-1", qos=1, timeout=5.0)
    )
    await asyncio.sleep(0)

    props = mock_conn.publish.await_args.kwargs["properties"]
    corr = props.CorrelationData

    msg = _make_error_msg(
        "WMO/CGU/viss/VIN-1/client-1/response",
        corr,
        reason_code=0x80,
        payload=b"",
    )
    rpc.handle_incoming(msg)

    with pytest.raises(RpcServerError) as exc_info:
        await task

    err = exc_info.value
    assert err.error_body is None
