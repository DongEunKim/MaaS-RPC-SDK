"""HandlerError.body 오류 응답 페이로드 단위 테스트."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from maas_server._dispatcher import Dispatcher
from maas_server._adapter import MqttProperties
from maas_server.context import RpcContext
from maas_server.exceptions import HandlerError
from maas_server.connection import IncomingMessage


def make_mock_adapter() -> MagicMock:
    m = MagicMock()
    m.publish = MagicMock()
    m.subscribe = MagicMock()
    m.set_message_callback = MagicMock()
    m.get_vin = MagicMock(return_value="VIN-TEST")
    return m


def make_dispatcher(adapter=None) -> Dispatcher:
    if adapter is None:
        adapter = make_mock_adapter()
    return Dispatcher(
        conn=adapter,
        thing_type="CGU",
        service_name="viss",
        vin="VIN-1",
    )


def make_incoming(action: str = "get") -> IncomingMessage:
    return IncomingMessage.from_raw(
        topic="WMT/CGU/viss/VIN-1/cid/request",
        payload=f'{{"action":"{action}"}}'.encode(),
        qos=1,
        correlation_data=b"corr-1",
        response_topic="WMO/CGU/viss/VIN-1/cid/response",
        user_props={},
    )


@pytest.mark.asyncio
async def test_handler_error_body_published() -> None:
    """HandlerError(body=...) 시 오류 응답에 body 페이로드가 포함된다."""
    adapter = make_mock_adapter()
    dispatcher = make_dispatcher(adapter)

    error_body = {"error_code": "SENSOR_FAULT", "sensor_id": 42}

    def handler(ctx: RpcContext):
        raise HandlerError("센서 오류", reason_code=0x80, body=error_body)

    dispatcher.register("get", handler)

    await dispatcher.handle(make_incoming("get"))

    adapter.publish.assert_called_once()
    published_payload = adapter.publish.call_args[0][1]
    assert published_payload != b""
    parsed = json.loads(published_payload.decode("utf-8"))
    assert parsed == error_body

    props: MqttProperties = adapter.publish.call_args[0][3]
    prop_dict = dict(props.user_properties)
    assert prop_dict["reason_code"] == "128"  # 0x80 == 128


@pytest.mark.asyncio
async def test_handler_error_no_body_empty_payload() -> None:
    """HandlerError에 body=None이면 오류 응답에 빈 페이로드가 발행된다."""
    adapter = make_mock_adapter()
    dispatcher = make_dispatcher(adapter)

    def handler(ctx: RpcContext):
        raise HandlerError("일반 오류", reason_code=0x80)

    dispatcher.register("get", handler)

    await dispatcher.handle(make_incoming("get"))

    adapter.publish.assert_called_once()
    published_payload = adapter.publish.call_args[0][1]
    assert published_payload == b""
