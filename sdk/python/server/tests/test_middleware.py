"""Dispatcher 미들웨어 체인 단위 테스트."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from maas_server._dispatcher import Dispatcher
from maas_server.context import RpcContext
from maas_server.exceptions import HandlerError
from maas_server._adapter import MqttProperties
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
async def test_middleware_runs_before_handler() -> None:
    """call_next 호출 전 코드가 핸들러보다 먼저 실행된다."""
    order: list[str] = []
    adapter = make_mock_adapter()
    dispatcher = make_dispatcher(adapter)

    async def mw(ctx: RpcContext, call_next):
        order.append("before")
        await call_next(ctx)
        order.append("after")

    dispatcher.add_middleware(mw)

    def handler(ctx: RpcContext):
        order.append("handler")
        return {}

    dispatcher.register("get", handler)

    await dispatcher.handle(make_incoming("get"))

    assert order == ["before", "handler", "after"]


@pytest.mark.asyncio
async def test_middleware_chain_order() -> None:
    """두 미들웨어가 등록 역순(last-in, first-executed)으로 실행된다."""
    order: list[str] = []
    adapter = make_mock_adapter()
    dispatcher = make_dispatcher(adapter)

    async def mw1(ctx: RpcContext, call_next):
        order.append("mw1_before")
        await call_next(ctx)
        order.append("mw1_after")

    async def mw2(ctx: RpcContext, call_next):
        order.append("mw2_before")
        await call_next(ctx)
        order.append("mw2_after")

    dispatcher.add_middleware(mw1)
    dispatcher.add_middleware(mw2)

    def handler(ctx: RpcContext):
        order.append("handler")
        return {}

    dispatcher.register("get", handler)

    await dispatcher.handle(make_incoming("get"))

    # last-in (mw2) first-executed
    assert order == ["mw2_before", "mw1_before", "handler", "mw1_after", "mw2_after"]


@pytest.mark.asyncio
async def test_middleware_can_raise_handler_error() -> None:
    """미들웨어에서 HandlerError 발생 시 오류 응답이 발행된다."""
    adapter = make_mock_adapter()
    dispatcher = make_dispatcher(adapter)

    async def mw(ctx: RpcContext, call_next):
        raise HandlerError("인증 실패", reason_code=0x87)

    dispatcher.add_middleware(mw)

    def handler(ctx: RpcContext):
        return {}

    dispatcher.register("get", handler)

    await dispatcher.handle(make_incoming("get"))

    # 미들웨어 HandlerError → 오류 응답이 발행되어야 한다
    adapter.publish.assert_called_once()
    props: MqttProperties = adapter.publish.call_args[0][3]
    prop_dict = dict(props.user_properties)
    assert prop_dict["reason_code"] == "135"  # 0x87 == 135


@pytest.mark.asyncio
async def test_no_middleware_backward_compat() -> None:
    """미들웨어 없을 때 기존 동작과 동일하게 핸들러가 실행되고 응답이 발행된다."""
    adapter = make_mock_adapter()
    dispatcher = make_dispatcher(adapter)

    called = []

    def handler(ctx: RpcContext):
        called.append(True)
        return {"ok": True}

    dispatcher.register("get", handler)

    await dispatcher.handle(make_incoming("get"))

    assert called == [True]
    adapter.publish.assert_called_once()
    props: MqttProperties = adapter.publish.call_args[0][3]
    prop_dict = dict(props.user_properties)
    assert prop_dict["reason_code"] == "0"


@pytest.mark.asyncio
async def test_response_props_merged_from_middleware() -> None:
    """미들웨어에서 ctx.response_props 설정 시 응답 User Property에 포함된다."""
    adapter = make_mock_adapter()
    dispatcher = make_dispatcher(adapter)

    async def mw(ctx: RpcContext, call_next):
        ctx.response_props["x-trace-id"] = "abc123"
        await call_next(ctx)

    dispatcher.add_middleware(mw)

    def handler(ctx: RpcContext):
        return {}

    dispatcher.register("get", handler)

    await dispatcher.handle(make_incoming("get"))

    adapter.publish.assert_called_once()
    props: MqttProperties = adapter.publish.call_args[0][3]
    prop_dict = dict(props.user_properties)
    assert prop_dict.get("x-trace-id") == "abc123"
