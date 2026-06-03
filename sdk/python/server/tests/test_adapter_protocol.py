"""MqttClientAdapter Protocol 및 Dispatcher 어댑터 연동 단위 테스트."""
import asyncio
from unittest.mock import MagicMock
import pytest
from maas_server._adapter import MqttClientAdapter, MqttProperties
from maas_server._adapters import PahoMqttAdapter
from maas_server._dispatcher import Dispatcher
from maas_server.connection import IncomingMessage


def make_mock_adapter() -> MagicMock:
    """MqttClientAdapter Protocol을 만족하는 Mock."""
    m = MagicMock()
    m.publish = MagicMock()
    m.subscribe = MagicMock()
    m.set_message_callback = MagicMock()
    m.get_vin = MagicMock(return_value="VIN-TEST")
    return m


def test_paho_adapter_satisfies_protocol() -> None:
    """PahoMqttAdapter가 MqttClientAdapter Protocol을 만족한다."""
    adapter = PahoMqttAdapter.__new__(PahoMqttAdapter)
    assert isinstance(adapter, MqttClientAdapter)


@pytest.mark.asyncio
async def test_dispatcher_uses_adapter_publish() -> None:
    """Dispatcher._reply_success가 adapter.publish를 호출한다."""
    mock_adapter = make_mock_adapter()
    dispatcher = Dispatcher(
        conn=mock_adapter,
        thing_type="CGU",
        service_name="viss",
        vin="VIN-1",
    )

    from maas_server.context import RpcContext
    ctx = RpcContext(
        thing_type="CGU",
        service="viss",
        action="get",
        vin="VIN-1",
        client_id="cid",
        payload={},
        correlation_id=b"corr",
        response_topic="WMO/CGU/viss/VIN-1/cid/response",
    )
    await dispatcher._reply_success(ctx, {"value": 42})
    mock_adapter.publish.assert_called_once()
    call_args = mock_adapter.publish.call_args
    assert call_args[0][0] == "WMO/CGU/viss/VIN-1/cid/response"
    assert isinstance(call_args[0][3], MqttProperties)


@pytest.mark.asyncio
async def test_dispatcher_reply_error_uses_adapter() -> None:
    """Dispatcher._reply_error_ctx가 adapter.publish를 호출하고 reason_code를 포함한다."""
    mock_adapter = make_mock_adapter()
    dispatcher = Dispatcher(
        conn=mock_adapter,
        thing_type="CGU",
        service_name="viss",
        vin="VIN-1",
    )
    from maas_server.context import RpcContext
    ctx = RpcContext(
        thing_type="CGU",
        service="viss",
        action="get",
        vin="VIN-1",
        client_id="cid",
        payload={},
        correlation_id=b"corr",
        response_topic="WMO/CGU/viss/VIN-1/cid/response",
    )
    await dispatcher._reply_error_ctx(ctx, 0x87, "권한 없음")
    mock_adapter.publish.assert_called_once()
    props: MqttProperties = mock_adapter.publish.call_args[0][3]
    user_prop_dict = dict(props.user_properties)
    assert user_prop_dict["reason_code"] == "135"  # 0x87 == 135
