"""GreengrassIpcAdapter 단위 테스트."""
import sys
import pytest
from unittest.mock import MagicMock, patch


def test_greengrass_adapter_import_error_without_awsiotsdk() -> None:
    """awsiotsdk 미설치 환경에서 connect() 시 명확한 ImportError."""
    with patch.dict(sys.modules, {"awsiot": None, "awsiot.greengrasscoreipc": None}):
        from maas_server._adapters import GreengrassIpcAdapter
        adapter = GreengrassIpcAdapter(vin="VIN-TEST")
        with pytest.raises(ImportError, match="awsiotsdk"):
            adapter.connect()


def test_greengrass_adapter_get_vin_from_env(monkeypatch) -> None:
    """VIN 미지정 시 AWS_IOT_THING_NAME 환경변수에서 취득."""
    monkeypatch.setenv("AWS_IOT_THING_NAME", "MY-THING")

    mock_ipc_client = MagicMock()
    mock_ipc_module = MagicMock()
    mock_ipc_v2 = MagicMock()
    mock_ipc_v2.GreengrassCoreIPCClientV2 = MagicMock(return_value=mock_ipc_client)

    with patch.dict(sys.modules, {
        "awsiot": MagicMock(),
        "awsiot.greengrasscoreipc": mock_ipc_module,
        "awsiot.greengrasscoreipc.clientV2": mock_ipc_v2,
    }):
        from importlib import reload
        import maas_server._adapters as adapters_mod
        reload(adapters_mod)

        adapter = adapters_mod.GreengrassIpcAdapter(vin=None)
        adapter.connect()
        assert adapter.get_vin() == "MY-THING"


def test_greengrass_adapter_publish_calls_ipc_client() -> None:
    """publish()가 IPC 클라이언트의 publish_to_iot_core를 호출한다."""
    mock_ipc_module = MagicMock()
    mock_ipc_v2 = MagicMock()
    mock_model = MagicMock()
    mock_model.QOS.AT_LEAST_ONCE = 1
    mock_model.MqttUserProperty = MagicMock(side_effect=lambda key, value: (key, value))

    with patch.dict(sys.modules, {
        "awsiot": MagicMock(),
        "awsiot.greengrasscoreipc": mock_ipc_module,
        "awsiot.greengrasscoreipc.clientV2": mock_ipc_v2,
        "awsiot.greengrasscoreipc.model": mock_model,
    }):
        from importlib import reload
        import maas_server._adapters as adapters_mod
        reload(adapters_mod)

        from maas_server._adapter import MqttProperties
        adapter = adapters_mod.GreengrassIpcAdapter(vin="VIN-GG")
        adapter.connect()
        # adapter._ipc_client은 sys.modules에서 가져온 mock의 인스턴스이므로 직접 참조
        ipc_client = adapter._ipc_client
        props = MqttProperties(
            correlation_data=b"corr",
            user_properties=[("reason_code", "0")],
        )
        adapter.publish("WMO/CGU/viss/VIN-GG/cid/response", b'{"value":1}', qos=1, props=props)
        ipc_client.publish_to_iot_core.assert_called_once()
