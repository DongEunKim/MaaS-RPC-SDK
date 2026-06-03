"""MaasServer mode 파라미터 및 팩토리 분기 단위 테스트."""
import pytest
from unittest.mock import patch, MagicMock
from maas_server import MaasServer


def test_mode_mqtt_requires_endpoint() -> None:
    """mode='mqtt'에서 endpoint 미지정 시 ValueError."""
    with pytest.raises(ValueError, match="endpoint"):
        MaasServer(
            thing_type="CGU",
            service_name="viss",
            vin="VIN-1",
            endpoint=None,
            mode="mqtt",
        )


def test_mode_mqtt_requires_vin() -> None:
    """mode='mqtt'에서 vin 미지정 시 ValueError."""
    with pytest.raises(ValueError, match="vin"):
        MaasServer(
            thing_type="CGU",
            service_name="viss",
            vin=None,
            endpoint="mqtt.example.com",
            mode="mqtt",
        )


def test_mode_mqtt_creates_paho_adapter() -> None:
    """mode='mqtt'에서 PahoMqttAdapter가 생성된다."""
    with patch("maas_server.server.PahoMqttAdapter", autospec=True) as MockAdapter:
        MockAdapter.return_value = MagicMock()
        server = MaasServer(
            thing_type="CGU",
            service_name="viss",
            vin="VIN-1",
            endpoint="mqtt.example.com",
            mode="mqtt",
        )
        MockAdapter.assert_called_once()
        assert server._adapter is MockAdapter.return_value


def test_mode_greengrass_creates_greengrass_adapter() -> None:
    """mode='greengrass'에서 GreengrassIpcAdapter가 생성된다."""
    with patch("maas_server.server.GreengrassIpcAdapter", autospec=True) as MockAdapter:
        MockAdapter.return_value = MagicMock()
        server = MaasServer(
            thing_type="CGU",
            service_name="viss",
            mode="greengrass",
        )
        MockAdapter.assert_called_once()


def test_mode_unknown_raises() -> None:
    """알 수 없는 mode 값은 ValueError."""
    with pytest.raises(ValueError, match="알 수 없는 mode"):
        MaasServer(
            thing_type="CGU",
            service_name="viss",
            vin="VIN-1",
            endpoint="mqtt.example.com",
            mode="invalid",
        )


def test_get_required_iot_core_permissions() -> None:
    """get_required_iot_core_permissions()가 올바른 accessControl dict를 반환한다."""
    result = MaasServer.get_required_iot_core_permissions("CGU", "viss", "VIN-1")
    assert "aws.greengrass.ipc.mqttproxy" in result
    perms = result["aws.greengrass.ipc.mqttproxy"]
    sub_key = "maas:CGU:viss:subscribe"
    assert sub_key in perms
    assert "WMT/CGU/viss/VIN-1/+/request" in perms[sub_key]["resources"]
    pub_key = "maas:CGU:viss:publish"
    assert pub_key in perms
    assert "WMO/CGU/viss/VIN-1/+/response" in perms[pub_key]["resources"]


def test_backward_compat_positional_vin_endpoint() -> None:
    """기존 방식(vin, endpoint 위치 인자)이 mode='mqtt'로 동작한다."""
    with patch("maas_server.server.PahoMqttAdapter") as MockAdapter:
        MockAdapter.return_value = MagicMock()
        server = MaasServer(
            thing_type="CGU",
            service_name="viss",
            vin="VIN-1",
            endpoint="mqtt.example.com",
        )
        MockAdapter.assert_called_once()
