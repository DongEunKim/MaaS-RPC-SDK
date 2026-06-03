"""서버 토픽 모듈 단위 테스트."""

from maas_server import topics


def test_parse_request() -> None:
    p = topics.parse_request("WMT/CGU/viss/VIN-1/cid/request")
    assert p is not None
    assert p.thing_type == "CGU"
    assert p.service == "viss"
    assert p.vin == "VIN-1"
    assert p.client_id == "cid"


def test_build_subscription() -> None:
    assert topics.build_subscription("CGU", "viss", "VIN-1") == (
        "WMT/CGU/viss/VIN-1/+/request"
    )


def test_build_offline_subscription() -> None:
    assert topics.build_offline_subscription("CGU", "viss", "VIN-1") == (
        "WMT/CGU/viss/VIN-1/+/offline"
    )


def test_parse_offline() -> None:
    assert topics.parse_offline("WMT/CGU/viss/VIN-1/cid-9/offline") == "cid-9"
    assert topics.parse_offline("WMT/CGU/viss/VIN-1/cid-9/request") is None
    assert topics.parse_offline("bad") is None
