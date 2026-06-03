"""토픽 빌더/파서 단위 테스트."""

from maas_client import topics


def test_build_request_response() -> None:
    t = topics.build_request("CGU", "viss", "VIN-1", "cid")
    assert t == "WMT/CGU/viss/VIN-1/cid/request"
    r = topics.build_response("CGU", "viss", "VIN-1", "cid")
    assert r == "WMO/CGU/viss/VIN-1/cid/response"


def test_parse_topic_roundtrip() -> None:
    raw = "WMT/CGU/viss/VIN-1/cid/request"
    p = topics.parse_topic(raw)
    assert p is not None
    assert p.direction == "WMT"
    assert p.thing_type == "CGU"
    assert p.service == "viss"
    assert p.vin == "VIN-1"
    assert p.client_id == "cid"
    assert p.suffix == "request"


def test_parse_topic_event_suffix_still_parseable() -> None:
    """event suffix는 deprecated이나 parse_topic이 여전히 파싱 가능해야 한다."""
    p = topics.parse_topic("WMO/CGU/viss/VIN-1/cid/event")
    assert p is not None
    assert p.suffix == "event"


def test_wildcards() -> None:
    assert "cid" in topics.build_response_wildcard("cid")


def test_server_subscription_pattern() -> None:
    s = topics.build_server_subscription("CGU", "viss", "VIN-1")
    assert s == "WMT/CGU/viss/VIN-1/+/request"


def test_build_offline() -> None:
    assert topics.build_offline("CGU", "viss", "VIN-1", "cid") == (
        "WMT/CGU/viss/VIN-1/cid/offline"
    )
