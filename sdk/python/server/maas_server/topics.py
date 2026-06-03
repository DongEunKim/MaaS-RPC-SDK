"""
WMT/WMO 토픽 빌더 및 파서 (서버 측).

토픽 구조:
    {WMT|WMO}/{ThingType}/{Service}/{VIN}/{ClientId}/{request|response}
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

_SEG_REQUEST = "request"
_SEG_RESPONSE = "response"
_SEG_OFFLINE = "offline"
_PREFIX_WMT = "WMT"
_PREFIX_WMO = "WMO"


@dataclass(frozen=True)
class ParsedRequestTopic:
    """파싱된 요청 토픽 세그먼트."""

    thing_type: str
    service: str
    vin: str
    client_id: str


def build_response(
    thing_type: str,
    service: str,
    vin: str,
    client_id: str,
) -> str:
    """응답 토픽: WMO/{thing_type}/{service}/{vin}/{client_id}/response."""
    return f"{_PREFIX_WMO}/{thing_type}/{service}/{vin}/{client_id}/{_SEG_RESPONSE}"


def build_subscription(
    thing_type: str,
    service_name: str,
    vin: str,
) -> str:
    """서버가 구독할 와일드카드 토픽: WMT/{thing_type}/{service_name}/{vin}/+/request."""
    return f"{_PREFIX_WMT}/{thing_type}/{service_name}/{vin}/+/{_SEG_REQUEST}"


def build_heartbeat(thing_type: str, service_name: str, vin: str) -> str:
    """서버 Heartbeat 토픽: WMO/{thing_type}/{service_name}/{vin}/heartbeat"""
    return f"{_PREFIX_WMO}/{thing_type}/{service_name}/{vin}/heartbeat"


def build_offline_subscription(
    thing_type: str,
    service_name: str,
    vin: str,
) -> str:
    """서버가 구독할 LWT(단절) 와일드카드 토픽: WMT/{tt}/{svc}/{vin}/+/offline."""
    return f"{_PREFIX_WMT}/{thing_type}/{service_name}/{vin}/+/{_SEG_OFFLINE}"


def parse_offline(topic: str) -> Optional[str]:
    """
    LWT(offline) 토픽에서 client_id를 추출한다.

    형식: WMT/{thing_type}/{service}/{vin}/{client_id}/offline. 아니면 None.
    """
    parts = topic.split("/")
    if len(parts) != 6:
        return None
    direction, _tt, _svc, _vin, client_id, suffix = parts
    if direction != _PREFIX_WMT or suffix != _SEG_OFFLINE:
        return None
    return client_id


def parse_request(topic: str) -> Optional[ParsedRequestTopic]:
    """
    요청 토픽을 파싱하여 ParsedRequestTopic 반환.

    형식: WMT/{thing_type}/{service}/{vin}/{client_id}/request
    유효하지 않으면 None 반환.
    """
    parts = topic.split("/")
    if len(parts) != 6:
        return None
    direction, thing_type, service, vin, client_id, suffix = parts
    if direction != _PREFIX_WMT or suffix != _SEG_REQUEST:
        return None
    return ParsedRequestTopic(
        thing_type=thing_type,
        service=service,
        vin=vin,
        client_id=client_id,
    )


def topic_matches(pattern: str, topic: str) -> bool:
    """MQTT + / # 와일드카드 토픽 패턴 매칭."""
    pattern_parts = pattern.split("/")
    topic_parts = topic.split("/")
    for i, pp in enumerate(pattern_parts):
        if pp == "#":
            return True
        if i >= len(topic_parts):
            return False
        if pp != "+" and pp != topic_parts[i]:
            return False
    return len(pattern_parts) == len(topic_parts)
