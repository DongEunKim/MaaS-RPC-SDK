"""
WMT/WMO 토픽 빌더 및 파서.

토픽 구조:
    {WMT|WMO}/{ThingType}/{Service}/{VIN}/{ClientId}/{request|response}
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


_SEG_REQUEST = "request"
_SEG_RESPONSE = "response"
_SEG_EVENT = "event"
_PREFIX_WMT = "WMT"
_PREFIX_WMO = "WMO"


@dataclass(frozen=True)
class ParsedTopic:
    """파싱된 토픽 세그먼트."""

    direction: str
    """WMT 또는 WMO."""

    thing_type: str
    """사물 타입 (예: CGU, SDM)."""

    service: str
    """서비스 이름 (예: viss, diagnostics)."""

    vin: str
    """장비 VIN."""

    client_id: str
    """클라이언트 식별자."""

    suffix: str
    """request | response | event."""


def build_request(
    thing_type: str,
    service: str,
    vin: str,
    client_id: str,
) -> str:
    """요청 토픽 생성: WMT/{thing_type}/{service}/{vin}/{client_id}/request."""
    return f"{_PREFIX_WMT}/{thing_type}/{service}/{vin}/{client_id}/{_SEG_REQUEST}"


def build_response(
    thing_type: str,
    service: str,
    vin: str,
    client_id: str,
) -> str:
    """응답 토픽 생성: WMO/{thing_type}/{service}/{vin}/{client_id}/response."""
    return f"{_PREFIX_WMO}/{thing_type}/{service}/{vin}/{client_id}/{_SEG_RESPONSE}"


def build_response_wildcard(client_id: str) -> str:
    """클라이언트가 자신의 모든 응답을 구독하는 와일드카드 토픽."""
    return f"{_PREFIX_WMO}/+/+/+/{client_id}/{_SEG_RESPONSE}"


def build_server_subscription(
    thing_type: str,
    service: str,
    vin: str,
) -> str:
    """서버가 구독할 요청 와일드카드 토픽."""
    return f"{_PREFIX_WMT}/{thing_type}/{service}/{vin}/+/{_SEG_REQUEST}"


def build_server_heartbeat(thing_type: str, service: str, vin: str) -> str:
    """서버 Heartbeat 구독 토픽: WMO/{thing_type}/{service}/{vin}/heartbeat"""
    return f"{_PREFIX_WMO}/{thing_type}/{service}/{vin}/heartbeat"


def build_offline(thing_type: str, service: str, vin: str, client_id: str) -> str:
    """LWT(단절) 토픽: WMT/{thing_type}/{service}/{vin}/{client_id}/offline"""
    return f"{_PREFIX_WMT}/{thing_type}/{service}/{vin}/{client_id}/offline"


def parse_topic(topic: str) -> Optional[ParsedTopic]:
    """
    토픽 문자열을 파싱하여 ParsedTopic 반환.

    유효하지 않은 형식이면 None 반환.
    """
    parts = topic.split("/")
    if len(parts) != 6:
        return None
    direction, thing_type, service, vin, client_id, suffix = parts
    if direction not in (_PREFIX_WMT, _PREFIX_WMO):
        return None
    if suffix not in (_SEG_REQUEST, _SEG_RESPONSE, _SEG_EVENT):
        return None
    return ParsedTopic(
        direction=direction,
        thing_type=thing_type,
        service=service,
        vin=vin,
        client_id=client_id,
        suffix=suffix,
    )
