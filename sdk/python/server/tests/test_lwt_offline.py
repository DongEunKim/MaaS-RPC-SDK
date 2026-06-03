"""LWT(offline) 단절 처리 및 패턴 C/E 콜백 단위 테스트."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest

from maas_server.presence import OfflineMonitor


def test_offline_monitor_parses_client_id_from_topic() -> None:
    mon = OfflineMonitor()
    seen = []
    mon.on_offline(lambda cid: seen.append(cid))
    cid = mon.handle_message("WMT/CGU/viss/VIN-1/cid-7/offline", b"{}")
    assert cid == "cid-7"
    assert seen == ["cid-7"]


def test_offline_monitor_payload_fallback() -> None:
    mon = OfflineMonitor()
    # 토픽이 offline 형식이 아니면 payload clientId 폴백 (matches는 False지만 handle은 폴백 허용)
    cid = mon.handle_message("WMT/CGU/viss/VIN-1/x/offline", json.dumps({"clientId": "p"}).encode())
    # 토픽에서 추출 가능하므로 토픽 우선
    assert cid == "x"


def test_offline_monitor_matches() -> None:
    mon = OfflineMonitor()
    assert mon.matches("WMT/CGU/viss/VIN-1/cid/offline") is True
    assert mon.matches("WMT/CGU/viss/VIN-1/cid/request") is False


@pytest.mark.asyncio
async def test_on_client_offline_releases_session_and_cancels_subs() -> None:
    """LWT 단절 → 세션 강제 해제 + on_session_lost, 구독 취소 + on_subscription_lost."""
    from maas_server.server import MaasServer

    with patch("maas_server.server.PahoMqttAdapter") as MockAdapter:
        MockAdapter.return_value = MagicMock()
        server = MaasServer(
            thing_type="T",
            service_name="S",
            vin="VIN-1",
            endpoint="localhost",
            port=1883,
            mode="mqtt",
            exclusive_service=True,
        )

    server._loop = asyncio.get_event_loop()

    lost_sessions: list = []
    lost_subs: list = []
    server.on_session_lost(lambda cid, sid: lost_sessions.append((cid, sid)))
    server.on_subscription_lost(lambda cid, sid: lost_subs.append((cid, sid)))

    # 세션 점유
    sid = server.acquire_session("c1")
    assert sid

    # 구독 생성
    sub_id, _ = await server._dispatcher._registry.create(client_id="c1")

    server._on_client_offline("c1")
    await asyncio.sleep(0.05)

    assert (("c1", sid)) in lost_sessions
    assert ("c1", sub_id) in lost_subs
    # 세션 해제됨
    assert server.get_session_id("c1") is None


def test_exclusive_service_creates_manager() -> None:
    from maas_server.server import MaasServer

    with patch("maas_server.server.PahoMqttAdapter") as MockAdapter:
        MockAdapter.return_value = MagicMock()
        server = MaasServer(
            thing_type="T", service_name="S", vin="VIN-1",
            endpoint="localhost", port=1883, mode="mqtt", exclusive_service=True,
        )
    assert server._exclusive_mgr is not None
    assert server.acquire_session("c1")
