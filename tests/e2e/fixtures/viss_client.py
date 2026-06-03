"""
VissClient: VISS API 메서드를 제공하는 MaasClientAsync 래퍼.
"""

from __future__ import annotations

import time

from maas_client.client_async import MaasClientAsync
from maas_client.models import RpcResponse, Subscription

from .viss_server import VISS_THING_TYPE, VISS_SERVICE, VISS_VIN


class VissClient:
    """VISS 서비스 클라이언트 픽스처."""

    def __init__(
        self,
        broker_host: str,
        broker_port: int,
        client_id: str,
        *,
        heartbeat_hint_interval: float = 10.0,
        heartbeat_timeout_multiplier: float = 3.0,
        first_heartbeat_timeout: float = 30.0,
    ) -> None:
        self._async = MaasClientAsync(
            endpoint=broker_host,
            port=broker_port,
            client_id=client_id,
            token_provider=None,
            use_wss=False,
            thing_type=VISS_THING_TYPE,
            service=VISS_SERVICE,
            vin=VISS_VIN,
            heartbeat_hint_interval=heartbeat_hint_interval,
            heartbeat_timeout_multiplier=heartbeat_timeout_multiplier,
            first_heartbeat_timeout=first_heartbeat_timeout,
        )
        self._rtt_ms: list[float] = []

    async def __aenter__(self) -> "VissClient":
        await self._async.connect()
        return self

    async def __aexit__(self, *args) -> None:
        await self._async.disconnect()

    async def connect(self) -> None:
        await self._async.connect()

    async def disconnect(self) -> None:
        await self._async.disconnect()

    # 패턴 A (QoS 0) — call_best_effort 편의 메서드 사용
    async def get_signal(self, path: str = "Vehicle.Speed") -> dict:
        t0 = time.monotonic()
        resp = await self._async.call_best_effort(
            "get_signal", params={"path": path}, timeout=5.0
        )
        self._rtt_ms.append((time.monotonic() - t0) * 1000.0)
        return resp.payload

    # 패턴 B (QoS 1) — call_reliable 편의 메서드 사용
    async def set_signal(self, path: str, value: float) -> RpcResponse:
        t0 = time.monotonic()
        resp = await self._async.call_reliable(
            "set_signal", params={"path": path, "value": value}, timeout=5.0
        )
        self._rtt_ms.append((time.monotonic() - t0) * 1000.0)
        return resp

    # 패턴 C (유한 스트리밍 — generator 자연 종료)
    async def stream_data(self, count: int = 5) -> list[dict]:
        results = []
        async for event in self._async.stream("stream_data", {"count": count}):
            if not event.is_eof:
                results.append(event.payload)
        return results

    # 패턴 C (무한 구독, open_subscription 위임)
    async def open_subscription(self, path: str = "Vehicle.Speed") -> Subscription:
        return await self._async.open_subscription(
            "subscribe_signal", {"path": path}
        )

    # 패턴 E (독점 세션)
    def exclusive_session(self):
        return self._async.exclusive_session()

    @property
    def rtt_ms(self) -> list[float]:
        """누적된 RTT 측정값 목록 (밀리초)."""
        return list(self._rtt_ms)

    def reset_rtt(self) -> None:
        """RTT 측정값을 초기화한다."""
        self._rtt_ms.clear()

    @property
    def client_id(self) -> str:
        return self._async.client_id
