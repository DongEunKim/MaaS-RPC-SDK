"""
멀티 클라이언트 강건성 E2E 시나리오.

TC-ROB-01  N개 클라이언트 동시 call — corr_id 격리
TC-ROB-02  독점 세션 경합 — 동시 acquire 시 ServerBusyError
TC-ROB-03  강제 단절 후 idle_timeout 경과 → 세션 재획득
TC-ROB-04  단일 클라이언트 20개 동시 call — 전부 완결
TC-ROB-05  Heartbeat 타임아웃 → SubscriptionCancelledError/ServerOfflineError
TC-ROB-06  멀티 클라이언트 × 유한 스트리밍 — corr_id 격리
"""

from __future__ import annotations

import asyncio

import pytest

from maas_client.exceptions import (
    ServerBusyError,
    SubscriptionCancelledError,
    ServerOfflineError,
)
from maas_client.models import RpcResponse

from .conftest import BrokerInfo
from .fixtures.viss_server import VissServer
from .fixtures.viss_client import VissClient


# ── TC-ROB-01: N개 클라이언트 동시 call — corr_id 격리 ───────────────────────

@pytest.mark.asyncio
async def test_rob_01_n_clients_concurrent_call(broker: BrokerInfo):
    N = 10
    sf = VissServer(broker.host, broker.tcp_port, client_id_suffix="rob01")
    sf.start()
    try:
        clients = [
            VissClient(broker.host, broker.tcp_port, client_id=f"rob01-c{i}")
            for i in range(N)
        ]
        for c in clients:
            await c.connect()
        try:
            results = await asyncio.gather(
                *[c.set_signal("Vehicle.Speed", float(i)) for i, c in enumerate(clients)]
            )
            assert len(results) == N
            assert all(r.reason_code == 0 for r in results)
            corr_ids = [r.correlation_id for r in results]
            assert len(set(corr_ids)) == N, "correlation_id 중복 발생"
        finally:
            for c in clients:
                await c.disconnect()
    finally:
        sf.stop()


# ── TC-ROB-02: 독점 세션 경합 — 동시 acquire 시 ServerBusyError ──────────────

@pytest.mark.asyncio
async def test_rob_02_exclusive_session_contention(broker: BrokerInfo):
    sf = VissServer(broker.host, broker.tcp_port, client_id_suffix="rob02")
    sf.start()
    try:
        client_a = VissClient(broker.host, broker.tcp_port, "rob02-a")
        client_b = VissClient(broker.host, broker.tcp_port, "rob02-b")
        await client_a.connect()
        await client_b.connect()
        try:
            results = await asyncio.gather(
                client_a._async.call("session_start", timeout=5.0),
                client_b._async.call("session_start", timeout=5.0),
                return_exceptions=True,
            )
            successes = [r for r in results if isinstance(r, RpcResponse)]
            errors    = [r for r in results if isinstance(r, ServerBusyError)]
            assert len(successes) == 1 and len(errors) == 1
        finally:
            await client_a.disconnect()
            await client_b.disconnect()
    finally:
        sf.stop()


# ── TC-ROB-03: 강제 단절 → LWT 세션 정리 → 세션 재획득 ───────────────────────

@pytest.mark.asyncio
async def test_rob_03_force_disconnect_session_cleanup(broker: BrokerInfo):
    sf = VissServer(
        broker.host, broker.tcp_port,
        client_id_suffix="rob03",
    )
    sf.start()
    try:
        client_a = VissClient(broker.host, broker.tcp_port, "rob03-a")
        await client_a.connect()
        await client_a._async.call("session_start", timeout=5.0)
        await client_a.disconnect()          # 강제 단절 (release 없음) → LWT 발행
        await asyncio.sleep(2.5)             # LWT 단절 정리 전파 대기
        client_b = VissClient(broker.host, broker.tcp_port, "rob03-b")
        async with client_b:
            resp = await client_b._async.call("session_start", timeout=5.0)
            assert resp.reason_code == 0
    finally:
        sf.stop()


# ── TC-ROB-04: 단일 클라이언트 20개 동시 call — 전부 완결 ────────────────────

@pytest.mark.asyncio
async def test_rob_04_single_client_high_concurrency(broker: BrokerInfo):
    N = 20
    sf = VissServer(broker.host, broker.tcp_port, client_id_suffix="rob04")
    sf.start()
    try:
        async with VissClient(broker.host, broker.tcp_port, "rob04-c") as client:
            results = await asyncio.gather(
                *[client.set_signal("Vehicle.Speed", float(i)) for i in range(N)],
                return_exceptions=True,
            )
        assert len(results) == N
        errors = [r for r in results if isinstance(r, Exception)]
        assert not errors, f"오류 발생: {errors}"
    finally:
        sf.stop()


# ── TC-ROB-05: Heartbeat 타임아웃 → SubscriptionCancelledError/ServerOfflineError

@pytest.mark.asyncio
async def test_rob_05_heartbeat_timeout_subscription_cancelled(broker: BrokerInfo):
    sf: VissServer | None = VissServer(
        broker.host, broker.tcp_port,
        heartbeat_interval=1.0,
        client_id_suffix="rob05",
    )
    sf.start()
    try:
        client = VissClient(
            broker.host, broker.tcp_port, "rob05-c",
            heartbeat_hint_interval=1.0,
            heartbeat_timeout_multiplier=2.0,
            first_heartbeat_timeout=5.0,
        )
        await client.connect()
        try:
            sub = await client.open_subscription()
            # 첫 이벤트 수신 확인
            first = await asyncio.wait_for(sub.events.__anext__(), timeout=5.0)
            assert not first.is_eof

            sf.stop()       # 서버 강제 종료 → Heartbeat 중단
            sf = None

            with pytest.raises((SubscriptionCancelledError, ServerOfflineError)):
                async for _ in sub.events:
                    pass
        finally:
            await client.disconnect()
    finally:
        if sf is not None:
            sf.stop()


# ── TC-ROB-06: 멀티 클라이언트 × 유한 스트리밍 — corr_id 격리 ────────────────

@pytest.mark.asyncio
async def test_rob_06_multi_client_streaming_isolated(broker: BrokerInfo):
    COUNT = 5
    sf = VissServer(broker.host, broker.tcp_port, client_id_suffix="rob06")
    sf.start()
    try:
        clients = [
            VissClient(broker.host, broker.tcp_port, f"rob06-c{i}")
            for i in range(3)
        ]
        for c in clients:
            await c.connect()
        try:
            results = await asyncio.gather(
                *[c.stream_data(count=COUNT) for c in clients]
            )
            for chunks in results:
                assert len(chunks) == COUNT, f"청크 수 불일치: {len(chunks)}"
                ns = [ch["n"] for ch in chunks]
                assert ns == list(range(COUNT)), f"순서 불일치: {ns}"
        finally:
            for c in clients:
                await c.disconnect()
    finally:
        sf.stop()
