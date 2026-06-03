"""
E2E 통합 테스트: 실제 MQTT 브로커 + 실제 MaasServer + 실제 MaasClientAsync.

mosquitto subprocess가 자동 기동되므로 브로커를 수동 기동할 필요 없다.

테스트 시나리오:
  [정상 흐름]
  TC-E2E-01  단일 RPC call (QoS 1) — 패턴 B
  TC-E2E-02  스트리밍 (패턴 C) — response 토픽 단일 경로
  TC-E2E-03  QoS 0 call — 패턴 A
  TC-E2E-04  동시 스트림 2개 — corr_id 독립 격리
  TC-E2E-05  독점 세션 (패턴 E) acquire/release 왕복

  [예외 시나리오]
  TC-E2E-06  서버 없음 → RpcTimeoutError
  TC-E2E-07  HandlerError(0x80) → RpcServerError(rc=0x80)
  TC-E2E-08  NotAuthorizedError(0x87) → NotAuthorizedError
  TC-E2E-09  미지원 action → RpcServerError(rc=0x90)
  TC-E2E-10  스트리밍 도중 서버 HandlerError → RpcServerError
  TC-E2E-11  세션 점유 중 재획득 → ServerBusyError
  TC-E2E-12  잘못된 JSON payload → RpcServerError(rc=0x99)
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import AsyncIterator

import pytest
import pytest_asyncio

from maas_client import MaasClient
from maas_client.client_async import MaasClientAsync
from maas_client.exceptions import (
    RpcTimeoutError,
    RpcServerError,
    NotAuthorizedError,
    ServerBusyError,
)
from maas_server import MaasServer, RpcContext
from maas_server.exceptions import (
    HandlerError,
    NotAuthorizedError as ServerNotAuthorizedError,
    PayloadError,
)

from .conftest import (
    BROKER_HOST,
    BROKER_PORT,
    THING_TYPE,
    SERVICE,
    VIN,
    ServerFixture,
    make_server,
    require_broker,
    BrokerInfo,
)

# ── 유틸 ──────────────────────────────────────────────────────────────────────

def _client(suffix: str = "", port: int = BROKER_PORT) -> MaasClientAsync:
    cid = f"e2e-client-{suffix or uuid.uuid4().hex[:6]}"
    return MaasClientAsync(
        endpoint=BROKER_HOST,
        port=port,
        client_id=cid,
        token_provider=None,
        use_wss=False,
        thing_type=THING_TYPE,
        service=SERVICE,
        vin=VIN,
    )


# ── TC-E2E-01: 단일 RPC call (QoS 1) ─────────────────────────────────────────

@require_broker
@pytest.mark.asyncio
async def test_e2e_01_single_call_qos1(broker: BrokerInfo):
    server = make_server("01", port=broker.tcp_port)

    @server.action("echo")
    def echo(ctx: RpcContext) -> dict:
        return {"echoed": ctx.payload.get("msg", "")}

    sf = ServerFixture(server)
    sf.start()
    try:
        async with _client("01", port=broker.tcp_port) as client:
            resp = await client.call("echo", {"msg": "hello"}, timeout=5.0)
        assert resp.payload == {"echoed": "hello"}
        assert resp.reason_code == 0
    finally:
        sf.stop()


# ── TC-E2E-02: 스트리밍 — response 토픽 단일 경로 ────────────────────────────

@require_broker
@pytest.mark.asyncio
async def test_e2e_02_streaming_via_response_topic(broker: BrokerInfo):
    """패턴 C: 서버가 청크를 response 토픽으로 발행하고, is_EOF=true로 종료."""
    server = make_server("02", port=broker.tcp_port)
    CHUNKS = [{"n": i, "value": i * 10} for i in range(5)]

    @server.action("stream_data", subscription=True)
    def stream_data(ctx: RpcContext):
        for chunk in CHUNKS:
            yield chunk

    sf = ServerFixture(server)
    sf.start()
    try:
        received = []
        eof_count = 0
        async with _client("02", port=broker.tcp_port) as client:
            async for event in client.stream("stream_data"):
                if event.is_eof:
                    eof_count += 1
                else:
                    received.append(event.payload)

        assert received == CHUNKS, f"청크 불일치: {received}"
        assert eof_count == 1, "EOF는 정확히 1회여야 한다"
    finally:
        sf.stop()


# ── TC-E2E-03: QoS 0 call (패턴 A) ───────────────────────────────────────────

@require_broker
@pytest.mark.asyncio
async def test_e2e_03_call_qos0(broker: BrokerInfo):
    server = make_server("03", port=broker.tcp_port)

    @server.action("ping")
    def ping(ctx: RpcContext) -> dict:
        return {"status": "pong"}

    sf = ServerFixture(server)
    sf.start()
    try:
        async with _client("03", port=broker.tcp_port) as client:
            resp = await client.call("ping", qos=0, timeout=5.0)
        assert resp.payload == {"status": "pong"}
    finally:
        sf.stop()


# ── TC-E2E-04: 동시 스트림 2개 — corr_id 독립 격리 ──────────────────────────

@require_broker
@pytest.mark.asyncio
async def test_e2e_04_concurrent_streams_isolated(broker: BrokerInfo):
    """두 스트림이 동일 클라이언트에서 동시에 진행될 때 서로 섞이지 않아야 한다."""
    server = make_server("04", port=broker.tcp_port)

    @server.action("count", subscription=True)
    def count(ctx: RpcContext):
        n = int(ctx.payload.get("n", 3))
        for i in range(n):
            yield {"i": i}

    sf = ServerFixture(server)
    sf.start()
    try:
        async with _client("04", port=broker.tcp_port) as client:
            async def collect(n: int) -> list:
                items = []
                async for ev in client.stream("count", {"n": n}):
                    if not ev.is_eof:
                        items.append(ev.payload["i"])
                return items

            results = await asyncio.gather(collect(3), collect(4))

        assert sorted(results[0]) == [0, 1, 2]
        assert sorted(results[1]) == [0, 1, 2, 3]
    finally:
        sf.stop()


# ── TC-E2E-05: 독점 세션 acquire / release (패턴 E) ──────────────────────────

@require_broker
@pytest.mark.asyncio
async def test_e2e_05_exclusive_session_acquire_release(broker: BrokerInfo):
    server = make_server("05", port=broker.tcp_port, exclusive_service=True)

    @server.action("session_start")
    def session_start(ctx: RpcContext) -> dict:
        sid = server.acquire_session(ctx.client_id)
        if sid is None:
            raise HandlerError("독점 세션 점유 중", reason_code=0x8A)
        return {"acquired": True}

    @server.action("session_stop")
    def session_stop(ctx: RpcContext) -> dict:
        server.release_session(ctx.client_id)
        return {"released": True}

    @server.action("do_work")
    def do_work(ctx: RpcContext) -> dict:
        return {"done": True}

    sf = ServerFixture(server)
    sf.start()
    try:
        async with _client("05", port=broker.tcp_port) as client:
            async with client.exclusive_session() as sess:
                result = await sess.call("do_work")
            assert result.payload == {"done": True}
    finally:
        sf.stop()


# ── TC-E2E-06: 서버 없음 → RpcTimeoutError ───────────────────────────────────

@require_broker
@pytest.mark.asyncio
async def test_e2e_06_no_server_timeout(broker: BrokerInfo):
    """응답 서버가 없으면 RpcTimeoutError가 발생해야 한다."""
    async with _client("06", port=broker.tcp_port) as client:
        with pytest.raises(RpcTimeoutError) as exc_info:
            await client.call("ghost_action", timeout=1.0)
    assert "e2e-svc" in str(exc_info.value)


# ── TC-E2E-07: HandlerError(0x80) → RpcServerError ───────────────────────────

@require_broker
@pytest.mark.asyncio
async def test_e2e_07_handler_error_unspecified(broker: BrokerInfo):
    server = make_server("07", port=broker.tcp_port)

    @server.action("broken")
    def broken(ctx: RpcContext) -> dict:
        raise HandlerError("하드웨어 오류", reason_code=0x80)

    sf = ServerFixture(server)
    sf.start()
    try:
        async with _client("07", port=broker.tcp_port) as client:
            with pytest.raises(RpcServerError) as exc_info:
                await client.call("broken", timeout=5.0)
        assert exc_info.value.reason_code == 0x80
    finally:
        sf.stop()


# ── TC-E2E-08: NotAuthorizedError(0x87) → 클라이언트 NotAuthorizedError ───────

@require_broker
@pytest.mark.asyncio
async def test_e2e_08_not_authorized(broker: BrokerInfo):
    server = make_server("08", port=broker.tcp_port)

    @server.action("protected")
    def protected(ctx: RpcContext) -> dict:
        raise ServerNotAuthorizedError("권한 없음")

    sf = ServerFixture(server)
    sf.start()
    try:
        async with _client("08", port=broker.tcp_port) as client:
            with pytest.raises(NotAuthorizedError):
                await client.call("protected", timeout=5.0)
    finally:
        sf.stop()


# ── TC-E2E-09: 미지원 action → RpcServerError(rc=0x90) ───────────────────────

@require_broker
@pytest.mark.asyncio
async def test_e2e_09_unknown_action(broker: BrokerInfo):
    """등록되지 않은 action을 호출하면 rc=0x90 오류가 반환되어야 한다."""
    server = make_server("09", port=broker.tcp_port)

    @server.action("known")
    def known(ctx: RpcContext) -> dict:
        return {}

    sf = ServerFixture(server)
    sf.start()
    try:
        async with _client("09", port=broker.tcp_port) as client:
            with pytest.raises(RpcServerError) as exc_info:
                await client.call("unknown_action_xyz", timeout=5.0)
        assert exc_info.value.reason_code == 0x90
    finally:
        sf.stop()


# ── TC-E2E-10: 스트리밍 도중 서버 에러 ───────────────────────────────────────

@require_broker
@pytest.mark.asyncio
async def test_e2e_10_streaming_server_error_midway(broker: BrokerInfo):
    """스트리밍 도중 HandlerError가 발생하면 클라이언트에 예외가 전파되어야 한다."""
    server = make_server("10", port=broker.tcp_port)

    @server.action("stream_then_fail", subscription=True)
    def stream_then_fail(ctx: RpcContext):
        yield {"n": 0}
        yield {"n": 1}
        raise HandlerError("중간 오류", reason_code=0x80)

    sf = ServerFixture(server)
    sf.start()
    try:
        async with _client("10", port=broker.tcp_port) as client:
            with pytest.raises(RpcServerError):
                async for _ in client.stream("stream_then_fail"):
                    pass
    finally:
        sf.stop()


# ── TC-E2E-11: 세션 점유 중 재획득 → ServerBusyError ─────────────────────────

@require_broker
@pytest.mark.asyncio
async def test_e2e_11_exclusive_session_busy(broker: BrokerInfo):
    """이미 세션을 점유한 상태에서 다른 클라이언트가 acquire하면 ServerBusyError."""
    server = make_server("11", port=broker.tcp_port, exclusive_service=True)

    @server.action("session_start")
    def session_start(ctx: RpcContext) -> dict:
        sid = server.acquire_session(ctx.client_id)
        if sid is None:
            raise HandlerError("독점 세션 점유 중", reason_code=0x8A)
        return {"acquired": True}

    @server.action("session_stop")
    def session_stop(ctx: RpcContext) -> dict:
        server.release_session(ctx.client_id)
        return {"released": True}

    sf = ServerFixture(server)
    sf.start()
    try:
        client_a = _client("11a", port=broker.tcp_port)
        client_b = _client("11b", port=broker.tcp_port)

        await client_a.connect()
        await client_b.connect()
        try:
            # A가 먼저 세션 획득
            await client_a.call("session_start", timeout=5.0)

            # B가 동일 VIN 세션 획득 시도 → ServerBusyError
            with pytest.raises(ServerBusyError):
                await client_b.call("session_start", timeout=5.0)

            # A가 세션 해제
            await client_a.call("session_stop", timeout=5.0)
        finally:
            await client_a.disconnect()
            await client_b.disconnect()
    finally:
        sf.stop()


# ── TC-E2E-12: 잘못된 JSON payload → RpcServerError(rc=0x99) ─────────────────

@require_broker
@pytest.mark.asyncio
async def test_e2e_12_invalid_json_payload(broker: BrokerInfo):
    """올바르지 않은 JSON을 보내면 서버가 rc=0x99로 응답해야 한다."""
    import paho.mqtt.client as mqtt_client_lib
    import paho.mqtt.enums as mqtt_enums

    server = make_server("12", port=broker.tcp_port)

    @server.action("any_action")
    def any_action(ctx: RpcContext) -> dict:
        return {}

    sf = ServerFixture(server)
    sf.start()

    # 잘못된 페이로드(non-JSON)를 직접 MQTT PUBLISH
    received_rc: list[int] = []
    done_event = asyncio.Event()

    loop = asyncio.get_running_loop()

    low_client = mqtt_client_lib.Client(
        callback_api_version=mqtt_enums.CallbackAPIVersion.VERSION2,
        client_id="e2e-raw-12",
        protocol=mqtt_client_lib.MQTTv5,
    )

    corr_id = b"test-corr-12"
    client_id = "e2e-raw-12"
    resp_topic = f"WMO/{THING_TYPE}/{SERVICE}/{VIN}/{client_id}/response"
    req_topic = f"WMT/{THING_TYPE}/{SERVICE}/{VIN}/{client_id}/request"

    def on_connect(c, userdata, flags, reason_code, props):
        c.subscribe(resp_topic, qos=1)

    def on_message(c, userdata, msg):
        # User Props에서 reason_code 추출 (키: "reason_code")
        rc_val = 0
        if msg.properties and hasattr(msg.properties, "UserProperty"):
            for k, v in (msg.properties.UserProperty or []):
                if k == "reason_code":
                    rc_val = int(v)
        received_rc.append(rc_val)
        loop.call_soon_threadsafe(done_event.set)

    low_client.on_connect = on_connect
    low_client.on_message = on_message
    low_client.connect(BROKER_HOST, broker.tcp_port)
    low_client.loop_start()

    # 잠시 대기 후 발행
    await asyncio.sleep(0.4)

    import paho.mqtt.properties as mqtt_props
    import paho.mqtt.packettypes as pkt_types

    props_obj = mqtt_props.Properties(pkt_types.PacketTypes.PUBLISH)
    props_obj.ResponseTopic = resp_topic
    props_obj.CorrelationData = corr_id

    low_client.publish(
        req_topic,
        payload=b"NOT_JSON{{{",
        qos=1,
        properties=props_obj,
    )

    try:
        await asyncio.wait_for(done_event.wait(), timeout=5.0)
        assert received_rc == [0x99], f"예상 rc=0x99, 실제: {received_rc}"
    finally:
        low_client.loop_stop()
        low_client.disconnect()
        sf.stop()
