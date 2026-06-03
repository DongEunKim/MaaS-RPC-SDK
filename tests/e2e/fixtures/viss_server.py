"""
VissServer: VISS 경량 차량 서비스 픽스처.

패턴 A~E를 커버하는 MaasServer 래퍼.
"""

from __future__ import annotations

import random
import threading
import time
from typing import Optional

from maas_server import MaasServer, RpcContext
from maas_server.exceptions import HandlerError

VISS_THING_TYPE = "CGU"
VISS_SERVICE    = "viss"
VISS_VIN        = "VIN-E2E-001"


class VissServer:
    """VISS 서비스 시뮬레이터. MaasServer 래퍼."""

    def __init__(
        self,
        broker_host: str,
        broker_port: int,
        *,
        heartbeat_interval: float = 3.0,
        client_id_suffix: str = "",
    ) -> None:
        import uuid
        cid = f"viss-server-{client_id_suffix or uuid.uuid4().hex[:6]}"
        self._server = MaasServer(
            thing_type=VISS_THING_TYPE,
            service_name=VISS_SERVICE,
            vin=VISS_VIN,
            endpoint=broker_host,
            port=broker_port,
            use_wss=False,
            client_id=cid,
            route_key="action",
            heartbeat_interval=heartbeat_interval,
            exclusive_service=True,
        )
        self._thread: Optional[threading.Thread] = None
        self._store: dict = {}
        self._register_handlers()

    def _register_handlers(self) -> None:
        server = self._server

        # 패턴 A — 경량 조회 (QoS 0)
        @server.action("get_signal")
        def get_signal(ctx: RpcContext) -> dict:
            path = ctx.payload.get("path", "Vehicle.Speed") if isinstance(ctx.payload, dict) else "Vehicle.Speed"
            return {"path": path, "value": 42.0, "ts": time.time()}

        # 패턴 B — 하드웨어 상태 변경 (QoS 1)
        @server.action("set_signal")
        def set_signal(ctx: RpcContext) -> dict:
            if isinstance(ctx.payload, dict):
                path  = ctx.payload.get("path", "")
                value = ctx.payload.get("value", None)
                self._store[path] = value
            return {"ok": True}

        # 패턴 C — 무한 구독 스트리밍
        @server.action("subscribe_signal", subscription=True)
        def subscribe_signal(ctx: RpcContext):
            deadline = time.monotonic() + 60.0
            while True:
                # cancel_event가 있으면 취소 여부 확인
                if ctx.cancel_event is not None and ctx.cancel_event.is_set():
                    return
                if time.monotonic() > deadline:
                    return
                yield {"value": random.random(), "ts": time.time()}
                # 취소 체크 후 대기
                if ctx.cancel_event is not None:
                    # 짧은 루프로 cancel_event를 0.1초간 체크
                    end = time.monotonic() + 0.1
                    while time.monotonic() < end:
                        if ctx.cancel_event.is_set():
                            return
                        time.sleep(0.01)
                else:
                    time.sleep(0.1)

        # 패턴 C(유한 스트림) — generator가 끝나면 자연 종료
        @server.action("stream_data", subscription=True)
        def stream_data(ctx: RpcContext):
            count = ctx.payload.get("count", 5) if isinstance(ctx.payload, dict) else 5
            for i in range(count):
                yield {"n": i, "value": float(i * 1.5)}

        # 패턴 E — 독점 세션 획득
        @server.action("session_start")
        def session_start(ctx: RpcContext) -> dict:
            sid = server.acquire_session(ctx.client_id)
            if sid is None:
                raise HandlerError("독점 세션 점유 중", reason_code=0x8A)
            return {"acquired": True}

        # 패턴 E — 독점 세션 해제
        @server.action("session_stop")
        def session_stop(ctx: RpcContext) -> dict:
            server.release_session(ctx.client_id)
            return {"released": True}

        # 패턴 E — 세션 점유자만 호출 가능 (비점유자는 exclusive_service가 자동 0x8A)
        @server.action("exclusive_control")
        def exclusive_control(ctx: RpcContext) -> dict:
            return {"controlled": True}

    def start(self, ready_timeout: float = 5.0) -> None:
        """백그라운드 스레드에서 서버를 기동한다."""
        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self._thread.start()
        time.sleep(0.6)

    def stop(self) -> None:
        """서버를 종료하고 스레드를 join한다."""
        self._server.stop()
        if self._thread:
            self._thread.join(timeout=5.0)

    @property
    def server(self) -> MaasServer:
        return self._server
