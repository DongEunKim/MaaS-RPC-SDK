"""
E2E 테스트 공통 픽스처.

mosquitto subprocess를 자동 기동하므로 외부 브로커 없이도 실행 가능하다.
서버 프로세스는 각 픽스처에서 스레드로 기동·종료한다.
"""

from __future__ import annotations

import json
import shutil
import socket
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest

from maas_client.client_async import MaasClientAsync
from maas_server import MaasServer

# ── 모듈 수준 상수 (기존 test_e2e_integration.py import 호환) ─────────────────

BROKER_HOST = "127.0.0.1"
BROKER_PORT = 1883          # 실제 포트는 broker 픽스처에서 동적 공급

THING_TYPE = "CGU"
SERVICE    = "e2e-svc"
VIN        = "VIN-E2E-001"


def require_broker(func):
    """mosquitto가 자동 기동되므로 skip 조건 없이 그대로 반환."""
    return func


# ── BrokerInfo ────────────────────────────────────────────────────────────────

@dataclass
class BrokerInfo:
    host: str
    tcp_port: int
    ws_port: int


# ── mosquitto 자동 기동 픽스처 ─────────────────────────────────────────────────

@pytest.fixture(scope="session")
def broker(tmp_path_factory):
    """mosquitto subprocess를 자동 기동하고 BrokerInfo를 반환한다."""
    mosquitto_bin = shutil.which("mosquitto") or "/usr/sbin/mosquitto"
    if not Path(mosquitto_bin).exists():
        pytest.skip("mosquitto not found")

    # 포트 두 개 동적 확보
    def _free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    tcp_port = _free_port()
    ws_port  = _free_port()

    # mosquitto.conf 동적 생성
    tmp_dir  = tmp_path_factory.mktemp("mosquitto")
    conf_path = tmp_dir / "mosquitto.conf"
    conf_path.write_text(
        f"listener {tcp_port}\n"
        f"protocol mqtt\n"
        f"listener {ws_port}\n"
        f"protocol websockets\n"
        f"allow_anonymous true\n"
    )

    proc = subprocess.Popen(
        [mosquitto_bin, "-c", str(conf_path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # health check — 최대 5초
    deadline = time.monotonic() + 5.0
    while True:
        try:
            with socket.create_connection(("127.0.0.1", tcp_port), timeout=0.2):
                break
        except OSError:
            if time.monotonic() > deadline:
                proc.terminate()
                pytest.fail("mosquitto did not start within 5 seconds")
            time.sleep(0.1)

    yield BrokerInfo(host="127.0.0.1", tcp_port=tcp_port, ws_port=ws_port)

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


# ── 서버 픽스처 헬퍼 ──────────────────────────────────────────────────────────

class ServerFixture:
    """MaasServer를 백그라운드 스레드에서 실행하는 헬퍼."""

    def __init__(self, server: MaasServer) -> None:
        self.server = server
        self._thread: threading.Thread | None = None

    def start(self, ready_timeout: float = 5.0) -> None:
        self._thread = threading.Thread(target=self.server.run, daemon=True)
        self._thread.start()
        # 서버가 브로커에 구독을 마칠 때까지 잠시 대기
        time.sleep(0.6)

    def stop(self) -> None:
        self.server.stop()
        if self._thread:
            self._thread.join(timeout=5.0)


def make_server(
    client_id_suffix: str = "",
    *,
    port: int = 1883,
    heartbeat_interval: float = 10.0,
    exclusive_service: bool = False,
) -> MaasServer:
    """테스트용 MaasServer 인스턴스 (기본 thing_type/service/vin)."""
    cid = f"e2e-server-{client_id_suffix or uuid.uuid4().hex[:6]}"
    return MaasServer(
        thing_type=THING_TYPE,
        service_name=SERVICE,
        vin=VIN,
        endpoint=BROKER_HOST,
        port=port,
        use_wss=False,
        client_id=cid,
        route_key="action",
        heartbeat_interval=heartbeat_interval,
        exclusive_service=exclusive_service,
    )


def make_client(suffix: str = "", *, port: int = 1883) -> MaasClientAsync:
    """테스트용 MaasClientAsync 인스턴스."""
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


# ── latency 수집 pytest 훅 ────────────────────────────────────────────────────

_latency_records: list[dict] = []
_rtt_registry: dict[str, list[float]] = {}


def register_rtt(nodeid: str, rtt_ms_list: list[float]) -> None:
    """테스트 코드에서 RTT 데이터를 conftest에 등록한다. nodeid는 request.node.nodeid."""
    _rtt_registry[nodeid] = list(rtt_ms_list)


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()
    setattr(item, f"rep_{rep.when}", rep)


@pytest.fixture(autouse=True)
def _record_latency(request):
    start = time.monotonic()
    yield
    elapsed = time.monotonic() - start
    passed = getattr(getattr(request.node, "rep_call", None), "passed", None)

    record: dict = {
        "name": request.node.nodeid,
        "duration_s": round(elapsed, 4),
        "passed": passed,
        "rtt_p50_ms": None,
        "rtt_p95_ms": None,
        "rtt_p99_ms": None,
    }

    rtt_data = _rtt_registry.pop(request.node.nodeid, None)
    if rtt_data:
        sorted_rtt = sorted(rtt_data)
        n = len(sorted_rtt)
        record["rtt_p50_ms"] = round(sorted_rtt[int(n * 0.50)], 2)
        record["rtt_p95_ms"] = round(sorted_rtt[min(int(n * 0.95), n - 1)], 2)
        record["rtt_p99_ms"] = round(sorted_rtt[min(int(n * 0.99), n - 1)], 2)

    _latency_records.append(record)


def pytest_sessionfinish(session, exitstatus):
    if not _latency_records:
        return
    out = Path("reports")
    out.mkdir(exist_ok=True)
    (out / "latency.json").write_text(
        json.dumps(_latency_records, ensure_ascii=False, indent=2)
    )
