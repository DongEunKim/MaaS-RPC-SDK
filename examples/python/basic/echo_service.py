#!/usr/bin/env python3
"""
로컬 Mosquitto에서 `maas-server-sdk`로 RPC 서비스를 띄우는 예제.

`docs/TOPIC_AND_ACL_SPEC.md` 의 WMT/WMO 토픽과 MQTT 5.0 Properties를 사용한다.
실행 전 저장소 루트에서 SDK를 설치한다::

    pip install -r requirements.txt

Usage:
    # 브로커: apt install mosquitto 후
    #        mosquitto -c SDK/mosquitto/mosquitto.conf -v  (저장소 루트)
    python SDK/examples/rpc_local_echo_service.py

환경변수:
    MQTT_HOST  브로커 호스트 (기본: 127.0.0.1)
    MQTT_PORT  TCP 포트 (기본: 1883)
"""

from __future__ import annotations

import logging
import os
import sys

from rpc_local_common import is_broker_connection_refused, print_broker_unavailable_hint

logging.basicConfig(level=logging.INFO)

try:
    from maas_server import MaasServer, RpcContext
    from maas_server.exceptions import ConnectionError as MaasBrokerConnectionError
except ImportError:
    print(
        "maas-server-sdk 가 필요합니다. 저장소 루트에서 "
        "`pip install -r requirements.txt` 또는 "
        "`bash SDK/server/python/install.sh --dev` 를 실행하세요.",
        file=sys.stderr,
    )
    raise SystemExit(1) from None


def main() -> None:
    """엔드포인트를 읽고 `get` 액션을 제공하는 서버를 실행한다."""
    host = os.environ.get("MQTT_HOST", "127.0.0.1")
    port = int(os.environ.get("MQTT_PORT", "1883"))

    server = MaasServer(
        thing_type="CGU",
        service_name="viss",
        vin="VIN-123456",
        endpoint=host,
        port=port,
        use_wss=False,
        client_id="example-viss-service",
        # TOPIC_AND_ACL_SPEC 과 동일: JSON 의 "action" 값으로 @server.action("…") 디스패치.
        # 생략 시에도 기본값은 "action". 다른 키를 쓰려면 route_key="method" 등으로 맞추고,
        # 라우팅 필드 없이 본문 전체만 쓰려면 route_key=None + @server.default 를 사용한다.
        route_key="action",
    )

    @server.action("get")
    def get_datapoint(ctx: RpcContext) -> dict:
        """요청 `path` 를 그대로 돌려주는 목 응답."""
        path = None
        if isinstance(ctx.payload, dict):
            path = ctx.payload.get("path")
        return {"path": path, "value": 42.0}

    print(
        f"[서비스] {host}:{port} 에 연결합니다… (대기: Ctrl+C)",
        flush=True,
    )
    try:
        server.run()
    except MaasBrokerConnectionError as exc:
        if is_broker_connection_refused(exc):
            print_broker_unavailable_hint(host, port)
            raise SystemExit(1) from None
        raise


if __name__ == "__main__":
    main()
