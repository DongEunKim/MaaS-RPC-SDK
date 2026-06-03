#!/usr/bin/env python3
"""
로컬 Mosquitto에서 `maas-client-sdk`로 RPC `call` 을 보내는 예제.

터미널 1에서 `rpc_local_echo_service.py` 를 먼저 실행한 뒤 이 스크립트를 실행한다.

실행 전 저장소 루트에서 SDK를 설치한다::

    pip install -r requirements.txt

Usage:
    # 브로커 기동 후 (README 「로컬 MQTT 브로커」 참고)
    python SDK/examples/rpc_local_call_client.py

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
    from maas_client import MaasClient
    from maas_client.exceptions import ConnectionError as MaasBrokerConnectionError
except ImportError:
    print(
        "maas-client-sdk 가 필요합니다. 저장소 루트에서 "
        "`pip install -r requirements.txt` 또는 "
        "`bash SDK/client/python/install.sh --dev` 를 실행하세요.",
        file=sys.stderr,
    )
    raise SystemExit(1) from None


def main() -> None:
    """로컬 브로커에 연결해 `get` RPC 를 한 번 호출한다."""
    host = os.environ.get("MQTT_HOST", "127.0.0.1")
    port = int(os.environ.get("MQTT_PORT", "1883"))

    client = MaasClient(
        endpoint=host,
        client_id="example-demo-client",
        token_provider=None,
        port=port,
        use_wss=False,
        thing_type="CGU",
        service="viss",
        vin="VIN-123456",
    )
    try:
        client.connect()
    except MaasBrokerConnectionError as exc:
        if is_broker_connection_refused(exc):
            print_broker_unavailable_hint(host, port)
            raise SystemExit(1) from None
        raise
    try:
        result = client.call(
            "get",
            {"path": "Vehicle.Speed"},
            timeout=15.0,
        )
        print("[클라이언트] 응답 payload:", result.payload)
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
