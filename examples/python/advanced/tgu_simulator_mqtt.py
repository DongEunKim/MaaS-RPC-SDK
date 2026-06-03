#!/usr/bin/env python3
"""
[구형·비권장] TGU 시뮬레이터 (원시 paho-mqtt).

현행 규격(`docs/TOPIC_AND_ACL_SPEC.md`)의 WMT 6단 토픽·MQTT 5.0 Properties와
맞지 않는 레거시 페이로드(`response_topic` JSON 필드 등)를 사용한다.

엔드투엔드 검증은 `rpc_local_echo_service.py` + `rpc_local_call_client.py`
 및 설치된 `maas-server-sdk` / `maas-client-sdk` 사용을 권장한다.

실제 MQTT 브로커에 연결하여 WMT/.../request 토픽을 구독하고,
수신 시 Mock 응답을 response_topic으로 발행한다.
RPC 테스트 시 TGU 역할을 대신한다.

Usage:
    # 터미널 1: Mosquitto 기동 (SDK/examples/README.md 참고)
    # 터미널 2: python SDK/examples/tgu_simulator_mqtt.py
    # 터미널 3: python SDK/examples/mqtt_topic_monitor.py  (선택)
    # 터미널 4: 클라이언트 앱(maas-client-sdk)으로 WMT 요청 발행

환경변수:
    MQTT_URL : MQTT 브로커 URL (기본: mqtt://localhost:1883)
"""

import json
import os
import sys
from datetime import datetime
from urllib.parse import urlparse

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("paho-mqtt 필요: pip install paho-mqtt")
    sys.exit(1)


def _parse_url(url: str) -> tuple[str, int, str, bool]:
    parsed = urlparse(url)
    scheme = (parsed.scheme or "mqtt").lower()
    host = parsed.hostname or "localhost"
    port = parsed.port
    path = parsed.path or "/"
    if path == "":
        path = "/"
    schemes = {
        "mqtt": (1883, "tcp", False),
        "mqtts": (8883, "tcp", True),
        "ws": (80, "websockets", False),
        "wss": (443, "websockets", True),
    }
    default_port, transport, use_ssl = schemes.get(scheme, (1883, "tcp", False))
    port = port or default_port
    return host, port, transport, use_ssl


def _build_mock_response(payload: dict) -> dict:
    """요청 payload에서 Mock 응답 생성."""
    request_id = payload.get("request_id", "")
    request = payload.get("request") or {}
    action = request.get("action", "unknown")

    result = {"action": action, "status": "ok"}
    if action == "readDTC":
        result["dtcList"] = []

    return {
        "request_id": request_id,
        "result": result,
        "error": None,
    }


def main() -> None:
    url = os.environ.get("MQTT_URL", "mqtt://localhost:1883")
    host, port, transport, use_ssl = _parse_url(url)

    client = mqtt.Client(
        client_id=f"tgu_sim_{os.getpid()}",
        transport=transport,
        protocol=mqtt.MQTTv311,
    )
    if transport == "websockets":
        parsed = urlparse(url)
        path = parsed.path or "/mqtt"
        client.ws_set_options(path=path)
    if use_ssl:
        client.tls_set()

    def on_connect(_c, _u, _f, rc):
        if rc == 0:
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"[{ts}] TGU 시뮬레이터 연결됨: {host}:{port}")
            client.subscribe("WMT/+/+/request", qos=0)
            print("[TGU] 구독: WMT/+/+/request")
        else:
            print(f"[TGU] 연결 실패: rc={rc}")

    def on_message(_c, _u, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"[TGU] payload 파싱 실패: {e}")
            return

        response_topic = payload.get("response_topic")
        if not response_topic:
            print("[TGU] response_topic 없음, 무시")
            return

        response = _build_mock_response(payload)
        client.publish(response_topic, json.dumps(response, ensure_ascii=False))
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] WMT 요청 처리 → {response_topic}")

    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(host, port, keepalive=60)
        print("[TGU] 대기 중... (Ctrl+C 종료)")
        client.loop_forever()
    except KeyboardInterrupt:
        client.disconnect()
        print("\n[TGU] 종료")


if __name__ == "__main__":
    main()
