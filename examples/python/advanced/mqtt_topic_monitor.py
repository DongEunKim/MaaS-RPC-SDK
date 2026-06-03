#!/usr/bin/env python3
"""
MQTT 토픽 모니터 — 모든 토픽 메시지 감시 (디버깅용).

브로커의 전체 또는 특정 패턴 토픽을 구독하여 메시지를 실시간으로 출력한다.

Usage:
    # 브로커: SDK/examples/README.md 「로컬 MQTT 브로커」 참고

    # 모든 토픽 모니터 (#)
    python SDK/examples/mqtt_topic_monitor.py

    # WMT/WMO RPC 토픽만 모니터
    python SDK/examples/mqtt_topic_monitor.py --filter "WMT/#" "WMO/#"

    # WebSocket 연결
    MQTT_URL=ws://localhost:9001 python SDK/examples/mqtt_topic_monitor.py

환경변수:
    MQTT_URL   : MQTT 브로커 URL (기본: mqtt://localhost:1883)
    MQTT_FILTER: 구독 필터, 쉼표 구분 (기본: #)
"""

import argparse
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
    """URL 파싱: host, port, transport, use_ssl."""
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


def _format_payload(payload: bytes) -> str:
    """payload를 읽기 쉬운 문자열로 변환."""
    try:
        s = payload.decode("utf-8")
        try:
            obj = json.loads(s)
            return json.dumps(obj, ensure_ascii=False, indent=2)
        except json.JSONDecodeError:
            return s
    except UnicodeDecodeError:
        return f"<binary {len(payload)} bytes>"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="MQTT 토픽 모니터 — 디버깅용 전체/패턴 구독"
    )
    parser.add_argument(
        "-f",
        "--filter",
        nargs="+",
        default=["#"],
        help="구독할 토픽 필터 (기본: #). 예: WMT/# WMO/#",
    )
    parser.add_argument(
        "-q",
        "--qos",
        type=int,
        default=0,
        choices=[0, 1, 2],
        help="QoS (기본: 0)",
    )
    args = parser.parse_args()

    url = os.environ.get("MQTT_URL", "mqtt://localhost:1883")
    host, port, transport, use_ssl = _parse_url(url)

    filters = args.filter
    if len(filters) == 1 and "," in filters[0]:
        filters = [f.strip() for f in filters[0].split(",")]

    client = mqtt.Client(transport=transport, protocol=mqtt.MQTTv311)
    if transport == "websockets":
        parsed = urlparse(url)
        path = parsed.path or "/mqtt"
        client.ws_set_options(path=path)
    if use_ssl:
        client.tls_set()

    def on_connect(_c, _u, _f, rc):
        if rc == 0:
            ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            print(f"[{ts}] 연결됨: {host}:{port} | 필터: {filters}", flush=True)
            for f in filters:
                client.subscribe(f, qos=args.qos)
            print(
                "[모니터] 구독 완료. WMT/WMO 확인 시 RPC는 이 메시지 출력 후 실행하세요.",
                flush=True,
            )
        else:
            print(f"[연결 실패] rc={rc}", flush=True)

    def on_message(_c, _u, msg):
        ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        topic = msg.topic
        payload_str = _format_payload(msg.payload)
        print(f"\n[{ts}] topic: {topic}", flush=True)
        print("-" * 60, flush=True)
        print(payload_str, flush=True)
        print("-" * 60, flush=True)

    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(host, port, keepalive=60)
        client.loop_forever()
    except KeyboardInterrupt:
        client.disconnect()
        print("\n모니터 종료")


if __name__ == "__main__":
    main()
