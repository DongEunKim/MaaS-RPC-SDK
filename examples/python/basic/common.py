"""
로컬 RPC 예제(`rpc_local_*`)에서 공통으로 쓰는 연결 실패 안내.

예제 스크립트와 같은 디렉터리에 두고, 스크립트 실행 시 ``sys.path``에
해당 디렉터리가 포함되므로 ``import rpc_local_common`` 으로 불러온다.
"""

from __future__ import annotations

import sys
from typing import Optional


def is_broker_connection_refused(exc: BaseException) -> bool:
    """
    errno 111(연결 거절) 또는 동등한 메시지가 예외 체인에 있으면 True.

    Args:
        exc: SDK 등에서 포장된 예외.

    Returns:
        브로커 미기동·포트 불일치 등으로 보이면 True.
    """
    cur: Optional[BaseException] = exc
    visited: set[int] = set()
    while cur is not None:
        i = id(cur)
        if i in visited:
            break
        visited.add(i)
        if isinstance(cur, ConnectionRefusedError):
            return True
        errno = getattr(cur, "errno", None)
        if errno == 111:
            return True
        cur = cur.__cause__ or cur.__context__
    return "Connection refused" in str(exc) or "Errno 111" in str(exc)


def print_broker_unavailable_hint(host: str, port: int) -> None:
    """
    브로커에 붙지 못했을 때 표준 오류로 다음 단계를 안내한다.

    Args:
        host: 시도한 호스트.
        port: 시도한 포트.
    """
    print(
        f"\n브로커에 연결할 수 없습니다 ({host}:{port}).\n"
        "  • Mosquitto: apt install mosquitto 후 (1883 충돌 시 systemctl stop mosquitto)\n"
        "      저장소 루트에서  mosquitto -c SDK/mosquitto/mosquitto.conf -v\n"
        "    자세한 설명: SDK/examples/README.md 의 「로컬 MQTT 브로커」\n"
        "  • 다른 브로커면 환경변수 MQTT_HOST, MQTT_PORT 를 맞추세요.\n",
        file=sys.stderr,
    )
