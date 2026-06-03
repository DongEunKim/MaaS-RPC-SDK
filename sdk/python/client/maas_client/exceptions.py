"""
maas-client-sdk 예외 정의.
"""

from typing import Any, Optional


class MaasError(Exception):
    """maas-client-sdk 기본 예외."""


class ConnectionError(MaasError):
    """MQTT 연결 실패 또는 연결 끊김."""


class RpcTimeoutError(MaasError):
    """RPC 응답 타임아웃."""

    def __init__(self, service: str, action: str, timeout: float) -> None:
        super().__init__(
            f"RPC 타임아웃: service={service}, action={action}, timeout={timeout}s"
        )
        self.service = service
        self.action = action
        self.timeout = timeout


class RpcExpiredError(RpcTimeoutError):
    """
    패턴 D(Time-bound): 시한이 만료되어 응답을 받지 못한 경우.

    ``RpcTimeoutError`` 의 하위 타입이므로 기존 타임아웃 처리 코드와 호환된다.
    """


class RpcServerError(MaasError):
    """서버가 오류 reason_code를 반환한 경우."""

    def __init__(self, reason_code: int, error_detail: str = "", error_body: Any = None) -> None:
        super().__init__(
            f"서버 오류: reason_code={reason_code:#04x}, detail={error_detail}"
        )
        self.reason_code = reason_code
        self.error_detail = error_detail
        self.error_body = error_body


class NotAuthorizedError(RpcServerError):
    """권한 없음 (reason_code=0x87)."""

    def __init__(self, error_detail: str = "", error_body: Any = None) -> None:
        super().__init__(0x87, error_detail, error_body=error_body)


class ServerBusyError(RpcServerError):
    """독점 세션 점유 중 (reason_code=0x8A)."""

    def __init__(self, error_detail: str = "", error_body: Any = None) -> None:
        super().__init__(0x8A, error_detail, error_body=error_body)


class PayloadError(MaasError):
    """페이로드 직렬화/역직렬화 오류."""


class SubscriptionCancelledError(MaasError):
    """서버가 구독을 강제 취소했을 때 발생. reason 필드에 취소 이유 포함."""

    def __init__(self, reason: Optional[str] = None) -> None:
        self.reason = reason
        msg = f"구독이 서버에 의해 취소됨: {reason}" if reason else "구독이 서버에 의해 취소됨"
        super().__init__(msg)


class ServerOfflineError(MaasError):
    """콜드 스타트 시 서버가 오프라인 상태여서 첫 Heartbeat를 받지 못한 경우."""

    def __init__(self, thing_type: str = "", service: str = "", vin: str = "") -> None:
        self.thing_type = thing_type
        self.service = service
        self.vin = vin
        target = f"{thing_type}/{service}/{vin}" if thing_type else "서버"
        super().__init__(f"서버 오프라인: {target}")
