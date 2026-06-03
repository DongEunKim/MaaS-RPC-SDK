"""
maas-server-sdk 예외 정의.
"""

from typing import Any


class MaasServerError(Exception):
    """maas-server-sdk 기본 예외."""


class ConnectionError(MaasServerError):
    """MQTT 연결 실패 또는 연결 끊김."""


class SessionBusyError(MaasServerError):
    """독점 세션이 다른 클라이언트에 의해 점유 중."""

    def __init__(self, locked_by: str) -> None:
        super().__init__(f"세션 점유 중: locked_by={locked_by}")
        self.locked_by = locked_by


class HandlerError(MaasServerError):
    """핸들러 실행 중 발생한 비즈니스 로직 오류."""

    def __init__(self, message: str, reason_code: int = 0x80, body: Any = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.body = body


class HardwareFaultError(HandlerError):
    """하드웨어 제어 오류 (reason_code=0x80)."""

    def __init__(self, message: str) -> None:
        super().__init__(message, reason_code=0x80)


class NotAuthorizedError(HandlerError):
    """권한 없음 (reason_code=0x87)."""

    def __init__(self, message: str = "권한 없음") -> None:
        super().__init__(message, reason_code=0x87)


class PayloadError(HandlerError):
    """페이로드 형식 오류 (reason_code=0x99)."""

    def __init__(self, message: str = "페이로드 형식 오류") -> None:
        super().__init__(message, reason_code=0x99)
