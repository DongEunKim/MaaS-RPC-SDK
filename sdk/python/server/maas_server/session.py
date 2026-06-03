"""
독점 세션 관리 (패턴 E).

서비스=VIN 1:1 이므로 동시에 단 하나의 클라이언트만 세션을 점유한다.
"""

from __future__ import annotations

import logging
import threading
import uuid
from typing import Optional

logger = logging.getLogger(__name__)


class ExclusiveSessionManager:
    """
    서비스 단위 단일 독점 세션 관리자 (패턴 E).

    서버는 VIN이 고정이므로 서비스=VIN 1:1로, 동시에 단 하나의 클라이언트만
    세션을 점유한다. 세션 ID는 UUID로 발번하며 thread-safe하다.
    """

    def __init__(self) -> None:
        # (client_id, session_id) 또는 None
        self._current: Optional[tuple[str, str]] = None
        self._mutex = threading.Lock()

    def acquire(self, client_id: str) -> Optional[str]:
        """
        세션 획득 시도.

        Returns:
            획득/재획득 성공 시 session_id(uuid). 다른 클라이언트가 점유 중이면 None.
        """
        with self._mutex:
            if self._current is not None:
                owner, sid = self._current
                if owner == client_id:
                    return sid  # 동일 클라이언트 재획득
                return None
            sid = str(uuid.uuid4())
            self._current = (client_id, sid)
            logger.info("독점 세션 획득: client_id=%s, session_id=%s", client_id, sid)
            return sid

    def release(self, client_id: str) -> bool:
        """세션 해제. 점유자만 해제 가능. 성공 시 True."""
        with self._mutex:
            if self._current is None:
                return False
            owner, _ = self._current
            if owner != client_id:
                return False
            self._current = None
            logger.info("독점 세션 해제: client_id=%s", client_id)
            return True

    def get_session_id(self, client_id: str) -> Optional[str]:
        """해당 client_id가 보유한 session_id. 없으면 None."""
        with self._mutex:
            if self._current is None:
                return None
            owner, sid = self._current
            return sid if owner == client_id else None

    def current(self) -> Optional[tuple[str, str]]:
        """현재 (client_id, session_id) 또는 None."""
        with self._mutex:
            return self._current

    def owner(self) -> Optional[str]:
        """현재 세션 점유 client_id. 없으면 None."""
        with self._mutex:
            return self._current[0] if self._current else None

    def force_release_by_client(self, client_id: str) -> Optional[str]:
        """
        클라이언트 단절 시 세션 강제 해제.

        Returns:
            해제된 session_id (해당 client_id가 점유 중이었던 경우), 아니면 None.
        """
        with self._mutex:
            if self._current is None:
                return None
            owner, sid = self._current
            if owner != client_id:
                return None
            self._current = None
            logger.info("단절로 독점 세션 강제 해제: client_id=%s, session_id=%s", client_id, sid)
            return sid
