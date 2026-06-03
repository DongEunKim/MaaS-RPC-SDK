"""
구독 세션 레지스트리.

스트리밍 핸들러의 취소 생명주기를 관리한다.
SDK가 subscription_id와 cancel_event를 생성하여 핸들러에 주입하고,
unsubscribe 요청 시 cancel_event를 통해 스트리밍을 중단한다.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Optional


class SubscriptionRegistry:
    """스트리밍 구독 세션 ID → cancel_event 매핑."""

    def __init__(self) -> None:
        self._subs: dict[str, asyncio.Event] = {}
        self._reasons: dict[str, Optional[str]] = {}
        self._clients: dict[str, str] = {}
        self._lock: asyncio.Lock = asyncio.Lock()

    async def create(self, client_id: str = "") -> tuple[str, asyncio.Event]:
        """새 구독 세션 생성. (subscription_id, cancel_event) 반환."""
        sid = str(uuid.uuid4())
        event = asyncio.Event()
        async with self._lock:
            self._subs[sid] = event
            self._clients[sid] = client_id
        return sid, event

    async def cancel(self, subscription_id: str, reason: Optional[str] = None) -> bool:
        """구독 취소. cancel_event를 set한다. reason 지정 시 서버 주도 강제 취소로 분류. 존재하지 않으면 False."""
        async with self._lock:
            event = self._subs.get(subscription_id)
            if event is None:
                return False
            self._reasons[subscription_id] = reason
            event.set()
            return True

    async def cancel_by_client(self, client_id: str) -> list[str]:
        """
        특정 client_id의 모든 구독 cancel_event 발화. reason 없음 = 정상 종료.

        Returns:
            취소(또는 이미 취소되어 매칭된) 구독 ID 목록. on_subscription_lost 콜백용.
        """
        cancelled: list[str] = []
        async with self._lock:
            for sid, cid in list(self._clients.items()):
                if cid == client_id:
                    cancelled.append(sid)
                    event = self._subs.get(sid)
                    if event and not event.is_set():
                        event.set()  # reason 없음 → EOF만 전송됨
        return cancelled

    async def get_by_client(self, client_id: str) -> list[str]:
        """특정 client_id가 보유한 구독 ID 목록."""
        async with self._lock:
            return [sid for sid, cid in self._clients.items() if cid == client_id]

    async def get_cancel_reason(self, subscription_id: str) -> Optional[str]:
        """취소 이유 반환. 클라이언트 주도 취소 또는 미취소면 None."""
        async with self._lock:
            return self._reasons.get(subscription_id)

    async def remove(self, subscription_id: str) -> None:
        """구독 세션 제거."""
        async with self._lock:
            self._subs.pop(subscription_id, None)
            self._reasons.pop(subscription_id, None)
            self._clients.pop(subscription_id, None)
