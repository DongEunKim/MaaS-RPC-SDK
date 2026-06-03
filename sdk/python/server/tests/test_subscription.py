"""SubscriptionRegistry 단위 테스트."""

from __future__ import annotations

import asyncio

import pytest

from maas_server.subscription import SubscriptionRegistry


@pytest.mark.asyncio
async def test_create_returns_unique_ids() -> None:
    """두 번 create() 시 다른 UUID가 반환된다."""
    registry = SubscriptionRegistry()
    sid1, _ = await registry.create()
    sid2, _ = await registry.create()
    assert sid1 != sid2


@pytest.mark.asyncio
async def test_cancel_sets_event() -> None:
    """cancel() 호출 후 해당 구독의 event가 set된다."""
    registry = SubscriptionRegistry()
    sid, event = await registry.create()

    assert not event.is_set()
    result = await registry.cancel(sid)
    assert result is True
    assert event.is_set()


@pytest.mark.asyncio
async def test_cancel_unknown_returns_false() -> None:
    """존재하지 않는 subscription_id에 cancel()하면 False를 반환한다."""
    registry = SubscriptionRegistry()
    result = await registry.cancel("non-existent-id")
    assert result is False


@pytest.mark.asyncio
async def test_remove_cleans_up() -> None:
    """remove() 후 cancel()이 False를 반환한다."""
    registry = SubscriptionRegistry()
    sid, event = await registry.create()

    await registry.remove(sid)
    result = await registry.cancel(sid)
    assert result is False
    assert not event.is_set()
