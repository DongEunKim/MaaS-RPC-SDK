"""생성자 바인딩 및 connect 시점 토큰 갱신 단위 테스트."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from maas_client.client_async import MaasClientAsync


def test_bound_routing_raises_when_partial_constructor() -> None:
    """thing_type/service/vin 중 일부만 지정하면 생성자에서 실패한다."""
    with pytest.raises(ValueError, match="모두 생략하거나 모두 지정"):
        MaasClientAsync(
            "127.0.0.1",
            "c1",
            thing_type="CGU",
            service=None,
            vin=None,
            use_wss=False,
        )


def test_call_short_form_raises_without_binding() -> None:
    """바인딩 없이 call(action) 형태면 ValueError."""
    client = MaasClientAsync("127.0.0.1", "c1", use_wss=False)

    async def _run() -> None:
        await client.call("get")

    with pytest.raises(ValueError, match="thing_type, service, vin"):
        asyncio.run(_run())


@pytest.mark.asyncio
async def test_call_three_positional_typeerror() -> None:
    """인자 3개는 허용되지 않는다."""
    client = MaasClientAsync(
        "127.0.0.1",
        "c1",
        use_wss=False,
        thing_type="CGU",
        service="viss",
        vin="VIN-1",
    )
    with pytest.raises(TypeError, match="call"):
        await client.call("a", "b", "c")


@pytest.mark.asyncio
async def test_connect_refreshes_token_each_time() -> None:
    """connect()마다 token_provider가 호출되어 set_token에 반영된다."""
    calls: list[str] = []

    def provider() -> str:
        calls.append("t")
        return f"tok-{len(calls)}"

    client = MaasClientAsync(
        "127.0.0.1",
        "c1",
        token_provider=provider,
        use_wss=False,
    )
    mock_conn = MagicMock()
    mock_conn.connect = AsyncMock()
    mock_conn.disconnect = AsyncMock()
    mock_conn.set_message_callback = MagicMock()
    mock_conn.set_token = MagicMock()
    client._conn = mock_conn
    client._rpc.setup_subscriptions = AsyncMock()

    await client.connect()
    await client.disconnect()
    await client.connect()
    await client.disconnect()

    assert calls == ["t", "t"]
    assert mock_conn.set_token.call_args_list[0][0][0] == "tok-1"
    assert mock_conn.set_token.call_args_list[1][0][0] == "tok-2"
    assert mock_conn.connect.await_count == 2


@pytest.mark.asyncio
async def test_call_bound_resolves_routing() -> None:
    """바인딩 시 call(action, params)가 올바른 축으로 _rpc.call에 전달된다."""
    client = MaasClientAsync(
        "127.0.0.1",
        "c1",
        use_wss=False,
        thing_type="CGU",
        service="viss",
        vin="VIN-9",
    )
    client._rpc.call = AsyncMock()
    await client.call("get", {"path": "x"}, timeout=3.0)
    client._rpc.call.assert_awaited_once()
    kwargs = client._rpc.call.await_args.kwargs
    assert kwargs["thing_type"] == "CGU"
    assert kwargs["service"] == "viss"
    assert kwargs["action"] == "get"
    assert kwargs["vin"] == "VIN-9"
    assert kwargs["params"] == {"path": "x"}
    assert kwargs["timeout"] == 3.0


def test_exclusive_session_zero_args_requires_binding() -> None:
    client = MaasClientAsync("127.0.0.1", "c1", use_wss=False)
    with pytest.raises(ValueError):
        client.exclusive_session()


def test_exclusive_session_three_args_no_binding_needed() -> None:
    client = MaasClientAsync("127.0.0.1", "c1", use_wss=False)
    ctx = client.exclusive_session("A", "b", "V")
    assert ctx._thing_type == "A"
    assert ctx._service == "b"
    assert ctx._vin == "V"


def test_exclusive_session_wrong_arity() -> None:
    client = MaasClientAsync(
        "127.0.0.1",
        "c1",
        use_wss=False,
        thing_type="CGU",
        service="viss",
        vin="VIN-1",
    )
    with pytest.raises(TypeError, match="0개"):
        client.exclusive_session("only_one")


@pytest.mark.asyncio
async def test_stream_bound_resolves_routing() -> None:
    client = MaasClientAsync(
        "127.0.0.1",
        "c1",
        use_wss=False,
        thing_type="T",
        service="S",
        vin="V",
    )

    collected: list[tuple] = []

    async def fake_stream(
        thing_type: str,
        service: str,
        action: str,
        vin: str,
        params: object = None,
        *,
        qos: int = 1,
    ):
        collected.append((thing_type, service, action, vin, params, qos))
        if False:
            yield  # pragma: no cover

    client._rpc.stream = fake_stream  # type: ignore[method-assign]

    async for _ in client.stream("act", {"k": 1}):
        pass

    assert collected == [("T", "S", "act", "V", {"k": 1}, 1)]
